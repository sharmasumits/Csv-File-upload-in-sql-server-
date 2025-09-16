# app.py
import os
import re
import json
import io
import time
import uuid
import tempfile
import pandas as pd
import numpy as np
import pyodbc
import streamlit as st
from datetime import datetime
from hashlib import sha256
from csv import Sniffer
import getpass


# ---------------------------
# Config / constants
# ---------------------------
HISTORY_FILE = "upload_history.json"  # local upload history (simple)
MAX_PREVIEW_ROWS = 500
DEFAULT_CHUNKSIZE = 50000  # chunk insert size

st.set_page_config(page_title="Data Importer (CSV/Excel) → SQL Server", layout="wide")

# ---------------------------
# Helpers: persist history
# ---------------------------
def read_history():
    if os.path.exists(HISTORY_FILE):
        try:
            return json.load(open(HISTORY_FILE, "r", encoding="utf-8"))
        except Exception:
            return []
    return []

def append_history(entry):
    h = read_history()
    h.insert(0, entry)
    # keep only last 200
    json.dump(h[:200], open(HISTORY_FILE, "w", encoding="utf-8"), indent=2, default=str)

# ---------------------------
# SQL connection helper
# ---------------------------
def get_connection(auth_type, server, database, username=None, password=None, timeout=30):
    # Use ODBC Driver 17 or default; you may adjust to your environment
    driver = "ODBC Driver 17 for SQL Server"
    if auth_type == "SQL":
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Connection Timeout={timeout};"
    else:
        # Windows Auth / integrated
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};Trusted_Connection=yes;Connection Timeout={timeout};"
    return pyodbc.connect(conn_str)

# ---------------------------
# Column cleaning & naming
# ---------------------------
def clean_col_name(col):
    new_col = str(col).strip()
    new_col = re.sub(r'\s+', '_', new_col)
    new_col = re.sub(r'[^\w]', '_', new_col)
    if new_col == "":
        new_col = "COL"
    return new_col

def clean_df_columns(df):
    df = df.copy()
    df.columns = [clean_col_name(c) for c in df.columns]
    # dedupe columns by appending suffix
    seen = {}
    cols = []
    for c in df.columns:
        if c not in seen:
            seen[c] = 1
            cols.append(c)
        else:
            seen[c] += 1
            cols.append(f"{c}_{seen[c]}")
    df.columns = cols
    return df

# ---------------------------
# dtype mapping helpers
# ---------------------------
MAIN_SQL_TYPES = ["BIGINT","INT","FLOAT","DECIMAL(18,4)","DATE","DATETIME","DATETIME2","NVARCHAR(MAX)","NVARCHAR(255)","BIT"]

def detect_default_sql_type(series):
    dtype = series.dtype
    if pd.api.types.is_integer_dtype(dtype):
        return "BIGINT"
    if pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    if pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME2"
    # length-based for strings
    max_len = series.astype(str).map(len).max()
    if pd.isna(max_len) or max_len < 1:
        return "NVARCHAR(255)"
    if max_len <= 255:
        return f"NVARCHAR({max_len})"
    return "NVARCHAR(MAX)"

# ---------------------------
# file reading with delimiter detection
# ---------------------------
def detect_delimiter(file_bytes):
    sample = file_bytes.decode('utf-8', errors='ignore')[:4096]
    try:
        sniffer = Sniffer()
        dialect = sniffer.sniff(sample, delimiters=[',',';','\t','|'])
        return dialect.delimiter
    except Exception:
        return ','
def read_uploaded_file(uploaded_file, file_type_hint=None, delimiter=None):
    """
    Reads uploaded CSV or Excel file into a pandas DataFrame.
    Supports auto delimiter detection (if provided).
    """

    name = uploaded_file.name.lower()

    if name.endswith(".csv") or (file_type_hint == "csv"):
        raw = uploaded_file.read()

        # Default delimiter = comma, override if provided
        sep = delimiter if delimiter else ","

        try:
            df = pd.read_csv(io.BytesIO(raw), sep=sep, engine="c", low_memory=False)
        except Exception:
            df = pd.read_csv(io.BytesIO(raw), sep=sep, engine="c", encoding="latin1", low_memory=False)

        return df

    elif name.endswith(".xlsx") or (file_type_hint == "excel"):
        # save temporarily to read with pandas
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded_file.read())
            tmp_path = tmp.name

        df = pd.read_excel(tmp_path, sheet_name=0)

        try:
            os.unlink(tmp_path)  # delete temp file
        except Exception:
            pass

        return df

    else:
        raise ValueError("Unsupported file type. Please upload CSV or Excel (.xlsx)")


# ---------------------------
# create table preview SQL
# ---------------------------
def build_create_table_sql(table_name, mapping_rows):
    cols = []
    for row in mapping_rows:
        tgt = row["Target Column"].strip()
        sql_type = row["SQL Datatype"].strip()
        cols.append(f"[{tgt}] {sql_type}")
    cols_part = ",\n  ".join(cols)
    sql = f"CREATE TABLE [{table_name}] (\n  {cols_part}\n);"
    return sql


# ---------------------------
# create table in DB
# ---------------------------
def create_table_in_db(conn, table_name, mapping_rows, drop_if_exists=False):
    cursor = conn.cursor()
    if drop_if_exists:
        cursor.execute(f"IF OBJECT_ID('{table_name}','U') IS NOT NULL DROP TABLE [{table_name}];")
        conn.commit()

    create_sql = build_create_table_sql(table_name, mapping_rows)
    try:
        cursor.execute(create_sql)
        conn.commit()
    except Exception as e:
        # Could be table exists — bubble up
        raise


# ---------------------------
# insert DataFrame in chunks
# ---------------------------
def insert_dataframe_chunked(conn, table_name, df, mapping_rows, chunksize=DEFAULT_CHUNKSIZE, progress_callback=None):
    cursor = conn.cursor()
    source_cols = [r["Source Column"] for r in mapping_rows]
    target_cols = [r["Target Column"] for r in mapping_rows]

    col_list = ", ".join([f"[{c}]" for c in target_cols])
    placeholders = ", ".join(["?"] * len(target_cols))
    insert_sql = f"INSERT INTO [{table_name}] ({col_list}) VALUES ({placeholders})"
    cursor.fast_executemany = True

    total = len(df)
    inserted = 0

    for start in range(0, total, chunksize):
        batch = df.iloc[start:start+chunksize][source_cols].copy()

        # convert datetimes to Python datetime objects
        for c in batch.columns:
            if pd.api.types.is_datetime64_any_dtype(batch[c].dtype):
                batch[c] = batch[c].apply(lambda x: x.to_pydatetime() if pd.notnull(x) else None)

        rows = [tuple(x) for x in batch.values.tolist()]
        if rows:
            cursor.executemany(insert_sql, rows)
            conn.commit()
            inserted += len(rows)

        if progress_callback:
            progress_callback(min(1.0, inserted / total), inserted, total)

    return inserted

# ---------------------------
# get tables & columns
# ---------------------------
def get_all_tables_and_columns(conn):
    q = """
    SELECT
      TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    return pd.read_sql(q, conn)

# ---------------------------
# UI: top-level layout
# ---------------------------
st.title("📊 Advanced CSV/Excel → SQL Server Tool")
st.write("Self-service uploader with mapping, validations, preview, confirm and export.")

# --- Authentication / simple demo login ---
st.sidebar.header("👤 User / Connection")
demo_mode = st.sidebar.checkbox("Demo mode (no auth)", value=True)
if demo_mode:
    st.sidebar.info("Demo mode: only local DB or accessible SQL is required. Replace with secure auth for production.")
auth_type = st.sidebar.selectbox("Authentication Type", ["Windows (Integrated)", "SQL (username/password)"])
if auth_type == "SQL (username/password)":
    sql_user = st.sidebar.text_input("SQL Username")
    sql_pass = st.sidebar.text_input("SQL Password", type="password")
else:
    sql_user, sql_pass = None, None

server = st.sidebar.text_input("SQL Server (host\\instance or host,port)", value="localhost")
database = st.sidebar.text_input("Database", value="vdat")

# Tabs for Import / Export / History
tabs = st.tabs(["Import", "Export", "History / Logs"])

# ---------------------------
# IMPORT TAB
# ---------------------------
with tabs[0]:
    st.header("📥 Import (CSV / Excel)")
    st.markdown("Upload a CSV or Excel file, map columns, validate, preview CREATE TABLE and then load.")

    uploaded = st.file_uploader("Upload CSV (.csv) or Excel (.xlsx)", type=["csv", "xlsx"])
    user_table = st.text_input("Target table name (will be created/appended)", value="")
    load_mode = st.radio("Load mode", ["Create (drop if exists)", "Append (to existing)"], index=0)
    drop_if_exists = (load_mode == "Create (drop if exists)")

    # delimiter selection (for csv)
    delim_choice = st.selectbox("CSV delimiter (auto-detect by default)", ["Auto", ",", ";", "\t", "|"], index=0)

    if uploaded is not None:
        try:
            # read file into df (do not mutate original upload)
            df = read_uploaded_file(uploaded, file_type_hint=None, delimiter=(None if delim_choice=="Auto" else delim_choice))
            df = clean_df_columns(df)
            st.success(f"Loaded file: {uploaded.name} — rows: {len(df)}, columns: {len(df.columns)}")
            st.subheader("Preview (first rows)")
            st.dataframe(df.head(MAX_PREVIEW_ROWS))

            # Data quality summary
            st.subheader("Data quality summary")
            dq = []
            for c in df.columns:
                s = df[c]
                dq.append({
                    "Column": c,
                    "dtype": str(s.dtype),
                    "nulls": int(s.isna().sum()),
                    "unique": int(s.nunique(dropna=True)),
                    "max_len": int(s.astype(str).map(len).max())
                })
            dq_df = pd.DataFrame(dq)
            st.dataframe(dq_df, use_container_width=True)

            # Prepare mapping dataframe with suggested SQL datatypes
            mapping_rows = []
            for c in df.columns:
                suggested = detect_default_sql_type(df[c])
                mapping_rows.append({
                    "Source Column": c,
                    "Target Column": c,
                    "SQL Datatype": suggested
                })
            mapping_df = pd.DataFrame(mapping_rows)

            st.subheader("Column mapping")
            st.markdown("Edit target column names and SQL datatypes. You can choose from the dropdown or type custom datatype.")
            # Using data_editor so user can edit mapping in-place
            edited_mapping = st.data_editor(
                mapping_df,
                column_config={
                    "Source Column": st.column_config.TextColumn("Source Column", disabled=True),
                    "Target Column": st.column_config.TextColumn("Target Column"),
                    "SQL Datatype": st.column_config.TextColumn("SQL Datatype")
                },
                use_container_width=True,
                num_rows="dynamic",
                key="mapping_editor_main"
            )

            # Build CREATE TABLE preview
            final_table_name = user_table.strip() or os.path.splitext(uploaded.name)[0]
            st.subheader("CREATE TABLE script (preview)")
            create_sql = build_create_table_sql(final_table_name, edited_mapping.to_dict(orient="records"))
            st.code(create_sql, language="sql")

            # validation: check for duplicate target columns or blank names
            tgt_names = edited_mapping["Target Column"].astype(str).apply(lambda x: x.strip()).tolist()
            if any([t=="" for t in tgt_names]):
                st.error("Target column names cannot be empty. Fix mapping.")
            elif len(set(tgt_names)) != len(tgt_names):
                st.error("Target column names contain duplicates. Make them unique.")
            else:
                confirm = st.checkbox("I have reviewed CREATE TABLE script and want to proceed")
                if confirm:
                    st.info("When you click LOAD, the operation will run and insert data into the database.")
                    if st.button("🚀 LOAD to SQL Server"):
                        # Attempt DB operations
                        conn = None
                        start_time = datetime.utcnow()
                        try:
                            conn = get_connection("SQL" if auth_type=="SQL (username/password)" else "Windows", server, database, sql_user, sql_pass)
                            # Create or drop
                            if drop_if_exists:
                                try:
                                    create_table_in_db(conn, final_table_name, edited_mapping.to_dict(orient="records"), drop_if_exists=True)
                                except Exception as e:
                                    st.warning(f"Create table attempt raised: {e}. Trying to continue (table may already exist).")
                            else:
                                # if append mode, ensure table exists (user responsibility)
                                pass

                            # Data quality fixes option: offer simple fixes before insert
                            st.write("Running basic validations before insert...")
                            # 1) type coercion checks: attempt conversions where SQL datatype demands
                            mapping_list = edited_mapping.to_dict(orient="records")
                            coercion_errors = []
                            for r in mapping_list:
                                src = r["Source Column"]
                                sqltype = r["SQL Datatype"].upper()
                                series = df[src]
                                # basic checks for numeric types
                                if "INT" in sqltype or sqltype.startswith("BIGINT") or sqltype.startswith("SMALLINT") or sqltype.startswith("TINYINT"):
                                    # try convert
                                    try:
                                        pd.to_numeric(series.dropna(), downcast="integer")
                                    except Exception:
                                        coercion_errors.append(f"Column {src} may contain non-integer values.")
                                if "FLOAT" in sqltype or "DECIMAL" in sqltype or "NUMERIC" in sqltype:
                                    try:
                                        pd.to_numeric(series.dropna(), errors="raise")
                                    except Exception:
                                        coercion_errors.append(f"Column {src} may contain non-numeric values.")
                                if "DATE" in sqltype or "TIME" in sqltype or "DATETIME" in sqltype:
                                    try:
                                        pd.to_datetime(series.dropna(), errors="raise")
                                    except Exception:
                                        coercion_errors.append(f"Column {src} may contain non-datetime values.")

                            if coercion_errors:
                                st.warning("Possible data type mismatches detected:")
                                for w in coercion_errors[:10]:
                                    st.write("- " + w)
                                st.info("You can fix CSV/Excel or adjust mapping to compatible datatypes, then retry.")
                                # still allow user to continue if they acknowledge
                                force = st.checkbox("Force load despite warnings (I know what I'm doing)")
                                if not force:
                                    st.stop()

                            # Progress UI
                            progress_bar = st.progress(0.0)
                            status_text = st.empty()
                            def _progress_cb(frac, inserted, total):
                                progress_bar.progress(frac)
                                status_text.info(f"Inserted {inserted:,} / {total:,} rows...")

                            # If create table requested and table doesn't exist, create
                            if drop_if_exists:
                                # create_table_in_db already attempted above
                                pass
                    
                                # If table doesn't exist and append selected, attempt to create
                                try:
                                    create_table_in_db(conn, final_table_name, mapping_list, drop_if_exists=False)
                                except Exception:
                                    # table may exist already; ignore
                                    pass

                            # Insert chunked
                            inserted = insert_dataframe_chunked(conn, final_table_name, df, mapping_list, chunksize=DEFAULT_CHUNKSIZE, progress_callback=_progress_cb)

                            end_time = datetime.utcnow()
                            duration = (end_time - start_time).total_seconds()
                            st.success(f"Completed. Inserted {inserted} rows in {duration:.1f}s.")
                            hist_entry = {
                                "id": str(uuid.uuid4()),
                                "timestamp": datetime.utcnow().isoformat(),
                                "file_name": uploaded.name,
                                "table": final_table_name,
                                "rows": int(len(df)),
                                "inserted": int(inserted),
                                "mode": load_mode,
                                "user": os.environ.get("USER") or os.environ.get("USERNAME")  # cross-platform
                            }
                            append_history(hist_entry)
                        except Exception as e:
                            st.error(f"Load failed: {e}")
                        finally:
                            if conn:
                                try:
                                    conn.close()
                                except Exception:
                                    pass

        except Exception as e:
            st.error(f"File read error: {e}")

# ---------------------------
# EXPORT TAB
# ---------------------------
with tabs[1]:
    st.header("📤 Export (Table or Custom SQL)")

    exp_auth = st.selectbox("Connection type for export", ["Use same connection as import (sidebar)", "Custom connection"])
    if exp_auth == "Custom connection":
        exp_auth_type = st.selectbox("Authentication Type", ["Windows (Integrated)", "SQL (username/password)"])
        if exp_auth_type == "SQL (username/password)":
            exp_user = st.text_input("Export SQL Username")
            exp_pass = st.text_input("Export SQL Password", type="password")
        else:
            exp_user, exp_pass = None, None
        exp_server = st.text_input("Export SQL Server", value=server)
        exp_db = st.text_input("Export Database", value=database)
    else:
        exp_auth_type = auth_type
        exp_user, exp_pass = sql_user, sql_pass
        exp_server, exp_db = server, database

    mode = st.radio("Export mode", ["Whole table", "Custom query"], index=0)
    conn_exp = None
    try:
        conn_exp = get_connection("SQL" if exp_auth_type=="SQL (username/password)" else "Windows", exp_server, exp_db, exp_user, exp_pass)
        if mode == "Whole table":
            tables = pd.read_sql("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME", conn_exp)
            tables["qualified"] = tables["TABLE_SCHEMA"] + "." + tables["TABLE_NAME"]
            sel = st.selectbox("Choose table to export", tables["qualified"].tolist())
            query = f"SELECT * FROM {sel}"
        else:
            query = st.text_area("Enter SQL query", value="SELECT TOP 100 * FROM INFORMATION_SCHEMA.COLUMNS")

        if st.button("Run & Export"):
            try:
                df_out = pd.read_sql(query, conn_exp)
                st.write(f"Query returned {len(df_out)} rows and {len(df_out.columns)} columns")
                st.dataframe(df_out.head(200), use_container_width=True)

                # download options: CSV or Excel
                to_csv = st.button("Download as CSV")
                to_excel = st.button("Download as Excel (.xlsx)")

                if to_csv:
                    csv_buf = io.StringIO()
                    df_out.to_csv(csv_buf, index=False)
                    st.download_button("Click to download CSV", csv_buf.getvalue(), file_name="export.csv", mime="text/csv")
                if to_excel:
                    out = io.BytesIO()
                    with pd.ExcelWriter(out, engine="openpyxl") as writer:
                        df_out.to_excel(writer, index=False, sheet_name="data")
                    st.download_button("Click to download Excel", out.getvalue(), file_name="export.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            except Exception as e:
                st.error(f"Export error: {e}")
    except Exception as e:
        st.error(f"Could not connect for export: {e}")
    finally:
        if conn_exp:
            try:
                conn_exp.close()
            except:
                pass

# ---------------------------
# HISTORY / LOGS TAB
# ---------------------------
with tabs[2]:
    st.header("📜 Upload History & Logs")
    hist = read_history()
    if hist:
        hist_df = pd.DataFrame(hist)
        st.dataframe(hist_df, use_container_width=True)
    else:
        st.info("No upload history yet. Completed uploads will appear here.")
    if st.button("Clear history"):
        try:
            if os.path.exists(HISTORY_FILE):
                os.remove(HISTORY_FILE)
            st.success("History cleared.")
        except Exception as e:
            st.error(f"Clear failed: {e}")
