from admin.utils import *

azk = AzureKeyVaultClient()
abl = AzureBlobUploader(kv_client=azk)
snf = SnowflakeClient(kv_client=azk)

# Streamlit File Uploader for multiple files
st.title("Upload Files to Azure Blob Storage")

uploaded_files = st.file_uploader(
    "Choose files", type=["csv", "txt", "pdf", "jpg", "png"], accept_multiple_files=True
)

if uploaded_files is not None:
    for uploaded_file in uploaded_files:
        # Get the file details
        file_details = {
            "filename": uploaded_file.name,
            "filetype": uploaded_file.type,
            "filesize": uploaded_file.size,
        }

        # Upload the file to Azure Blob Storage
        result_message = abl.upload_file(uploaded_file, uploaded_file.name)
        st.success(result_message)


if st.button("Recalculate Database"):
    try:
        procedures = {
            "Truncate RAW schema": "CALL BUDGET.RAW.TRUNCATE_RAW_TABLES();",
            "RAW procedure [REVOLUT]": "CALL BUDGET.RAW.COPY_FILES_TO_RAW_REVOLUT();",
            "RAW procedure [CSOB]": "CALL BUDGET.RAW.COPY_FILES_TO_RAW_CSOB();",
            "RAW procedure [HIERARCHY]": "CALL BUDGET.RAW.COPY_FILES_TO_HIERARCHY();",
            "CORE procedure [REVOLUT]": "CALL BUDGET.CORE.RAW2CORE_REV();",
            "CORE procedure [CSOB]": "CALL BUDGET.CORE.RAW2CORE_CSOB();",
            "CORE procedure [HIERARCHY]": "CALL BUDGET.CORE.RAW2CORE_HIERARCHY();",
            "CORE procedure [C2C MANUAL ADJUSTMENTS]": "CALL BUDGET.CORE.CORE2CORE_MANUAL_ADJ();",
        }

        for label, sql in procedures.items():
            snf.run_query(sql)
            st.write(f"{label} executed successfully!")
    except Exception as e:
        st.write(f"Error: {e}")  # Display error message if any


# Query to fetch data from Snowflake
query = "Select * from BUDGET.CORE.HIERARCHY where owner = 'Peter'" 

# Load data into Pandas DataFrame
df = snf.run_query_df(query)

# Display the DataFrame using Streamlit
st.title("Snowflake Data Viewer")
st.write("Here is the data from Snowflake:")
st.dataframe(df)


# Query to fetch data from Snowflake
query_rules = "Select * from BUDGET.CORE.RULES_TABLE" 

# Load data into Pandas DataFrame
df_rules = snf.run_query_df(query_rules)

# Display the DataFrame using Streamlit
st.title("Rules Table Viewer")
st.write("Defined rules for hierarchy mapping:")
st.dataframe(df_rules)


# Query to fetch data from Snowflake
query_mh = """Select * from BUDGET.MART.BUDGET where owner = 'Peter' and L1 is null
order by transaction_date desc;"""

# Load data into Pandas DataFrame
df_mh = snf.run_query_df(query_mh)


# Display the DataFrame using Streamlit
st.title("Record with Missing Hierarchy")
st.write("Transaction data with missing hierarchy:")
st.dataframe(df_mh)



# Editable Dataframe with caching and refresh mechanism

TZ = pytz.timezone("Europe/Prague")

# --- session state init ---
if "refresh_token" not in st.session_state:
    st.session_state.refresh_token = 0

def bump_refresh():
    """Forces load_data() cache miss + triggers rerun."""
    st.session_state.refresh_token += 1
    st.cache_data.clear()
    st.rerun()


@st.cache_data(show_spinner="Loading missing hierarchies...")
def load_data(refresh_token: int) -> pd.DataFrame:
    # refresh_token is unused in logic, but forces cache key changes
    query = f"""
    WITH tx AS (
        SELECT DISTINCT prod_hierarchy, source_system
        FROM BUDGET.CORE.TRANSACTION
        WHERE owner = 'Peter'
          AND prod_hierarchy IS NOT NULL
    )
    SELECT
        MD5(tx.prod_hierarchy) AS HIERARCHY_HK,
        tx.prod_hierarchy      AS PROD_HIERARCHY_ID,
        h.L1,
        h.L2,
        h.L3,
        'Peter'              AS OWNER
    FROM tx
    LEFT JOIN BUDGET.CORE.HIERARCHY h
      ON tx.prod_hierarchy = h.prod_hierarchy_id
     AND h.owner = 'Peter'
    WHERE h.prod_hierarchy_id IS NULL
    ORDER BY 1, 2
    """
    return snf.run_query_df(query)


st.write("### Editable Table (missing in HIERARCHY)")

df = load_data(st.session_state.refresh_token)

with st.form("hierarchy_form", clear_on_submit=False):
    edited_df = st.data_editor(
        df,
        num_rows="dynamic",
        use_container_width=True,
        key="hierarchy_editor",
    )

    col1, col2 = st.columns([1, 1])
    with col1:
        submit = st.form_submit_button("Insert Data into Snowflake")
    with col2:
        refresh = st.form_submit_button("Reload from Snowflake")

if refresh:
    bump_refresh()

if submit:
    # Keep only rows the user actually filled
    to_insert = edited_df.copy()
    to_insert = to_insert[to_insert["L1"].notna()].copy()

    if to_insert.empty:
        st.warning("Nothing to insert (fill at least L1).")
    else:
        # Add timestamp as proper datetime (prefer datetime over string)
        to_insert["LOAD_DATETIME"] = datetime.now(TZ)

        try:
            # Write to Snowflake
            snf.sf_write_pandas(to_insert, table_name="HIERARCHY")

            # Export to Azure (your method expects LOAD_DATETIME column)
            abl.export_hierarchy_csv(to_insert)

            st.success(f"Inserted {len(to_insert)} rows and updated Azure CSV.")

            # Force reload so table reflects what's now in Snowflake
            bump_refresh()

        except Exception as e:
            st.error(f"Insert/export failed: {e}")