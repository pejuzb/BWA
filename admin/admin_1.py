from admin.utils import *

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
        result_message = upload_to_blob(uploaded_file, uploaded_file.name)
        st.success(result_message)


# conn = snowflake.connector.connect(
#     user=secrets_get('svc-snf-user'),
#     private_key=pem_to_snowflake_der(normalize_pem(secrets_get('svc-snf-rsa-key'))),          
#     account=secrets_get('svc-snf-acc'),
#     warehouse="COMPUTE_WH",
#     database="BUDGET",
#     schema="RAW",
#     role="PUBLIC",
# )

# cur = snowflake_connection().cursor()




if st.button("Recalculate Database"):
    try:
        #cur.execute("CALL BUDGET.RAW.TRUNCATE_RAW_TABLES();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.RAW.TRUNCATE_RAW_TABLES();")
        st.write("Raw schema truncated!")

        # Execute the stored procedures
        #cur.execute("CALL BUDGET.RAW.COPY_FILES_TO_RAW_REVOLUT();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.RAW.COPY_FILES_TO_RAW_REVOLUT();")
        st.write("Raw procedure [REVOLUT] executed successfully!")

        #cur.execute("CALL BUDGET.RAW.COPY_FILES_TO_RAW_CSOB();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.RAW.COPY_FILES_TO_RAW_CSOB();")
        st.write("Raw procedure [CSOB] executed successfully!")

        #cur.execute("CALL BUDGET.RAW.COPY_FILES_TO_HIERARCHY();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.RAW.COPY_FILES_TO_HIERARCHY();")
        st.write("Raw procedure [HIERARCHY] executed successfully!")

        #cur.execute("CALL BUDGET.CORE.RAW2CORE_REV();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.CORE.RAW2CORE_REV();")
        st.write("Core procedure [REVOLUT] executed successfully!")

        #cur.execute("CALL BUDGET.CORE.RAW2CORE_CSOB();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.CORE.RAW2CORE_CSOB();")
        st.write("Core procedure [CSOB] executed successfully!")

        #cur.execute("CALL BUDGET.CORE.RAW2CORE_HIERARCHY();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.CORE.RAW2CORE_HIERARCHY();")
        st.write("Core procedure [HIERARCHY] executed successfully!")

        #cur.execute("CALL BUDGET.CORE.CORE2CORE_MANUAL_ADJ();")
        snowflake_run_query(azure_authenticate(), "CALL BUDGET.CORE.CORE2CORE_MANUAL_ADJ();")
        st.write("Core procedure [C2C MANUAL ADJUSTMENTS] executed successfully!")

    except Exception as e:
        st.write(f"Error: {e}")  # Display error message if any


# Query to fetch data from Snowflake
query = "Select * from BUDGET.CORE.HIERARCHY where owner = 'Peter'" 

# Load data into Pandas DataFrame
df = snowflake_run_query_df(snowflake_connection(), query)


# Display the DataFrame using Streamlit
st.title("Snowflake Data Viewer")
st.write("Here is the data from Snowflake:")
st.dataframe(df)


# Query to fetch data from Snowflake
query_mh = """Select * from BUDGET.MART.BUDGET where owner = 'Peter' and L1 is null
order by transaction_date desc;"""

# Load data into Pandas DataFrame
df_mh = snowflake_run_query_df(snowflake_connection(), query_mh)

# Display the DataFrame using Streamlit
st.title("Record with Missing Hierarchy")
st.write("Transaction data with missing hierarchy:")
st.dataframe(df_mh)


def export_csv(df_update):
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient(
        account_url=secrets_get("sc-storage"), credential=azure_authenticate()
    )

    # Get the container client
    container_client = blob_service_client.get_container_client(container="snfdb")

    # Create a blob client for the specific blob
    blob_client = container_client.get_blob_client(
        blob="peter/inputs/input_hierarchy_peter.csv"
    )

    # Download the blob's content as text
    blob_data = blob_client.download_blob().content_as_text()

    # Convert the text data to a DataFrame
    df = pd.read_csv(StringIO(blob_data), delimiter=";")

    df.columns = df.columns.str.upper()

    if df_update.empty:
        return

    df_update = df_update[["PROD_HIERARCHY_ID", "L1", "L2", "L3", "LOAD_DATETIME"]]
    df_update = df_update.rename(columns={"LOAD_DATETIME": "AZURE_INSERT_DATETIME"})

    df_update.columns = df.columns

    df_combined = pd.concat([df, df_update], ignore_index=True)
    # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df_combined.to_csv(csv_buffer, index=False, sep=";")

    # Upload the CSV to Azure Blob Storage
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

    # st.write("CSV exported successfully!")

    # Display the DataFrame
    return True


# Function to query data from Snowflake
@st.cache_data  # Caches the data to avoid querying every time
def load_data():
    query = """with test as (
        Select distinct prod_hierarchy,source_system from BUDGET.CORE.TRANSACTION as a
        where a.owner = 'Peter'
        )

        Select
        --a.source_system,
        MD5(a.prod_hierarchy) as HIERARCHY_HK,
        a.prod_hierarchy as PROD_HIERARCHY_ID,
        b.L1,
        b.L2,
        b.L3,
        'Peter' as OWNER

        from test as a
        left join (Select * from BUDGET.CORE.HIERARCHY where owner = 'Peter') as b
        on a.prod_hierarchy = b.prod_hierarchy_id
        where HIERARCHY_HK is null
        order by 1,2"""

    df = snowflake_run_query_df(snowflake_connection(), query)
    return df


# Function to insert DataFrame back into Snowflake
def insert_data(df):
    #conn.cursor().execute("USE SCHEMA CORE")
    success, nchunks, nrows, _ = write_pandas(snowflake_connection(), df, "CORE.HIERARCHY")
    if success:
        st.success(f"Successfully inserted {nrows} rows into Snowflake!")
        st.cache_data.clear()
    else:
        st.error("Failed to insert data.")


# Load data from Snowflake
df = load_data()

# Display editable DataFrame
st.write("### Editable Table")
edited_df = st.data_editor(df, num_rows="dynamic")

# Button to insert updated data
if st.button("Insert Data into Snowflake"):
    edited_df["LOAD_DATETIME"] = datetime.now(pytz.timezone("Europe/Prague")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    edited_df = edited_df[edited_df["L1"].notnull()]
    insert_data(edited_df)
    export_csv(edited_df)

# Add a "Refresh Cache" button
if st.button("Refresh Cache"):
    st.cache_data.clear()  # Clear the cache
    st.success("Cache cleared!")


# Close the cursor and connection
#cur.close()
# conn.close()
