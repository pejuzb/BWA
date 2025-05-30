import streamlit as st
from azure.storage.blob import BlobServiceClient
import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
load_dotenv()
from datetime import datetime
import pytz  # Importing pytz for timezone handling
import time
from io import StringIO

# Azure Key Vault
client_id = os.getenv('AZURE_CLIENT_ID')
tenant_id = os.getenv('AZURE_TENANT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
vault_url = os.getenv('AZURE_VAULT_URL')

# Create a credential object
credentials = ClientSecretCredential(
    client_id=client_id,
    tenant_id=tenant_id,
    client_secret=client_secret
)

# Function to get secrets from Azure Key Vault
def secrets_get(secret_name):
    secret_client = SecretClient(vault_url=vault_url, credential=credentials)
    secret = secret_client.get_secret(secret_name)
    return secret.value

def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient(account_url=secrets_get("sc-storage"), credential=credentials)

        # Get a container client
        blob_client = blob_service_client.get_container_client(container='snfdb')

        # Define the folder path within the container
        folder_path = "jan/inputs/"

        # Check if the filename contains 'SK7075000000004024135645' or 'account-statement'
        if 'SK7075000000004024135645' in filename or 'account-statement' in filename:
            # Determine the string to look for in existing blobs based on the filename
            file_keyword = 'SK7075000000004024135645' if 'SK7075000000004024135645' in filename else 'account-statement'

            # Check for existing files containing the specific keyword in 'jan/inputs'
            existing_blobs = blob_client.list_blobs(name_starts_with=folder_path)
            for existing_blob in existing_blobs:
                if file_keyword in existing_blob.name:
                    # Move the existing file to the 'jan/processed_files' folder
                    source_blob = existing_blob.name
                    target_blob = f"jan/processed_files/{existing_blob.name.split('/')[-1]}"

                    # Copy the existing blob to the new location
                    blob_client.get_blob_client(target_blob).start_copy_from_url(blob_client.get_blob_client(source_blob).url)

                    # Delete the original blob
                    blob_client.delete_blob(source_blob)

        # Define the full path for the new file within the container
        full_filename = f"{folder_path}{filename}"

        # Read the file content
        file_data = file.read()

        # Upload the file content to the blob within 'jan/inputs' folder
        blob_client.upload_blob(data=file_data, name=full_filename, overwrite=True)
        return f"File {filename} uploaded successfully to 'jan/inputs'!"
    
    except Exception as e:
        return f"Error uploading file: {e}"

# Streamlit File Uploader for multiple files
st.title("Upload Files to Azure Blob Storage")

uploaded_files = st.file_uploader("Choose files", type=['csv', 'txt', 'pdf', 'jpg', 'png'], accept_multiple_files=True)

if uploaded_files is not None:
    for uploaded_file in uploaded_files:
        # Get the file details
        file_details = {
            "filename": uploaded_file.name,
            "filetype": uploaded_file.type,
            "filesize": uploaded_file.size
        }

        # Display file details
        #st.write(file_details)

        # Upload the file to Azure Blob Storage
        result_message = upload_to_blob(uploaded_file, uploaded_file.name)
        st.success(result_message)

# Snowflake connection
conn = snowflake.connector.connect(
    user=secrets_get('snf-user-app'),
    password=secrets_get('snf-password-app'),
    account=secrets_get('snf-account'),
    warehouse='COMPUTE_WH',
    database='BUDGET',
    schema='RAW',
    role='PUBLIC'
)


cur = conn.cursor()

if st.button("Recalculate Database"):
    try:
        cur.execute("call truncate_raw_tables();")
       
        # Execute the stored procedures
        cur.execute("CALL COPY_FILES_TO_RAW_REVOLUT();")
        st.write("Raw procedure [REVOLUT] executed successfully!")

        cur.execute("CALL COPY_FILES_TO_RAW_CSOB();")
        st.write("Raw procedure [CSOB] executed successfully!")

        cur.execute("CALL COPY_FILES_TO_HIERARCHY();")
        st.write("Raw procedure [HIERARCHY] executed successfully!")

        cur.execute("CALL BUDGET.CORE.RAW2CORE_REV();")
        st.write("Core procedure [REVOLUT] executed successfully!")

        cur.execute("CALL BUDGET.CORE.RAW2CORE_CSOB();")
        st.write("Core procedure [CSOB] executed successfully!")

        cur.execute("CALL BUDGET.CORE.RAW2CORE_HIERARCHY();")
        st.write("Core procedure [HIERARCHY] executed successfully!")

        cur.execute("CALL BUDGET.CORE.CORE2CORE_MANUAL_ADJ();")
        st.write("Core procedure [C2C MANUAL ADJUSTMENTS] executed successfully!")

    except Exception as e:
        st.write(f"Error: {e}")  # Display error message if any


# Query to fetch data from Snowflake
query = "Select * from BUDGET.CORE.HIERARCHY where owner = 'Jan'"

# Load data into Pandas DataFrame
df = pd.read_sql(query, conn)

# Display the DataFrame using Streamlit
st.title('Snowflake Data Viewer')
st.write("Here is the data from Snowflake:")
st.dataframe(df)




# Query to fetch data from Snowflake
query_mh = """Select * from BUDGET.MART.BUDGET where owner = 'Jan' and L1 is null
order by transaction_date desc;"""

# Load data into Pandas DataFrame
df_mh = pd.read_sql(query_mh, conn)

# Display the DataFrame using Streamlit
st.title('Record with Missing Hierarchy')
st.write("Transaction data with missing hierarchy:")
st.dataframe(df_mh)




# Function to query data from Snowflake
@st.cache_data  # Caches the data to avoid querying every time
def load_data():
        query = """with test as (
        Select distinct prod_hierarchy,source_system from BUDGET.CORE.TRANSACTION as a
        where a.owner = 'Jan'
        )

        Select
        --a.source_system,
        MD5(a.prod_hierarchy) as HIERARCHY_HK,
        a.prod_hierarchy as PROD_HIERARCHY_ID,
        b.L1,
        b.L2,
        b.L3,
        'Jan' as OWNER

        from test as a
        left join (Select * from BUDGET.CORE.HIERARCHY where owner = 'Jan') as b
        on a.prod_hierarchy = b.prod_hierarchy_id
        where HIERARCHY_HK is null
        order by 1,2"""
        
        df = pd.read_sql(query, conn)
        return df

# Function to insert DataFrame back into Snowflake
def insert_data(df):
    conn.cursor().execute("USE SCHEMA CORE")
    success, nchunks, nrows, _ = write_pandas(conn, df, 'HIERARCHY')
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


# def export_csv():
#     try:
#         # Create a blob service client using the connection string or account URL
#         blob_service_client = BlobServiceClient(account_url=secrets_get("sc-storage"), credential=credentials)

#         # Create a container client for the specified container (without the 'blob' argument)
#         container_client = blob_service_client.get_container_client('snfdb')

#         # Create a blob client for the specific blob (file) you want to upload
#         blob_client = container_client.get_blob_client('azure_export_oco.csv')


#         # Query to fetch data from Snowflake
#         query_hier = "Select * from BUDGET.CORE.HIERARCHY where owner = 'Jan'"

#         # Load data into Pandas DataFrame
#         df_H = pd.read_sql(query_hier, conn)

#         if df_H.empty:
#             st.write("No data to export. The DataFrame is empty.")
#             return  # This terminates the process if no data is available

#         # Convert DataFrame to CSV in memory
#         csv_buffer = StringIO()
#         df_H.to_csv(csv_buffer, index=False)

#         # Upload the CSV to Azure Blob Storage
#         blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

#         st.write("CSV exported successfully!")
        
#     except Exception as e:
#         st.write(f"Error exporting data: {e}")

def export_csv(df_update):
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient(account_url=secrets_get("sc-storage"), credential=credentials)

    # Get the container client
    container_client = blob_service_client.get_container_client(container="snfdb")

    # Create a blob client for the specific blob
    blob_client = container_client.get_blob_client(blob="jan/inputs/input_hierarchy_jan.csv")

    # Download the blob's content as text
    blob_data = blob_client.download_blob().content_as_text()

    # Convert the text data to a DataFrame
    df = pd.read_csv(StringIO(blob_data),delimiter=";")

    df.columns = df.columns.str.upper()

    if df_update.empty:
        return
    
    df_update = df_update[['PROD_HIERARCHY_ID','L1','L2','L3','LOAD_DATETIME']]
    df_update = df_update.rename(columns={'LOAD_DATETIME': 'AZURE_INSERT_DATETIME'})

    df_update.columns = df.columns
    df_combined = pd.concat([df, df_update], ignore_index=True)

     # Convert DataFrame to CSV in memory
    csv_buffer = StringIO()
    df_combined.to_csv(csv_buffer, index=False, sep = ';')

    

    # Upload the CSV to Azure Blob Storage
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

    #st.write("CSV exported successfully!")
    
    # Display the DataFrame
    return True


# Button to insert updated data
if st.button("Insert Data into Snowflake"):
    edited_df['LOAD_DATETIME'] = datetime.now(pytz.timezone('Europe/Prague')).strftime('%Y-%m-%d %H:%M:%S')
    edited_df = edited_df[edited_df['L1'].notnull()]
    insert_data(edited_df)
    export_csv(edited_df)

# Add a "Refresh Cache" button
if st.button("Refresh Cache"):
    st.cache_data.clear()  # Clear the cache
    st.success("Cache cleared!")

# Close the cursor and connection
cur.close()
#conn.close()



