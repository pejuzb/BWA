import streamlit as st
from azure.storage.blob import BlobServiceClient
import os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv
import snowflake.connector
load_dotenv()

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

# Function to upload file to Azure Blob Storage
def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient(account_url=secrets_get("sc-storage"), credential=credentials)

        # Create a blob client
        blob_client = blob_service_client.get_container_client(container='snfdb')

        # Read the file content
        file_data = file.read()

        # Upload the file content to the blob
        blob_client.upload_blob(data=file_data, name=filename, overwrite=True)
        return f"File {filename} uploaded successfully!"
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
        st.write(file_details)

        # Upload the file to Azure Blob Storage
        result_message = upload_to_blob(uploaded_file, uploaded_file.name)
        st.write(result_message)

# Snowflake connection
conn = snowflake.connector.connect(
    user=secrets_get('snf-user'),
    password=secrets_get('snf-password'),
    account=secrets_get('snf-account'),
    warehouse='COMPUTE_WH',
    database='BUDGET',
    schema='RAW',
    role='ACCOUNTADMIN'
)

cur = conn.cursor()

if st.button("Load files"):
    try:
        # Execute the stored procedures
        cur.execute("CALL COPY_FILES_TO_RAW_REVOLUT();")
        st.write("First stored procedure [REVOLUT] executed successfully!")

        cur.execute("CALL COPY_FILES_TO_RAW_CSOB();")
        st.write("Second stored procedure [CSOB] executed successfully!")

    except Exception as e:
        st.write(f"Error: {e}")  # Display error message if any

# Close the cursor and connection
cur.close()
conn.close()