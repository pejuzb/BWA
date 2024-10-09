import streamlit as st
from azure-storage-blob import BlobServiceClient
import os as os
from azure-identity import ClientSecretCredential
from azure-keyvault-secrets import SecretClient


st.header("Upload a files")
st.write(f"You are logged in as {st.session_state.role}.")


client_id = os.environ('AZURE_CLIENT_ID')
tenant_id = os.environ('AZURE_TENANT_ID')
client_secret = os.environ('AZURE_CLIENT_SECRET')
vault_url = os.environ('AZURE_VAULT_URL')

#print(client_id,tenant_id,client_secret,vault_url)


secret_name = "sc-test"

#create a credential

credentials = ClientSecretCredential(
    client_id = client_id,
    tenant_id = tenant_id,
    client_secret = client_secret)


secret_client = SecretClient(vault_url=vault_url, credential=credentials)
secret = secret_client.get_secret(secret_name)
st.write(secret.value)


# Azure Storage Connection Information
connection_string = ''
container_name = ''

# Function to upload file to Azure Blob Storage
def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Create a blob client
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

        # Upload the file to the blob
        blob_client.upload_blob(file, overwrite=True)
        return f"File {filename} uploaded successfully!"
    except Exception as e:
        return f"Error uploading file: {e}"

# Streamlit File Uploader
st.title("Upload Files to Azure Blob Storage")

uploaded_file = st.file_uploader("Choose a file", type=['csv', 'txt', 'pdf', 'jpg', 'png'])

if uploaded_file is not None:
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
