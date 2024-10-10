import streamlit as st
from azure.storage.blob import BlobServiceClient
import os as os
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from dotenv import load_dotenv


st.header("Upload a files")
st.write(f"You are logged in as {st.session_state.role}.")


load_dotenv()

client_id = os.getenv('AZURE_CLIENT_ID')
tenant_id = os.getenv('AZURE_TENANT_ID')
client_secret = os.getenv('AZURE_CLIENT_SECRET')
vault_url = os.getenv('AZURE_VAULT_URL')
#storage_url = os.getenv('AZURE_STORAGE_URL')


#st.write(f"This is my storage url: {storage_url}")
# st.write(f"This is my vault url: {vault_url}")
# st.write(f"This is my client id: {client_id}")
# st.write(f"This is my tenant id: {tenant_id}")





#create a credential object

credentials = ClientSecretCredential(
    client_id = client_id,
    tenant_id = tenant_id,
    client_secret = client_secret)



secret_client = SecretClient(vault_url=vault_url, credential=credentials)
secret_name = "sc-storage"
secret = secret_client.get_secret(secret_name)
#st.write(secret.value)

storage_url = secret.value
container_name = 'snfdb'

# Function to upload file to Azure Blob Storage
def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient(account_url=storage_url, credential=credentials)

        # Create a blob client
        blob_client = blob_service_client.get_container_client(container=container_name)

        # Read the file content
        file_data = file.read()

        # Upload the file content to the blob
        blob_client.upload_blob(data=file_data, name=filename, overwrite=True)
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
