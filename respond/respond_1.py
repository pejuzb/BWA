import streamlit as st
from azure.storage.blob import BlobServiceClient
import os as os
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

st.header("Upload a files")
st.write(f"You are logged in as {st.session_state.role}.")


# Replace with your Key Vault URL (available on the Key Vault page in Azure)
key_vault_url = "https://pjvault.vault.azure.net/"

# Authenticate with DefaultAzureCredential, which works for various environments (Azure CLI, Managed Identity, etc.)
credential = DefaultAzureCredential()

# Create a client to access Key Vault secrets
client = SecretClient(vault_url=key_vault_url, credential=credential)

# Replace with the name of the secret you want to access
secret_name = "azureconnection"

# Retrieve the secret value from Key Vault
retrieved_secret = client.get_secret(secret_name)

# Use the secret value in your application
st.write(f"The secret value is: {retrieved_secret.value}")


# # Get secrets from environment variables
# azure_storage_connection = os.getenv('AZURE_STORAGE_CONNECTION')
# azure_storage_container = os.getenv('AZURE_STORAGE_CONTAINER')

# # Print the secrets (they will be masked in the GitHub Actions logs)
# st.write('This is just a text')
# st.write(f"AZURE_STORAGE_CONNECTION: {azure_storage_connection}")
# st.write(f"AZURE_STORAGE_CONTAINER: {azure_storage_container}")



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
