import streamlit as st
from azure.storage.blob import BlobServiceClient
import os as os

st.header("Upload a files")
st.write(f"You are logged in as {st.session_state.role}.")


# Get secrets from environment variables
azure_storage_connection = os.getenv('AZURE_STORAGE_CONNECTION')
azure_storage_container = os.getenv('AZURE_STORAGE_CONTAINER')

# Print the secrets (they will be masked in the GitHub Actions logs)
st.write('This is just a text')
st.write(f"AZURE_STORAGE_CONNECTION: {azure_storage_connection}")
st.write(f"AZURE_STORAGE_CONTAINER: {azure_storage_container}")



# Azure Storage Connection Information
connection_string = azure_storage_connection
container_name = azure_storage_container

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
