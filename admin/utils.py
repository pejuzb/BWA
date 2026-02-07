# Standard library
import os
import re
import time
from datetime import datetime
from io import StringIO
from textwrap import wrap

# Third-party
import pandas as pd
import pytz  # timezone handling
import snowflake.connector
import streamlit as st
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

# Load env variables
load_dotenv()


# Function to get secrets from Azure Key Vault
def secrets_get(secret_name):
    try:
        secret_client = SecretClient(vault_url=vault_url, credential=credentials)
        secret = secret_client.get_secret(secret_name)
        # st.write(f"Successfully retrieved secret: {secret_name}")
        return secret.value
    except ClientAuthenticationError as e:
        st.write("Authentication failed. Please check your Azure credentials.")
        st.write(e)
    except HttpResponseError as e:
        st.write("Failed to retrieve secret due to HTTP error.")
        st.write(e)
    except Exception as e:
        st.write("An unexpected error occurred.")
        st.write(e)



def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient(
            account_url=secrets_get("sc-storage"), credential=credentials
        )

        # Get a container client
        blob_client = blob_service_client.get_container_client(container="snfdb")

        # Define the folder path within the container
        folder_path = "peter/inputs/"

        # Check if the filename contains 'pohyby' or 'account-statement'
        if "pohyby" in filename or "account-statement" in filename:
            # Determine the string to look for in existing blobs based on the filename
            file_keyword = "pohyby" if "pohyby" in filename else "account-statement"

            # Check for existing files containing the specific keyword in 'peter/inputs'
            existing_blobs = blob_client.list_blobs(name_starts_with=folder_path)
            for existing_blob in existing_blobs:
                if file_keyword in existing_blob.name:
                    # Move the existing file to the 'peter/processed_files' folder
                    source_blob = existing_blob.name
                    target_blob = (
                        f"peter/processed_files/{existing_blob.name.split('/')[-1]}"
                    )

                    # Copy the existing blob to the new location
                    blob_client.get_blob_client(target_blob).start_copy_from_url(
                        blob_client.get_blob_client(source_blob).url
                    )

                    # Delete the original blob
                    blob_client.delete_blob(source_blob)

        # Define the full path for the new file within the container
        full_filename = f"{folder_path}{filename}"

        # Read the file content
        file_data = file.read()

        # Upload the file content to the blob within 'peter/inputs' folder
        blob_client.upload_blob(data=file_data, name=full_filename, overwrite=True)
        return f"File {filename} uploaded successfully to 'peter/inputs'!"

    except Exception as e:
        return f"Error uploading file: {e}"



def pem_to_snowflake_der(pem_bytes: bytes) -> bytes:
    p_key = serialization.load_pem_private_key(
        pem_bytes,
        password=None,                 # <-- important (unencrypted key)
        backend=default_backend(),
    )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

def normalize_pem(pem_text: str) -> bytes:
    pem_text = pem_text.strip()

    # If it contains literal "\n", convert to real newlines
    pem_text = pem_text.replace("\\n", "\n")

    # If it's still basically one line, rebuild PEM formatting
    if "\n" not in pem_text:
        pem_text = pem_text.replace("-----BEGIN PRIVATE KEY-----", "-----BEGIN PRIVATE KEY-----\n")
        pem_text = pem_text.replace("-----END PRIVATE KEY-----", "\n-----END PRIVATE KEY-----")
        # Remove spaces/newlines inside, then re-wrap base64
        m = re.search(r"-----BEGIN PRIVATE KEY-----\s*(.*?)\s*-----END PRIVATE KEY-----", pem_text, re.S)
        if not m:
            raise ValueError("Could not find PEM header/footer in secret value.")
        b64 = re.sub(r"\s+", "", m.group(1))
        pem_text = "-----BEGIN PRIVATE KEY-----\n" + "\n".join(wrap(b64, 64)) + "\n-----END PRIVATE KEY-----\n"

    return pem_text.encode("utf-8")
