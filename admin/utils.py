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
import altair as alt
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import seaborn as sns
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError
from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.storage.blob import BlobServiceClient
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas
from st_aggrid import AgGrid, GridOptionsBuilder

# Load env variables
load_dotenv()


# What to do after secret expiration
# 1. generate new client secret in Azure portal (App registrations -> your app -> Certificates & secrets)
# 2. update AZURE CLIENT_SECRET in App Services -> Settings -> Environment variables
# 3. update secrets in Git Hub Actions secrets 
# 4. restart the app service

# # Azure Key Vault
# client_id = os.getenv("AZURE_CLIENT_ID")
# tenant_id = os.getenv("AZURE_TENANT_ID")
# client_secret = os.getenv("AZURE_CLIENT_SECRET")
# vault_url = os.getenv("AZURE_VAULT_URL")


# def azure_authenticate():
#     try:
#         credentials = ClientSecretCredential(
#             client_id=client_id, tenant_id=tenant_id, client_secret=client_secret
#         )
#         # st.write("Successfully authenticated with Azure Key Vault.")
#         return credentials
#     except ClientAuthenticationError as e:
#         st.write("Authentication failed. Please check your Azure credentials.")
#         st.write(e)
#     except Exception as e:
#         st.write("An unexpected error occurred during Azure authentication.")
#         st.write(e)


# # Function to get secrets from Azure Key Vault
# def secrets_get(secret_name):
#     try:
#         secret_client = SecretClient(vault_url=vault_url, credential=azure_authenticate())
#         secret = secret_client.get_secret(secret_name)
#         # st.write(f"Successfully retrieved secret: {secret_name}")
#         return secret.value
#     except ClientAuthenticationError as e:
#         st.write("Authentication failed. Please check your Azure credentials.")
#         st.write(e)
#     except HttpResponseError as e:
#         st.write("Failed to retrieve secret due to HTTP error.")
#         st.write(e)
#     except Exception as e:
#         st.write("An unexpected error occurred.")
#         st.write(e)


class AzureKeyVaultClient:
    def __init__(
        self,
        vault_url: str | None = None,
        client_id: str | None = None,
        tenant_id: str | None = None,
        client_secret: str | None = None,
    ):
        self.vault_url = vault_url or os.getenv("AZURE_VAULT_URL")
        self.client_id = client_id or os.getenv("AZURE_CLIENT_ID")
        self.tenant_id = tenant_id or os.getenv("AZURE_TENANT_ID")
        self.client_secret = client_secret or os.getenv("AZURE_CLIENT_SECRET")

        self._validate_env()

    def _validate_env(self):
        missing = [
            name
            for name, value in {
                "AZURE_VAULT_URL": self.vault_url,
                "AZURE_CLIENT_ID": self.client_id,
                "AZURE_TENANT_ID": self.tenant_id,
                "AZURE_CLIENT_SECRET": self.client_secret,
            }.items()
            if not value
        ]

        if missing:
            raise EnvironmentError(
                f"Missing Azure Key Vault environment variables: {', '.join(missing)}"
            )

    def _authenticate(self) -> ClientSecretCredential:
        """Create Azure ClientSecretCredential."""
        try:
            return ClientSecretCredential(
                client_id=self.client_id,
                tenant_id=self.tenant_id,
                client_secret=self.client_secret,
            )
        except ClientAuthenticationError as e:
            st.write("Authentication failed. Please check your Azure credentials.")
            st.write(e)
            raise
        except Exception as e:
            st.write("Unexpected error during Azure authentication.")
            st.write(e)
            raise

    def _secret_client(self) -> SecretClient:
        """Create a SecretClient."""
        return SecretClient(
            vault_url=self.vault_url,
            credential=self._authenticate(),
        )

    def get_secret(self, secret_name: str) -> str:
        """Retrieve a secret value from Azure Key Vault."""
        try:
            secret_client = self._secret_client()
            return secret_client.get_secret(secret_name).value
        except ClientAuthenticationError as e:
            st.write("Authentication failed while retrieving secret.")
            st.write(e)
            raise
        except HttpResponseError as e:
            st.write(f"Failed to retrieve secret '{secret_name}'.")
            st.write(e)
            raise
        except Exception as e:
            st.write("Unexpected error while retrieving secret.")
            st.write(e)
            raise


def upload_to_blob(file, filename):
    try:
        # Create a blob service client using the connection string
        blob_service_client = BlobServiceClient(
            account_url=secrets_get("sc-storage"), credential=azure_authenticate()
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




class SnowflakeClient:
    def __init__(
        self,
        kv_client: AzureKeyVaultClient,
        warehouse: str = "COMPUTE_WH",
        database: str = "BUDGET",
        schema: str = "RAW",
        role: str = "PUBLIC",
    ):
        self.kv = kv_client
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

    # -------------------------
    # Key handling (standalone)
    # -------------------------
    @staticmethod
    def normalize_pem(pem_text: str) -> bytes:
        pem_text = pem_text.strip()

        # If it contains literal "\n", convert to real newlines
        pem_text = pem_text.replace("\\n", "\n")

        # If it's still basically one line, rebuild PEM formatting
        if "\n" not in pem_text:
            pem_text = pem_text.replace(
                "-----BEGIN PRIVATE KEY-----",
                "-----BEGIN PRIVATE KEY-----\n",
            )
            pem_text = pem_text.replace(
                "-----END PRIVATE KEY-----",
                "\n-----END PRIVATE KEY-----",
            )

            # Remove spaces/newlines inside, then re-wrap base64
            m = re.search(
                r"-----BEGIN PRIVATE KEY-----\s*(.*?)\s*-----END PRIVATE KEY-----",
                pem_text,
                re.S,
            )
            if not m:
                raise ValueError("Could not find PEM header/footer in secret value.")

            b64 = re.sub(r"\s+", "", m.group(1))
            pem_text = (
                "-----BEGIN PRIVATE KEY-----\n"
                + "\n".join(wrap(b64, 64))
                + "\n-----END PRIVATE KEY-----\n"
            )

        return pem_text.encode("utf-8")

    @staticmethod
    def pem_to_snowflake_der(pem_bytes: bytes) -> bytes:
        p_key = serialization.load_pem_private_key(
            pem_bytes,
            password=None,  # important (unencrypted key)
            backend=default_backend(),
        )
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @classmethod
    def private_key_from_secret(cls, pem_text: str) -> bytes:
        """
        Convenience: secret text -> normalized PEM -> Snowflake DER bytes
        """
        return cls.pem_to_snowflake_der(cls.normalize_pem(pem_text))

    # -------------------------
    # Connection + queries
    # -------------------------
    def _connect(self):
        """Create and return a Snowflake connection (internal use)."""
        try:
            return snowflake.connector.connect(
                user=self.kv.get_secret("svc-snf-user"),
                private_key=self.private_key_from_secret(
                    self.kv.get_secret("svc-snf-rsa-key")
                ),
                account=self.kv.get_secret("svc-snf-acc"),
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                role=self.role,
            )
        except Exception as e:
            st.write("Error connecting to Snowflake:")
            st.write(e)
            raise

    def run_query(self, sql: str, params=None):
        """Execute SQL and return raw rows."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params) if params else cur.execute(sql)
                return cur.fetchall()
        finally:
            conn.close()

    def run_query_df(self, sql: str, params=None) -> pd.DataFrame:
        """Execute SQL and return a pandas DataFrame."""
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params) if params else cur.execute(sql)
                return pd.DataFrame.from_records(
                    cur.fetchall(),
                    columns=[c[0] for c in cur.description],
                )
        finally:
            conn.close()