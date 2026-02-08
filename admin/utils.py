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


class SnowflakeClient:
    def __init__(
        self,
        kv_client: AzureKeyVaultClient,
        warehouse: str = "SVC_APP_WH",
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

    def sf_write_pandas(self, df: pd.DataFrame, table_name: str, schema: str = "CORE") -> tuple[bool, int, int]:
        conn = self._connect()
        try:
            # Common return: (success, nchunks, nrows, output)
            success, nchunks, nrows, *_ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=self.database,
                schema=schema,
            )
            return bool(success), int(nchunks), int(nrows)
        except Exception as e:
            st.write("Error writing DataFrame to Snowflake:")
            st.write(e)
            return False, 0, 0
        finally:
            conn.close()

# # Function to insert DataFrame back into Snowflake
# def insert_data(df):
#     #conn.cursor().execute("USE SCHEMA CORE")
#     success, nchunks, nrows, _ = write_pandas(snf._connect, df, table_name= "HIERARCHY", schema="CORE")
#     if success:
#         st.success(f"Successfully inserted {nrows} rows into Snowflake!")
#         st.cache_data.clear()
#     else:
#         st.error("Failed to insert data.")



class AzureBlobUploader:
    def __init__(
        self,
        kv_client: AzureKeyVaultClient,
        container: str = "snfdb",
        input_folder: str = "peter/inputs/",
        processed_folder: str = "peter/processed_files/",
    ):
        self.kv = kv_client
        self.container = container
        self.input_folder = input_folder
        self.processed_folder = processed_folder

        self.blob_service_client = BlobServiceClient(
            account_url=self.kv.get_secret("sc-storage"),
            credential=self.kv._authenticate(),
        )
        self.container_client = self.blob_service_client.get_container_client(
            container=self.container
        )

    def upload_file(self, file, filename: str) -> str:
        try:
            self._archive_existing_file(filename)

            full_blob_name = f"{self.input_folder}{filename}"
            file_data = file.read()

            self.container_client.upload_blob(
                name=full_blob_name,
                data=file_data,
                overwrite=True,
            )

            return f"File {filename} uploaded successfully to '{self.input_folder}'!"

        except Exception as e:
            return f"Error uploading file: {e}"

    def _archive_existing_file(self, filename: str) -> None:
        file_keyword = self._extract_keyword(filename)
        if not file_keyword:
            return

        existing_blobs = self.container_client.list_blobs(
            name_starts_with=self.input_folder
        )

        for blob in existing_blobs:
            if file_keyword in blob.name:
                self._move_blob_to_processed(blob.name)

    def _move_blob_to_processed(self, source_blob: str) -> None:
        target_blob = (
            f"{self.processed_folder}{source_blob.split('/')[-1]}"
        )

        source_client = self.container_client.get_blob_client(source_blob)
        target_client = self.container_client.get_blob_client(target_blob)

        target_client.start_copy_from_url(source_client.url)
        self.container_client.delete_blob(source_blob)

    def export_hierarchy_csv(
        self,
        df_update: pd.DataFrame,
        blob_path: str = "peter/inputs/input_hierarchy_peter.csv",
        delimiter: str = ";",
    ) -> bool:
        """
        Download existing hierarchy CSV from Azure Blob,
        append new rows, and upload it back.

        Returns True if export was performed, False otherwise.
        """

        if df_update.empty:
            return False

        blob_client = self.container_client.get_blob_client(blob_path)

        # Download existing CSV
        blob_data = blob_client.download_blob().content_as_text()
        df_existing = pd.read_csv(StringIO(blob_data), delimiter=delimiter)

        # Normalize columns
        df_existing.columns = df_existing.columns.str.upper()

        df_update = df_update[
            ["PROD_HIERARCHY_ID", "L1", "L2", "L3", "LOAD_DATETIME"]
        ].rename(columns={"LOAD_DATETIME": "AZURE_INSERT_DATETIME"})

        df_update.columns = df_existing.columns

        # Combine
        df_combined = pd.concat([df_existing, df_update], ignore_index=True)

        # Upload back to Azure
        csv_buffer = StringIO()
        df_combined.to_csv(csv_buffer, index=False, sep=delimiter)

        blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

        return True
    
    @staticmethod
    def _extract_keyword(filename: str) -> str | None:
        if "pohyby" in filename:
            return "pohyby"
        if "account-statement" in filename:
            return "account-statement"
        return None