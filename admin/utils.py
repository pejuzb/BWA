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