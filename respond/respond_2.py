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
from st_aggrid import AgGrid, GridOptionsBuilder  #add import for GridOptionsBuilder
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.ticker as ticker


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


# Snowflake connection
conn = snowflake.connector.connect(
    user=secrets_get('snf-user'),
    password=secrets_get('snf-password'),
    account=secrets_get('snf-account'),
    #warehouse='COMPUTE_WH',
    database='BUDGET',
    schema='RAW',
    role='ACCOUNTADMIN'
)


cur = conn.cursor()

# Query to fetch data from Snowflake
query = "Select * from BUDGET.MART.BUDGET where owner = 'Peter'"

# Load data into Pandas DataFrame
data = pd.read_sql(query, conn)

# Display the DataFrame using Streamlit
st.title('Snowflake Data Viewer')
st.write("Here is the data from Snowflake:")
st.dataframe(data)

#AgGrid(data, height=400)


# Filter out rows where L1 is 'TD Synnex'
df_filtered = data[data['L1'] != 'TD Synnex']



#Group by L1 and month, and sum the amounts
monthly_expenses = df_filtered.groupby(['L1','REPORTING_DATE'])['AMOUNT'].sum()

# Take absolute values of sums
monthly_expenses = monthly_expenses.abs()

st.dataframe(monthly_expenses)


data_chart = pd.read_sql("""Select 
    REPORTING_DATE,
    L1,
    ABS(SUM(AMOUNT)) as AMOUNT FROM BUDGET.MART.BUDGET
    WHERE L1 <> 'TD Synnex' and YEAR = 2024
    GROUP BY ALL;""", conn)

# Display the DataFrame using Streamlit
st.title('Snowflake Data Viewer')
st.write("Monthly expenses:")
st.dataframe(data_chart)

st.bar_chart(data_chart, x="REPORTING_DATE", y="AMOUNT", color="L1", horizontal=False)

#st.bar_chart(df_filtered, x="YEAR", y="AMOUNT", color="L1", stack=False)

#st.bar_chart(
    # data_chart, 
    # x=["REPORTING_DATE","L1"],
    # y="AMOUNT")


# Close the cursor and connection
cur.close()
conn.close()


