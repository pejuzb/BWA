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

# Group by L1 and month, and sum the amounts
monthly_expenses = df_filtered.groupby(['L1', 'YEAR', 'MONTH'])['AMOUNT'].sum().unstack(level=0)

# Take absolute values of sums
monthly_expenses = monthly_expenses.abs()

# Plot the data as a stacked bar chart
plt.figure(figsize=(12, 8))  # Increase the figure size for better readability
ax = monthly_expenses.plot(kind='bar', stacked=True, ax=plt.gca(), width=0.8)  # Increase the width of bars for better visibility

plt.title('Monthly Expenses by L1 Category', fontsize=16)  # Increase title font size
plt.xlabel('Year-Month', fontsize=14)  # Increase x-axis label font size
plt.ylabel('Total Amount', fontsize=14)  # Increase y-axis label font size
plt.xticks(rotation=45, ha='right', fontsize=10)  # Rotate x-axis labels for better readability and adjust font size

# Add thousand separators to y-axis labels
formatter = ticker.StrMethodFormatter('{x:,.0f}')
plt.gca().yaxis.set_major_formatter(formatter)

# Add horizontal gridlines with increased transparency
plt.grid(axis='y', linestyle='--', alpha=0.5)

# Move legend to the bottom horizontally with increased font size and number of columns
plt.legend(title='L1 Category', bbox_to_anchor=(0.5, -0.2), loc='upper center', ncol=len(monthly_expenses.columns), fontsize=10)

plt.tight_layout()
plt.show()

#st.bar_chart(
    # data_chart, 
    # x=["REPORTING_DATE","L1"],
    # y="AMOUNT")


# Close the cursor and connection
cur.close()
conn.close()


