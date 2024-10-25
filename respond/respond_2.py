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
import altair as alt


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
query = "Select * from BUDGET.MART.BUDGET where owner = 'Jan' and TRANSACTION_HK <> '0d5b5bccddb88ab98eac67945c00c1f1'"

# Load data into Pandas DataFrame
data = pd.read_sql(query, conn)

# Display the DataFrame using Streamlit
st.title('Jans Budget Data Viewer')
#AgGrid(data, height=400)

with st.container(border=True):
    st.write("Chart of Monthly Expenses (No Income Included)") 
    data_chart = pd.read_sql("""Select 
        REPORTING_DATE,
        L1,
        ABS(SUM(AMOUNT)) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Prijem' and YEAR = YEAR(current_date) and OWNER = 'Jan'
        and TRANSACTION_HK <> '0d5b5bccddb88ab98eac67945c00c1f1'
        GROUP BY ALL;""", conn)
    
    
    # Create an Altair bar chart
    chart = alt.Chart(data_chart).mark_bar(size=25).encode(
        x='REPORTING_DATE:T',
        y='AMOUNT:Q',
        color='L1:N'
    ).properties(
        width=600,  # Set the width of the chart
        height=400  # Set the height of the chart
    ).configure_axis(
        labelFontSize=14,  # Adjust axis label size
        titleFontSize=16,  # Adjust axis title size
    ).configure_legend(
        titleFontSize=16,  # Adjust legend title size
        labelFontSize=14   # Adjust legend label size
    )
    
    # Display the chart in Streamlit
    st.altair_chart(chart, use_container_width=True)



with st.container(border=True):
    st.write("Chart of Monthly Income Sources")  
    data_chart_incom = pd.read_sql("""Select SUM(amount) as INCOME,
                             L2 as TYPE_OF_INCOME, 
                             REPORTING_DATE from BUDGET.MART.BUDGET 
                             where owner = 'Jan' and L1 = 'Prijem'
                             group by all;""", conn)
    
    
    # Create an Altair bar chart
    chart = alt.Chart(data_chart_incom).mark_bar(size=25).encode(
        x='REPORTING_DATE:T',
        y='INCOME:Q',
        color='TYPE_OF_INCOME:N'
    ).properties(
        width=600,  # Set the width of the chart
        height=400  # Set the height of the chart
    ).configure_axis(
        labelFontSize=14,  # Adjust axis label size
        titleFontSize=16,  # Adjust axis title size
    ).configure_legend(
        titleFontSize=16,  # Adjust legend title size
        labelFontSize=14   # Adjust legend label size
    )
    
    # Display the chart in Streamlit
    st.altair_chart(chart, use_container_width=True)

with st.container(border=True):
    st.write("Chart of Monthly P&L")  

    data_chart_2 = pd.read_sql("""Select 
        REPORTING_DATE,
        SUM(AMOUNT) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE YEAR = YEAR(current_date) and OWNER = 'Jan' 
        AND TRANSACTION_HK <> '0d5b5bccddb88ab98eac67945c00c1f1'
        GROUP BY ALL;""", conn)
    
    
    # Add a color column based on the AMOUNT value
    data_chart_2['color'] = data_chart_2['AMOUNT'].apply(lambda x: 'green' if x > 0 else 'red')
    
    # Create an Altair bar chart
    chart_2 = alt.Chart(data_chart_2).mark_bar(size = 25).encode(
        x='REPORTING_DATE:T',
        y='AMOUNT:Q',
        color=alt.condition(
            alt.datum.AMOUNT > 0,  # Condition for positive values
            alt.value('green'),     # Color if condition is true
            alt.value('red')        # Color if condition is false
        )
    ).properties(
        width=600,
        height=400
    
    ).configure_axis(
        labelFontSize=14,  # Adjust axis label size
        titleFontSize=16,  # Adjust axis title size
    ).configure_legend(
        titleFontSize=16,  # Adjust legend title size
        labelFontSize=14   # Adjust legend label size
    )

    # Display the chart in Streamlit
    st.altair_chart(chart_2, use_container_width=True)

with st.container(border=True):
    st.write("Chart of Monthly Expense Development")  
    st.line_chart(data_chart, x="REPORTING_DATE", y="AMOUNT", color="L1")

#st.write("Here is the raw data from Snowflake:")
st.dataframe(data)


# Close the cursor and connection
cur.close()
conn.close()


