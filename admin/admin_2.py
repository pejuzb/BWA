from admin.utils import *

conn = snowflake.connector.connect(
    user=secrets_get('svc-snf-user'),
    private_key=pem_to_snowflake_der(normalize_pem(secrets_get('svc-snf-rsa-key'))),          
    account=secrets_get('svc-snf-acc'),
    warehouse="COMPUTE_WH",
    database="BUDGET",
    schema="RAW",
    role="PUBLIC",
)

cur = conn.cursor()

# Query to fetch data from Snowflake
query = "Select * from BUDGET.MART.BUDGET where owner = 'Peter'"

# Load data into Pandas DataFrame
data = pd.read_sql(query, conn)

# Display the DataFrame using Streamlit
st.title('Peters Budget Data Viewer')
#AgGrid(data, height=400)


with st.container(border=True):
    st.write("Chart of Monthly Expenses (No Income Included)") 
    data_chart = pd.read_sql("""Select 
        REPORTING_DATE,
        L1,
        ABS(SUM(AMOUNT)) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Income' and transaction_date >= date_trunc(month,dateadd(month,-12,current_date)) and OWNER = 'Peter' 
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
    st.write("Chart of Monthly P&L")  

    data_chart_2 = pd.read_sql("""Select 
        REPORTING_DATE,
        SUM(AMOUNT) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE transaction_date >= date_trunc(month,dateadd(month,-12,current_date)) and OWNER = 'Peter' 
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
#conn.close()


