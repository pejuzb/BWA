from admin.utils import *
import plotly.express as px

azk = AzureKeyVaultClient()
snf = SnowflakeClient(kv_client=azk)

# ============================================================
# 1. KPI METRICS DASHBOARD
# ============================================================
st.title("📊 Budget Analytics Dashboard")

kpi_data = snf.run_query_df("""
    SELECT 
        ABS(SUM(CASE WHEN L1 <> 'Income' THEN AMOUNT ELSE 0 END)) as total_expenses,
        COUNT(CASE WHEN L1 IS NULL THEN 1 END) as unclassified_count
    FROM BUDGET.MART.BUDGET
    WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) 
      AND OWNER = 'Peter'
""")

# Get category stats
category_stats = snf.run_query_df("""
    SELECT 
        L1,
        ABS(SUM(AMOUNT)) as category_total
    FROM BUDGET.MART.BUDGET
    WHERE L1 <> 'Income' 
      AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
      AND OWNER = 'Peter'
    GROUP BY L1
    ORDER BY category_total DESC
    LIMIT 1
""")

if not kpi_data.empty:
    col1, col2, col3, col4 = st.columns(4)
    
    total_exp = kpi_data['TOTAL_EXPENSES'].iloc[0]
    unclass_count = int(kpi_data['UNCLASSIFIED_COUNT'].iloc[0])
    
    with col1:
        st.metric("💰 Total Expenses YTD", f"${total_exp:,.0f}")
    with col2:
        # Calculate months with data
        months_with_data = snf.run_query_df("""
            SELECT COUNT(DISTINCT REPORTING_DATE) as months
            FROM BUDGET.MART.BUDGET
            WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE()) AND OWNER = 'Peter'
        """)
        months = int(months_with_data['MONTHS'].iloc[0]) or 1
        avg_monthly = total_exp / months
        st.metric("📅 Avg Monthly", f"${avg_monthly:,.0f}")
    with col3:
        if not category_stats.empty:
            top_cat = category_stats['L1'].iloc[0]
            st.metric("🏆 Top Category", top_cat)
        else:
            st.metric("🏆 Top Category", "N/A")
    with col4:
        st.metric("⚠️ Unclassified", f"{unclass_count} txns")

st.divider()

# ============================================================
# 2. EXISTING CHARTS
# ============================================================
with st.container(border=True):
    st.write("Chart of Monthly Expenses (No Income Included)") 
    data_chart = snf.run_query_df("""Select 
        REPORTING_DATE,
        L1,
        ABS(SUM(AMOUNT)) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Income' and year(transaction_date) = year(current_date()) and OWNER = 'Peter' 
        GROUP BY ALL;""")
    
    
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

    data_chart_2 = snf.run_query_df("""Select 
        REPORTING_DATE,
        SUM(AMOUNT) as AMOUNT FROM BUDGET.MART.BUDGET
        WHERE year(transaction_date) = year(current_date()) and OWNER = 'Peter' 
        GROUP BY ALL;""")
    
    
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

# ============================================================
# 3. HIERARCHICAL TREEMAP (L1 → L2 → L3 Breakdown)
# ============================================================
with st.container(border=True):
    st.write("🗂️ Hierarchical Spending Breakdown (Treemap)")
    
    treemap_data = snf.run_query_df("""
        SELECT 
            COALESCE(L1, 'Unclassified') as L1,
            COALESCE(L2, 'No L2') as L2,
            COALESCE(L3, 'No L3') as L3,
            ABS(SUM(AMOUNT)) as AMOUNT
        FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Income' 
          AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
          AND OWNER = 'Peter'
        GROUP BY L1, L2, L3
        HAVING SUM(AMOUNT) < 0
    """)
    
    if not treemap_data.empty:
        fig = px.treemap(
            treemap_data,
            path=['L1', 'L2', 'L3'],
            values='AMOUNT',
            color='AMOUNT',
            color_continuous_scale='RdYlGn_r',
            title='Spending Distribution by Category Hierarchy'
        )
        fig.update_layout(height=500)
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# 4. CATEGORY TREND LINES
# ============================================================
with st.container(border=True):
    st.write("📈 Category Trends Over Time")
    
    trend_data = snf.run_query_df("""
        SELECT 
            REPORTING_DATE,
            L1,
            ABS(SUM(AMOUNT)) as AMOUNT
        FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Income' 
          AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
          AND OWNER = 'Peter'
        GROUP BY REPORTING_DATE, L1
        ORDER BY REPORTING_DATE
    """)
    
    if not trend_data.empty:
        chart_trend = alt.Chart(trend_data).mark_line(point=True).encode(
            x=alt.X('REPORTING_DATE:T', title='Month'),
            y=alt.Y('AMOUNT:Q', title='Amount ($)'),
            color=alt.Color('L1:N', title='Category'),
            tooltip=['REPORTING_DATE:T', 'L1:N', 'AMOUNT:Q']
        ).properties(
            height=400
        ).configure_axis(
            labelFontSize=14,
            titleFontSize=16
        ).configure_legend(
            titleFontSize=16,
            labelFontSize=14
        )
        
        st.altair_chart(chart_trend, use_container_width=True)

# ============================================================
# 5. SOURCE SYSTEM SPLIT (REV vs CSOB)
# ============================================================
with st.container(border=True):
    st.write("🏦 Spending by Bank Source")
    
    source_data = snf.run_query_df("""
        SELECT 
            SOURCE_SYSTEM,
            ABS(SUM(AMOUNT)) as AMOUNT
        FROM BUDGET.MART.BUDGET
        WHERE L1 <> 'Income'
          AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
          AND OWNER = 'Peter'
        GROUP BY SOURCE_SYSTEM
    """)
    
    if not source_data.empty:
        col_left, col_right = st.columns([1, 1])
        
        with col_left:
            fig_pie = px.pie(
                source_data,
                values='AMOUNT',
                names='SOURCE_SYSTEM',
                title='Bank Distribution',
                hole=0.4
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col_right:
            st.dataframe(
                source_data,
                column_config={
                    "SOURCE_SYSTEM": "Bank",
                    "AMOUNT": st.column_config.NumberColumn("Total", format="$%.2f")
                },
                hide_index=True,
                use_container_width=True
            )

# ============================================================
# 6. MONTH-OVER-MONTH COMPARISON
# ============================================================
with st.container(border=True):
    st.write("📊 Month-over-Month Change")
    
    mom_data = snf.run_query_df("""
        WITH monthly AS (
            SELECT 
                REPORTING_DATE,
                ABS(SUM(CASE WHEN L1 <> 'Income' THEN AMOUNT ELSE 0 END)) as expenses
            FROM BUDGET.MART.BUDGET
            WHERE YEAR(transaction_date) = YEAR(CURRENT_DATE())
              AND OWNER = 'Peter'
            GROUP BY REPORTING_DATE
            ORDER BY REPORTING_DATE
        )
        SELECT 
            REPORTING_DATE,
            expenses as current_month,
            LAG(expenses, 1) OVER (ORDER BY REPORTING_DATE) as previous_month,
            expenses - LAG(expenses, 1) OVER (ORDER BY REPORTING_DATE) as change_amount,
            ROUND(((expenses - LAG(expenses, 1) OVER (ORDER BY REPORTING_DATE)) / 
                   NULLIF(LAG(expenses, 1) OVER (ORDER BY REPORTING_DATE), 0)) * 100, 2) as change_pct
        FROM monthly
    """)
    
    if not mom_data.empty:
        st.dataframe(
            mom_data,
            column_config={
                "REPORTING_DATE": st.column_config.DateColumn("Month", format="MMM YYYY"),
                "CURRENT_MONTH": st.column_config.NumberColumn("Current", format="$%.2f"),
                "PREVIOUS_MONTH": st.column_config.NumberColumn("Previous", format="$%.2f"),
                "CHANGE_AMOUNT": st.column_config.NumberColumn("$ Change", format="$%.2f"),
                "CHANGE_PCT": st.column_config.NumberColumn("% Change", format="%.2f%%")
            },
            hide_index=True,
            use_container_width=True
        )

# ============================================================
# 7. TRANSACTION DRILLDOWN TABLE
# ============================================================
with st.container(border=True):
    st.write("🔍 Transaction Drilldown")
    
    # Filter controls
    col_filter1, col_filter2 = st.columns(2)
    
    with col_filter1:
        categories = snf.run_query_df("""
            SELECT DISTINCT L1 
            FROM BUDGET.MART.BUDGET 
            WHERE OWNER = 'Peter' AND L1 IS NOT NULL
            ORDER BY L1
        """)
        selected_category = st.selectbox(
            "Filter by Category",
            options=["All"] + categories['L1'].tolist()
        )
    
    with col_filter2:
        selected_month = st.selectbox(
            "Filter by Month",
            options=["All", "Current Month", "Last 3 Months"]
        )
    
    # Build query dynamically
    category_filter = f"AND L1 = '{selected_category}'" if selected_category != "All" else ""
    
    if selected_month == "Current Month":
        date_filter = "AND MONTH(transaction_date) = MONTH(CURRENT_DATE())"
    elif selected_month == "Last 3 Months":
        date_filter = "AND transaction_date >= DATEADD(month, -3, CURRENT_DATE())"
    else:
        date_filter = ""
    
    transactions = snf.run_query_df(f"""
        SELECT 
            TRANSACTION_DATE,
            DESCRIPTION,
            L1,
            L2,
            L3,
            AMOUNT,
            SOURCE_SYSTEM
        FROM BUDGET.MART.BUDGET
        WHERE OWNER = 'Peter'
          AND YEAR(transaction_date) = YEAR(CURRENT_DATE())
          {category_filter}
          {date_filter}
        ORDER BY TRANSACTION_DATE DESC
        LIMIT 100
    """)
    
    if not transactions.empty:
        st.dataframe(
            transactions,
            column_config={
                "TRANSACTION_DATE": st.column_config.DateColumn("Date", format="DD/MM/YYYY"),
                "DESCRIPTION": "Description",
                "L1": "Category",
                "L2": "Subcategory",
                "L3": "Detail",
                "AMOUNT": st.column_config.NumberColumn("Amount", format="$%.2f"),
                "SOURCE_SYSTEM": "Bank"
            },
            hide_index=True,
            use_container_width=True,
            height=400
        )

