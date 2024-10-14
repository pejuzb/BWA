import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

# Sample DataFrame
data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'David'],
    'Age': [25, 30, 35, 40],
    'Salary': [50000, 60000, 70000, 80000],
    'Department': ['HR', 'Engineering', 'Marketing', 'Finance']
}

df = pd.DataFrame(data)

# Set up the Streamlit app
st.title("Streamlit Ag-Grid Integration Example")
st.write("This is an interactive Ag-Grid table where you can edit, sort, and filter data.")

# Build grid options (editable, sortable, filterable)
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_default_column(editable=True, sortable=True, filterable=True)
gridOptions = gb.build()

# Display the grid
grid_response = AgGrid(
    df, 
    gridOptions=gridOptions, 
    editable=True, 
    height=300, 
    theme='blue'  # Available themes: 'light', 'dark', 'blue', 'material'
)

# Display updated data after edits
st.write("Updated DataFrame (after any edits):")
st.dataframe(grid_response['data'])

# Option to download the edited data as CSV
csv = grid_response['data'].to_csv(index=False).encode('utf-8')
st.download_button(
    label="Download Updated Data as CSV",
    data=csv,
    file_name='updated_data.csv',
    mime='text/csv',
)
