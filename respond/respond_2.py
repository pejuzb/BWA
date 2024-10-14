import streamlit as st
import pandas as pd
from mitosheet import MitoSheet
from streamlit.components.v1 import iframe

# Display Streamlit app title
st.title("Mito Pivot Table in Streamlit")

# Load a sample DataFrame (You can replace this with your actual DataFrame)
data = {'Category': ['A', 'B', 'A', 'B', 'A', 'B'],
        'Values': [100, 200, 150, 220, 120, 180],
        'Sales': [400, 300, 350, 320, 330, 310]}

df = pd.DataFrame(data)

# Create a MitoSheet instance
mitosheet = MitoSheet()

# Load your dataframe into the MitoSheet
mitosheet.df = df

# Display the Mito pivot table
mitosheet.launch()

# Export the Mito iframe to display in Streamlit
iframe("mitosheet", width=700, height=500)
