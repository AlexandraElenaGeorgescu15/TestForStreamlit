import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime

# Page layout
st.set_page_config(layout="wide")

# Title and introduction message
st.title("Data Validator")
st.caption("Validate and manage the matched data using the DATA_VALIDATION view")

# Connect to Snowflake using the connector
def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],  # Use the correct keys from Streamlit secrets
        password=st.secrets["SNOWFLAKE_PASSWORD"],  # Use the correct keys from Streamlit secrets
        account=st.secrets["SNOWFLAKE_ACCOUNT"],  # Use the correct keys from Streamlit secrets
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],  # Use the correct keys from Streamlit secrets
        database=st.secrets["SNOWFLAKE_DATABASE"]  # Use the correct keys from Streamlit secrets
    )

# Initialize the connection
conn = init_connection()

# Title and introduction message
st.title("Data Validator")
st.caption("Validate and manage the matched data using the DATA_VALIDATION view")

# Function to fetch data from Snowflake
def fetch_data():
    query = "SELECT * FROM UTIL_DB.PUBLIC.DATA_VALIDATION"
    df = pd.read_sql(query, conn)
    if 'ACCEPT_REJECT' in df.columns:
        df['ACCEPT_REJECT'] = df['ACCEPT_REJECT'].fillna(False).astype(bool)
    return df

# Function to update the 'ACCEPTED' status in the Snowflake table
def update_accepted_status(dataframe):
    for index, row in dataframe.iterrows():
        if pd.notna(row['ID']) and pd.notna(row['ACCEPT_REJECT']):
            query = f"""
            UPDATE UTIL_DB.PUBLIC.DATA_VALIDATION
            SET ACCEPT_REJECT = {int(row['ACCEPT_REJECT'])}
            WHERE ID = {int(row['ID'])}
            """
            conn.cursor().execute(query)
        else:
            st.warning(f"Row with ID {row['ID']} or ACCEPT_REJECT has NaN values and will be skipped.")

# Fetch the data
df = fetch_data()

# Display read-only data in a clean table format
st.subheader("Data Overview")
st.table(df[['ID', 'BDID', 'PRODUCT', 'COMPONENTGROUP', 'TS_ITEM', 'BD_ITEM', 'MATCH_STATUS', 'USER_COMMENT', 'COMMENT_TIMESTAMP']])

# Editable table section for ACCEPT_REJECT column
st.subheader("Quick ACCEPT_REJECT Update")

with st.form("accept_reject_form"):
    updated_values = []
    
    for i, row in df.iterrows():
        col1, col2 = st.columns([1, 5])  # Adjust the column widths for better readability
        
        with col1:
            st.write(f"ID: {row['ID']}")
        
        with col2:
            accept_reject_val = st.checkbox(f"ACCEPT/REJECT for {row['ID']}", value=row['ACCEPT_REJECT'], key=f"accept_reject_{row['ID']}")
            updated_values.append({'ID': row['ID'], 'ACCEPT_REJECT': accept_reject_val})

    # Submit button to save changes
    submit_button = st.form_submit_button("Submit Changes")

    if submit_button:
        with st.spinner("Updating ACCEPT_REJECT values..."):
            for row in updated_values:
                update_accepted_status(pd.DataFrame([row]))
            st.success("ACCEPT_REJECT values updated successfully!")
        
        # Display the updated table
        st.dataframe(fetch_data(), use_container_width=True)
