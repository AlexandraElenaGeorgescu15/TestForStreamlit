import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime

# Page layout
st.set_page_config(layout="wide")

# Snowflake connection details
def create_snowflake_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"]
    )

def test_connection():
    conn = create_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_USER()")
    result = cursor.fetchone()
    st.write(f"Connected as: {result[0]}")
    conn.close()

# Title and introduction message
st.title("Data Validator")
st.caption("Validate and manage the matched data using the DATA_VALIDATION view")

# Function to fetch data from Snowflake
def fetch_data():
    conn = create_snowflake_connection()
    query = "SELECT * FROM DATA_VALIDATION"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure boolean columns are correctly typed and fill null values
    if 'ACCEPT_REJECT' in df.columns:
        df['ACCEPT_REJECT'] = df['ACCEPT_REJECT'].fillna(False).astype(bool)
    return df

# Function to update the 'ACCEPT_REJECT' status in Snowflake table
def update_accepted_status(dataframe):
    conn = create_snowflake_connection()
    cursor = conn.cursor()

    for index, row in dataframe.iterrows():
        # Check for NaN values in the ID and ACCEPT_REJECT columns
        if pd.notna(row['ID']) and pd.notna(row['ACCEPT_REJECT']):
            query = f"""
            UPDATE DATA_VALIDATION
            SET ACCEPT_REJECT = {int(row['ACCEPT_REJECT'])}
            WHERE ID = {int(row['ID'])}
            """
            cursor.execute(query)
        else:
            st.warning(f"Row with ID {row['ID']} or ACCEPT_REJECT has NaN values and will be skipped.")
    
    conn.commit()
    cursor.close()
    conn.close()

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
        # Display ID and ACCEPT_REJECT checkbox in two columns
        col1, col2 = st.columns([1, 5])  # Adjust the column widths for better readability
        
        with col1:
            st.write(f"ID: {row['ID']}")
        
        with col2:
            # Editable ACCEPT_REJECT checkbox
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
