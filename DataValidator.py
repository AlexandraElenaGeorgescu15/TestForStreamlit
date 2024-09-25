import streamlit as st
import pandas as pd
import snowflake.connector
from st_aggrid import AgGrid, GridOptionsBuilder
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

# Function to fetch data from Snowflake
def fetch_data():
    conn = create_snowflake_connection()
    query = "SELECT * FROM UTIL_DB.PUBLIC.DATA_VALIDATION"
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure ACCEPT_REJECT column has default values for dropdown
    if 'ACCEPT_REJECT' in df.columns:
        df['ACCEPT_REJECT'] = df['ACCEPT_REJECT'].fillna("Pending")  # Default value for dropdown
    return df

# Function to update the Snowflake table
def update_table(dataframe):
    conn = create_snowflake_connection()
    cursor = conn.cursor()
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for index, row in dataframe.iterrows():
        query = f"""
        UPDATE UTIL_DB.PUBLIC.DATA_VALIDATION
        SET ACCEPT_REJECT = '{row['ACCEPT_REJECT']}',
            USER_COMMENT = '{row['USER_COMMENT']}',
            COMMENT_TIMESTAMP = '{current_timestamp}'
        WHERE ID = {row['ID']}
        """
        cursor.execute(query)

    conn.commit()
    cursor.close()
    conn.close()

# Fetch data from Snowflake
df = fetch_data()

# Hide the 'PRODUCT' column from the displayed table
df_display = df.drop(columns=['PRODUCT'])

# Dropdown options for ACCEPT_REJECT column
options = ['Pending', 'Approved', 'Rejected']

# Configure the table with AgGrid
gb = GridOptionsBuilder.from_dataframe(df_display)

# Add blue background color for ACCEPT_REJECT and USER_COMMENT columns
blue_style = {'background-color': 'lightblue'}

# Configure column 'ACCEPT_REJECT' with blue style
gb.configure_column(
    'ACCEPT_REJECT',
    editable=True,
    cellEditor='agSelectCellEditor',
    cellEditorParams={'values': options},  # Specify dropdown options
    cellStyle=blue_style  # Add blue color to the column
)

# Configure column 'USER_COMMENT' with blue style
gb.configure_column('USER_COMMENT', editable=True, cellStyle=blue_style)  # Allow editing for USER_COMMENT and set style

# Build the grid options
grid_options = gb.build()

# Display the table with dropdown in ACCEPT_REJECT and editable USER_COMMENT column
st.header('Editable Table with Dropdown for ACCEPT_REJECT and Editable USER_COMMENT (Blue Styled)')
grid_response = AgGrid(
    df_display,
    gridOptions=grid_options,
    update_mode='MODEL_CHANGED',
    editable=True
)

# Get the updated dataframe after user interaction
updated_df = grid_response['data']

# Submit button to save changes back to Snowflake
if st.button('Submit Changes'):
    with st.spinner("Updating Snowflake table..."):
        # Merge the PRODUCT column back into the updated dataframe before updating the database
        updated_df_with_product = pd.merge(updated_df, df[['ID', 'PRODUCT']], on='ID')
        update_table(updated_df_with_product)  # Save the updated data to Snowflake
        st.success("Table updated successfully!")
        st.dataframe(fetch_data().drop(columns=['PRODUCT']), use_container_width=True)  # Refresh the table display, hiding PRODUCT column
