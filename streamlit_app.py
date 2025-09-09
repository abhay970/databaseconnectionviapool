import streamlit as st
import pandas as pd
import json
# from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.exceptions import SnowparkSQLException
cnx = st.connection("snowflake")
session = cnx.session()

# Page configuration
st.set_page_config(
    page_title="Database Connector",
    page_icon="🗄️",
    layout="wide"
)

# Database configuration templates
DATABASE_CONFIGS = {
    "JDE": {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "url_format": "jdbc:oracle:thin:@//{}",
        "url_placeholder": "host:port/service_name (e.g., 10.25.3.5:1521/e920pdb.jdevcn03.jdevcn.oraclevcn.com)",
        "username_placeholder": "JDE Username (e.g., NLBTEST)",
        "password_placeholder": "JDE Password",
        "pool_placeholder": "jde-dev",
        "sample_tables": ["TESTDTA.F574211", "TESTDTA.F0101", "TESTDTA.F4211", "TESTDTA.F0411", "TESTDTA.F03B11"],
        "sample_query": "SELECT * FROM TESTDTA.FV574211 WHERE ROWNUM <= 5",
        "has_token": False
    },
    "SAP": {
        "driver": "com.sap.db.jdbc.Driver",
        "url_format": "jdbc:sap://{}",
        "url_placeholder": "host:port (e.g., sap-server:30015)",
        "username_placeholder": "SAP Username",
        "password_placeholder": "SAP Password",
        "pool_placeholder": "sap-dev",
        "sample_tables": ["MARA", "VBAK", "VBAP", "KNA1", "BKPF"],
        "sample_query": "SELECT * FROM products",
        "has_token": False
    },
    "Salesforce": {
        "driver": "cdata.jdbc.salesforce.SalesforceDriver",
        "url_format": "jdbc:salesforce:AuthScheme=Basic;User={};Password={};SecurityToken={}",
        "url_placeholder": "Not applicable for Salesforce",
        "username_placeholder": "Salesforce Username (e.g., pavan.s@techkasetti.com)",
        "password_placeholder": "Salesforce Password",
        "token_placeholder": "Salesforce Security Token (e.g., D6NG9atUbTtImJgYs75SWieU)",
        "pool_placeholder": "salesforce-dev",
        "sample_tables": ["Account", "Contact", "Orders", "Lead"],
        "sample_query": "SELECT Id, Name FROM Account LIMIT 5",
        "has_token": True
    },
}

# Initialize session
# @st.cache_resource
# def get_snowflake_session():
#     """Get active Snowflake session"""
#     try:
#         # return get_active_session()
#     except Exception as e:
#         st.error(f"Failed to get Snowflake session: {str(e)}")
#         return None

# Main application
def main():
    st.title("🗄️ Database Connector")
    st.markdown("Connect to various databases through Snowflake")
    
    # Get Snowflake session
    # session = get_snowflake_session()
    # if not session:
    #     st.error("Cannot establish Snowflake session")
    #     return
    
    # Create tabs for different functionalities
    tab1, tab2 = st.tabs(["📝 Connect Database", "🔍 Query Database"])
    
    with tab1:
        st.header("Database Connection Setup")
        
        # Database type selection OUTSIDE the form so it updates immediately
        st.subheader("Database Selection")
        database_type = st.selectbox(
            "Select Database Type",
            options=list(DATABASE_CONFIGS.keys()),
            index=0,
            help="Choose the type of database you want to connect to"
        )
        
        # Get configuration for selected database
        config = DATABASE_CONFIGS[database_type]
        
        # Connection form - now with dynamic content based on selection
        with st.form("database_connection_form"):            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(f"{database_type} Connection Details")
                
                # Pool name (left column)
                pool_name = st.text_input(
                    "Pool Name",
                    placeholder=config["pool_placeholder"],
                    help="Unique identifier for this database connection pool"
                )
                
                # Username (left column)
                username = st.text_input(
                    f"{database_type} Username",
                    placeholder=config["username_placeholder"]
                )
            
            with col2:
                st.subheader(f"{database_type} Credentials")
                
                # Password (right column)
                password = st.text_input(
                    f"{database_type} Password",
                    placeholder=config["password_placeholder"],
                    type="password"
                )
                
                # Handle different input for different databases
                if database_type == "Salesforce":
                    # Security Token (right column for Salesforce)
                    security_token = st.text_input(
                        f"{database_type} Security Token",
                        placeholder=config["token_placeholder"],
                        type="password",
                        help="Salesforce Security Token from your profile settings"
                    )
                    url_input = None
                else:
                    # Host/URL input (right column for other databases)
                    url_input = st.text_input(
                        "Host:Port/Service",
                        placeholder=config["url_placeholder"],
                        help=f"Format: {config['url_placeholder']}"
                    )
                    security_token = None
            
            # Submit button
            submitted = st.form_submit_button(f"🔗 Connect to {database_type}", use_container_width=True, type="primary")
            
            if submitted:
                # Validate required fields based on database type
                if database_type == "Salesforce":
                    if not all([pool_name, username, password, security_token]):
                        st.error("Please fill in all required fields: Pool Name, Username, Password, and Security Token")
                    else:
                        connect_database(session, database_type, pool_name, None, username, password, security_token)
                else:
                    if not all([url_input, pool_name, username, password]):
                        st.error("Please fill in all required fields")
                    else:
                        connect_database(session, database_type, pool_name, url_input, username, password)
    
    # with tab2:
    #     st.header("Query Database")
        
    #     # Database selection for querying
    #     if 'db_connections' in st.session_state and st.session_state.db_connections:
    #         available_pools = list(st.session_state.db_connections.keys())
    #         selected_pool = st.selectbox(
    #             "Select Connected Database Pool",
    #             options=available_pools,
    #             help="Choose from your connected database pools"
    #         )
            
    #         # Get database type for selected pool
    #         if selected_pool:
    #             db_type = st.session_state.db_connections[selected_pool]['database_type']
    #             config = DATABASE_CONFIGS[db_type]
                
    #             # Quick query templates
    #             st.subheader(f"📋 {db_type} Quick Queries")
    #             col1, col2, col3 = st.columns(3)
                
    #             with col1:
    #                 if st.button("📊 Test Connection", use_container_width=True):
    #                     if db_type == "JDE":
    #                         st.session_state.query_text = "SELECT 1 FROM DUAL"
    #                     elif db_type == "SAP":
    #                         st.session_state.query_text = "SELECT 1 FROM DUMMY"
    #                     elif db_type == "Salesforce":
    #                         st.session_state.query_text = "SELECT Id FROM Account LIMIT 1"
    #                     else:
    #                         st.session_state.query_text = "SELECT 1"
    #                     st.session_state.selected_pool_query = selected_pool
                
    #             with col2:
    #                 if st.button(f"📋 Sample {config['sample_tables'][0]}", use_container_width=True):
    #                     st.session_state.query_text = config["sample_query"]
    #                     st.session_state.selected_pool_query = selected_pool
                
    #             with col3:
    #                 if st.button("🗂️ Record Count", use_container_width=True):
    #                     main_table = config['sample_tables'][0]
    #                     if db_type == "Salesforce":
    #                         st.session_state.query_text = f"SELECT COUNT() FROM {main_table}"
    #                     else:
    #                         st.session_state.query_text = f"SELECT COUNT(*) as RECORD_COUNT FROM {main_table}"
    #                     st.session_state.selected_pool_query = selected_pool
                
    #             # Main query form
    #             with st.form("database_query_form"):
    #                 query_text = st.text_area(
    #                     f"{db_type} SQL Query",
    #                     value=st.session_state.get("query_text", config["sample_query"]),
    #                     height=150,
    #                     help=f"Enter your {db_type} SQL query here"
    #                 )
                    
    #                 # Common tables reference
    #                 st.markdown(f"**📚 Common {db_type} Tables:**")
    #                 tables_display = " | ".join([f"`{table}`" for table in config["sample_tables"]])
    #                 st.markdown(tables_display)
                    
    #                 query_submitted = st.form_submit_button(f"🚀 Execute {db_type} Query", use_container_width=True, type="primary")
                    
    #                 if query_submitted:
    #                     if not query_text:
    #                         st.error("Please provide a query")
    #                     else:
    #                         execute_database_query(session, selected_pool, query_text, db_type)
    #     else:
    #         st.info("No database connections available. Please connect to a database first.")
    with tab2:
        st.header("Query Database")
        
        # Database selection for querying
        if 'db_connections' in st.session_state and st.session_state.db_connections:
            available_pools = list(st.session_state.db_connections.keys())
            selected_pool = st.selectbox(
                "Select Connected Database Pool",
                options=available_pools,
                help="Choose from your connected database pools",
                key="pool_selector"  # Add key for tracking changes
            )
            
            # Get database type for selected pool
            if selected_pool:
                db_type = st.session_state.db_connections[selected_pool]['database_type']
                config = DATABASE_CONFIGS[db_type]
                
                # AUTO-UPDATE: Check if pool selection changed and update query automatically
                if 'last_selected_pool' not in st.session_state:
                    st.session_state.last_selected_pool = selected_pool
                    st.session_state.query_text = config["sample_query"]  # Set initial sample query
                elif st.session_state.last_selected_pool != selected_pool:
                    # Pool changed - update to new database's sample query
                    st.session_state.last_selected_pool = selected_pool
                    st.session_state.query_text = config["sample_query"]
                    # Clear any previous selected pool query
                    if 'selected_pool_query' in st.session_state:
                        del st.session_state.selected_pool_query
                
                # Quick query templates
                st.subheader(f"📋 {db_type} Quick Queries")
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    if st.button("📊 Test Connection", use_container_width=True):
                        if db_type == "JDE":
                            st.session_state.query_text = "SELECT 1 FROM DUAL"
                        elif db_type == "SAP":
                            st.session_state.query_text = "SELECT Name FROM Table_Name"
                        elif db_type == "Salesforce":
                            st.session_state.query_text = "SELECT Id FROM Account LIMIT 1"
                        else:
                            st.session_state.query_text = "SELECT 1"
                        st.session_state.selected_pool_query = selected_pool
                        st.rerun()  # Force refresh to show updated query
                
                with col2:
                    if st.button(f"📋 Sample {config['sample_tables'][0]}", use_container_width=True):
                        st.session_state.query_text = config["sample_query"]
                        st.session_state.selected_pool_query = selected_pool
                        st.rerun()  # Force refresh to show updated query
                
                with col3:
                    if st.button("🗂️ Record Count", use_container_width=True):
                        main_table = config['sample_tables'][0]
                        if db_type == "Salesforce":
                            st.session_state.query_text = f"SELECT COUNT() FROM {main_table}"
                        else:
                            st.session_state.query_text = f"SELECT COUNT(*) as RECORD_COUNT FROM {main_table}"
                        st.session_state.selected_pool_query = selected_pool
                        st.rerun()  # Force refresh to show updated query
                
                # Main query form
                with st.form("database_query_form"):
                    # Get the current query text, ensuring it matches the selected database type
                    current_query = st.session_state.get("query_text", config["sample_query"])
                    
                    query_text = st.text_area(
                        f"{db_type} SQL Query",
                        value=current_query,
                        height=150,
                        help=f"Enter your {db_type} SQL query here",
                        key=f"query_text_{selected_pool}"  # Unique key per pool
                    )
                    
                    # Common tables reference
                    st.markdown(f"**📚 Common {db_type} Tables:**")
                    tables_display = " | ".join([f"`{table}`" for table in config["sample_tables"]])
                    st.markdown(tables_display)
                    
                    query_submitted = st.form_submit_button(f"🚀 Execute {db_type} Query", use_container_width=True, type="primary")
                    
                    if query_submitted:
                        if not query_text:
                            st.error("Please provide a query")
                        else:
                            execute_database_query(session, selected_pool, query_text, db_type)

def connect_database(session, database_type, pool_name, url_input, username, password, security_token=None):
    """Connect to database using external function call"""
    try:
        with st.spinner(f"Connecting to {database_type} database..."):
            # Get configuration for the database type
            config = DATABASE_CONFIGS[database_type]
            
            # Construct full JDBC URL based on database type
            if database_type == "Salesforce":
                # For Salesforce, construct URL with username, password, and token
                full_jdbc_url = config["url_format"].format(username, password, security_token)
            else:
                # For other databases, use the URL input
                full_jdbc_url = config["url_format"].format(url_input)
            
            driver_name = config["driver"]
            
            # Call external function
            result = session.sql(f"""
                SELECT datasource_add(
                    '{pool_name}',
                    '{full_jdbc_url}',
                    '{username}',
                    '{password}',
                    '{driver_name}'
                )
            """).collect()
            
            if result:
                # Advanced celebratory success display
                st.balloons()  # Streamlit balloons animation
                
                # Database-specific success messages with advanced emojis
                success_messages = {
                    "Salesforce": "🌟✨ Salesforce CRM Connected! 🚀🎯 Ready to sync customer data! 📊💼",
                    "JDE": "⚡🏭 JDE Oracle Connected! 🔧⚙️ Enterprise system ready! 💪🌟",
                    "SAP": "🎯🏆 SAP System Connected! 🚀💎 Business intelligence activated! ⚡📈"
                }
                
                st.success(f"🎉 {success_messages.get(database_type, f'✅ {database_type} Database connected successfully!')} 🎉")
                
                # Advanced status display with multiple columns
                st.markdown("### 🔥 Connection Status Dashboard 🔥")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("🔌 Connection", "ACTIVE", delta="Connected")
                with col2:
                    st.metric("🏢 Database", database_type, delta="Ready")
                with col3:
                    st.metric("👤 User", username, delta="Authenticated")
                with col4:
                    st.metric("🎯 Pool", pool_name, delta="Configured")
                
                # Store connection info in session state
                if 'db_connections' not in st.session_state:
                    st.session_state.db_connections = {}
                
                connection_info = {
                    'username': username,
                    'database_type': database_type,
                    'status': 'connected',
                    'full_url': full_jdbc_url,
                    'driver': driver_name
                }
                
                # Add URL for non-Salesforce databases
                if database_type != "Salesforce":
                    connection_info['url'] = url_input
                else:
                    connection_info['has_token'] = True
                
                st.session_state.db_connections[pool_name] = connection_info
                
                # Advanced next steps with emojis
                st.markdown("### 🚀 What's Next?")
                next_steps_col1, next_steps_col2 = st.columns(2)
                
                with next_steps_col1:
                    st.info(f"🔍 **Ready to Query:** Switch to 'Query Database' tab to start exploring your {database_type} data!")
                
                with next_steps_col2:
                    st.success(f"📊 **Available Tables:** {len(config['sample_tables'])} sample tables ready for querying!")
            
    except SnowparkSQLException as e:
        st.error(f"Snowflake SQL Error: {str(e)}")
    except Exception as e:
        st.error(f"{database_type} Connection error: {str(e)}")

def execute_database_query(session, pool_name, query_text, database_type):
    """Execute query on database using external function call"""
    try:
        with st.spinner(f"Executing {database_type} query..."):
            # Escape single quotes in query
            escaped_query = query_text.replace("'", "''")
            
            # Call external function
            result = session.sql(f"""
                SELECT datasource_query(
                    '{pool_name}',
                    '{escaped_query}'
                )
            """).collect()
            
            if result:
                response_data = result[0][0]
                
                # Handle different response types
                try:
                    # Try to parse as JSON first
                    if isinstance(response_data, str):
                        response_json = json.loads(response_data)
                    else:
                        response_json = response_data
                    
                    # Check if response contains tabular data (list of objects)
                    if isinstance(response_json, list) and response_json:
                        # Convert to DataFrame for better display
                        df = pd.DataFrame(response_json)
                        st.success(f"✅ {database_type} Query executed successfully!")
                        
                        # Display metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Records", len(df))
                        with col2:
                            st.metric("Columns", len(df.columns))
                        with col3:
                            st.metric("Database", database_type)
                        
                        # Display data
                        st.dataframe(df, use_container_width=True)
                        
                    elif isinstance(response_json, dict):
                        st.success(f"✅ {database_type} Query executed successfully!")
                        st.json(response_json)
                    else:
                        st.success(f"✅ {database_type} Query executed successfully!")
                        st.write("**Response:**")
                        st.write(response_json)
                        
                except json.JSONDecodeError:
                    # If not JSON, handle as plain text
                    response_text = str(response_data)
                    
                    if any(word in response_text.lower() for word in ['error', 'fail', 'exception']):
                        st.error(f"❌ {database_type} Query failed: {response_text}")
                    else:
                        st.success(f"✅ {database_type} Query executed successfully!")
                        st.text_area("Query Result:", response_text, height=200)
                
                except Exception as parse_error:
                    st.success(f"✅ {database_type} Query executed successfully!")
                    st.text_area("Query Result:", str(response_data), height=200)
            
    except SnowparkSQLException as e:
        st.error(f"Snowflake SQL Error: {str(e)}")
    except Exception as e:
        st.error(f"{database_type} Query execution error: {str(e)}")

# Sidebar with database connection summary
def display_sidebar():
    st.sidebar.title("🗄️ Database Connections")
    
    if 'db_connections' in st.session_state and st.session_state.db_connections:
        for pool_name, conn_info in st.session_state.db_connections.items():
            with st.sidebar.expander(f"🔌 {pool_name}"):
                st.write(f"**Type:** {conn_info['database_type']}")
                if 'url' in conn_info:
                    st.write(f"**URL:** {conn_info['url']}")
                st.write(f"**User:** {conn_info['username']}")
                st.write(f"**Status:** {conn_info['status']}")
                if conn_info.get('has_token'):
                    st.write("**Token:** ✓ Configured")
    else:
        st.sidebar.info("No active database connections")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📝 Supported Databases")
    
    for db_type in DATABASE_CONFIGS.keys():
        st.sidebar.markdown(f"• **{db_type}**")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💡 Instructions")
    st.sidebar.markdown("""
    1. **Select Database:** Choose database type from dropdown
    2. **Fill Details:** Enter connection details
    3. **Connect:** Click connect button
    4. **Query:** Switch to Query tab to run SQL
    
    **Salesforce Note:** Requires Username, Password, Security Token, and Pool Name
    """)

if __name__ == "__main__":
    display_sidebar()
    main()
