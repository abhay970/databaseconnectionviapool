import streamlit as st
import pandas as pd
import json
import traceback
from datetime import datetime
import time

# Database-specific imports (install these packages)
try:
    import cx_Oracle
    ORACLE_AVAILABLE = True
except ImportError:
    ORACLE_AVAILABLE = False
    
try:
    from simple_salesforce import Salesforce
    SALESFORCE_AVAILABLE = True
except ImportError:
    SALESFORCE_AVAILABLE = False

try:
    import pyodbc
    PYODBC_AVAILABLE = True
except ImportError:
    PYODBC_AVAILABLE = False

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
        "url_format": "{}:1521/{}",
        "url_placeholder": "host:port/service_name (e.g., 10.25.3.5:1521/e920pdb)",
        "username_placeholder": "JDE Username (e.g., NLBTEST)",
        "password_placeholder": "JDE Password",
        "pool_placeholder": "jde-dev",
        "sample_tables": ["TESTDTA.F574211", "TESTDTA.F0101", "TESTDTA.F4211", "TESTDTA.F0411", "TESTDTA.F03B11"],
        "sample_query": "SELECT * FROM TESTDTA.F0101 WHERE ROWNUM <= 5",
        "has_token": False,
        "library_required": "cx_Oracle",
        "available": ORACLE_AVAILABLE
    },
    "SAP": {
        "driver": "com.sap.db.jdbc.Driver",
        "url_format": "{}:30015",
        "url_placeholder": "host:port (e.g., sap-server:30015)",
        "username_placeholder": "SAP Username",
        "password_placeholder": "SAP Password",
        "pool_placeholder": "sap-dev",
        "sample_tables": ["MARA", "VBAK", "VBAP", "KNA1", "BKPF"],
        "sample_query": "SELECT TOP 5 * FROM MARA",
        "has_token": False,
        "library_required": "pyodbc or hdbcli",
        "available": PYODBC_AVAILABLE
    },
    "Salesforce": {
        "driver": "salesforce_api",
        "url_format": "https://login.salesforce.com",
        "url_placeholder": "Not applicable for Salesforce",
        "username_placeholder": "Salesforce Username (e.g., user@company.com)",
        "password_placeholder": "Salesforce Password",
        "token_placeholder": "Salesforce Security Token",
        "pool_placeholder": "salesforce-dev",
        "sample_tables": ["Account", "Contact", "Opportunity", "Lead", "Case"],
        "sample_query": "SELECT Id, Name FROM Account LIMIT 5",
        "has_token": True,
        "library_required": "simple-salesforce",
        "available": SALESFORCE_AVAILABLE
    },
}

# Initialize Snowflake connection
@st.cache_resource
def init_snowflake_connection():
    """Initialize Snowflake connection for storing metadata"""
    try:
        cnx = st.connection("snowflake")
        session = cnx.session()
        return session
    except Exception as e:
        st.error(f"Snowflake connection failed: {str(e)}")
        return None

def create_metadata_tables(session):
    """Create tables to store connection metadata"""
    try:
        session.sql("""
            CREATE TABLE IF NOT EXISTS DB_CONNECTION_METADATA (
                pool_name STRING,
                database_type STRING,
                host STRING,
                username STRING,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                last_used TIMESTAMP,
                status STRING DEFAULT 'active',
                PRIMARY KEY (pool_name)
            )
        """).collect()
        
        session.sql("""
            CREATE TABLE IF NOT EXISTS DB_QUERY_HISTORY (
                id STRING DEFAULT UUID_STRING(),
                pool_name STRING,
                query_text STRING,
                execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                success BOOLEAN,
                error_message STRING,
                row_count INTEGER,
                PRIMARY KEY (id)
            )
        """).collect()
        return True
    except Exception as e:
        st.error(f"Failed to create metadata tables: {str(e)}")
        return False

class DatabaseConnector:
    def __init__(self):
        self.connections = {}
    
    def connect_oracle(self, host, port, service_name, username, password):
        """Connect to Oracle database (for JDE)"""
        try:
            # Parse host:port/service format
            if '/' in host:
                host_port, service = host.split('/', 1)
                if ':' in host_port:
                    host, port = host_port.split(':', 1)
                    port = int(port)
                else:
                    host = host_port
                    port = 1521
                service_name = service
            else:
                service_name = service_name or 'ORCL'
                port = port or 1521
            
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
            connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
            return connection
        except Exception as e:
            raise Exception(f"Oracle connection failed: {str(e)}")
    
    def connect_salesforce(self, username, password, security_token):
        """Connect to Salesforce"""
        try:
            # Combine password and security token
            password_with_token = password + security_token
            sf = Salesforce(username=username, password=password_with_token)
            return sf
        except Exception as e:
            raise Exception(f"Salesforce connection failed: {str(e)}")
    
    def connect_sap(self, host, port, username, password):
        """Connect to SAP HANA"""
        try:
            # This is a simplified example - you might need hdbcli for SAP HANA
            # or specific SAP connectors depending on your SAP system
            if ':' in host:
                host, port = host.split(':', 1)
            
            connection_string = f"DRIVER={{HDBODBC}};SERVERNODE={host}:{port};UID={username};PWD={password}"
            connection = pyodbc.connect(connection_string)
            return connection
        except Exception as e:
            raise Exception(f"SAP connection failed: {str(e)}")
    
    def test_connection(self, database_type, **kwargs):
        """Test database connection"""
        try:
            if database_type == "JDE":
                conn = self.connect_oracle(
                    kwargs['host'], 
                    kwargs.get('port', 1521), 
                    kwargs.get('service_name'), 
                    kwargs['username'], 
                    kwargs['password']
                )
                # Test with simple query
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM DUAL")
                cursor.fetchone()
                cursor.close()
                conn.close()
                return True, "Connection successful"
            
            elif database_type == "Salesforce":
                sf = self.connect_salesforce(
                    kwargs['username'], 
                    kwargs['password'], 
                    kwargs['security_token']
                )
                # Test with simple query
                sf.query("SELECT Id FROM Account LIMIT 1")
                return True, "Connection successful"
            
            elif database_type == "SAP":
                conn = self.connect_sap(
                    kwargs['host'], 
                    kwargs.get('port', 30015), 
                    kwargs['username'], 
                    kwargs['password']
                )
                cursor = conn.cursor()
                cursor.execute("SELECT 1 FROM SYS.DUMMY")
                cursor.fetchone()
                cursor.close()
                conn.close()
                return True, "Connection successful"
            
            return False, "Unsupported database type"
        
        except Exception as e:
            return False, str(e)
    
    def execute_query(self, database_type, query, **kwargs):
        """Execute query on database"""
        try:
            if database_type == "JDE":
                conn = self.connect_oracle(
                    kwargs['host'], 
                    kwargs.get('port', 1521), 
                    kwargs.get('service_name'), 
                    kwargs['username'], 
                    kwargs['password']
                )
                
                cursor = conn.cursor()
                cursor.execute(query)
                
                # Fetch results
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                cursor.close()
                conn.close()
                
                # Convert to DataFrame
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    return True, df, len(rows)
                else:
                    return True, pd.DataFrame(), 0
            
            elif database_type == "Salesforce":
                sf = self.connect_salesforce(
                    kwargs['username'], 
                    kwargs['password'], 
                    kwargs['security_token']
                )
                
                # Execute SOQL query
                result = sf.query(query)
                
                if result['records']:
                    # Convert to DataFrame
                    records = []
                    for record in result['records']:
                        # Remove Salesforce metadata
                        clean_record = {k: v for k, v in record.items() if not k.startswith('attributes')}
                        records.append(clean_record)
                    
                    df = pd.DataFrame(records)
                    return True, df, len(records)
                else:
                    return True, pd.DataFrame(), 0
            
            elif database_type == "SAP":
                conn = self.connect_sap(
                    kwargs['host'], 
                    kwargs.get('port', 30015), 
                    kwargs['username'], 
                    kwargs['password']
                )
                
                cursor = conn.cursor()
                cursor.execute(query)
                
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                
                cursor.close()
                conn.close()
                
                if rows:
                    df = pd.DataFrame(rows, columns=columns)
                    return True, df, len(rows)
                else:
                    return True, pd.DataFrame(), 0
            
            return False, "Unsupported database type", 0
        
        except Exception as e:
            return False, f"Query execution failed: {str(e)}", 0

# Initialize database connector
@st.cache_resource
def get_database_connector():
    return DatabaseConnector()

def main():
    st.title("🗄️ Universal Database Connector")
    st.markdown("Connect to JDE, SAP, Salesforce and execute queries seamlessly")
    
    # Initialize Snowflake session for metadata
    session = init_snowflake_connection()
    if session and create_metadata_tables(session):
        pass  # Tables created successfully
    
    # Initialize database connector
    db_connector = get_database_connector()
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📝 Connect Database", "🔍 Query Database", "📊 Connection Status"])
    
    with tab1:
        st.header("Database Connection Setup")
        
        # Check library availability
        st.subheader("📋 Library Status")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if ORACLE_AVAILABLE:
                st.success("✅ Oracle (JDE) - Ready")
            else:
                st.error("❌ Oracle (JDE) - Install: `pip install cx_Oracle`")
        
        with col2:
            if SALESFORCE_AVAILABLE:
                st.success("✅ Salesforce - Ready")
            else:
                st.error("❌ Salesforce - Install: `pip install simple-salesforce`")
        
        with col3:
            if PYODBC_AVAILABLE:
                st.success("✅ SAP - Ready")
            else:
                st.error("❌ SAP - Install: `pip install pyodbc`")
        
        st.markdown("---")
        
        # Database selection
        st.subheader("🎯 Database Selection")
        database_type = st.selectbox(
            "Select Database Type",
            options=list(DATABASE_CONFIGS.keys()),
            index=0,
            help="Choose the type of database you want to connect to"
        )
        
        config = DATABASE_CONFIGS[database_type]
        
        # Show availability status
        if not config["available"]:
            st.warning(f"⚠️ {database_type} connector not available. Install required library: `{config['library_required']}`")
        
        # Connection form
        with st.form("database_connection_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(f"{database_type} Connection Details")
                
                pool_name = st.text_input(
                    "Pool Name",
                    placeholder=config["pool_placeholder"],
                    help="Unique identifier for this database connection"
                )
                
                username = st.text_input(
                    f"{database_type} Username",
                    placeholder=config["username_placeholder"]
                )
            
            with col2:
                st.subheader(f"{database_type} Credentials")
                
                password = st.text_input(
                    f"{database_type} Password",
                    placeholder=config["password_placeholder"],
                    type="password"
                )
                
                if database_type == "Salesforce":
                    security_token = st.text_input(
                        "Security Token",
                        placeholder=config["token_placeholder"],
                        type="password",
                        help="Salesforce Security Token from profile settings"
                    )
                    host_input = None
                else:
                    host_input = st.text_input(
                        "Host:Port/Service",
                        placeholder=config["url_placeholder"],
                        help=f"Format: {config['url_placeholder']}"
                    )
                    security_token = None
            
            # Test and Connect buttons
            col_test, col_connect = st.columns(2)
            
            with col_test:
                test_submitted = st.form_submit_button(
                    f"🧪 Test {database_type} Connection", 
                    use_container_width=True
                )
            
            with col_connect:
                connect_submitted = st.form_submit_button(
                    f"🔗 Save {database_type} Connection", 
                    use_container_width=True, 
                    type="primary"
                )
            
            if test_submitted and config["available"]:
                test_database_connection(db_connector, database_type, host_input, username, password, security_token)
            
            if connect_submitted and config["available"]:
                if database_type == "Salesforce":
                    if not all([pool_name, username, password, security_token]):
                        st.error("Please fill in all required fields")
                    else:
                        save_database_connection(session, pool_name, database_type, None, username, password, security_token)
                else:
                    if not all([pool_name, host_input, username, password]):
                        st.error("Please fill in all required fields")
                    else:
                        save_database_connection(session, pool_name, database_type, host_input, username, password)

    with tab2:
        st.header("Query Database")
        query_database_tab(session, db_connector)
    
    with tab3:
        st.header("Connection Status")
        display_connection_status(session)

def test_database_connection(db_connector, database_type, host, username, password, security_token=None):
    """Test database connection"""
    with st.spinner(f"Testing {database_type} connection..."):
        try:
            kwargs = {'username': username, 'password': password}
            
            if database_type == "Salesforce":
                kwargs['security_token'] = security_token
            else:
                kwargs['host'] = host
            
            success, message = db_connector.test_connection(database_type, **kwargs)
            
            if success:
                st.success(f"✅ {database_type} connection test successful!")
                st.info(message)
            else:
                st.error(f"❌ {database_type} connection test failed!")
                st.error(message)
        
        except Exception as e:
            st.error(f"Connection test error: {str(e)}")

def save_database_connection(session, pool_name, database_type, host, username, password, security_token=None):
    """Save database connection metadata"""
    try:
        with st.spinner(f"Saving {database_type} connection..."):
            if session:
                # Store metadata (not actual credentials for security)
                session.sql(f"""
                    MERGE INTO DB_CONNECTION_METADATA AS target
                    USING (SELECT 
                        '{pool_name}' AS pool_name,
                        '{database_type}' AS database_type,
                        '{host or 'salesforce.com'}' AS host,
                        '{username}' AS username,
                        'active' AS status
                    ) AS source
                    ON target.pool_name = source.pool_name
                    WHEN MATCHED THEN 
                        UPDATE SET 
                            database_type = source.database_type,
                            host = source.host,
                            username = source.username,
                            status = source.status,
                            last_used = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN
                        INSERT (pool_name, database_type, host, username, status)
                        VALUES (source.pool_name, source.database_type, source.host, source.username, source.status)
                """).collect()
            
            # Store in session state for current session
            if 'db_connections' not in st.session_state:
                st.session_state.db_connections = {}
            
            connection_info = {
                'database_type': database_type,
                'host': host or 'salesforce.com',
                'username': username,
                'password': password,  # In production, use secure storage
                'status': 'active'
            }
            
            if security_token:
                connection_info['security_token'] = security_token
            
            st.session_state.db_connections[pool_name] = connection_info
            
            # Success message
            st.balloons()
            st.success(f"🎉 {database_type} connection '{pool_name}' saved successfully! 🎉")
            
            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("🔌 Status", "SAVED", delta="Active")
            with col2:
                st.metric("🏢 Database", database_type, delta="Ready")
            with col3:
                st.metric("👤 User", username, delta="Configured")
            with col4:
                st.metric("🎯 Pool", pool_name, delta="Available")
    
    except Exception as e:
        st.error(f"Failed to save connection: {str(e)}")

def query_database_tab(session, db_connector):
    """Query database tab content"""
    if 'db_connections' not in st.session_state or not st.session_state.db_connections:
        st.info("No database connections available. Please connect to a database first.")
        return
    
    # Pool selection
    available_pools = list(st.session_state.db_connections.keys())
    selected_pool = st.selectbox(
        "Select Database Pool",
        options=available_pools,
        help="Choose from your connected database pools"
    )
    
    if not selected_pool:
        return
    
    conn_info = st.session_state.db_connections[selected_pool]
    database_type = conn_info['database_type']
    config = DATABASE_CONFIGS[database_type]
    
    # Quick query buttons
    st.subheader(f"📋 {database_type} Quick Queries")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📊 Test Connection", use_container_width=True):
            if database_type == "JDE":
                query_text = "SELECT 1 FROM DUAL"
            elif database_type == "SAP":
                query_text = "SELECT 1 FROM SYS.DUMMY"
            elif database_type == "Salesforce":
                query_text = "SELECT Id FROM Account LIMIT 1"
            
            execute_database_query(session, db_connector, selected_pool, query_text)
    
    with col2:
        if st.button(f"📋 Sample Data", use_container_width=True):
            execute_database_query(session, db_connector, selected_pool, config["sample_query"])
    
    with col3:
        if st.button("🗂️ Record Count", use_container_width=True):
            main_table = config['sample_tables'][0]
            if database_type == "Salesforce":
                query_text = f"SELECT COUNT() FROM {main_table}"
            else:
                query_text = f"SELECT COUNT(*) as RECORD_COUNT FROM {main_table}"
            
            execute_database_query(session, db_connector, selected_pool, query_text)
    
    # Main query form
    with st.form("database_query_form"):
        query_text = st.text_area(
            f"{database_type} SQL Query",
            value=config["sample_query"],
            height=150,
            help=f"Enter your {database_type} SQL query here"
        )
        
        # Common tables reference
        st.markdown(f"**📚 Common {database_type} Tables:**")
        tables_display = " | ".join([f"`{table}`" for table in config["sample_tables"]])
        st.markdown(tables_display)
        
        query_submitted = st.form_submit_button(
            f"🚀 Execute {database_type} Query", 
            use_container_width=True, 
            type="primary"
        )
        
        if query_submitted:
            if not query_text:
                st.error("Please provide a query")
            else:
                execute_database_query(session, db_connector, selected_pool, query_text)

def execute_database_query(session, db_connector, pool_name, query_text):
    """Execute database query"""
    try:
        conn_info = st.session_state.db_connections[pool_name]
        database_type = conn_info['database_type']
        
        with st.spinner(f"Executing {database_type} query..."):
            # Prepare connection parameters
            kwargs = {
                'username': conn_info['username'],
                'password': conn_info['password']
            }
            
            if database_type == "Salesforce":
                kwargs['security_token'] = conn_info.get('security_token', '')
            else:
                kwargs['host'] = conn_info['host']
            
            # Execute query
            success, result, row_count = db_connector.execute_query(database_type, query_text, **kwargs)
            
            if success:
                st.success(f"✅ {database_type} Query executed successfully!")
                
                # Display metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Records", row_count)
                with col2:
                    st.metric("Database", database_type)
                with col3:
                    st.metric("Pool", pool_name)
                
                # Display results
                if isinstance(result, pd.DataFrame) and not result.empty:
                    st.dataframe(result, use_container_width=True)
                    
                    # Download button
                    csv = result.to_csv(index=False)
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=f"{database_type}_{pool_name}_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                else:
                    st.info("Query executed successfully but returned no data.")
                
                # Log query history
                if session:
                    try:
                        escaped_query = query_text.replace("'", "''")
                        session.sql(f"""
                            INSERT INTO DB_QUERY_HISTORY 
                            (pool_name, query_text, success, row_count)
                            VALUES ('{pool_name}', '{escaped_query}', TRUE, {row_count})
                        """).collect()
                    except Exception as log_error:
                        st.warning(f"Failed to log query: {str(log_error)}")
            
            else:
                st.error(f"❌ {database_type} Query failed!")
                st.error(result)  # result contains error message
                
                # Log failed query
                if session:
                    try:
                        escaped_query = query_text.replace("'", "''")
                        escaped_error = str(result).replace("'", "''")
                        session.sql(f"""
                            INSERT INTO DB_QUERY_HISTORY 
                            (pool_name, query_text, success, error_message, row_count)
                            VALUES ('{pool_name}', '{escaped_query}', FALSE, '{escaped_error}', 0)
                        """).collect()
                    except Exception as log_error:
                        st.warning(f"Failed to log query: {str(log_error)}")
    
    except Exception as e:
        st.error(f"Query execution error: {str(e)}")
        st.error("Full error details:")
        st.text(traceback.format_exc())

def display_connection_status(session):
    """Display connection status and history"""
    st.subheader("🔌 Active Connections")
    
    if 'db_connections' in st.session_state and st.session_state.db_connections:
        for pool_name, conn_info in st.session_state.db_connections.items():
            with st.expander(f"🗄️ {pool_name} ({conn_info['database_type']})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Database:** {conn_info['database_type']}")
                    st.write(f"**Host:** {conn_info.get('host', 'N/A')}")
                    st.write(f"**Username:** {conn_info['username']}")
                
                with col2:
                    st.write(f"**Status:** {conn_info['status']}")
                    if conn_info.get('security_token'):
                        st.write("**Token:** ✓ Configured")
                    
                    # Test connection button
                    if st.button(f"🧪 Test {pool_name}", key=f"test_{pool_name}"):
                        db_connector = get_database_connector()
                        test_database_connection(
                            db_connector,
                            conn_info['database_type'],
                            conn_info.get('host'),
                            conn_info['username'],
                            conn_info['password'],
                            conn_info.get('security_token')
                        )
    else:
        st.info("No active database connections.")
    
    # Query history
    if session:
        st.subheader("📊 Query History")
        try:
            history_data = session.sql("""
                SELECT pool_name, database_type, success, execution_time, row_count, error_message
                FROM DB_QUERY_HISTORY h
                JOIN DB_CONNECTION_METADATA m ON h.pool_name = m.pool_name
                ORDER BY execution_time DESC
                LIMIT 10
            """).collect()
            
            if history_data:
                history_df = pd.DataFrame([
                    {
                        'Pool': row[0],
                        'Database': row[1],
                        'Success': '✅' if row[2] else '❌',
                        'Time': row[3],
                        'Rows': row[4] or 0,
                        'Error': row[5] or ''
                    }
                    for row in history_data
                ])
                st.dataframe(history_df, use_container_width=True)
            else:
                st.info("No query history available.")
        
        except Exception as e:
            st.warning(f"Could not load query history: {str(e)}")

# Sidebar
def display_sidebar():
    st.sidebar.title("🗄️ Database Connector")
    st.sidebar.markdown("### 📋 Supported Databases")
    
    for db_type, config in DATABASE_CONFIGS.items():
        status = "✅" if config["available"] else "❌"
        st.sidebar.markdown(f"{status} **{db_type}**")
        if not config["available"]:
            st.sidebar.caption(f"Install: {config['library_required']}")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 💡 Quick Start")
    st.sidebar.markdown("""
    1. Install required packages
    2. Configure database connection
    3. Test connection
    4. Save and start querying
    """)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 📦 Required Packages")
    st.sidebar.code("""
pip install cx_Oracle
pip install simple-salesforce
pip install pyodbc
    """)

if __name__ == "__main__":
    display_sidebar()
    main()
