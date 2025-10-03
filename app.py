import streamlit as st
import json
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql, workspace, jobs
import tempfile
import time

# Configure page
st.set_page_config(
    page_title="Retail Insight Cockpit - Setup",
    page_icon=None,
    layout="wide"
)

# Initialize Databricks client
@st.cache_resource
def get_databricks_client():
    return WorkspaceClient()

w = get_databricks_client()

# App state management
if 'deployment_status' not in st.session_state:
    st.session_state.deployment_status = {}
if 'genie_config' not in st.session_state:
    st.session_state.genie_config = {}

st.title("Retail Insight Cockpit - Setup Wizard")
st.markdown("Configure and deploy your complete retail analytics solution with dashboards and Genie AI")

# Configuration tabs
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "Basic Configuration",
    "Data Mapping",
    "Dashboard Selection",
    "Genie AI Setup",
    "Deploy"
])

with tab1:
    st.header("Basic Configuration")

    col1, col2 = st.columns(2)

    with col1:
        catalog_name = st.text_input(
            "Catalog Name",
            value="retail_demo",
            help="Unity Catalog name where tables will be created"
        )

        schema_name = st.text_input(
            "Schema Name",
            value="cockpit",
            help="Schema name within the catalog"
        )

        warehouse_name = st.selectbox(
            "SQL Warehouse",
            options=[w.name for w in w.warehouses.list()],
            help="SQL Warehouse for running queries"
        )

    with col2:
        st.subheader("Environment Settings")

        generate_sample_data = st.checkbox(
            "Generate Sample Data",
            value=True,
            help="Create sample retail data for demonstration"
        )

        data_size = st.selectbox(
            "Sample Data Size",
            ["Small (1K records)", "Medium (10K records)", "Large (100K records)"],
            index=1
        )

        enable_optimization = st.checkbox(
            "Enable Delta Optimizations",
            value=True,
            help="Auto-optimize, auto-compact, and liquid clustering"
        )

with tab2:
    st.header("Data Source Mapping")
    st.markdown("Map your existing data sources or use our schema")

    data_source_option = st.radio(
        "Data Source Strategy",
        ["Use Built-in Schema", "Map Existing Tables", "Hybrid Approach"]
    )

    if data_source_option == "Map Existing Tables":
        st.subheader("Map Your Existing Tables")

        table_mappings = {}
        required_tables = [
            "sales", "products", "stores", "customers",
            "inventory", "suppliers", "promotions", "categories"
        ]

        for table in required_tables:
            col1, col2 = st.columns([1, 2])
            with col1:
                st.write(f"**{table.title()}**")
            with col2:
                table_mappings[table] = st.text_input(
                    f"Source table for {table}",
                    placeholder=f"your_catalog.your_schema.{table}_table",
                    key=f"mapping_{table}"
                )

        st.session_state.table_mappings = table_mappings

    elif data_source_option == "Hybrid Approach":
        st.info("This will create missing tables and map existing ones")

with tab3:
    st.header("Dashboard Selection")
    st.markdown("Choose which role-based dashboards to deploy")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Operational Dashboards")

        deploy_store_manager = st.checkbox(
            "Store Manager Cockpit",
            value=True,
            help="Daily operations, sales targets, inventory alerts"
        )

        deploy_supply_chain = st.checkbox(
            "Supply Chain Insights",
            value=True,
            help="Inventory optimization, supplier performance"
        )

    with col2:
        st.subheader("Strategic Dashboards")

        deploy_merchandiser = st.checkbox(
            "Merchandiser Analytics",
            value=True,
            help="Product performance, pricing, promotional ROI"
        )

        deploy_executive = st.checkbox(
            "Executive Summary",
            value=True,
            help="High-level KPIs, trends, strategic insights"
        )

    st.subheader("Dashboard Customization")

    refresh_interval = st.selectbox(
        "Auto-refresh Interval",
        ["15 minutes", "30 minutes", "1 hour", "4 hours", "Manual only"],
        index=1
    )

    dashboard_theme = st.selectbox(
        "Dashboard Theme",
        ["Databricks Default", "Dark Mode", "Light Mode", "Custom"],
        index=0
    )

with tab4:
    st.header("Genie AI Configuration")
    st.markdown("Set up natural language querying with Genie AI")

    enable_genie = st.checkbox(
        "Enable Genie AI",
        value=True,
        help="Allow natural language queries across your data"
    )

    if enable_genie:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("Genie Space Configuration")

            genie_space_name = st.text_input(
                "Genie Space Name",
                value=f"Retail Insight Cockpit - {catalog_name.title()}",
                help="Name for your Genie AI space"
            )

            genie_description = st.text_area(
                "Space Description",
                value="AI-powered natural language interface for retail analytics",
                help="Description of your Genie space"
            )

            business_domain = st.selectbox(
                "Business Domain",
                ["Retail & E-commerce", "Fashion Retail", "Grocery Retail", "Electronics Retail", "General Retail"],
                help="Industry context for better AI understanding"
            )

        with col2:
            st.subheader("User Roles & Sample Questions")

            selected_roles = st.multiselect(
                "Enable for User Roles",
                ["Store Manager", "Merchandiser", "Supply Chain", "Executive", "Data Analyst"],
                default=["Store Manager", "Merchandiser", "Supply Chain", "Executive"]
            )

            enable_sample_questions = st.checkbox(
                "Include Sample Questions",
                value=True,
                help="Pre-populate with role-specific sample questions"
            )

            custom_context = st.text_area(
                "Additional Business Context",
                placeholder="Add any specific business rules, KPIs, or domain knowledge...",
                help="Custom context to improve Genie AI responses"
            )

        st.subheader("Advanced Genie Settings")

        col1, col2, col3 = st.columns(3)

        with col1:
            genie_permissions = st.selectbox(
                "Default Permissions",
                ["All Users", "Specific Groups", "Admin Only"],
                help="Who can access the Genie space"
            )

        with col2:
            query_timeout = st.selectbox(
                "Query Timeout",
                ["30 seconds", "1 minute", "2 minutes", "5 minutes"],
                index=2
            )

        with col3:
            enable_query_history = st.checkbox(
                "Enable Query History",
                value=True,
                help="Track and learn from user queries"
            )

        # Store Genie configuration
        st.session_state.genie_config = {
            "enabled": enable_genie,
            "space_name": genie_space_name,
            "description": genie_description,
            "business_domain": business_domain,
            "selected_roles": selected_roles,
            "enable_sample_questions": enable_sample_questions,
            "custom_context": custom_context,
            "permissions": genie_permissions,
            "query_timeout": query_timeout,
            "enable_query_history": enable_query_history
        }

with tab5:
    st.header("Deploy Retail Insight Cockpit")

    # Configuration summary
    st.subheader("Deployment Summary")

    col1, col2 = st.columns(2)

    with col1:
        st.info(f"""
        **Target Environment:**
        - Catalog: `{catalog_name}`
        - Schema: `{schema_name}`
        - Warehouse: `{warehouse_name}`
        """)

        dashboard_count = sum([
            deploy_store_manager, deploy_supply_chain,
            deploy_merchandiser, deploy_executive
        ])

        st.success(f"""
        **Components to Deploy:**
        - Data Tables & Views
        - {dashboard_count} Dashboards
        - {'Enabled' if enable_genie else 'Disabled'} Genie AI Space
        - {'Enabled' if generate_sample_data else 'Disabled'} Sample Data
        """)

    with col2:
        if enable_genie:
            st.info(f"""
            **Genie AI Configuration:**
            - Space: `{genie_space_name}`
            - Roles: {', '.join(selected_roles)}
            - Domain: `{business_domain}`
            """)

    # Deployment controls
    st.subheader("Deployment Controls")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("Validate Configuration", type="secondary"):
            with st.spinner("Validating configuration..."):
                validation_results = validate_configuration(
                    catalog_name, schema_name, warehouse_name
                )

                if validation_results["valid"]:
                    st.success("Configuration validated successfully!")
                else:
                    st.error(f"Validation failed: {validation_results['error']}")

    with col2:
        if st.button("Generate Preview", type="secondary"):
            preview_config = generate_deployment_preview(
                catalog_name, schema_name, warehouse_name,
                deploy_store_manager, deploy_merchandiser,
                deploy_supply_chain, deploy_executive,
                st.session_state.genie_config
            )

            st.json(preview_config)

    with col3:
        if st.button("Deploy Everything", type="primary"):
            deploy_retail_cockpit(
                catalog_name, schema_name, warehouse_name,
                generate_sample_data, data_size,
                deploy_store_manager, deploy_merchandiser,
                deploy_supply_chain, deploy_executive,
                st.session_state.genie_config
            )

# Deployment functions
def validate_configuration(catalog, schema, warehouse):
    try:
        # Check catalog access
        catalogs = [c.name for c in w.catalogs.list()]
        if catalog not in catalogs:
            return {"valid": False, "error": f"Catalog '{catalog}' not found"}

        # Check warehouse
        warehouses = [wh.name for wh in w.warehouses.list()]
        if warehouse not in warehouses:
            return {"valid": False, "error": f"Warehouse '{warehouse}' not found"}

        return {"valid": True}
    except Exception as e:
        return {"valid": False, "error": str(e)}

def generate_deployment_preview(catalog, schema, warehouse, sm, mer, sc, exec, genie_config):
    config = {
        "target": f"{catalog}.{schema}",
        "warehouse": warehouse,
        "components": {
            "tables": ["sales", "products", "stores", "customers", "inventory", "suppliers", "promotions", "categories"],
            "views": ["daily_sales_agg", "store_performance", "inventory_alerts", "customer_segments", "product_performance", "promotional_performance"],
            "dashboards": []
        }
    }

    if sm: config["components"]["dashboards"].append("Store Manager Cockpit")
    if mer: config["components"]["dashboards"].append("Merchandiser Analytics")
    if sc: config["components"]["dashboards"].append("Supply Chain Insights")
    if exec: config["components"]["dashboards"].append("Executive Summary")

    if genie_config.get("enabled"):
        config["genie_ai"] = {
            "space_name": genie_config.get("space_name"),
            "roles": genie_config.get("selected_roles", []),
            "domain": genie_config.get("business_domain")
        }

    return config

def deploy_retail_cockpit(catalog, schema, warehouse, sample_data, data_size, sm, mer, sc, exec, genie_config):
    progress_bar = st.progress(0)
    status_text = st.empty()

    try:
        # Step 1: Create tables
        status_text.text("Creating data tables...")
        create_data_tables(catalog, schema)
        progress_bar.progress(20)

        # Step 2: Generate sample data
        if sample_data:
            status_text.text("Generating sample data...")
            generate_sample_data_job(catalog, schema, data_size)
            progress_bar.progress(40)

        # Step 3: Create analytical views
        status_text.text("Creating analytical views...")
        create_analytical_views(catalog, schema)
        progress_bar.progress(60)

        # Step 4: Deploy dashboards
        status_text.text("Deploying dashboards...")
        deploy_selected_dashboards(catalog, schema, warehouse, sm, mer, sc, exec)
        progress_bar.progress(80)

        # Step 5: Setup Genie AI
        if genie_config.get("enabled"):
            status_text.text("Setting up Genie AI...")
            setup_genie_ai(catalog, schema, genie_config)

        progress_bar.progress(100)
        status_text.text("Deployment completed successfully!")

        st.success("Retail Insight Cockpit deployed successfully!")

        # Show next steps
        st.subheader("Next Steps")
        st.markdown(f"""
        1. **Access Dashboards**: Go to Databricks workspace > Dashboards
        2. **Test Genie AI**: Navigate to Genie spaces and try sample questions
        3. **Configure Permissions**: Set up user access for different roles
        4. **Customize**: Modify dashboards and queries as needed

        **Your deployment details:**
        - Data: `{catalog}.{schema}`
        - Warehouse: `{warehouse}`
        - Dashboards: {sum([sm, mer, sc, exec])} deployed
        - Genie AI: {'Enabled' if genie_config.get('enabled') else 'Disabled'}
        """)

    except Exception as e:
        st.error(f"Deployment failed: {str(e)}")
        status_text.text("Deployment failed")

def create_data_tables(catalog, schema):
    # Execute table creation notebook
    job_run = w.jobs.run_now(job_id=get_job_id("create_tables"))
    wait_for_job_completion(job_run.run_id)

def generate_sample_data_job(catalog, schema, size):
    # Execute data generation notebook with size parameter
    job_run = w.jobs.run_now(
        job_id=get_job_id("generate_data"),
        notebook_params={"data_size": size}
    )
    wait_for_job_completion(job_run.run_id)

def create_analytical_views(catalog, schema):
    # Execute analytical views notebook
    job_run = w.jobs.run_now(job_id=get_job_id("create_views"))
    wait_for_job_completion(job_run.run_id)

def deploy_selected_dashboards(catalog, schema, warehouse, sm, mer, sc, exec):
    # Deploy selected dashboards programmatically
    dashboard_configs = []

    if sm: dashboard_configs.append("store_manager")
    if mer: dashboard_configs.append("merchandiser")
    if sc: dashboard_configs.append("supply_chain")
    if exec: dashboard_configs.append("executive")

    for dashboard_type in dashboard_configs:
        create_dashboard(dashboard_type, catalog, schema, warehouse)

def setup_genie_ai(catalog, schema, genie_config):
    # Setup Genie AI space with configuration
    genie_setup_params = {
        "catalog_name": catalog,
        "schema_name": schema,
        "space_name": genie_config.get("space_name"),
        "business_domain": genie_config.get("business_domain"),
        "selected_roles": ",".join(genie_config.get("selected_roles", [])),
        "custom_context": genie_config.get("custom_context", "")
    }

    job_run = w.jobs.run_now(
        job_id=get_job_id("setup_genie"),
        notebook_params=genie_setup_params
    )
    wait_for_job_completion(job_run.run_id)

def get_job_id(job_name):
    # This would map to actual job IDs in the DAB
    job_mapping = {
        "create_tables": "create_tables_job",
        "generate_data": "generate_data_job",
        "create_views": "create_views_job",
        "setup_genie": "setup_genie_job"
    }
    return job_mapping.get(job_name)

def wait_for_job_completion(run_id):
    while True:
        run = w.jobs.get_run(run_id)
        if run.state.life_cycle_state in ["TERMINATED", "SKIPPED"]:
            if run.state.result_state == "SUCCESS":
                return True
            else:
                raise Exception(f"Job failed: {run.state.state_message}")
        time.sleep(5)

def create_dashboard(dashboard_type, catalog, schema, warehouse):
    # Load dashboard configuration and create via SDK
    dashboard_configs = {
        "store_manager": get_store_manager_config(),
        "merchandiser": get_merchandiser_config(),
        "supply_chain": get_supply_chain_config(),
        "executive": get_executive_config()
    }

    config = dashboard_configs[dashboard_type]

    # Replace placeholders
    config_str = json.dumps(config)
    config_str = config_str.replace("{{ warehouse_id }}", get_warehouse_id(warehouse))
    config_str = config_str.replace("retail_demo.cockpit", f"{catalog}.{schema}")

    updated_config = json.loads(config_str)

    # Create dashboard via SDK
    w.lakeview.create(**updated_config)

def get_warehouse_id(warehouse_name):
    for warehouse in w.warehouses.list():
        if warehouse.name == warehouse_name:
            return warehouse.id
    raise Exception(f"Warehouse {warehouse_name} not found")

def get_store_manager_config():
    return {
        "display_name": "Store Manager Cockpit",
        "description": "Daily operations dashboard for store managers",
        "tags": ["retail", "store-operations"],
        "widgets": [
            {
                "title": "Today's Sales vs Target",
                "widget_type": "chart",
                "position": {"size_x": 6, "size_y": 4, "row": 0, "col": 0},
                "query": "SELECT store_name, daily_sales, daily_sales_target, (daily_sales/daily_sales_target)*100 as achievement_pct FROM store_performance WHERE date = CURRENT_DATE()"
            },
            {
                "title": "Hourly Sales Trend",
                "widget_type": "chart",
                "position": {"size_x": 6, "size_y": 4, "row": 0, "col": 6},
                "query": "SELECT HOUR(sale_timestamp) as hour, SUM(total_amount) as sales FROM sales WHERE DATE(sale_timestamp) = CURRENT_DATE() GROUP BY HOUR(sale_timestamp) ORDER BY hour"
            },
            {
                "title": "Inventory Alerts",
                "widget_type": "table",
                "position": {"size_x": 12, "size_y": 4, "row": 4, "col": 0},
                "query": "SELECT product_name, ending_inventory, reorder_point, alert_type FROM inventory_alerts_materialized WHERE priority = 'CRITICAL' ORDER BY ending_inventory ASC LIMIT 20"
            }
        ]
    }

def get_merchandiser_config():
    return {
        "display_name": "Merchandiser Analytics",
        "description": "Product performance and promotional ROI dashboard",
        "tags": ["retail", "merchandising"],
        "widgets": [
            {
                "title": "Category Performance (Last 30 Days)",
                "widget_type": "chart",
                "position": {"size_x": 12, "size_y": 4, "row": 0, "col": 0},
                "query": "SELECT c.category_name, SUM(s.total_amount) as revenue FROM sales s JOIN products p ON s.product_id = p.product_id JOIN categories c ON p.category_id = c.category_id WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS GROUP BY c.category_name ORDER BY revenue DESC"
            }
        ]
    }

def get_supply_chain_config():
    return {
        "display_name": "Supply Chain Insights",
        "description": "Inventory optimization and supplier performance",
        "tags": ["retail", "supply-chain"],
        "widgets": [
            {
                "title": "Inventory Alerts Overview",
                "widget_type": "chart",
                "position": {"size_x": 6, "size_y": 3, "row": 0, "col": 0},
                "query": "SELECT alert_type, COUNT(*) as count FROM inventory_alerts_materialized GROUP BY alert_type"
            }
        ]
    }

def get_executive_config():
    return {
        "display_name": "Executive Summary",
        "description": "High-level KPIs and strategic insights",
        "tags": ["retail", "executive"],
        "widgets": [
            {
                "title": "Revenue Trend (Last 90 Days)",
                "widget_type": "chart",
                "position": {"size_x": 12, "size_y": 4, "row": 0, "col": 0},
                "query": "SELECT DATE(sale_date) as date, SUM(total_amount) as revenue FROM sales WHERE sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS GROUP BY DATE(sale_date) ORDER BY date"
            }
        ]
    }

# Sidebar with help
with st.sidebar:
    st.header("Help & Resources")

    st.markdown("""
    **Quick Start:**
    1. Configure basic settings
    2. Choose data strategy
    3. Select dashboards
    4. Configure Genie AI
    5. Deploy everything

    **Support:**
    - [Documentation](https://docs.databricks.com)
    - [Video Tutorials](https://databricks.com/learn)
    - [Community Forum](https://community.databricks.com)
    """)

    if st.button("Reset Configuration"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.rerun()