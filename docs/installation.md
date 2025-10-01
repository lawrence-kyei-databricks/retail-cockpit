# Installation Guide

This guide walks you through installing and configuring the Retail Insight Cockpit on your Databricks environment.

## Prerequisites

### System Requirements

- **Databricks Runtime**: 13.3 LTS or higher
- **Unity Catalog**: Enabled and configured
- **Databricks CLI**: Version 0.18+ installed locally
- **SQL Warehouse**: Pro or Serverless (Medium or larger recommended)
- **Genie**: Enabled in your workspace (contact Databricks if not available)

### Required Permissions

You'll need the following permissions in your Databricks workspace:

- **Unity Catalog Admin** or ability to create catalogs and schemas
- **Workspace Admin** or ability to create jobs, clusters, and dashboards
- **SQL Admin** or ability to create and manage SQL warehouses
- **Can Manage** permissions on the target catalog for Unity Catalog

### Data Requirements

The Retail Insight Cockpit works with:

- **Existing retail data**: Map your current tables to the expected schema
- **Sample data generation**: Use provided scripts to generate realistic demo data
- **Hybrid approach**: Use some existing data and supplement with generated data

## Quick Start Installation

### Option 1: Automated Script Installation (Recommended)

1. **Clone or download** the Retail Insight Cockpit repository
2. **Configure Databricks CLI** if not already done:
   ```bash
   pip install databricks-cli
   databricks configure --token
   ```
3. **Run the deployment script**:
   ```bash
   cd retail-insight-cockpit
   ./scripts/deploy.sh --env dev --catalog retail_analytics_dev --workspace https://your-workspace.cloud.databricks.com
   ```

The script will automatically:
- Validate prerequisites
- Deploy the Asset Bundle
- Create tables and sample data
- Set up analytical views
- Configure Genie integration
- Create user groups and permissions

### Option 2: Manual Installation

If you prefer to install manually or need custom configuration:

#### Step 1: Setup Unity Catalog

```sql
-- Create catalog
CREATE CATALOG IF NOT EXISTS retail_analytics_dev
COMMENT 'Retail Analytics Catalog for Insight Cockpit';

-- Create schema
CREATE SCHEMA IF NOT EXISTS retail_analytics_dev.cockpit
COMMENT 'Retail Insight Cockpit schema';

-- Use the schema
USE retail_analytics_dev.cockpit;
```

#### Step 2: Deploy Asset Bundle

```bash
# Navigate to project directory
cd retail-insight-cockpit

# Deploy for development
databricks bundle deploy --target dev \
  --var catalog_name=retail_analytics_dev \
  --var schema_name=cockpit \
  --var workspace_url=https://your-workspace.cloud.databricks.com
```

#### Step 3: Create Tables and Data

Run the setup notebooks in order:

1. **Create Tables**:
   ```bash
   databricks workspace import ./src/setup/01_create_tables.py /retail-cockpit/setup/01_create_tables
   # Run the notebook with parameters: catalog_name, schema_name
   ```

2. **Generate Sample Data**:
   ```bash
   databricks workspace import ./src/setup/02_generate_sample_data.py /retail-cockpit/setup/02_generate_sample_data
   # Run the notebook (takes 10-15 minutes)
   ```

3. **Create Analytical Views**:
   ```bash
   databricks workspace import ./src/setup/03_create_analytical_views.py /retail-cockpit/setup/03_create_analytical_views
   # Run the notebook
   ```

#### Step 4: Setup Genie Integration

```bash
# Upload Genie configuration
databricks workspace import ./genie/retail_prompt_library.py /retail-cockpit/genie/retail_prompt_library
databricks workspace import ./genie/genie_integration_setup.py /retail-cockpit/genie/genie_integration_setup

# Run Genie setup notebooks
```

#### Step 5: Import Dashboards

```bash
# Import dashboard definitions
databricks workspace import ./dashboards/store_manager_dashboard.json /retail-cockpit/dashboards/store_manager
databricks workspace import ./dashboards/merchandiser_dashboard.json /retail-cockpit/dashboards/merchandiser
databricks workspace import ./dashboards/supply_chain_dashboard.json /retail-cockpit/dashboards/supply_chain
databricks workspace import ./dashboards/executive_dashboard.json /retail-cockpit/dashboards/executive
```

## Environment-Specific Installation

### Development Environment

```bash
./scripts/deploy.sh --env dev --catalog retail_analytics_dev --workspace https://dev.databricks.com
```

**Characteristics**:
- Small clusters for cost efficiency
- 7-day data retention
- Sample data generation included
- All features enabled for testing

### Staging Environment

```bash
./scripts/deploy.sh --env staging --catalog retail_analytics_staging --workspace https://staging.databricks.com
```

**Characteristics**:
- Medium clusters for realistic performance testing
- 30-day data retention
- Production-like configuration
- Full monitoring enabled

### Production Environment

```bash
./scripts/deploy.sh --env prod --catalog retail_analytics --workspace https://prod.databricks.com --skip-data
```

**Characteristics**:
- Large clusters for optimal performance
- 90-day data retention
- No sample data generation (use your real data)
- Full security and monitoring

## Configuration Options

### Table Mapping Configuration

Edit `config/table_mappings.yml` to map your existing data:

```yaml
# Map your existing tables to expected schema
customer_table_mappings:
  sales: "your_catalog.your_schema.transactions"
  customers: "your_catalog.crm.customer_master"
  products: "your_catalog.mdm.product_catalog"
  stores: "your_catalog.operations.store_locations"
  inventory: "your_catalog.supply_chain.inventory_levels"
```

### Dashboard Configuration

Customize dashboard parameters in the JSON files:

```json
{
  "parameters": [
    {
      "name": "store_id",
      "defaultValue": "YOUR_STORE_ID"
    },
    {
      "name": "date_range",
      "defaultValue": "30"
    }
  ]
}
```

### Genie Configuration

Customize the AI assistant behavior:

```python
# In genie/retail_prompt_library.py
genie_config = {
    "instructions": "Your custom instructions for retail context",
    "role_customization": {
        "store_manager": {
            "focus": "Your specific focus areas",
            "scope": "Data scope for this role"
        }
    }
}
```

## Validation and Testing

### Automated Validation

The installation includes built-in validation:

```bash
# Run validation
python src/utils/deployment_utils.py validate --catalog retail_analytics_dev --schema cockpit
```

### Manual Testing

Verify each component:

1. **Data Access**:
   ```sql
   -- Check table row counts
   SELECT 'sales' as table_name, COUNT(*) as row_count FROM sales
   UNION ALL
   SELECT 'customers', COUNT(*) FROM customers
   UNION ALL
   SELECT 'products', COUNT(*) FROM products;
   ```

2. **Dashboard Access**:
   - Open each dashboard in the Databricks workspace
   - Verify data loads and visualizations work
   - Test parameter filtering

3. **Genie Integration**:
   - Ask sample questions like "How are sales trending?"
   - Verify responses are relevant and accurate
   - Test role-specific prompts

### Common Issues and Solutions

#### Issue: "Catalog not found"

**Solution**: Ensure you have Unity Catalog admin permissions:
```sql
GRANT USE CATALOG ON CATALOG retail_analytics_dev TO `your_user@company.com`;
```

#### Issue: "Genie not available"

**Solution**: Contact your Databricks account team to enable Genie in your workspace.

#### Issue: "Sample data generation takes too long"

**Solution**: Use smaller sample sizes:
```python
# In generate_sample_data.py
customers_data = generate_customers(500)  # Reduced from 1000
sales_data = generate_sales_transactions(5000)  # Reduced from 10000
```

#### Issue: "Dashboard widgets don't load"

**Solution**: Check SQL warehouse permissions:
```sql
GRANT USE ON WAREHOUSE your_warehouse TO `your_group`;
```

## Next Steps

After successful installation:

1. **Configure User Access**: [User Management Guide](user-management.md)
2. **Customize Dashboards**: [Dashboard Customization Guide](customization.md)
3. **Set Up Data Pipelines**: [Data Pipeline Guide](data-pipelines.md)
4. **Train End Users**: [User Training Guide](user-training.md)

## Support

For installation issues:

1. Check the [Troubleshooting Guide](troubleshooting.md)
2. Review Databricks logs in the workspace
3. Contact your Databricks Solutions Architect
4. File issues in the project repository

## Security Considerations

- All data remains within your Databricks environment
- Unity Catalog provides row and column-level security
- Genie queries respect existing data permissions
- Regular security audits recommended for production

---

**Estimated Installation Time**: 30-60 minutes for automated installation, 2-3 hours for manual installation with customization.