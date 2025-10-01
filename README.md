# Retail Insight Cockpit

A comprehensive, plug-and-play retail analytics solution built for the Databricks Lakehouse Platform.

## Overview

The Retail Insight Cockpit provides role-based AI/BI dashboards and analytics for retail organizations, enabling business users to access actionable insights through self-service dashboards, natural language queries via Genie, and governed data on Unity Catalog.

### Key Features

- **Role-Based Dashboards**: Pre-built dashboards for Store Managers, Merchandisers, Supply Chain, and Executives
- **Genie Integration**: Natural language querying with curated prompt libraries
- **Unity Catalog Governance**: Secure, governed access to retail data
- **Plug-and-Play Deployment**: Quick setup with minimal configuration
- **Extensible Architecture**: Easy to customize and extend for specific needs

### Included Personas & Dashboards

1. **Store Manager Cockpit**
   - Daily sales performance vs targets
   - Footfall and conversion metrics
   - Inventory stockouts and alerts
   - Customer churn risk analysis

2. **Merchandiser Analytics**
   - Category and SKU performance
   - Seasonal trend analysis
   - Promotional campaign lift
   - Sell-through rates and inventory turns

3. **Supply Chain Insights**
   - Inventory levels and stockout tracking
   - Fulfillment delays and bottlenecks
   - Supplier performance metrics
   - Demand forecasting accuracy

4. **Executive Summary**
   - Revenue and margin KPIs
   - Year-over-year performance
   - Regional and channel comparisons
   - Exception reporting and alerts

## Quick Start

### Prerequisites

- Databricks workspace with Unity Catalog enabled
- Databricks CLI configured
- Appropriate permissions to create catalogs, schemas, and dashboards

### Installation

1. **Clone and Deploy**
   ```bash
   git clone <repository-url>
   cd retail-insight-cockpit
   databricks bundle deploy --target dev
   ```

2. **Configure Your Data**
   - Update `config/table_mappings.yml` with your table names
   - Run the setup job: `databricks jobs run-now setup_retail_data`

3. **Access Dashboards**
   - Dashboards will be available in your Databricks workspace
   - Share with appropriate user groups based on roles

### Configuration

Edit `config/table_mappings.yml` to map your existing tables:

```yaml
source_tables:
  sales: "your_catalog.your_schema.sales_table"
  customers: "your_catalog.your_schema.customers_table"
  inventory: "your_catalog.your_schema.inventory_table"
  # ... additional mappings
```

## Architecture

```
retail-insight-cockpit/
├── databricks.yml              # Asset Bundle configuration
├── config/                     # Configuration files
├── src/                        # Source code
│   ├── setup/                  # Initial setup notebooks
│   ├── pipelines/              # Data processing pipelines
│   └── utils/                  # Utility functions
├── dashboards/                 # Dashboard definitions
├── sql/                        # SQL queries and views
├── genie/                      # Genie prompt libraries
└── docs/                       # Documentation
```

## Deployment Environments

- **Development**: `databricks bundle deploy --target dev`
- **Staging**: `databricks bundle deploy --target staging`
- **Production**: `databricks bundle deploy --target prod`

## Documentation

- [Installation Guide](docs/installation.md)
- [Configuration Guide](docs/configuration.md)
- [Dashboard User Guide](docs/dashboards.md)
- [Genie Integration](docs/genie.md)
- [Extending the Solution](docs/extending.md)

## Support

For issues and questions:
- Review the documentation in the `docs/` folder
- Check the troubleshooting guide
- Contact your Databricks Solutions Architect

## License

This solution is provided as-is for demonstration and educational purposes.