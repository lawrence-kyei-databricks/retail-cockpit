# Retail Insight Cockpit for Databricks

A comprehensive, production-ready retail analytics solution featuring AI-powered insights, role-based dashboards, and a no-code deployment interface.

## Overview

The Retail Insight Cockpit is a **complete plug-and-play retail analytics platform** built for Databricks. It provides instant insights across store operations, merchandising, supply chain, and executive decision-making through an intuitive web interface that requires zero coding.

### Key Features

- **No-Code Deployment**: Web-based configuration UI powered by Streamlit
- **4 Role-Based Dashboards**: Tailored analytics for Store Managers, Merchandisers, Supply Chain, and Executives
- **AI-Powered Insights**: Integrated Genie AI for natural language queries
- **Complete Data Model**: 8 optimized tables with 7 analytical views
- **Production Ready**: Enterprise-grade architecture with automated pipelines
- **One-Click Setup**: Deploy entire solution in minutes

## Quick Start

### Prerequisites

- Databricks Workspace with Unity Catalog enabled
- Databricks CLI configured (`pip install databricks-cli`)
- SQL Warehouse for query execution
- Appropriate permissions for catalog/schema creation

### Installation

#### Option 1: Databricks App (Recommended)

Deploy as a Databricks App with UI configuration:

```bash
# Clone the repository
git clone https://github.com/databricks/retail-insight-cockpit.git
cd retail-insight-cockpit

# Deploy the app
./deploy_app.sh retail-insight-cockpit dev
```

Once deployed, access the web interface to:
1. Configure your catalog and schema
2. Select which dashboards to deploy
3. Set up Genie AI with business context
4. Click deploy to create everything automatically

#### Option 2: Direct Asset Bundle Deployment

For automated deployment without UI:

```bash
# Deploy infrastructure and data
databricks bundle deploy --target dev

# Run setup job
databricks jobs run-now --job-id <setup_job_id>
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Configuration Layer                      │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │
│  │  Streamlit  │ │  Databricks │ │    Genie    │            │
│  │     UI      │ │ Asset Bundle│ │     AI      │            │
│  └─────────────┘ └─────────────┘ └─────────────┘            │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                      Analytics Layer                        │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │
│  │   Store     │ │ Merchandiser│ │Supply Chain │ Executive  │
│  │   Manager   │ │  Analytics  │ │  Insights   │  Summary   │
│  └─────────────┘ └─────────────┘ └─────────────┘            │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                        Data Layer                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │
│  │    Unity    │ │    Delta    │ │     SQL     │            │
│  │   Catalog   │ │    Tables   │ │  Warehouse  │            │
│  └─────────────┘ └─────────────┘ └─────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

## Data Model

### Core Tables
- **sales** - Transaction records with customer and product details
- **products** - Product catalog with categories and pricing
- **stores** - Store locations with performance metrics
- **customers** - Customer profiles with loyalty tiers
- **inventory** - Stock levels and movement tracking
- **suppliers** - Vendor information and ratings
- **promotions** - Campaign definitions and performance
- **categories** - Product category hierarchy

### Analytical Views
- **daily_sales_agg** - Daily sales summaries by store
- **store_performance** - Store KPIs vs targets
- **inventory_alerts** - Stock-out and replenishment alerts
- **customer_segments** - Customer value segmentation
- **product_performance** - Product sales and profitability
- **promotional_performance** - Campaign ROI analysis
- **store_comparison** - Cross-store benchmarking

## Dashboards

### Store Manager Cockpit
Real-time operational insights including:
- Sales vs target performance
- Inventory alerts and stockouts
- Hourly sales patterns
- Top products and customers
- Critical action items

### Merchandiser Analytics
Product and pricing optimization with:
- Category performance trends
- Product profitability rankings
- Promotional effectiveness
- Pricing analysis
- Markdown recommendations

### Supply Chain Insights
Inventory and supplier management featuring:
- Inventory health metrics
- Replenishment priorities
- Supplier scorecards
- Slow-moving inventory
- Reorder recommendations

### Executive Summary
Strategic KPIs and trends showing:
- Revenue and growth metrics
- Regional performance
- Store rankings
- Category mix analysis
- Monthly trends

## Genie AI Integration

Ask questions in natural language:

**Store Manager Examples:**
- "How are today's sales compared to target?"
- "Which products are out of stock?"
- "What time of day has the most sales?"

**Executive Examples:**
- "What's our revenue trend vs last year?"
- "Which regions are growing fastest?"
- "What are our top performing stores?"

**40+ pre-configured sample questions** across all roles with retail-specific business context.

## Configuration Options

The web UI provides extensive configuration:

### Data Configuration
- Choose between sample data or connect existing tables
- Configure data volumes for demos
- Map your existing schema to the solution

### Dashboard Selection
- Enable/disable specific dashboards
- Configure refresh intervals
- Set visualization themes

### Genie AI Setup
- Define business context
- Add custom KPIs and rules
- Configure user permissions
- Set query timeouts

## Project Structure

```
retail-insight-cockpit/
├── app.py                     # Streamlit configuration UI
├── databricks.yml             # Asset Bundle configuration
├── databricks-app.yml         # App deployment config
├── src/
│   └── setup/
│       ├── 01_create_tables.py
│       ├── 02_generate_sample_data.py
│       ├── 03_create_analytical_views.py
│       └── 04_create_dashboards.py
├── dashboards/
│   ├── store_manager_dashboard.json
│   ├── merchandiser_dashboard.json
│   ├── supply_chain_dashboard.json
│   └── executive_dashboard.json
├── setup_genie.py             # Genie AI configuration
└── deploy_app.sh              # Deployment automation
```

## Deployment Guide

### Development Environment
```bash
./deploy_app.sh retail-cockpit-dev dev
```

### Production Environment
```bash
./deploy_app.sh retail-cockpit-prod prod
```

### Post-Deployment Steps

1. **Access the Web UI** at the provided URL
2. **Configure Settings** through the interface
3. **Deploy Infrastructure** with one click
4. **Verify Dashboards** in Databricks workspace
5. **Test Genie Queries** with sample questions

## Requirements

- Databricks Runtime 13.3 LTS or higher
- Unity Catalog enabled
- SQL Warehouse (Small or larger)
- Python 3.8+

## Security & Governance

- **Unity Catalog**: Full data governance and lineage
- **Role-Based Access**: Granular permissions by user role
- **Data Encryption**: At-rest and in-transit encryption
- **Audit Logging**: Complete activity tracking
- **Compliance Ready**: GDPR, CCPA, SOC2 compatible

## Performance & Optimization

- **Delta Lake**: ACID transactions with time travel
- **Auto-Optimization**: Automatic file compaction
- **Liquid Clustering**: Improved query performance
- **Materialized Views**: Pre-computed aggregations
- **Incremental Processing**: Efficient data updates

## Support & Documentation

### Documentation
- [Deployment Guide](DEPLOYMENT_GUIDE.md)
- [App User Guide](README_APP.md)
- [API Documentation](docs/api.md)
- [Troubleshooting](docs/troubleshooting.md)

### Getting Help
- **Issues**: [GitHub Issues](https://github.com/databricks/retail-insight-cockpit/issues)
- **Discussions**: [GitHub Discussions](https://github.com/databricks/retail-insight-cockpit/discussions)
- **Documentation**: [Databricks Docs](https://docs.databricks.com)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup
```bash
# Clone the repo
git clone https://github.com/databricks/retail-insight-cockpit.git

# Install dependencies
pip install -r app_requirements.txt

# Run tests
python -m pytest tests/

# Run locally (for testing UI)
streamlit run app.py
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with:
- [Databricks](https://databricks.com) - Unified Analytics Platform
- [Delta Lake](https://delta.io) - Open-source storage layer
- [Streamlit](https://streamlit.io) - Web app framework
- [Apache Spark](https://spark.apache.org) - Distributed processing

## Roadmap

### Coming Soon
- [ ] Real-time streaming analytics
- [ ] Mobile dashboard views
- [ ] Advanced ML forecasting models
- [ ] Custom metric builder UI
- [ ] Multi-language support
- [ ] Export to PowerBI/Tableau

### Future Enhancements
- [ ] Automated anomaly detection
- [ ] Predictive inventory optimization
- [ ] Customer journey analytics
- [ ] A/B testing framework
- [ ] Cost optimization recommendations

---

**Ready to transform your retail analytics?** Deploy the Retail Insight Cockpit today and get instant insights across your entire retail operation - no coding required!

For enterprise support and customization, contact your Databricks account team.
