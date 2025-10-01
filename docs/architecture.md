# Architecture Guide

This document describes the technical architecture of the Retail Insight Cockpit, including data architecture, infrastructure components, security model, and extensibility framework.

## Overview

The Retail Insight Cockpit is built on the Databricks Lakehouse Platform, leveraging Unity Catalog for governance, Delta Lake for storage, and Genie for natural language analytics. The solution follows a modular, scalable architecture designed for enterprise retail organizations.

### Architecture Principles

- **Unified Data Platform**: Single source of truth for all retail data
- **Role-Based Access**: Secure, governed access based on user personas
- **Real-Time Insights**: Near real-time data processing and alerting
- **Natural Language Interface**: Conversational AI for business users
- **Extensible Design**: Easy to customize and extend for specific needs
- **Enterprise Security**: Comprehensive governance and compliance

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Retail Insight Cockpit                  │
├─────────────────────────────────────────────────────────────┤
│                   Presentation Layer                       │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │  Store   │ │Merchan-  │ │ Supply   │ │Executive │      │
│  │ Manager  │ │  diser   │ │  Chain   │ │ Summary  │      │
│  │Dashboard │ │Dashboard │ │Dashboard │ │Dashboard │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│              ┌─────────────────────────────────────┐       │
│              │         Genie AI Assistant         │       │
│              │    (Natural Language Interface)     │       │
│              └─────────────────────────────────────┘       │
├─────────────────────────────────────────────────────────────┤
│                  Analytics Layer                           │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │  Daily   │ │  Store   │ │Inventory │ │Customer  │      │
│  │  Sales   │ │Performan │ │ Alerts   │ │Segments  │      │
│  │   Agg    │ │    ce    │ │          │ │          │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│              ┌─────────────────────────────────────┐       │
│              │        Product Performance          │       │
│              │     & Promotional Analytics         │       │
│              └─────────────────────────────────────┘       │
├─────────────────────────────────────────────────────────────┤
│                    Data Layer                              │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐      │
│  │  Sales   │ │Customers │ │ Products │ │  Stores  │      │
│  │   Data   │ │   Data   │ │   Data   │ │   Data   │      │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘      │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐                   │
│  │Inventory │ │Promotions│ │Suppliers │                   │
│  │   Data   │ │   Data   │ │   Data   │                   │
│  └──────────┘ └──────────┘ └──────────┘                   │
├─────────────────────────────────────────────────────────────┤
│                Infrastructure Layer                        │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │            Databricks Lakehouse Platform               │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐      │ │
│  │  │Unity Catalog│ │ Delta Lake  │ │SQL Warehouse│      │ │
│  │  │ Governance  │ │   Storage   │ │  Compute    │      │ │
│  │  └─────────────┘ └─────────────┘ └─────────────┘      │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Data Architecture

### Unity Catalog Structure

```
retail_analytics_catalog
├── cockpit_schema
│   ├── Core Tables
│   │   ├── sales
│   │   ├── customers
│   │   ├── products
│   │   ├── stores
│   │   ├── inventory
│   │   ├── promotions
│   │   └── suppliers
│   ├── Analytical Views
│   │   ├── daily_sales_agg
│   │   ├── store_performance
│   │   ├── inventory_alerts
│   │   ├── customer_segments
│   │   ├── product_performance
│   │   └── promotional_performance
│   ├── Materialized Tables
│   │   ├── daily_sales_agg_materialized
│   │   ├── store_performance_materialized
│   │   ├── inventory_alerts_materialized
│   │   └── customer_segments_materialized
│   └── Genie Configuration
│       ├── genie_configuration
│       ├── genie_sample_questions
│       └── genie_dashboard_widgets
└── metadata_schema
    ├── pipeline_execution_log
    ├── data_quality_checks
    └── audit_logs
```

### Data Flow Architecture

```
External Data Sources
        ↓
Data Ingestion Layer (Delta Live Tables)
        ↓
Bronze Layer (Raw Data)
        ↓
Silver Layer (Cleaned & Validated)
        ↓
Gold Layer (Business-Ready Analytics)
        ↓
Presentation Layer (Dashboards & Genie)
```

#### Bronze Layer (Raw Data)
- **Purpose**: Store raw, unprocessed data as ingested
- **Format**: Delta tables with full history
- **Schema**: Flexible schema to accommodate source changes
- **Retention**: Long-term storage for audit and reprocessing

#### Silver Layer (Cleaned & Validated)
- **Purpose**: Cleaned, validated, and deduplicated data
- **Format**: Delta tables with enforced schema
- **Quality**: Data quality checks and validation rules
- **Transformations**: Basic cleaning and standardization

#### Gold Layer (Business-Ready Analytics)
- **Purpose**: Aggregated, business-ready datasets
- **Format**: Optimized Delta tables and views
- **Performance**: Pre-computed aggregations and indexes
- **Business Logic**: Applied business rules and calculations

## Infrastructure Components

### Compute Resources

#### SQL Warehouse Configuration

**Development Environment**
```yaml
warehouse_type: PRO
cluster_size: Small
min_clusters: 1
max_clusters: 2
auto_stop_minutes: 30
spot_instance_policy: COST_OPTIMIZED
```

**Production Environment**
```yaml
warehouse_type: PRO
cluster_size: Large
min_clusters: 2
max_clusters: 10
auto_stop_minutes: 120
spot_instance_policy: RELIABILITY_OPTIMIZED
```

#### Job Cluster Configuration

**Daily Aggregations**
```yaml
spark_version: "13.3.x-scala2.12"
node_type: "i3.xlarge"
num_workers: 2-8 (auto-scaling)
cluster_policy: "data-processing-policy"
libraries:
  - pypi: "databricks-feature-store"
```

### Storage Architecture

#### Delta Lake Tables

**Partitioning Strategy**
- **Sales**: Partitioned by `sale_date` (daily partitions)
- **Inventory**: Partitioned by `inventory_date` (daily partitions)
- **Customers**: No partitioning (manageable size)
- **Products**: Partitioned by `category_id` (balanced distribution)

**Optimization Settings**
```sql
-- Auto-optimization enabled
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.deletedFileRetentionDuration' = 'interval 7 days'
)
```

**Z-Ordering**
```sql
-- Optimize for common query patterns
OPTIMIZE sales ZORDER BY (store_id, product_id);
OPTIMIZE customers ZORDER BY (customer_id, loyalty_tier);
OPTIMIZE inventory ZORDER BY (store_id, product_id, alert_type);
```

## Security Architecture

### Unity Catalog Governance

#### Catalog-Level Permissions
```sql
-- Catalog administrators
GRANT ALL PRIVILEGES ON CATALOG retail_analytics TO `retail_admins`;

-- Schema-level access
GRANT USE CATALOG, USE SCHEMA ON CATALOG retail_analytics TO `retail_users`;
```

#### Table-Level Security
```sql
-- Row-level security example
CREATE OR REPLACE FUNCTION store_access_filter(store_id STRING)
RETURNS BOOLEAN
RETURN
  CASE
    WHEN is_member('store_managers') THEN
      store_id IN (SELECT store_id FROM user_store_access WHERE user = current_user())
    WHEN is_member('executives') THEN TRUE
    ELSE FALSE
  END;

-- Apply row filter
ALTER TABLE sales SET ROW FILTER store_access_filter(store_id) ON (store_id);
```

#### Column-Level Security
```sql
-- Mask sensitive customer data
CREATE OR REPLACE FUNCTION mask_pii(col STRING)
RETURNS STRING
RETURN
  CASE
    WHEN is_member('customer_service') THEN col
    WHEN is_member('analysts') THEN CONCAT(LEFT(col, 3), '***')
    ELSE '***'
  END;

-- Apply column mask
ALTER TABLE customers ALTER COLUMN email SET MASK mask_pii;
```

### Role-Based Access Control

#### User Groups and Permissions

**Store Managers**
```sql
-- Store-level data access
GRANT SELECT ON TABLE sales TO `store_managers`;
GRANT SELECT ON TABLE inventory_alerts TO `store_managers`;
GRANT SELECT ON TABLE customer_segments TO `store_managers`;

-- Row-level security: own stores only
-- (Applied via row filters)
```

**Merchandisers**
```sql
-- Product and category data access
GRANT SELECT ON TABLE product_performance TO `merchandisers`;
GRANT SELECT ON TABLE promotional_performance TO `merchandisers`;
GRANT SELECT ON TABLE daily_sales_agg TO `merchandisers`;

-- Full product visibility across stores
```

**Supply Chain Managers**
```sql
-- Inventory and supplier data access
GRANT SELECT ON TABLE inventory TO `supply_chain_managers`;
GRANT SELECT ON TABLE suppliers TO `supply_chain_managers`;
GRANT SELECT ON TABLE inventory_alerts TO `supply_chain_managers`;

-- Company-wide inventory visibility
```

**Executives**
```sql
-- Strategic data access
GRANT SELECT ON SCHEMA retail_analytics.cockpit TO `executives`;

-- Full visibility with aggregated views
```

### Data Privacy and Compliance

#### PII Protection
- **Encryption**: Data encrypted at rest and in transit
- **Masking**: Dynamic data masking for sensitive fields
- **Audit**: Complete audit trail of data access
- **Retention**: Automated data retention policies

#### Compliance Features
- **GDPR**: Right to be forgotten implementation
- **SOX**: Financial data controls and audit trails
- **PCI**: Secure handling of payment-related data
- **CCPA**: California privacy law compliance

## Genie AI Architecture

### Natural Language Processing

#### Genie Space Configuration
```python
genie_space = {
    "data_sources": [
        {
            "catalog": "retail_analytics",
            "schema": "cockpit",
            "description": "Retail analytics data with sales, inventory, customers"
        }
    ],
    "instructions": "Retail domain expert with understanding of KPIs",
    "semantic_model": "retail_semantic_model.yml"
}
```

#### Semantic Model
```yaml
# retail_semantic_model.yml
entities:
  - name: store
    primary_key: store_id
    attributes:
      - store_name
      - region
      - store_type

  - name: product
    primary_key: product_id
    attributes:
      - product_name
      - category_name
      - brand

relationships:
  - from: sales
    to: store
    type: many_to_one
    join_key: store_id

  - from: sales
    to: product
    type: many_to_one
    join_key: product_id

metrics:
  - name: total_sales
    expression: SUM(total_amount)
    description: "Total sales revenue"

  - name: units_sold
    expression: SUM(quantity)
    description: "Total units sold"
```

### Prompt Engineering

#### Role-Specific Context
```python
store_manager_context = """
You are assisting a store manager who needs:
- Daily operational insights
- Inventory alerts and actions
- Customer service optimization
- Staff performance guidance

Focus on actionable, immediate insights with clear next steps.
Always include context about time periods and comparisons.
"""

merchandiser_context = """
You are assisting a merchandiser who needs:
- Product performance analysis
- Pricing and promotion optimization
- Category management insights
- Seasonal trend analysis

Provide strategic recommendations with supporting data.
Include profitability and competitive positioning insights.
"""
```

## Performance Optimization

### Query Optimization

#### Materialized Views Strategy
```sql
-- Pre-compute expensive aggregations
CREATE MATERIALIZED VIEW daily_store_summary AS
SELECT
  sale_date,
  store_id,
  SUM(total_amount) as daily_sales,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT transaction_id) as transaction_count
FROM sales
GROUP BY sale_date, store_id;
```

#### Caching Strategy
```python
# Cache frequently accessed tables
spark.sql("CACHE TABLE product_performance")
spark.sql("CACHE TABLE inventory_alerts")
spark.sql("CACHE TABLE customer_segments")
```

### Data Pipeline Optimization

#### Delta Live Tables Configuration
```python
# Optimized DLT pipeline
@dlt.table(
    name="silver_sales",
    comment="Cleaned and validated sales data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "store_id,sale_date"
    }
)
@dlt.expect_or_drop("valid_sale_date", "sale_date IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "total_amount > 0")
def silver_sales():
    return (
        dlt.read("bronze_sales")
        .select("*")
        .withColumn("sale_date", F.to_date("sale_timestamp"))
    )
```

## Monitoring and Observability

### Pipeline Monitoring

#### Execution Tracking
```sql
CREATE TABLE pipeline_execution_log (
  execution_id STRING,
  pipeline_name STRING,
  start_time TIMESTAMP,
  end_time TIMESTAMP,
  status STRING,
  records_processed BIGINT,
  error_message STRING
);
```

#### Data Quality Monitoring
```sql
CREATE TABLE data_quality_checks (
  check_id STRING,
  table_name STRING,
  check_type STRING,
  check_result BOOLEAN,
  error_count BIGINT,
  check_timestamp TIMESTAMP
);
```

### Performance Monitoring

#### Query Performance
- **Slow Query Detection**: Automatic identification of queries > 30 seconds
- **Resource Usage**: Monitoring of CPU, memory, and I/O utilization
- **Concurrency**: Tracking of concurrent user sessions and queries

#### Dashboard Performance
- **Load Time Monitoring**: Track dashboard widget load times
- **User Experience**: Monitor user interaction patterns
- **Error Tracking**: Capture and alert on dashboard errors

## Disaster Recovery

### Backup Strategy

#### Data Backup
```sql
-- Automated backups to secondary region
CREATE TABLE backup_sales
USING DELTA
LOCATION 's3://backup-bucket/retail-analytics/sales'
AS SELECT * FROM sales;
```

#### Configuration Backup
- **Asset Bundle**: Version controlled in Git
- **Dashboard Definitions**: Exported and stored in source control
- **Genie Configuration**: Backed up to object storage

### Recovery Procedures

#### RTO/RPO Targets
- **Recovery Time Objective (RTO)**: 4 hours
- **Recovery Point Objective (RPO)**: 1 hour
- **Data Freshness**: Real-time to 15 minutes

## Extensibility Framework

### Adding New Dashboards

#### Template Structure
```json
{
  "version": "1.0",
  "displayName": "New Role Dashboard",
  "description": "Custom dashboard for specific role",
  "parameters": [...],
  "layout": {
    "widgets": [...]
  },
  "genie": {
    "enabled": true,
    "suggestedQueries": [...]
  }
}
```

### Custom Analytics

#### Adding New Metrics
```python
# Custom metric calculation
def calculate_customer_lifetime_value(customer_df, sales_df):
    return (
        customer_df
        .join(sales_df, "customer_id")
        .groupBy("customer_id")
        .agg(
            F.sum("total_amount").alias("total_spent"),
            F.datediff(F.max("sale_date"), F.min("sale_date")).alias("customer_days")
        )
        .withColumn("daily_value", F.col("total_spent") / F.col("customer_days"))
        .withColumn("projected_clv", F.col("daily_value") * 365)
    )
```

### Integration Points

#### External Data Sources
```python
# External API integration example
def ingest_weather_data():
    weather_df = (
        spark.read
        .format("json")
        .option("multiline", "true")
        .load("s3://external-data/weather/")
    )

    weather_df.write.mode("append").saveAsTable("external.weather_data")
```

#### Third-Party Tools
- **BI Tools**: Power BI, Tableau connectivity via JDBC/ODBC
- **Data Science**: MLflow integration for predictive models
- **Alerting**: Slack, email, PagerDuty integrations
- **ERP Systems**: SAP, Oracle integration via APIs

## Deployment Architecture

### Environment Strategy

#### Multi-Environment Setup
```yaml
environments:
  development:
    catalog: "retail_analytics_dev"
    cluster_size: "Small"
    data_retention: "7 days"

  staging:
    catalog: "retail_analytics_staging"
    cluster_size: "Medium"
    data_retention: "30 days"

  production:
    catalog: "retail_analytics"
    cluster_size: "Large"
    data_retention: "90 days"
```

### CI/CD Pipeline

#### Asset Bundle Deployment
```yaml
# .github/workflows/deploy.yml
name: Deploy Retail Cockpit
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy to Staging
        run: databricks bundle deploy --target staging
      - name: Run Tests
        run: databricks jobs run-now --job-id $VALIDATION_JOB_ID
      - name: Deploy to Production
        if: success()
        run: databricks bundle deploy --target prod
```

## Cost Optimization

### Resource Management

#### Compute Optimization
- **Auto-scaling**: Dynamic cluster sizing based on workload
- **Spot Instances**: Cost-optimized compute for non-critical workloads
- **Auto-termination**: Automatic cluster shutdown when idle

#### Storage Optimization
- **Data Lifecycle**: Automated archival of old data
- **Compression**: Delta Lake automatic compression
- **Vacuum**: Regular cleanup of unused data files

### Monitoring and Alerts

#### Cost Tracking
```sql
-- Cost monitoring query
SELECT
  DATE_TRUNC('month', usage_date) as month,
  SUM(cost_usd) as monthly_cost,
  SUM(dbu_hours) as total_dbu_hours
FROM system.billing.usage
WHERE workspace_id = 'retail-cockpit-workspace'
GROUP BY DATE_TRUNC('month', usage_date)
ORDER BY month DESC;
```

---

This architecture provides a robust, scalable foundation for retail analytics while maintaining flexibility for future enhancements and integrations.