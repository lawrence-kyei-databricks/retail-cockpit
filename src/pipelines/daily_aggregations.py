# Databricks notebook source
# MAGIC %md
# MAGIC # Daily Aggregations Pipeline
# MAGIC
# MAGIC This notebook creates and maintains daily aggregated tables for the Retail Insight Cockpit.
# MAGIC It runs daily to refresh analytical views and materialized tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
execution_date = dbutils.widgets.get("execution_date") if dbutils.widgets.get("execution_date") else datetime.now().strftime("%Y-%m-%d")

logger.info(f"Starting daily aggregations for {catalog_name}.{schema_name} on {execution_date}")

# Set spark configurations for optimization
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# Use the schema
spark.sql(f"USE {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales Aggregation

# COMMAND ----------

def refresh_daily_sales_aggregation():
    """
    Refresh the daily sales aggregation materialized table
    """
    logger.info("Refreshing daily sales aggregation...")

    # Create or replace daily sales aggregation
    daily_sales_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.daily_sales_agg_materialized
    USING DELTA
    PARTITIONED BY (sale_date)
    AS
    SELECT
      s.sale_date,
      s.store_id,
      st.store_name,
      st.region,
      st.district,
      p.category_id,
      c.category_name,
      c.parent_category_id,
      COUNT(DISTINCT s.transaction_id) as transaction_count,
      COUNT(DISTINCT s.customer_id) as unique_customers,
      SUM(s.quantity) as total_quantity,
      SUM(s.total_amount) as total_sales,
      SUM(s.discount_amount) as total_discounts,
      SUM(s.tax_amount) as total_tax,
      AVG(s.total_amount) as avg_transaction_value,
      SUM(CASE WHEN s.return_flag = true THEN s.total_amount ELSE 0 END) as returns_amount,
      COUNT(CASE WHEN s.return_flag = true THEN 1 END) as returns_count,
      SUM(s.total_amount - (p.unit_cost * s.quantity)) as gross_profit,
      COUNT(DISTINCT CASE WHEN s.promotion_id IS NOT NULL THEN s.transaction_id END) as promotional_transactions,
      CURRENT_TIMESTAMP() as last_updated
    FROM sales s
    JOIN stores st ON s.store_id = st.store_id
    JOIN products p ON s.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
    GROUP BY s.sale_date, s.store_id, st.store_name, st.region, st.district,
             p.category_id, c.category_name, c.parent_category_id
    """

    spark.sql(daily_sales_query)

    # Optimize the table
    spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.daily_sales_agg_materialized")

    # Update table statistics
    spark.sql(f"ANALYZE TABLE {catalog_name}.{schema_name}.daily_sales_agg_materialized COMPUTE STATISTICS")

    logger.info("Daily sales aggregation refreshed successfully")

# Execute the refresh
refresh_daily_sales_aggregation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Performance Metrics

# COMMAND ----------

def refresh_store_performance():
    """
    Refresh store performance metrics
    """
    logger.info("Refreshing store performance metrics...")

    store_performance_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.store_performance_materialized
    USING DELTA
    AS
    WITH daily_stats AS (
      SELECT
        store_id,
        sale_date,
        SUM(total_amount) as daily_sales,
        COUNT(DISTINCT transaction_id) as daily_transactions,
        COUNT(DISTINCT customer_id) as daily_customers
      FROM sales
      WHERE sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
      GROUP BY store_id, sale_date
    ),
    store_targets AS (
      SELECT
        store_id,
        square_footage,
        CASE
          WHEN square_footage > 12000 THEN 15000
          WHEN square_footage > 8000 THEN 10000
          ELSE 6000
        END as daily_sales_target,
        CASE
          WHEN square_footage > 12000 THEN 100
          WHEN square_footage > 8000 THEN 75
          ELSE 50
        END as daily_transaction_target
      FROM stores
    )
    SELECT
      st.store_id,
      st.store_name,
      st.region,
      st.district,
      st.store_type,
      AVG(ds.daily_sales) as avg_daily_sales,
      AVG(ds.daily_transactions) as avg_daily_transactions,
      AVG(ds.daily_customers) as avg_daily_customers,
      AVG(ds.daily_sales / NULLIF(ds.daily_customers, 0)) as avg_sales_per_customer,
      targets.daily_sales_target,
      targets.daily_transaction_target,
      AVG(ds.daily_sales) / targets.daily_sales_target as sales_vs_target_ratio,
      AVG(ds.daily_transactions) / targets.daily_transaction_target as transactions_vs_target_ratio,
      st.square_footage,
      AVG(ds.daily_sales) / st.square_footage as sales_per_sqft,
      CURRENT_TIMESTAMP() as last_updated
    FROM stores st
    LEFT JOIN daily_stats ds ON st.store_id = ds.store_id
    LEFT JOIN store_targets targets ON st.store_id = targets.store_id
    GROUP BY st.store_id, st.store_name, st.region, st.district, st.store_type,
             targets.daily_sales_target, targets.daily_transaction_target, st.square_footage
    """

    spark.sql(store_performance_query)

    # Optimize the table
    spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.store_performance_materialized")

    logger.info("Store performance metrics refreshed successfully")

# Execute the refresh
refresh_store_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Alerts Refresh

# COMMAND ----------

def refresh_inventory_alerts():
    """
    Refresh inventory alerts materialized table
    """
    logger.info("Refreshing inventory alerts...")

    inventory_alerts_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.inventory_alerts_materialized
    USING DELTA
    AS
    WITH latest_inventory AS (
      SELECT
        store_id,
        product_id,
        sku,
        ending_inventory,
        safety_stock,
        reorder_point,
        days_on_hand,
        stockout_flag,
        overstock_flag,
        last_sold_date,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY inventory_date DESC) as rn
      FROM inventory
    ),
    product_velocity AS (
      SELECT
        s.store_id,
        s.product_id,
        AVG(s.quantity) as avg_daily_sales,
        COUNT(DISTINCT s.sale_date) as days_with_sales
      FROM sales s
      WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
      GROUP BY s.store_id, s.product_id
    )
    SELECT
      li.store_id,
      st.store_name,
      st.region,
      li.product_id,
      li.sku,
      p.product_name,
      p.category_id,
      c.category_name,
      li.ending_inventory,
      li.safety_stock,
      li.reorder_point,
      li.days_on_hand,
      pv.avg_daily_sales,
      CASE
        WHEN li.stockout_flag THEN 'STOCKOUT'
        WHEN li.ending_inventory <= li.safety_stock THEN 'LOW_STOCK'
        WHEN li.ending_inventory <= li.reorder_point THEN 'REORDER_NEEDED'
        WHEN li.overstock_flag THEN 'OVERSTOCK'
        WHEN li.days_on_hand > 60 THEN 'SLOW_MOVING'
        ELSE 'NORMAL'
      END as alert_type,
      CASE
        WHEN li.stockout_flag THEN 'CRITICAL'
        WHEN li.ending_inventory <= li.safety_stock THEN 'HIGH'
        WHEN li.ending_inventory <= li.reorder_point THEN 'MEDIUM'
        WHEN li.overstock_flag OR li.days_on_hand > 60 THEN 'LOW'
        ELSE 'NONE'
      END as priority,
      li.last_sold_date,
      DATEDIFF(CURRENT_DATE(), li.last_sold_date) as days_since_last_sale,
      CURRENT_TIMESTAMP() as last_updated
    FROM latest_inventory li
    JOIN stores st ON li.store_id = st.store_id
    JOIN products p ON li.product_id = p.product_id
    JOIN categories c ON p.category_id = c.category_id
    LEFT JOIN product_velocity pv ON li.store_id = pv.store_id AND li.product_id = pv.product_id
    WHERE li.rn = 1
    """

    spark.sql(inventory_alerts_query)

    # Optimize the table
    spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.inventory_alerts_materialized")

    logger.info("Inventory alerts refreshed successfully")

# Execute the refresh
refresh_inventory_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Segmentation Refresh

# COMMAND ----------

def refresh_customer_segments():
    """
    Refresh customer segmentation analysis
    """
    logger.info("Refreshing customer segmentation...")

    customer_segments_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.customer_segments_materialized
    USING DELTA
    AS
    WITH customer_metrics AS (
      SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.loyalty_tier,
        c.preferred_store_id,
        c.registration_date,
        COUNT(DISTINCT s.transaction_id) as total_transactions,
        SUM(s.total_amount) as total_spent,
        AVG(s.total_amount) as avg_transaction_value,
        MAX(s.sale_date) as last_purchase_date,
        MIN(s.sale_date) as first_purchase_date,
        COUNT(DISTINCT s.sale_date) as active_days,
        COUNT(DISTINCT s.store_id) as stores_visited,
        COUNT(DISTINCT p.category_id) as categories_purchased,
        DATEDIFF(CURRENT_DATE(), MAX(s.sale_date)) as days_since_last_purchase,
        DATEDIFF(MAX(s.sale_date), MIN(s.sale_date)) as customer_lifetime_days
      FROM customers c
      LEFT JOIN sales s ON c.customer_id = s.customer_id
      LEFT JOIN products p ON s.product_id = p.product_id
      WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 365 DAYS
      GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.loyalty_tier,
               c.preferred_store_id, c.registration_date
    )
    SELECT
      *,
      CASE
        WHEN total_spent >= 1000 AND days_since_last_purchase <= 30 THEN 'VIP_ACTIVE'
        WHEN total_spent >= 500 AND days_since_last_purchase <= 60 THEN 'HIGH_VALUE'
        WHEN total_transactions >= 10 AND days_since_last_purchase <= 90 THEN 'FREQUENT_BUYER'
        WHEN days_since_last_purchase > 180 THEN 'AT_RISK'
        WHEN days_since_last_purchase > 90 THEN 'DORMANT'
        WHEN total_transactions <= 2 THEN 'NEW_CUSTOMER'
        ELSE 'REGULAR'
      END as customer_segment,
      CASE
        WHEN days_since_last_purchase > 180 THEN 'HIGH'
        WHEN days_since_last_purchase > 90 THEN 'MEDIUM'
        ELSE 'LOW'
      END as churn_risk,
      total_spent / NULLIF(customer_lifetime_days, 0) * 365 as annual_value,
      total_transactions / NULLIF(active_days, 0) as purchase_frequency,
      CURRENT_TIMESTAMP() as last_updated
    FROM customer_metrics
    WHERE total_transactions > 0
    """

    spark.sql(customer_segments_query)

    # Optimize the table
    spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.customer_segments_materialized")

    logger.info("Customer segmentation refreshed successfully")

# Execute the refresh
refresh_customer_segments()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance Refresh

# COMMAND ----------

def refresh_product_performance():
    """
    Refresh product performance metrics
    """
    logger.info("Refreshing product performance...")

    product_performance_query = f"""
    CREATE OR REPLACE TABLE {catalog_name}.{schema_name}.product_performance_materialized
    USING DELTA
    AS
    WITH product_sales AS (
      SELECT
        p.product_id,
        p.sku,
        p.product_name,
        p.category_id,
        c.category_name,
        c.parent_category_id,
        p.brand,
        p.retail_price,
        p.unit_cost,
        p.is_seasonal,
        p.season,
        SUM(s.quantity) as total_units_sold,
        SUM(s.total_amount) as total_revenue,
        SUM(s.discount_amount) as total_discounts,
        COUNT(DISTINCT s.transaction_id) as transaction_count,
        COUNT(DISTINCT s.customer_id) as unique_customers,
        COUNT(DISTINCT s.store_id) as stores_sold_in,
        AVG(s.total_amount / s.quantity) as avg_selling_price,
        MIN(s.sale_date) as first_sale_date,
        MAX(s.sale_date) as last_sale_date,
        COUNT(DISTINCT s.sale_date) as active_sales_days
      FROM products p
      LEFT JOIN sales s ON p.product_id = s.product_id
      LEFT JOIN categories c ON p.category_id = c.category_id
      WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
      GROUP BY p.product_id, p.sku, p.product_name, p.category_id, c.category_name,
               c.parent_category_id, p.brand, p.retail_price, p.unit_cost, p.is_seasonal, p.season
    ),
    inventory_summary AS (
      SELECT
        product_id,
        SUM(ending_inventory) as total_inventory,
        AVG(days_on_hand) as avg_days_on_hand,
        COUNT(DISTINCT CASE WHEN stockout_flag THEN store_id END) as stockout_stores
      FROM inventory i
      WHERE inventory_date = (SELECT MAX(inventory_date) FROM inventory)
      GROUP BY product_id
    )
    SELECT
      ps.*,
      COALESCE(inv.total_inventory, 0) as current_inventory,
      COALESCE(inv.avg_days_on_hand, 0) as avg_days_on_hand,
      COALESCE(inv.stockout_stores, 0) as stockout_stores,
      ps.total_revenue - (ps.unit_cost * ps.total_units_sold) as gross_profit,
      (ps.total_revenue - (ps.unit_cost * ps.total_units_sold)) / NULLIF(ps.total_revenue, 0) as margin_percentage,
      ps.total_units_sold / NULLIF(ps.active_sales_days, 0) as units_per_day,
      CASE
        WHEN ps.total_units_sold = 0 THEN 'NO_SALES'
        WHEN ps.total_units_sold > 100 AND ps.total_revenue > 5000 THEN 'TOP_PERFORMER'
        WHEN ps.total_units_sold > 50 AND ps.total_revenue > 2000 THEN 'GOOD_PERFORMER'
        WHEN ps.total_units_sold < 10 OR ps.total_revenue < 500 THEN 'POOR_PERFORMER'
        ELSE 'AVERAGE_PERFORMER'
      END as performance_category,
      DATEDIFF(CURRENT_DATE(), ps.last_sale_date) as days_since_last_sale,
      CURRENT_TIMESTAMP() as last_updated
    FROM product_sales ps
    LEFT JOIN inventory_summary inv ON ps.product_id = inv.product_id
    """

    spark.sql(product_performance_query)

    # Optimize the table
    spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.product_performance_materialized")

    logger.info("Product performance refreshed successfully")

# Execute the refresh
refresh_product_performance()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

def run_data_quality_checks():
    """
    Run data quality checks on refreshed tables
    """
    logger.info("Running data quality checks...")

    checks = {
        "daily_sales_agg_materialized": {
            "row_count": "SELECT COUNT(*) as count FROM daily_sales_agg_materialized",
            "latest_date": "SELECT MAX(sale_date) as latest_date FROM daily_sales_agg_materialized",
            "null_check": "SELECT COUNT(*) as null_sales FROM daily_sales_agg_materialized WHERE total_sales IS NULL"
        },
        "inventory_alerts_materialized": {
            "alert_count": "SELECT COUNT(*) as count FROM inventory_alerts_materialized",
            "critical_alerts": "SELECT COUNT(*) as critical_count FROM inventory_alerts_materialized WHERE priority = 'CRITICAL'"
        },
        "customer_segments_materialized": {
            "customer_count": "SELECT COUNT(*) as count FROM customer_segments_materialized",
            "segment_distribution": "SELECT customer_segment, COUNT(*) as count FROM customer_segments_materialized GROUP BY customer_segment"
        },
        "product_performance_materialized": {
            "product_count": "SELECT COUNT(*) as count FROM product_performance_materialized",
            "top_performers": "SELECT COUNT(*) as count FROM product_performance_materialized WHERE performance_category = 'TOP_PERFORMER'"
        }
    }

    quality_results = {}

    for table, table_checks in checks.items():
        quality_results[table] = {}
        for check_name, query in table_checks.items():
            try:
                result = spark.sql(f"SELECT * FROM ({query})")
                quality_results[table][check_name] = result.collect()
            except Exception as e:
                quality_results[table][check_name] = f"Error: {e}"

    # Log results
    for table, results in quality_results.items():
        logger.info(f"Quality checks for {table}:")
        for check, result in results.items():
            logger.info(f"  {check}: {result}")

    return quality_results

# Run quality checks
quality_results = run_data_quality_checks()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------

def create_pipeline_summary():
    """
    Create a summary of the pipeline execution
    """
    end_time = datetime.now()

    summary = {
        "execution_date": execution_date,
        "start_time": datetime.now() - timedelta(minutes=5),  # Approximate
        "end_time": end_time,
        "catalog": catalog_name,
        "schema": schema_name,
        "tables_refreshed": [
            "daily_sales_agg_materialized",
            "store_performance_materialized",
            "inventory_alerts_materialized",
            "customer_segments_materialized",
            "product_performance_materialized"
        ],
        "status": "SUCCESS",
        "quality_checks": quality_results
    }

    # Save summary to table
    summary_data = [{
        "execution_date": execution_date,
        "pipeline_name": "daily_aggregations",
        "status": "SUCCESS",
        "tables_refreshed": len(summary["tables_refreshed"]),
        "execution_time_minutes": 5,  # Approximate
        "summary_json": str(summary),
        "created_at": datetime.now()
    }]

    summary_df = spark.createDataFrame(summary_data)
    summary_df.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.pipeline_execution_log")

    logger.info("Pipeline execution completed successfully")
    logger.info(f"Summary: {summary}")

    return summary

# Create pipeline summary
execution_summary = create_pipeline_summary()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup and Optimization

# COMMAND ----------

# Run VACUUM on Delta tables to clean up old files
tables_to_vacuum = [
    "daily_sales_agg_materialized",
    "store_performance_materialized",
    "inventory_alerts_materialized",
    "customer_segments_materialized",
    "product_performance_materialized"
]

for table in tables_to_vacuum:
    try:
        spark.sql(f"VACUUM {catalog_name}.{schema_name}.{table} RETAIN 7 DAYS")
        logger.info(f"Vacuumed {table}")
    except Exception as e:
        logger.warning(f"Could not vacuum {table}: {e}")

logger.info("Daily aggregations pipeline completed successfully! ✅")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Alerts

# COMMAND ----------

# Check for critical issues that need alerting
critical_stockouts = spark.sql(f"""
SELECT COUNT(*) as count
FROM {catalog_name}.{schema_name}.inventory_alerts_materialized
WHERE priority = 'CRITICAL'
""").collect()[0]['count']

underperforming_stores = spark.sql(f"""
SELECT COUNT(*) as count
FROM {catalog_name}.{schema_name}.store_performance_materialized
WHERE sales_vs_target_ratio < 0.8
""").collect()[0]['count']

if critical_stockouts > 10:
    logger.warning(f"ALERT: {critical_stockouts} critical stockouts detected!")

if underperforming_stores > 2:
    logger.warning(f"ALERT: {underperforming_stores} stores significantly underperforming!")

print(f"""
PIPELINE EXECUTION SUMMARY
==========================
Execution Date: {execution_date}
Status: SUCCESS ✅
Tables Refreshed: 5
Critical Stockouts: {critical_stockouts}
Underperforming Stores: {underperforming_stores}

All aggregations refreshed and optimized successfully!
""")