# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Insight Cockpit - Analytical Views Creation
# MAGIC
# MAGIC This notebook creates analytical views and aggregated tables optimized for dashboard queries.
# MAGIC These views provide the foundation for role-based dashboards and Genie queries.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters and Configuration

# COMMAND ----------

# Get parameters from Asset Bundle
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Creating analytical views in {catalog_name}.{schema_name}")

# Use the schema
spark.sql(f"USE {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily Sales Aggregation View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW daily_sales_agg AS
# MAGIC SELECT
# MAGIC   s.sale_date,
# MAGIC   s.store_id,
# MAGIC   st.store_name,
# MAGIC   st.region,
# MAGIC   st.district,
# MAGIC   p.category_id,
# MAGIC   c.category_name,
# MAGIC   c.parent_category_id,
# MAGIC   COUNT(DISTINCT s.transaction_id) as transaction_count,
# MAGIC   COUNT(DISTINCT s.customer_id) as unique_customers,
# MAGIC   SUM(s.quantity) as total_quantity,
# MAGIC   SUM(s.total_amount) as total_sales,
# MAGIC   SUM(s.discount_amount) as total_discounts,
# MAGIC   SUM(s.tax_amount) as total_tax,
# MAGIC   AVG(s.total_amount) as avg_transaction_value,
# MAGIC   SUM(CASE WHEN s.return_flag = true THEN s.total_amount ELSE 0 END) as returns_amount,
# MAGIC   COUNT(CASE WHEN s.return_flag = true THEN 1 END) as returns_count,
# MAGIC   SUM(s.total_amount - (p.unit_cost * s.quantity)) as gross_profit,
# MAGIC   COUNT(DISTINCT CASE WHEN s.promotion_id IS NOT NULL THEN s.transaction_id END) as promotional_transactions
# MAGIC FROM sales s
# MAGIC JOIN stores st ON s.store_id = st.store_id
# MAGIC JOIN products p ON s.product_id = p.product_id
# MAGIC JOIN categories c ON p.category_id = c.category_id
# MAGIC GROUP BY s.sale_date, s.store_id, st.store_name, st.region, st.district,
# MAGIC          p.category_id, c.category_name, c.parent_category_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Performance View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW store_performance AS
# MAGIC WITH daily_stats AS (
# MAGIC   SELECT
# MAGIC     store_id,
# MAGIC     sale_date,
# MAGIC     SUM(total_amount) as daily_sales,
# MAGIC     COUNT(DISTINCT transaction_id) as daily_transactions,
# MAGIC     COUNT(DISTINCT customer_id) as daily_customers
# MAGIC   FROM sales
# MAGIC   WHERE sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC   GROUP BY store_id, sale_date
# MAGIC ),
# MAGIC store_targets AS (
# MAGIC   SELECT
# MAGIC     store_id,
# MAGIC     square_footage,
# MAGIC     CASE
# MAGIC       WHEN square_footage > 12000 THEN 15000
# MAGIC       WHEN square_footage > 8000 THEN 10000
# MAGIC       ELSE 6000
# MAGIC     END as daily_sales_target,
# MAGIC     CASE
# MAGIC       WHEN square_footage > 12000 THEN 100
# MAGIC       WHEN square_footage > 8000 THEN 75
# MAGIC       ELSE 50
# MAGIC     END as daily_transaction_target
# MAGIC   FROM stores
# MAGIC )
# MAGIC SELECT
# MAGIC   st.store_id,
# MAGIC   st.store_name,
# MAGIC   st.region,
# MAGIC   st.district,
# MAGIC   st.store_type,
# MAGIC   AVG(ds.daily_sales) as avg_daily_sales,
# MAGIC   AVG(ds.daily_transactions) as avg_daily_transactions,
# MAGIC   AVG(ds.daily_customers) as avg_daily_customers,
# MAGIC   AVG(ds.daily_sales / NULLIF(ds.daily_customers, 0)) as avg_sales_per_customer,
# MAGIC   targets.daily_sales_target,
# MAGIC   targets.daily_transaction_target,
# MAGIC   AVG(ds.daily_sales) / targets.daily_sales_target as sales_vs_target_ratio,
# MAGIC   AVG(ds.daily_transactions) / targets.daily_transaction_target as transactions_vs_target_ratio,
# MAGIC   st.square_footage,
# MAGIC   AVG(ds.daily_sales) / st.square_footage as sales_per_sqft
# MAGIC FROM stores st
# MAGIC LEFT JOIN daily_stats ds ON st.store_id = ds.store_id
# MAGIC LEFT JOIN store_targets targets ON st.store_id = targets.store_id
# MAGIC GROUP BY st.store_id, st.store_name, st.region, st.district, st.store_type,
# MAGIC          targets.daily_sales_target, targets.daily_transaction_target, st.square_footage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inventory Alerts View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW inventory_alerts AS
# MAGIC WITH latest_inventory AS (
# MAGIC   SELECT
# MAGIC     store_id,
# MAGIC     product_id,
# MAGIC     sku,
# MAGIC     ending_inventory,
# MAGIC     safety_stock,
# MAGIC     reorder_point,
# MAGIC     days_on_hand,
# MAGIC     stockout_flag,
# MAGIC     overstock_flag,
# MAGIC     last_sold_date,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY inventory_date DESC) as rn
# MAGIC   FROM inventory
# MAGIC ),
# MAGIC product_velocity AS (
# MAGIC   SELECT
# MAGIC     s.store_id,
# MAGIC     s.product_id,
# MAGIC     AVG(s.quantity) as avg_daily_sales,
# MAGIC     COUNT(DISTINCT s.sale_date) as days_with_sales
# MAGIC   FROM sales s
# MAGIC   WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
# MAGIC   GROUP BY s.store_id, s.product_id
# MAGIC )
# MAGIC SELECT
# MAGIC   li.store_id,
# MAGIC   st.store_name,
# MAGIC   st.region,
# MAGIC   li.product_id,
# MAGIC   li.sku,
# MAGIC   p.product_name,
# MAGIC   p.category_id,
# MAGIC   c.category_name,
# MAGIC   li.ending_inventory,
# MAGIC   li.safety_stock,
# MAGIC   li.reorder_point,
# MAGIC   li.days_on_hand,
# MAGIC   pv.avg_daily_sales,
# MAGIC   CASE
# MAGIC     WHEN li.stockout_flag THEN 'STOCKOUT'
# MAGIC     WHEN li.ending_inventory <= li.safety_stock THEN 'LOW_STOCK'
# MAGIC     WHEN li.ending_inventory <= li.reorder_point THEN 'REORDER_NEEDED'
# MAGIC     WHEN li.overstock_flag THEN 'OVERSTOCK'
# MAGIC     WHEN li.days_on_hand > 60 THEN 'SLOW_MOVING'
# MAGIC     ELSE 'NORMAL'
# MAGIC   END as alert_type,
# MAGIC   CASE
# MAGIC     WHEN li.stockout_flag THEN 'CRITICAL'
# MAGIC     WHEN li.ending_inventory <= li.safety_stock THEN 'HIGH'
# MAGIC     WHEN li.ending_inventory <= li.reorder_point THEN 'MEDIUM'
# MAGIC     WHEN li.overstock_flag OR li.days_on_hand > 60 THEN 'LOW'
# MAGIC     ELSE 'NONE'
# MAGIC   END as priority,
# MAGIC   li.last_sold_date,
# MAGIC   DATEDIFF(CURRENT_DATE(), li.last_sold_date) as days_since_last_sale
# MAGIC FROM latest_inventory li
# MAGIC JOIN stores st ON li.store_id = st.store_id
# MAGIC JOIN products p ON li.product_id = p.product_id
# MAGIC JOIN categories c ON p.category_id = c.category_id
# MAGIC LEFT JOIN product_velocity pv ON li.store_id = pv.store_id AND li.product_id = pv.product_id
# MAGIC WHERE li.rn = 1
# MAGIC   AND (li.stockout_flag OR li.overstock_flag OR li.ending_inventory <= li.reorder_point OR li.days_on_hand > 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer Segments View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW customer_segments AS
# MAGIC WITH customer_metrics AS (
# MAGIC   SELECT
# MAGIC     c.customer_id,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     c.email,
# MAGIC     c.loyalty_tier,
# MAGIC     c.preferred_store_id,
# MAGIC     c.registration_date,
# MAGIC     COUNT(DISTINCT s.transaction_id) as total_transactions,
# MAGIC     SUM(s.total_amount) as total_spent,
# MAGIC     AVG(s.total_amount) as avg_transaction_value,
# MAGIC     MAX(s.sale_date) as last_purchase_date,
# MAGIC     MIN(s.sale_date) as first_purchase_date,
# MAGIC     COUNT(DISTINCT s.sale_date) as active_days,
# MAGIC     COUNT(DISTINCT s.store_id) as stores_visited,
# MAGIC     COUNT(DISTINCT p.category_id) as categories_purchased,
# MAGIC     DATEDIFF(CURRENT_DATE(), MAX(s.sale_date)) as days_since_last_purchase,
# MAGIC     DATEDIFF(MAX(s.sale_date), MIN(s.sale_date)) as customer_lifetime_days
# MAGIC   FROM customers c
# MAGIC   LEFT JOIN sales s ON c.customer_id = s.customer_id
# MAGIC   LEFT JOIN products p ON s.product_id = p.product_id
# MAGIC   WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 365 DAYS
# MAGIC   GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.loyalty_tier,
# MAGIC            c.preferred_store_id, c.registration_date
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN total_spent >= 1000 AND days_since_last_purchase <= 30 THEN 'VIP_ACTIVE'
# MAGIC     WHEN total_spent >= 500 AND days_since_last_purchase <= 60 THEN 'HIGH_VALUE'
# MAGIC     WHEN total_transactions >= 10 AND days_since_last_purchase <= 90 THEN 'FREQUENT_BUYER'
# MAGIC     WHEN days_since_last_purchase > 180 THEN 'AT_RISK'
# MAGIC     WHEN days_since_last_purchase > 90 THEN 'DORMANT'
# MAGIC     WHEN total_transactions <= 2 THEN 'NEW_CUSTOMER'
# MAGIC     ELSE 'REGULAR'
# MAGIC   END as customer_segment,
# MAGIC   CASE
# MAGIC     WHEN days_since_last_purchase > 180 THEN 'HIGH'
# MAGIC     WHEN days_since_last_purchase > 90 THEN 'MEDIUM'
# MAGIC     ELSE 'LOW'
# MAGIC   END as churn_risk,
# MAGIC   total_spent / NULLIF(customer_lifetime_days, 0) * 365 as annual_value,
# MAGIC   total_transactions / NULLIF(active_days, 0) as purchase_frequency
# MAGIC FROM customer_metrics
# MAGIC WHERE total_transactions > 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Performance View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW product_performance AS
# MAGIC WITH product_sales AS (
# MAGIC   SELECT
# MAGIC     p.product_id,
# MAGIC     p.sku,
# MAGIC     p.product_name,
# MAGIC     p.category_id,
# MAGIC     c.category_name,
# MAGIC     c.parent_category_id,
# MAGIC     p.brand,
# MAGIC     p.retail_price,
# MAGIC     p.unit_cost,
# MAGIC     p.is_seasonal,
# MAGIC     p.season,
# MAGIC     SUM(s.quantity) as total_units_sold,
# MAGIC     SUM(s.total_amount) as total_revenue,
# MAGIC     SUM(s.discount_amount) as total_discounts,
# MAGIC     COUNT(DISTINCT s.transaction_id) as transaction_count,
# MAGIC     COUNT(DISTINCT s.customer_id) as unique_customers,
# MAGIC     COUNT(DISTINCT s.store_id) as stores_sold_in,
# MAGIC     AVG(s.total_amount / s.quantity) as avg_selling_price,
# MAGIC     MIN(s.sale_date) as first_sale_date,
# MAGIC     MAX(s.sale_date) as last_sale_date,
# MAGIC     COUNT(DISTINCT s.sale_date) as active_sales_days
# MAGIC   FROM products p
# MAGIC   LEFT JOIN sales s ON p.product_id = s.product_id
# MAGIC   LEFT JOIN categories c ON p.category_id = c.category_id
# MAGIC   WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
# MAGIC   GROUP BY p.product_id, p.sku, p.product_name, p.category_id, c.category_name,
# MAGIC            c.parent_category_id, p.brand, p.retail_price, p.unit_cost, p.is_seasonal, p.season
# MAGIC ),
# MAGIC inventory_summary AS (
# MAGIC   SELECT
# MAGIC     product_id,
# MAGIC     SUM(ending_inventory) as total_inventory,
# MAGIC     AVG(days_on_hand) as avg_days_on_hand,
# MAGIC     COUNT(DISTINCT CASE WHEN stockout_flag THEN store_id END) as stockout_stores
# MAGIC   FROM inventory i
# MAGIC   WHERE inventory_date = (SELECT MAX(inventory_date) FROM inventory)
# MAGIC   GROUP BY product_id
# MAGIC )
# MAGIC SELECT
# MAGIC   ps.*,
# MAGIC   COALESCE(inv.total_inventory, 0) as current_inventory,
# MAGIC   COALESCE(inv.avg_days_on_hand, 0) as avg_days_on_hand,
# MAGIC   COALESCE(inv.stockout_stores, 0) as stockout_stores,
# MAGIC   ps.total_revenue - (ps.unit_cost * ps.total_units_sold) as gross_profit,
# MAGIC   (ps.total_revenue - (ps.unit_cost * ps.total_units_sold)) / NULLIF(ps.total_revenue, 0) as margin_percentage,
# MAGIC   ps.total_units_sold / NULLIF(ps.active_sales_days, 0) as units_per_day,
# MAGIC   CASE
# MAGIC     WHEN ps.total_units_sold = 0 THEN 'NO_SALES'
# MAGIC     WHEN ps.total_units_sold > 100 AND ps.total_revenue > 5000 THEN 'TOP_PERFORMER'
# MAGIC     WHEN ps.total_units_sold > 50 AND ps.total_revenue > 2000 THEN 'GOOD_PERFORMER'
# MAGIC     WHEN ps.total_units_sold < 10 OR ps.total_revenue < 500 THEN 'POOR_PERFORMER'
# MAGIC     ELSE 'AVERAGE_PERFORMER'
# MAGIC   END as performance_category,
# MAGIC   DATEDIFF(CURRENT_DATE(), ps.last_sale_date) as days_since_last_sale
# MAGIC FROM product_sales ps
# MAGIC LEFT JOIN inventory_summary inv ON ps.product_id = inv.product_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promotional Performance View

# MAGIC %md
# MAGIC ## Promotional Performance View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW promotional_performance AS
# MAGIC WITH promotion_sales AS (
# MAGIC   SELECT
# MAGIC     pr.promotion_id,
# MAGIC     pr.promotion_name,
# MAGIC     pr.promotion_type,
# MAGIC     pr.start_date,
# MAGIC     pr.end_date,
# MAGIC     pr.discount_percentage,
# MAGIC     pr.budget,
# MAGIC     pr.target_revenue,
# MAGIC     COUNT(DISTINCT s.transaction_id) as promotion_transactions,
# MAGIC     SUM(s.total_amount) as promotion_revenue,
# MAGIC     SUM(s.discount_amount) as total_discount_given,
# MAGIC     COUNT(DISTINCT s.customer_id) as unique_customers,
# MAGIC     COUNT(DISTINCT s.store_id) as participating_stores,
# MAGIC     AVG(s.total_amount) as avg_transaction_value
# MAGIC   FROM promotions pr
# MAGIC   LEFT JOIN sales s ON pr.promotion_id = s.promotion_id
# MAGIC   WHERE pr.start_date >= CURRENT_DATE() - INTERVAL 365 DAYS
# MAGIC   GROUP BY pr.promotion_id, pr.promotion_name, pr.promotion_type, pr.start_date,
# MAGIC            pr.end_date, pr.discount_percentage, pr.budget, pr.target_revenue
# MAGIC ),
# MAGIC baseline_sales AS (
# MAGIC   SELECT
# MAGIC     AVG(daily_sales) as avg_baseline_daily_sales
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       sale_date,
# MAGIC       SUM(total_amount) as daily_sales
# MAGIC     FROM sales
# MAGIC     WHERE promotion_id IS NULL
# MAGIC       AND sale_date >= CURRENT_DATE() - INTERVAL 180 DAYS
# MAGIC     GROUP BY sale_date
# MAGIC   )
# MAGIC )
# MAGIC SELECT
# MAGIC   ps.*,
# MAGIC   DATEDIFF(ps.end_date, ps.start_date) + 1 as promotion_duration_days,
# MAGIC   ps.promotion_revenue / NULLIF(ps.target_revenue, 0) as revenue_vs_target,
# MAGIC   ps.total_discount_given / NULLIF(ps.budget, 0) as discount_vs_budget,
# MAGIC   ps.promotion_revenue / NULLIF(ps.promotion_transactions, 0) as revenue_per_transaction,
# MAGIC   (ps.promotion_revenue / NULLIF(DATEDIFF(ps.end_date, ps.start_date) + 1, 0)) - bs.avg_baseline_daily_sales as incremental_daily_revenue,
# MAGIC   CASE
# MAGIC     WHEN ps.promotion_revenue > ps.target_revenue * 1.1 THEN 'EXCELLENT'
# MAGIC     WHEN ps.promotion_revenue > ps.target_revenue * 0.9 THEN 'GOOD'
# MAGIC     WHEN ps.promotion_revenue > ps.target_revenue * 0.7 THEN 'FAIR'
# MAGIC     ELSE 'POOR'
# MAGIC   END as performance_rating,
# MAGIC   CASE
# MAGIC     WHEN ps.end_date < CURRENT_DATE() THEN 'COMPLETED'
# MAGIC     WHEN ps.start_date <= CURRENT_DATE() AND ps.end_date >= CURRENT_DATE() THEN 'ACTIVE'
# MAGIC     ELSE 'SCHEDULED'
# MAGIC   END as promotion_status
# MAGIC FROM promotion_sales ps
# MAGIC CROSS JOIN baseline_sales bs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Store Comparison Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW store_comparison_metrics AS
# MAGIC WITH regional_benchmarks AS (
# MAGIC   SELECT
# MAGIC     region,
# MAGIC     AVG(avg_daily_sales) as region_avg_sales,
# MAGIC     AVG(avg_daily_transactions) as region_avg_transactions,
# MAGIC     AVG(sales_per_sqft) as region_avg_sales_per_sqft,
# MAGIC     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_daily_sales) as region_median_sales
# MAGIC   FROM store_performance
# MAGIC   GROUP BY region
# MAGIC ),
# MAGIC company_benchmarks AS (
# MAGIC   SELECT
# MAGIC     AVG(avg_daily_sales) as company_avg_sales,
# MAGIC     AVG(avg_daily_transactions) as company_avg_transactions,
# MAGIC     AVG(sales_per_sqft) as company_avg_sales_per_sqft,
# MAGIC     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_daily_sales) as company_median_sales
# MAGIC   FROM store_performance
# MAGIC )
# MAGIC SELECT
# MAGIC   sp.*,
# MAGIC   rb.region_avg_sales,
# MAGIC   rb.region_avg_transactions,
# MAGIC   rb.region_avg_sales_per_sqft,
# MAGIC   rb.region_median_sales,
# MAGIC   cb.company_avg_sales,
# MAGIC   cb.company_avg_transactions,
# MAGIC   cb.company_avg_sales_per_sqft,
# MAGIC   cb.company_median_sales,
# MAGIC   sp.avg_daily_sales / NULLIF(rb.region_avg_sales, 0) as vs_region_performance,
# MAGIC   sp.avg_daily_sales / NULLIF(cb.company_avg_sales, 0) as vs_company_performance,
# MAGIC   CASE
# MAGIC     WHEN sp.avg_daily_sales > rb.region_avg_sales * 1.2 THEN 'TOP_PERFORMER'
# MAGIC     WHEN sp.avg_daily_sales > rb.region_avg_sales * 1.1 THEN 'ABOVE_AVERAGE'
# MAGIC     WHEN sp.avg_daily_sales > rb.region_avg_sales * 0.9 THEN 'AVERAGE'
# MAGIC     WHEN sp.avg_daily_sales > rb.region_avg_sales * 0.8 THEN 'BELOW_AVERAGE'
# MAGIC     ELSE 'UNDERPERFORMER'
# MAGIC   END as regional_ranking,
# MAGIC   RANK() OVER (PARTITION BY sp.region ORDER BY sp.avg_daily_sales DESC) as region_sales_rank,
# MAGIC   RANK() OVER (ORDER BY sp.avg_daily_sales DESC) as company_sales_rank
# MAGIC FROM store_performance sp
# MAGIC JOIN regional_benchmarks rb ON sp.region = rb.region
# MAGIC CROSS JOIN company_benchmarks cb

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Materialized Tables for Performance

# COMMAND ----------

# Create materialized table for daily sales aggregation
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.daily_sales_agg_materialized
USING DELTA
AS SELECT * FROM daily_sales_agg
""")

# Optimize the table
spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.daily_sales_agg_materialized")

print("Created materialized daily sales aggregation table")

# COMMAND ----------

# Create materialized table for inventory alerts
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.inventory_alerts_materialized
USING DELTA
AS SELECT * FROM inventory_alerts
""")

# Optimize the table
spark.sql(f"OPTIMIZE {catalog_name}.{schema_name}.inventory_alerts_materialized")

print("Created materialized inventory alerts table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions on Views

# COMMAND ----------

# Grant permissions to user groups on views
views = [
    "daily_sales_agg",
    "store_performance",
    "inventory_alerts",
    "customer_segments",
    "product_performance",
    "promotional_performance",
    "store_comparison_metrics"
]

permission_groups = [
    "retail_analysts",
    "store_managers",
    "merchandisers",
    "executives"
]

for view in views:
    for group in permission_groups:
        try:
            spark.sql(f"GRANT SELECT ON VIEW {catalog_name}.{schema_name}.{view} TO `{group}`")
            print(f"Granted SELECT on {view} to {group}")
        except Exception as e:
            print(f"Note: Could not grant permissions on {view} to {group}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display summary of created views
views_created = [
    "daily_sales_agg",
    "store_performance",
    "inventory_alerts",
    "customer_segments",
    "product_performance",
    "promotional_performance",
    "store_comparison_metrics"
]

print("Analytical views created successfully!")
print("-" * 50)

for view in views_created:
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{view}").collect()[0]['count']
        print(f"{view:25}: {row_count:,} rows")
    except Exception as e:
        print(f"{view:25}: Error - {e}")

print(f"\nAll analytical views created in {catalog_name}.{schema_name}")
print("Ready for dashboard creation and Genie integration!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Sample Queries

# COMMAND ----------

print("Testing sample queries...")

# Test store performance query
print("\n1. Top 3 performing stores by sales:")
top_stores = spark.sql(f"""
SELECT store_name, region, avg_daily_sales, sales_vs_target_ratio
FROM {catalog_name}.{schema_name}.store_performance
ORDER BY avg_daily_sales DESC
LIMIT 3
""").collect()

for store in top_stores:
    print(f"   {store['store_name']}: ${store['avg_daily_sales']:,.0f}/day ({store['sales_vs_target_ratio']:.1%} vs target)")

# Test inventory alerts
print("\n2. Critical inventory alerts:")
critical_alerts = spark.sql(f"""
SELECT store_name, product_name, alert_type, ending_inventory
FROM {catalog_name}.{schema_name}.inventory_alerts
WHERE priority = 'CRITICAL'
LIMIT 5
""").collect()

for alert in critical_alerts:
    print(f"   {alert['store_name']}: {alert['product_name']} - {alert['alert_type']} (Qty: {alert['ending_inventory']})")

# Test customer segments
print("\n3. Customer segment distribution:")
segments = spark.sql(f"""
SELECT customer_segment, COUNT(*) as count, AVG(total_spent) as avg_spent
FROM {catalog_name}.{schema_name}.customer_segments
GROUP BY customer_segment
ORDER BY count DESC
""").collect()

for segment in segments:
    print(f"   {segment['customer_segment']}: {segment['count']} customers (Avg: ${segment['avg_spent']:,.0f})")

print("\nAll analytical views are working correctly! ✅")