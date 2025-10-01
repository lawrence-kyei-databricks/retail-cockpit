-- Store Manager Dashboard Queries
-- These queries power the Store Manager Cockpit dashboard
-- Optimized for single-store or store-manager-level analysis

-- Query 1: Daily Sales Performance vs Target
-- Shows current day, week, and month performance against targets
SELECT
  'Today' as period,
  COALESCE(SUM(total_sales), 0) as actual_sales,
  MAX(daily_sales_target) as target_sales,
  COALESCE(SUM(total_sales), 0) / MAX(daily_sales_target) as performance_ratio,
  CASE
    WHEN COALESCE(SUM(total_sales), 0) / MAX(daily_sales_target) >= 1.1 THEN 'Excellent'
    WHEN COALESCE(SUM(total_sales), 0) / MAX(daily_sales_target) >= 1.0 THEN 'On Target'
    WHEN COALESCE(SUM(total_sales), 0) / MAX(daily_sales_target) >= 0.9 THEN 'Close'
    ELSE 'Below Target'
  END as status
FROM daily_sales_agg dsa
JOIN store_performance sp ON dsa.store_id = sp.store_id
WHERE dsa.sale_date = CURRENT_DATE()
  AND dsa.store_id = '{{ store_id }}'

UNION ALL

SELECT
  'This Week' as period,
  COALESCE(SUM(total_sales), 0) as actual_sales,
  MAX(daily_sales_target) * 7 as target_sales,
  COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * 7) as performance_ratio,
  CASE
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * 7) >= 1.1 THEN 'Excellent'
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * 7) >= 1.0 THEN 'On Target'
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * 7) >= 0.9 THEN 'Close'
    ELSE 'Below Target'
  END as status
FROM daily_sales_agg dsa
JOIN store_performance sp ON dsa.store_id = sp.store_id
WHERE dsa.sale_date >= DATE_TRUNC('week', CURRENT_DATE())
  AND dsa.store_id = '{{ store_id }}'

UNION ALL

SELECT
  'This Month' as period,
  COALESCE(SUM(total_sales), 0) as actual_sales,
  MAX(daily_sales_target) * DAY(LAST_DAY(CURRENT_DATE())) as target_sales,
  COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * DAY(LAST_DAY(CURRENT_DATE()))) as performance_ratio,
  CASE
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * DAY(LAST_DAY(CURRENT_DATE()))) >= 1.1 THEN 'Excellent'
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * DAY(LAST_DAY(CURRENT_DATE()))) >= 1.0 THEN 'On Target'
    WHEN COALESCE(SUM(total_sales), 0) / (MAX(daily_sales_target) * DAY(LAST_DAY(CURRENT_DATE()))) >= 0.9 THEN 'Close'
    ELSE 'Below Target'
  END as status
FROM daily_sales_agg dsa
JOIN store_performance sp ON dsa.store_id = sp.store_id
WHERE dsa.sale_date >= DATE_TRUNC('month', CURRENT_DATE())
  AND dsa.store_id = '{{ store_id }}';

-- Query 2: Hourly Sales Trend (Last 7 Days)
-- Shows sales pattern by hour to identify peak times
SELECT
  HOUR(sale_timestamp) as hour_of_day,
  AVG(total_amount) as avg_transaction_value,
  COUNT(*) as transaction_count,
  SUM(total_amount) as total_sales
FROM sales
WHERE sale_date >= CURRENT_DATE() - INTERVAL 7 DAYS
  AND store_id = '{{ store_id }}'
GROUP BY HOUR(sale_timestamp)
ORDER BY hour_of_day;

-- Query 3: Top 10 Selling Products (Last 30 Days)
-- Identifies best-performing products for the store
SELECT
  p.product_name,
  p.sku,
  c.category_name,
  SUM(s.quantity) as units_sold,
  SUM(s.total_amount) as revenue,
  AVG(s.total_amount / s.quantity) as avg_selling_price,
  COUNT(DISTINCT s.customer_id) as unique_customers,
  SUM(s.total_amount - (p.unit_cost * s.quantity)) as gross_profit
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND s.store_id = '{{ store_id }}'
GROUP BY p.product_name, p.sku, c.category_name, p.unit_cost
ORDER BY revenue DESC
LIMIT 10;

-- Query 4: Critical Inventory Alerts
-- Shows stockouts and low inventory requiring immediate attention
SELECT
  ia.product_name,
  ia.sku,
  ia.category_name,
  ia.ending_inventory,
  ia.safety_stock,
  ia.reorder_point,
  ia.alert_type,
  ia.priority,
  ia.days_since_last_sale,
  COALESCE(ia.avg_daily_sales, 0) as avg_daily_sales,
  CASE
    WHEN ia.avg_daily_sales > 0 THEN ia.ending_inventory / ia.avg_daily_sales
    ELSE 999
  END as days_of_stock_remaining
FROM inventory_alerts ia
WHERE ia.store_id = '{{ store_id }}'
  AND ia.priority IN ('CRITICAL', 'HIGH')
ORDER BY
  CASE ia.priority
    WHEN 'CRITICAL' THEN 1
    WHEN 'HIGH' THEN 2
    ELSE 3
  END,
  ia.ending_inventory ASC;

-- Query 5: Customer Traffic and Conversion
-- Daily customer metrics with conversion rates
SELECT
  sale_date,
  COUNT(DISTINCT transaction_id) as transactions,
  COUNT(DISTINCT customer_id) as unique_customers,
  SUM(total_amount) as daily_sales,
  AVG(total_amount) as avg_transaction_value,
  -- Assume footfall is 1.5x unique customers (industry estimate)
  COUNT(DISTINCT customer_id) * 1.5 as estimated_footfall,
  COUNT(DISTINCT transaction_id) / (COUNT(DISTINCT customer_id) * 1.5) as conversion_rate
FROM sales
WHERE sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND store_id = '{{ store_id }}'
GROUP BY sale_date
ORDER BY sale_date DESC;

-- Query 6: Returns Analysis
-- Track returns by product and reason
SELECT
  p.product_name,
  p.sku,
  c.category_name,
  COUNT(*) as return_count,
  SUM(s.total_amount) as return_value,
  AVG(s.total_amount) as avg_return_value,
  MAX(s.sale_date) as last_return_date
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE s.return_flag = true
  AND s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND s.store_id = '{{ store_id }}'
GROUP BY p.product_name, p.sku, c.category_name
ORDER BY return_value DESC
LIMIT 10;

-- Query 7: Customer Loyalty Analysis
-- Breakdown of customers by loyalty tier and behavior
SELECT
  cs.loyalty_tier,
  COUNT(*) as customer_count,
  AVG(cs.total_spent) as avg_spent,
  AVG(cs.total_transactions) as avg_transactions,
  AVG(cs.avg_transaction_value) as avg_transaction_value,
  COUNT(CASE WHEN cs.churn_risk = 'HIGH' THEN 1 END) as high_churn_risk,
  COUNT(CASE WHEN cs.days_since_last_purchase <= 30 THEN 1 END) as active_last_30_days
FROM customer_segments cs
WHERE cs.preferred_store_id = '{{ store_id }}'
GROUP BY cs.loyalty_tier
ORDER BY avg_spent DESC;

-- Query 8: Category Performance Comparison
-- Sales performance by category for the store
SELECT
  c.category_name,
  COUNT(DISTINCT s.product_id) as products_sold,
  SUM(s.quantity) as total_units,
  SUM(s.total_amount) as total_revenue,
  AVG(s.total_amount / s.quantity) as avg_price,
  SUM(s.total_amount - (p.unit_cost * s.quantity)) as gross_profit,
  (SUM(s.total_amount - (p.unit_cost * s.quantity)) / SUM(s.total_amount)) * 100 as margin_percent
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  AND s.store_id = '{{ store_id }}'
GROUP BY c.category_name
ORDER BY total_revenue DESC;

-- Query 9: Weekly Performance Trend
-- Week-over-week performance comparison
WITH weekly_sales AS (
  SELECT
    DATE_TRUNC('week', sale_date) as week_start,
    SUM(total_amount) as weekly_sales,
    COUNT(DISTINCT transaction_id) as weekly_transactions,
    COUNT(DISTINCT customer_id) as weekly_customers
  FROM sales
  WHERE sale_date >= CURRENT_DATE() - INTERVAL 12 WEEKS
    AND store_id = '{{ store_id }}'
  GROUP BY DATE_TRUNC('week', sale_date)
),
weekly_comparison AS (
  SELECT
    week_start,
    weekly_sales,
    weekly_transactions,
    weekly_customers,
    LAG(weekly_sales) OVER (ORDER BY week_start) as prev_week_sales,
    LAG(weekly_transactions) OVER (ORDER BY week_start) as prev_week_transactions
  FROM weekly_sales
)
SELECT
  week_start,
  weekly_sales,
  weekly_transactions,
  weekly_customers,
  prev_week_sales,
  CASE
    WHEN prev_week_sales > 0 THEN ((weekly_sales - prev_week_sales) / prev_week_sales) * 100
    ELSE 0
  END as sales_growth_percent,
  CASE
    WHEN prev_week_transactions > 0 THEN ((weekly_transactions - prev_week_transactions) / prev_week_transactions) * 100
    ELSE 0
  END as transaction_growth_percent
FROM weekly_comparison
WHERE week_start >= CURRENT_DATE() - INTERVAL 8 WEEKS
ORDER BY week_start DESC;

-- Query 10: Promotional Impact
-- Current and recent promotions performance
SELECT
  pp.promotion_name,
  pp.promotion_type,
  pp.start_date,
  pp.end_date,
  pp.promotion_status,
  pp.promotion_revenue,
  pp.total_discount_given,
  pp.promotion_transactions,
  pp.unique_customers,
  pp.performance_rating,
  pp.incremental_daily_revenue
FROM promotional_performance pp
WHERE pp.participating_stores = 'All'
   OR pp.participating_stores LIKE '%{{ store_id }}%'
ORDER BY pp.start_date DESC
LIMIT 10;