-- Merchandiser Dashboard Queries
-- These queries power the Merchandiser Analytics dashboard
-- Focused on product performance, category trends, pricing, and promotional effectiveness

-- Query 1: Category Performance Overview
-- High-level category performance metrics with trends
SELECT
  c.category_name,
  c.parent_category_id,
  pc.category_name as department_name,
  COUNT(DISTINCT p.product_id) as total_products,
  COUNT(DISTINCT CASE WHEN pp.performance_category = 'TOP_PERFORMER' THEN p.product_id END) as top_performers,
  COUNT(DISTINCT CASE WHEN pp.performance_category = 'POOR_PERFORMER' THEN p.product_id END) as poor_performers,
  SUM(pp.total_revenue) as category_revenue,
  SUM(pp.gross_profit) as category_profit,
  AVG(pp.margin_percentage) as avg_margin_percent,
  SUM(pp.total_units_sold) as total_units_sold,
  AVG(pp.avg_days_on_hand) as avg_inventory_days,
  COUNT(DISTINCT CASE WHEN pp.stockout_stores > 0 THEN p.product_id END) as products_with_stockouts
FROM categories c
LEFT JOIN categories pc ON c.parent_category_id = pc.category_id
JOIN products p ON c.category_id = p.category_id
LEFT JOIN product_performance pp ON p.product_id = pp.product_id
WHERE c.category_level = 2  -- Category level (not department or subcategory)
GROUP BY c.category_name, c.parent_category_id, pc.category_name
ORDER BY category_revenue DESC;

-- Query 2: Product Performance Ranking
-- Detailed product analysis with performance scoring
WITH product_scores AS (
  SELECT
    pp.*,
    CASE
      WHEN pp.performance_category = 'TOP_PERFORMER' THEN 5
      WHEN pp.performance_category = 'GOOD_PERFORMER' THEN 4
      WHEN pp.performance_category = 'AVERAGE_PERFORMER' THEN 3
      WHEN pp.performance_category = 'POOR_PERFORMER' THEN 2
      ELSE 1
    END as performance_score,
    ROW_NUMBER() OVER (PARTITION BY pp.category_id ORDER BY pp.total_revenue DESC) as category_rank
  FROM product_performance pp
)
SELECT
  ps.sku,
  ps.product_name,
  ps.category_name,
  ps.brand,
  ps.total_revenue,
  ps.total_units_sold,
  ps.gross_profit,
  ps.margin_percentage,
  ps.performance_category,
  ps.category_rank,
  ps.current_inventory,
  ps.avg_days_on_hand,
  ps.stockout_stores,
  ps.days_since_last_sale,
  CASE
    WHEN ps.margin_percentage > 40 THEN 'High Margin'
    WHEN ps.margin_percentage > 25 THEN 'Medium Margin'
    ELSE 'Low Margin'
  END as margin_tier,
  CASE
    WHEN ps.avg_days_on_hand > 60 THEN 'Slow Moving'
    WHEN ps.avg_days_on_hand > 30 THEN 'Normal'
    ELSE 'Fast Moving'
  END as velocity_tier
FROM product_scores ps
WHERE ps.category_rank <= 20  -- Top 20 in each category
ORDER BY ps.total_revenue DESC;

-- Query 3: Seasonal Trend Analysis
-- Monthly sales trends by category and season
SELECT
  DATE_TRUNC('month', s.sale_date) as sales_month,
  c.category_name,
  p.season,
  p.is_seasonal,
  SUM(s.total_amount) as monthly_revenue,
  SUM(s.quantity) as monthly_units,
  AVG(s.total_amount / s.quantity) as avg_selling_price,
  COUNT(DISTINCT s.store_id) as stores_with_sales,
  COUNT(DISTINCT s.customer_id) as unique_customers,
  SUM(s.total_amount - (p.unit_cost * s.quantity)) as monthly_profit
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 12 MONTHS
  AND c.category_level = 2
GROUP BY DATE_TRUNC('month', s.sale_date), c.category_name, p.season, p.is_seasonal
ORDER BY sales_month DESC, monthly_revenue DESC;

-- Query 4: Pricing Analysis and Optimization
-- Price performance analysis with elasticity insights
WITH price_analysis AS (
  SELECT
    p.product_id,
    p.sku,
    p.product_name,
    p.retail_price as current_price,
    p.unit_cost,
    AVG(s.total_amount / s.quantity) as avg_selling_price,
    MIN(s.total_amount / s.quantity) as min_selling_price,
    MAX(s.total_amount / s.quantity) as max_selling_price,
    STDDEV(s.total_amount / s.quantity) as price_volatility,
    SUM(s.quantity) as total_units_sold,
    SUM(s.total_amount) as total_revenue,
    COUNT(DISTINCT s.sale_date) as active_days,
    SUM(CASE WHEN s.discount_amount > 0 THEN s.quantity ELSE 0 END) as discounted_units,
    SUM(s.discount_amount) as total_discounts
  FROM products p
  JOIN sales s ON p.product_id = s.product_id
  WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
  GROUP BY p.product_id, p.sku, p.product_name, p.retail_price, p.unit_cost
)
SELECT
  pa.sku,
  pa.product_name,
  pa.current_price,
  pa.avg_selling_price,
  pa.current_price - pa.avg_selling_price as price_variance,
  (pa.current_price - pa.avg_selling_price) / pa.current_price * 100 as price_variance_percent,
  pa.unit_cost,
  (pa.avg_selling_price - pa.unit_cost) / pa.avg_selling_price * 100 as actual_margin_percent,
  pa.total_units_sold,
  pa.total_revenue,
  pa.discounted_units / pa.total_units_sold * 100 as discount_rate_percent,
  pa.total_discounts / pa.total_revenue * 100 as discount_impact_percent,
  CASE
    WHEN pa.price_volatility / pa.avg_selling_price > 0.15 THEN 'High Volatility'
    WHEN pa.price_volatility / pa.avg_selling_price > 0.05 THEN 'Medium Volatility'
    ELSE 'Stable Pricing'
  END as price_stability,
  CASE
    WHEN pa.avg_selling_price > pa.current_price * 1.05 THEN 'Underpriced'
    WHEN pa.avg_selling_price < pa.current_price * 0.95 THEN 'Overpriced'
    ELSE 'Optimal'
  END as pricing_recommendation
FROM price_analysis pa
ORDER BY pa.total_revenue DESC;

-- Query 5: Promotional Effectiveness Analysis
-- Deep dive into promotion performance by category and type
SELECT
  pp.promotion_name,
  pp.promotion_type,
  pp.start_date,
  pp.end_date,
  pp.promotion_duration_days,
  pp.discount_percentage,
  pp.promotion_revenue,
  pp.total_discount_given,
  pp.promotion_transactions,
  pp.unique_customers,
  pp.performance_rating,
  pp.incremental_daily_revenue,
  pp.revenue_vs_target,
  pp.promotion_revenue / pp.promotion_transactions as avg_promotion_transaction,
  pp.total_discount_given / pp.promotion_revenue * 100 as discount_rate,
  -- Calculate ROI (incremental revenue vs discount cost)
  CASE
    WHEN pp.total_discount_given > 0 THEN
      (pp.incremental_daily_revenue * pp.promotion_duration_days) / pp.total_discount_given
    ELSE 0
  END as promotion_roi,
  CASE
    WHEN pp.performance_rating = 'EXCELLENT' THEN 'Repeat'
    WHEN pp.performance_rating = 'GOOD' THEN 'Consider Repeat'
    WHEN pp.performance_rating = 'FAIR' THEN 'Optimize'
    ELSE 'Discontinue'
  END as recommendation
FROM promotional_performance pp
WHERE pp.start_date >= CURRENT_DATE() - INTERVAL 365 DAYS
ORDER BY pp.promotion_roi DESC;

-- Query 6: New Product Introduction Analysis
-- Performance tracking for recently launched products
SELECT
  p.product_id,
  p.sku,
  p.product_name,
  p.category_name,
  p.brand,
  p.launch_date,
  DATEDIFF(CURRENT_DATE(), p.launch_date) as days_since_launch,
  COALESCE(pp.total_units_sold, 0) as units_sold,
  COALESCE(pp.total_revenue, 0) as revenue,
  COALESCE(pp.unique_customers, 0) as customers_reached,
  COALESCE(pp.stores_sold_in, 0) as store_penetration,
  COALESCE(pp.margin_percentage, 0) as margin_percent,
  COALESCE(pp.current_inventory, 0) as current_stock,
  CASE
    WHEN p.launch_date <= CURRENT_DATE() - INTERVAL 90 DAYS THEN
      CASE
        WHEN COALESCE(pp.total_revenue, 0) > 10000 THEN 'Successful Launch'
        WHEN COALESCE(pp.total_revenue, 0) > 5000 THEN 'Moderate Success'
        WHEN COALESCE(pp.total_revenue, 0) > 1000 THEN 'Slow Start'
        ELSE 'Poor Performance'
      END
    ELSE 'Too Early to Assess'
  END as launch_performance,
  -- Weekly velocity since launch
  COALESCE(pp.total_revenue, 0) / (DATEDIFF(CURRENT_DATE(), p.launch_date) / 7.0) as weekly_revenue_velocity
FROM products p
LEFT JOIN product_performance pp ON p.product_id = pp.product_id
WHERE p.launch_date >= CURRENT_DATE() - INTERVAL 365 DAYS
ORDER BY p.launch_date DESC;

-- Query 7: Competitor Price Comparison (Simulated)
-- Analysis of pricing position vs estimated market prices
WITH market_prices AS (
  SELECT
    category_id,
    AVG(retail_price) as category_avg_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY retail_price) as category_median_price,
    MIN(retail_price) as category_min_price,
    MAX(retail_price) as category_max_price
  FROM products
  WHERE is_active = true
  GROUP BY category_id
)
SELECT
  p.sku,
  p.product_name,
  c.category_name,
  p.brand,
  p.retail_price,
  mp.category_avg_price,
  mp.category_median_price,
  p.retail_price - mp.category_avg_price as price_vs_avg,
  (p.retail_price - mp.category_avg_price) / mp.category_avg_price * 100 as price_vs_avg_percent,
  CASE
    WHEN p.retail_price > mp.category_avg_price * 1.2 THEN 'Premium Pricing'
    WHEN p.retail_price > mp.category_avg_price * 1.1 THEN 'Above Market'
    WHEN p.retail_price < mp.category_avg_price * 0.9 THEN 'Below Market'
    WHEN p.retail_price < mp.category_avg_price * 0.8 THEN 'Value Pricing'
    ELSE 'Market Rate'
  END as pricing_position,
  COALESCE(pp.total_units_sold, 0) as units_sold,
  COALESCE(pp.margin_percentage, 0) as margin_percent
FROM products p
JOIN categories c ON p.category_id = c.category_id
JOIN market_prices mp ON p.category_id = mp.category_id
LEFT JOIN product_performance pp ON p.product_id = pp.product_id
WHERE p.is_active = true
ORDER BY p.retail_price DESC;

-- Query 8: Inventory Turnover by Category
-- Inventory management metrics for merchandising decisions
SELECT
  c.category_name,
  COUNT(DISTINCT p.product_id) as total_products,
  SUM(COALESCE(pp.current_inventory, 0)) as total_inventory_units,
  SUM(COALESCE(pp.total_revenue, 0)) as category_revenue,
  AVG(COALESCE(pp.avg_days_on_hand, 0)) as avg_days_on_hand,
  SUM(COALESCE(pp.total_units_sold, 0)) /
    NULLIF(SUM(COALESCE(pp.current_inventory, 0)), 0) as inventory_turnover_ratio,
  COUNT(CASE WHEN pp.avg_days_on_hand > 60 THEN 1 END) as slow_moving_products,
  COUNT(CASE WHEN pp.stockout_stores > 0 THEN 1 END) as products_with_stockouts,
  SUM(CASE WHEN pp.avg_days_on_hand > 60 THEN pp.current_inventory ELSE 0 END) as slow_moving_inventory_value,
  -- Calculate dead stock (no sales in 90 days)
  COUNT(CASE WHEN pp.days_since_last_sale > 90 THEN 1 END) as dead_stock_products,
  CASE
    WHEN AVG(COALESCE(pp.avg_days_on_hand, 0)) > 60 THEN 'Overstocked'
    WHEN AVG(COALESCE(pp.avg_days_on_hand, 0)) > 30 THEN 'Normal'
    WHEN AVG(COALESCE(pp.avg_days_on_hand, 0)) > 15 THEN 'Lean'
    ELSE 'Understocked'
  END as inventory_status
FROM categories c
JOIN products p ON c.category_id = p.category_id
LEFT JOIN product_performance pp ON p.product_id = pp.product_id
WHERE c.category_level = 2
GROUP BY c.category_name
ORDER BY category_revenue DESC;

-- Query 9: Cross-Category Analysis
-- Market basket analysis and category relationships
WITH category_combinations AS (
  SELECT
    s1.transaction_id,
    c1.category_name as category1,
    c2.category_name as category2,
    s1.total_amount + s2.total_amount as combined_value
  FROM sales s1
  JOIN sales s2 ON s1.transaction_id = s2.transaction_id AND s1.product_id != s2.product_id
  JOIN products p1 ON s1.product_id = p1.product_id
  JOIN products p2 ON s2.product_id = p2.product_id
  JOIN categories c1 ON p1.category_id = c1.category_id
  JOIN categories c2 ON p2.category_id = c2.category_id
  WHERE s1.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
    AND c1.category_level = 2 AND c2.category_level = 2
    AND c1.category_name < c2.category_name  -- Avoid duplicates
)
SELECT
  category1,
  category2,
  COUNT(*) as co_purchase_frequency,
  AVG(combined_value) as avg_combined_value,
  COUNT(*) * 100.0 / (
    SELECT COUNT(DISTINCT transaction_id)
    FROM sales
    WHERE sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
  ) as co_purchase_rate_percent,
  CASE
    WHEN COUNT(*) > 100 THEN 'Strong Affinity'
    WHEN COUNT(*) > 50 THEN 'Moderate Affinity'
    WHEN COUNT(*) > 20 THEN 'Weak Affinity'
    ELSE 'Rare Combination'
  END as affinity_strength
FROM category_combinations
GROUP BY category1, category2
HAVING COUNT(*) >= 10  -- Minimum threshold for significance
ORDER BY co_purchase_frequency DESC
LIMIT 20;

-- Query 10: Markdown and Clearance Analysis
-- Track products requiring markdowns or clearance
SELECT
  p.sku,
  p.product_name,
  c.category_name,
  p.brand,
  p.retail_price,
  p.unit_cost,
  COALESCE(pp.current_inventory, 0) as current_inventory,
  COALESCE(pp.avg_days_on_hand, 0) as days_on_hand,
  COALESCE(pp.days_since_last_sale, 0) as days_since_last_sale,
  COALESCE(pp.total_revenue, 0) as revenue_90_days,
  COALESCE(pp.margin_percentage, 0) as current_margin,
  p.retail_price * COALESCE(pp.current_inventory, 0) as inventory_value,
  CASE
    WHEN pp.days_since_last_sale > 90 AND pp.current_inventory > 10 THEN 'Immediate Clearance'
    WHEN pp.avg_days_on_hand > 120 THEN 'Heavy Markdown Needed'
    WHEN pp.avg_days_on_hand > 90 THEN 'Markdown Candidate'
    WHEN pp.avg_days_on_hand > 60 THEN 'Monitor for Markdown'
    ELSE 'No Action Needed'
  END as markdown_recommendation,
  CASE
    WHEN pp.days_since_last_sale > 90 THEN p.retail_price * 0.5  -- 50% off
    WHEN pp.avg_days_on_hand > 120 THEN p.retail_price * 0.6   -- 40% off
    WHEN pp.avg_days_on_hand > 90 THEN p.retail_price * 0.7    -- 30% off
    WHEN pp.avg_days_on_hand > 60 THEN p.retail_price * 0.8    -- 20% off
    ELSE p.retail_price
  END as suggested_markdown_price
FROM products p
JOIN categories c ON p.category_id = c.category_id
LEFT JOIN product_performance pp ON p.product_id = pp.product_id
WHERE p.is_active = true
  AND (pp.avg_days_on_hand > 60 OR pp.days_since_last_sale > 60)
ORDER BY
  CASE
    WHEN pp.days_since_last_sale > 90 THEN 1
    WHEN pp.avg_days_on_hand > 120 THEN 2
    WHEN pp.avg_days_on_hand > 90 THEN 3
    ELSE 4
  END,
  inventory_value DESC;