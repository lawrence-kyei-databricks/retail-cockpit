-- Executive Dashboard Queries
-- These queries power the Executive Summary dashboard
-- Focused on high-level KPIs, performance trends, and strategic insights

-- Query 1: Executive KPI Summary
-- Top-level metrics for executive overview
WITH current_period AS (
  SELECT
    SUM(total_sales) as current_revenue,
    SUM(total_sales - (p.unit_cost * dsa.total_quantity)) as current_profit,
    COUNT(DISTINCT dsa.store_id) as active_stores,
    SUM(transaction_count) as total_transactions,
    SUM(unique_customers) as total_customers
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('month', CURRENT_DATE())
),
previous_period AS (
  SELECT
    SUM(total_sales) as prev_revenue,
    SUM(total_sales - (p.unit_cost * dsa.total_quantity)) as prev_profit,
    SUM(transaction_count) as prev_transactions,
    SUM(unique_customers) as prev_customers
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 1 MONTH)
    AND dsa.sale_date < DATE_TRUNC('month', CURRENT_DATE())
),
ytd_performance AS (
  SELECT
    SUM(total_sales) as ytd_revenue,
    SUM(total_sales - (p.unit_cost * dsa.total_quantity)) as ytd_profit
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('year', CURRENT_DATE())
)
SELECT
  'Revenue' as metric_name,
  CONCAT('$', FORMAT_NUMBER(cp.current_revenue, 0)) as current_value,
  CONCAT(
    CASE WHEN pp.prev_revenue > 0 THEN
      ROUND((cp.current_revenue - pp.prev_revenue) / pp.prev_revenue * 100, 1)
    ELSE 0 END,
    '%'
  ) as month_over_month,
  CONCAT('$', FORMAT_NUMBER(yp.ytd_revenue, 0)) as ytd_value,
  'Monthly' as frequency
FROM current_period cp, previous_period pp, ytd_performance yp

UNION ALL

SELECT
  'Gross Profit' as metric_name,
  CONCAT('$', FORMAT_NUMBER(cp.current_profit, 0)) as current_value,
  CONCAT(
    CASE WHEN pp.prev_profit > 0 THEN
      ROUND((cp.current_profit - pp.prev_profit) / pp.prev_profit * 100, 1)
    ELSE 0 END,
    '%'
  ) as month_over_month,
  CONCAT('$', FORMAT_NUMBER(yp.ytd_profit, 0)) as ytd_value,
  'Monthly' as frequency
FROM current_period cp, previous_period pp, ytd_performance yp

UNION ALL

SELECT
  'Profit Margin' as metric_name,
  CONCAT(ROUND(cp.current_profit / cp.current_revenue * 100, 1), '%') as current_value,
  CONCAT(
    ROUND(cp.current_profit / cp.current_revenue * 100, 1) -
    ROUND(pp.prev_profit / pp.prev_revenue * 100, 1),
    'pp'
  ) as month_over_month,
  CONCAT(ROUND(yp.ytd_profit / yp.ytd_revenue * 100, 1), '%') as ytd_value,
  'Monthly' as frequency
FROM current_period cp, previous_period pp, ytd_performance yp

UNION ALL

SELECT
  'Active Stores' as metric_name,
  cp.active_stores as current_value,
  '0' as month_over_month,
  cp.active_stores as ytd_value,
  'Count' as frequency
FROM current_period cp, previous_period pp, ytd_performance yp;

-- Query 2: Revenue Trend Analysis
-- Monthly revenue trend with year-over-year comparison
WITH monthly_revenue AS (
  SELECT
    DATE_TRUNC('month', dsa.sale_date) as month_year,
    SUM(dsa.total_sales) as monthly_revenue,
    SUM(dsa.total_sales - (p.unit_cost * dsa.total_quantity)) as monthly_profit,
    COUNT(DISTINCT dsa.store_id) as active_stores,
    SUM(dsa.transaction_count) as monthly_transactions
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  WHERE dsa.sale_date >= CURRENT_DATE() - INTERVAL 24 MONTHS
  GROUP BY DATE_TRUNC('month', dsa.sale_date)
),
yoy_comparison AS (
  SELECT
    mr.month_year,
    mr.monthly_revenue,
    mr.monthly_profit,
    mr.active_stores,
    mr.monthly_transactions,
    LAG(mr.monthly_revenue, 12) OVER (ORDER BY mr.month_year) as revenue_last_year,
    LAG(mr.monthly_profit, 12) OVER (ORDER BY mr.month_year) as profit_last_year
  FROM monthly_revenue mr
)
SELECT
  month_year,
  monthly_revenue,
  monthly_profit,
  monthly_profit / monthly_revenue * 100 as profit_margin_percent,
  active_stores,
  monthly_transactions,
  revenue_last_year,
  CASE WHEN revenue_last_year > 0 THEN
    (monthly_revenue - revenue_last_year) / revenue_last_year * 100
  ELSE 0 END as yoy_revenue_growth,
  CASE WHEN profit_last_year > 0 THEN
    (monthly_profit - profit_last_year) / profit_last_year * 100
  ELSE 0 END as yoy_profit_growth
FROM yoy_comparison
WHERE month_year >= CURRENT_DATE() - INTERVAL 12 MONTHS
ORDER BY month_year;

-- Query 3: Regional Performance Comparison
-- Performance metrics by region with rankings
WITH regional_metrics AS (
  SELECT
    st.region,
    COUNT(DISTINCT st.store_id) as store_count,
    SUM(dsa.total_sales) as region_revenue,
    SUM(dsa.total_sales - (p.unit_cost * dsa.total_quantity)) as region_profit,
    SUM(dsa.transaction_count) as region_transactions,
    SUM(dsa.unique_customers) as region_customers,
    AVG(dsa.total_sales / dsa.transaction_count) as avg_transaction_value,
    SUM(dsa.total_sales) / COUNT(DISTINCT st.store_id) as revenue_per_store,
    SUM(dsa.total_sales) / SUM(st.square_footage) as revenue_per_sqft
  FROM daily_sales_agg dsa
  JOIN stores st ON dsa.store_id = st.store_id
  JOIN products p ON dsa.category_id = p.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('quarter', CURRENT_DATE())
  GROUP BY st.region
),
regional_targets AS (
  SELECT
    region,
    region_revenue,
    -- Simulate targets based on store count and type
    store_count * 150000 as revenue_target,
    region_profit / region_revenue * 100 as profit_margin,
    35.0 as profit_margin_target
  FROM regional_metrics
)
SELECT
  rm.region,
  rm.store_count,
  rm.region_revenue,
  rm.region_profit,
  rm.profit_margin,
  rt.profit_margin_target,
  rm.revenue_per_store,
  rm.revenue_per_sqft,
  rm.avg_transaction_value,
  rt.revenue_target,
  rm.region_revenue / rt.revenue_target as revenue_vs_target,
  RANK() OVER (ORDER BY rm.region_revenue DESC) as revenue_rank,
  RANK() OVER (ORDER BY rm.profit_margin DESC) as margin_rank,
  RANK() OVER (ORDER BY rm.revenue_per_store DESC) as efficiency_rank,
  CASE
    WHEN rm.region_revenue / rt.revenue_target >= 1.1 THEN 'Exceeding'
    WHEN rm.region_revenue / rt.revenue_target >= 1.0 THEN 'Meeting'
    WHEN rm.region_revenue / rt.revenue_target >= 0.9 THEN 'Close'
    ELSE 'Below Target'
  END as performance_status
FROM regional_metrics rm
JOIN regional_targets rt ON rm.region = rt.region
ORDER BY rm.region_revenue DESC;

-- Query 4: Category Mix Analysis
-- Revenue contribution and trends by category
WITH category_performance AS (
  SELECT
    c.category_name,
    c.parent_category_id,
    SUM(dsa.total_sales) as category_revenue,
    SUM(dsa.total_sales - (p.unit_cost * dsa.total_quantity)) as category_profit,
    SUM(dsa.total_quantity) as units_sold,
    COUNT(DISTINCT dsa.store_id) as stores_selling,
    AVG(p.retail_price) as avg_price_point
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  JOIN categories c ON p.category_id = c.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('quarter', CURRENT_DATE())
    AND c.category_level = 2
  GROUP BY c.category_name, c.parent_category_id
),
total_performance AS (
  SELECT SUM(category_revenue) as total_revenue FROM category_performance
)
SELECT
  cp.category_name,
  cp.category_revenue,
  cp.category_profit,
  cp.category_revenue / tp.total_revenue * 100 as revenue_mix_percent,
  cp.category_profit / cp.category_revenue * 100 as profit_margin_percent,
  cp.units_sold,
  cp.stores_selling,
  cp.avg_price_point,
  RANK() OVER (ORDER BY cp.category_revenue DESC) as revenue_rank,
  -- Compare to previous quarter
  LAG(cp.category_revenue) OVER (ORDER BY cp.category_name) as prev_quarter_revenue,
  CASE
    WHEN cp.category_revenue / tp.total_revenue >= 0.15 THEN 'Major Category'
    WHEN cp.category_revenue / tp.total_revenue >= 0.10 THEN 'Core Category'
    WHEN cp.category_revenue / tp.total_revenue >= 0.05 THEN 'Supporting Category'
    ELSE 'Niche Category'
  END as category_importance
FROM category_performance cp
CROSS JOIN total_performance tp
ORDER BY cp.category_revenue DESC;

-- Query 5: Customer Segmentation Analysis
-- High-level customer insights and loyalty metrics
WITH customer_value_segments AS (
  SELECT
    cs.customer_segment,
    COUNT(*) as customer_count,
    AVG(cs.total_spent) as avg_customer_value,
    SUM(cs.total_spent) as segment_revenue,
    AVG(cs.total_transactions) as avg_transactions,
    AVG(cs.avg_transaction_value) as avg_basket_size,
    COUNT(CASE WHEN cs.churn_risk = 'HIGH' THEN 1 END) as high_risk_customers,
    AVG(cs.days_since_last_purchase) as avg_days_since_purchase
  FROM customer_segments cs
  GROUP BY cs.customer_segment
),
total_customers AS (
  SELECT
    SUM(customer_count) as total_count,
    SUM(segment_revenue) as total_revenue
  FROM customer_value_segments
)
SELECT
  cvs.customer_segment,
  cvs.customer_count,
  cvs.customer_count / tc.total_count * 100 as customer_mix_percent,
  cvs.avg_customer_value,
  cvs.segment_revenue,
  cvs.segment_revenue / tc.total_revenue * 100 as revenue_contribution_percent,
  cvs.avg_transactions,
  cvs.avg_basket_size,
  cvs.high_risk_customers,
  cvs.high_risk_customers / cvs.customer_count * 100 as churn_risk_percent,
  cvs.avg_days_since_purchase,
  CASE
    WHEN cvs.customer_segment IN ('VIP_ACTIVE', 'HIGH_VALUE') THEN 'Retain & Grow'
    WHEN cvs.customer_segment = 'FREQUENT_BUYER' THEN 'Upsell Opportunity'
    WHEN cvs.customer_segment IN ('AT_RISK', 'DORMANT') THEN 'Win-Back Campaign'
    WHEN cvs.customer_segment = 'NEW_CUSTOMER' THEN 'Onboarding Focus'
    ELSE 'Standard Engagement'
  END as strategic_priority
FROM customer_value_segments cvs
CROSS JOIN total_customers tc
ORDER BY cvs.segment_revenue DESC;

-- Query 6: Operational Efficiency Metrics
-- Key operational KPIs and productivity measures
WITH efficiency_metrics AS (
  SELECT
    COUNT(DISTINCT s.store_id) as total_stores,
    SUM(s.square_footage) as total_square_footage,
    AVG(sp.avg_daily_sales) as avg_store_daily_sales,
    AVG(sp.sales_per_sqft) as avg_sales_per_sqft,
    AVG(sp.avg_daily_transactions) as avg_daily_transactions,
    AVG(sp.avg_daily_customers) as avg_daily_customers,
    AVG(sp.avg_sales_per_customer) as avg_sales_per_customer
  FROM stores s
  JOIN store_performance sp ON s.store_id = sp.store_id
),
inventory_efficiency AS (
  SELECT
    SUM(i.ending_inventory * p.unit_cost) as total_inventory_value,
    COUNT(CASE WHEN ia.alert_type = 'STOCKOUT' THEN 1 END) as total_stockouts,
    COUNT(CASE WHEN ia.alert_type = 'OVERSTOCK' THEN 1 END) as total_overstock,
    COUNT(DISTINCT ia.product_id) as total_products
  FROM inventory i
  JOIN products p ON i.product_id = p.product_id
  LEFT JOIN inventory_alerts ia ON i.product_id = ia.product_id AND i.store_id = ia.store_id
  WHERE i.inventory_date = (SELECT MAX(inventory_date) FROM inventory)
),
sales_efficiency AS (
  SELECT
    SUM(s.total_amount) as total_sales_90_days,
    COUNT(DISTINCT s.transaction_id) as total_transactions_90_days,
    COUNT(DISTINCT s.customer_id) as unique_customers_90_days
  FROM sales s
  WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
)
SELECT
  'Sales per Square Foot' as metric_name,
  CONCAT('$', ROUND(se.total_sales_90_days / em.total_square_footage * 4, 0)) as annual_value,
  'Target: $400' as target_benchmark,
  CASE
    WHEN se.total_sales_90_days / em.total_square_footage * 4 >= 400 THEN 'Exceeding'
    WHEN se.total_sales_90_days / em.total_square_footage * 4 >= 350 THEN 'Meeting'
    ELSE 'Below Target'
  END as performance_status

UNION ALL

SELECT
  'Inventory Turnover (Annual)' as metric_name,
  ROUND(se.total_sales_90_days * 4 / ie.total_inventory_value, 1) as annual_value,
  'Target: 6.0x' as target_benchmark,
  CASE
    WHEN se.total_sales_90_days * 4 / ie.total_inventory_value >= 6.0 THEN 'Exceeding'
    WHEN se.total_sales_90_days * 4 / ie.total_inventory_value >= 5.0 THEN 'Meeting'
    ELSE 'Below Target'
  END as performance_status

UNION ALL

SELECT
  'Service Level (In-Stock %)' as metric_name,
  ROUND((ie.total_products - ie.total_stockouts) / ie.total_products * 100, 1) as annual_value,
  'Target: 95%' as target_benchmark,
  CASE
    WHEN (ie.total_products - ie.total_stockouts) / ie.total_products >= 0.95 THEN 'Exceeding'
    WHEN (ie.total_products - ie.total_stockouts) / ie.total_products >= 0.90 THEN 'Meeting'
    ELSE 'Below Target'
  END as performance_status

UNION ALL

SELECT
  'Average Transaction Value' as metric_name,
  CONCAT('$', ROUND(se.total_sales_90_days / se.total_transactions_90_days, 2)) as annual_value,
  'Target: $45' as target_benchmark,
  CASE
    WHEN se.total_sales_90_days / se.total_transactions_90_days >= 45 THEN 'Exceeding'
    WHEN se.total_sales_90_days / se.total_transactions_90_days >= 40 THEN 'Meeting'
    ELSE 'Below Target'
  END as performance_status

FROM efficiency_metrics em, inventory_efficiency ie, sales_efficiency se;

-- Query 7: Promotional ROI Analysis
-- Executive view of promotion effectiveness and ROI
SELECT
  pp.promotion_type,
  COUNT(*) as promotion_count,
  SUM(pp.promotion_revenue) as total_promotion_revenue,
  SUM(pp.total_discount_given) as total_discount_investment,
  AVG(pp.incremental_daily_revenue) as avg_daily_lift,
  SUM(pp.promotion_revenue) / SUM(pp.total_discount_given) as overall_roi,
  AVG(pp.revenue_vs_target) as avg_target_achievement,
  COUNT(CASE WHEN pp.performance_rating = 'EXCELLENT' THEN 1 END) as excellent_promos,
  COUNT(CASE WHEN pp.performance_rating IN ('FAIR', 'POOR') THEN 1 END) as underperforming_promos,
  -- Calculate incremental margin
  SUM(pp.incremental_daily_revenue * pp.promotion_duration_days) as total_incremental_revenue,
  CASE
    WHEN SUM(pp.promotion_revenue) / SUM(pp.total_discount_given) >= 3.0 THEN 'High ROI'
    WHEN SUM(pp.promotion_revenue) / SUM(pp.total_discount_given) >= 2.0 THEN 'Good ROI'
    WHEN SUM(pp.promotion_revenue) / SUM(pp.total_discount_given) >= 1.5 THEN 'Acceptable ROI'
    ELSE 'Poor ROI'
  END as roi_category,
  CASE
    WHEN AVG(pp.revenue_vs_target) >= 1.1 THEN 'Continue & Expand'
    WHEN AVG(pp.revenue_vs_target) >= 0.9 THEN 'Continue with Optimization'
    ELSE 'Reevaluate Strategy'
  END as strategic_recommendation
FROM promotional_performance pp
WHERE pp.start_date >= CURRENT_DATE() - INTERVAL 365 DAYS
GROUP BY pp.promotion_type
ORDER BY overall_roi DESC;

-- Query 8: Exception Reporting
-- Key alerts and issues requiring executive attention
WITH critical_issues AS (
  -- Stockouts impacting revenue
  SELECT
    'Critical Stockouts' as issue_type,
    COUNT(*) as issue_count,
    SUM(ia.days_since_last_sale * COALESCE(ia.avg_daily_sales, 0) * p.retail_price) as financial_impact,
    'Lost Revenue' as impact_type,
    'HIGH' as severity
  FROM inventory_alerts ia
  JOIN products p ON ia.product_id = p.product_id
  WHERE ia.alert_type = 'STOCKOUT' AND ia.priority = 'CRITICAL'

  UNION ALL

  -- Poor performing stores
  SELECT
    'Underperforming Stores' as issue_type,
    COUNT(*) as issue_count,
    SUM((sp.daily_sales_target - sp.avg_daily_sales) * 30) as financial_impact,
    'Revenue Gap' as impact_type,
    'MEDIUM' as severity
  FROM store_performance sp
  WHERE sp.sales_vs_target_ratio < 0.8

  UNION ALL

  -- High churn risk customers
  SELECT
    'Customer Churn Risk' as issue_type,
    COUNT(*) as issue_count,
    SUM(cs.total_spent * 0.3) as financial_impact,  -- Assume 30% revenue at risk
    'Customer Lifetime Value' as impact_type,
    'MEDIUM' as severity
  FROM customer_segments cs
  WHERE cs.churn_risk = 'HIGH' AND cs.customer_segment IN ('VIP_ACTIVE', 'HIGH_VALUE')

  UNION ALL

  -- Supplier performance issues
  SELECT
    'Supplier Issues' as issue_type,
    COUNT(*) as issue_count,
    0 as financial_impact,  -- Operational impact
    'Service Disruption' as impact_type,
    'LOW' as severity
  FROM suppliers s
  WHERE s.performance_rating < 3.0 AND s.is_active = true
)
SELECT
  issue_type,
  issue_count,
  financial_impact,
  impact_type,
  severity,
  CASE
    WHEN severity = 'HIGH' THEN 'Immediate Action Required'
    WHEN severity = 'MEDIUM' THEN 'Address This Week'
    ELSE 'Monitor and Plan'
  END as recommended_action,
  CASE
    WHEN issue_type = 'Critical Stockouts' THEN 'Emergency replenishment and supplier escalation'
    WHEN issue_type = 'Underperforming Stores' THEN 'Store manager coaching and performance review'
    WHEN issue_type = 'Customer Churn Risk' THEN 'Targeted retention campaigns and outreach'
    WHEN issue_type = 'Supplier Issues' THEN 'Supplier performance review and alternatives evaluation'
  END as action_plan
FROM critical_issues
ORDER BY
  CASE severity
    WHEN 'HIGH' THEN 1
    WHEN 'MEDIUM' THEN 2
    ELSE 3
  END,
  financial_impact DESC;

-- Query 9: Market Share and Competitive Position (Simulated)
-- Estimated market position and growth opportunities
WITH market_simulation AS (
  SELECT
    c.category_name,
    SUM(dsa.total_sales) as our_category_sales,
    -- Simulate total addressable market
    SUM(dsa.total_sales) * (2.5 + RANDOM() * 2) as estimated_total_market,
    COUNT(DISTINCT dsa.store_id) as our_store_count,
    AVG(p.retail_price) as our_avg_price
  FROM daily_sales_agg dsa
  JOIN products p ON dsa.category_id = p.category_id
  JOIN categories c ON p.category_id = c.category_id
  WHERE dsa.sale_date >= DATE_TRUNC('quarter', CURRENT_DATE())
    AND c.category_level = 2
  GROUP BY c.category_name
)
SELECT
  category_name,
  our_category_sales,
  estimated_total_market,
  our_category_sales / estimated_total_market * 100 as estimated_market_share,
  estimated_total_market - our_category_sales as market_opportunity,
  our_store_count,
  our_avg_price,
  CASE
    WHEN our_category_sales / estimated_total_market >= 0.20 THEN 'Market Leader'
    WHEN our_category_sales / estimated_total_market >= 0.15 THEN 'Strong Position'
    WHEN our_category_sales / estimated_total_market >= 0.10 THEN 'Competitive'
    WHEN our_category_sales / estimated_total_market >= 0.05 THEN 'Growing'
    ELSE 'Emerging'
  END as market_position,
  CASE
    WHEN our_category_sales / estimated_total_market < 0.10 THEN 'High Growth Potential'
    WHEN our_category_sales / estimated_total_market < 0.20 THEN 'Moderate Growth Potential'
    ELSE 'Market Defense Strategy'
  END as growth_strategy
FROM market_simulation
ORDER BY estimated_market_share DESC;

-- Query 10: Executive Summary Scorecard
-- Overall business health scorecard
WITH scorecard_metrics AS (
  SELECT
    -- Financial Health
    AVG(CASE WHEN scm.regional_ranking = 'TOP_PERFORMER' THEN 4
             WHEN scm.regional_ranking = 'ABOVE_AVERAGE' THEN 3
             WHEN scm.regional_ranking = 'AVERAGE' THEN 2
             ELSE 1 END) as financial_score,

    -- Operational Excellence
    AVG(CASE WHEN ia.priority = 'NONE' THEN 4
             WHEN ia.priority = 'LOW' THEN 3
             WHEN ia.priority = 'MEDIUM' THEN 2
             ELSE 1 END) as operational_score,

    -- Customer Satisfaction (based on churn risk)
    AVG(CASE WHEN cs.churn_risk = 'LOW' THEN 4
             WHEN cs.churn_risk = 'MEDIUM' THEN 2
             ELSE 1 END) as customer_score,

    -- Growth Trajectory
    4 as growth_score  -- Simulated based on positive trends
  FROM store_comparison_metrics scm
  CROSS JOIN inventory_alerts ia
  CROSS JOIN customer_segments cs
)
SELECT
  'Financial Performance' as dimension,
  financial_score as score,
  CASE WHEN financial_score >= 3.5 THEN 'Excellent'
       WHEN financial_score >= 3.0 THEN 'Good'
       WHEN financial_score >= 2.5 THEN 'Fair'
       ELSE 'Needs Attention' END as rating,
  'Revenue growth and profitability metrics' as description

UNION ALL

SELECT
  'Operational Excellence' as dimension,
  operational_score as score,
  CASE WHEN operational_score >= 3.5 THEN 'Excellent'
       WHEN operational_score >= 3.0 THEN 'Good'
       WHEN operational_score >= 2.5 THEN 'Fair'
       ELSE 'Needs Attention' END as rating,
  'Inventory management and service levels' as description

UNION ALL

SELECT
  'Customer Retention' as dimension,
  customer_score as score,
  CASE WHEN customer_score >= 3.5 THEN 'Excellent'
       WHEN customer_score >= 3.0 THEN 'Good'
       WHEN customer_score >= 2.5 THEN 'Fair'
       ELSE 'Needs Attention' END as rating,
  'Customer loyalty and churn prevention' as description

UNION ALL

SELECT
  'Growth Potential' as dimension,
  growth_score as score,
  CASE WHEN growth_score >= 3.5 THEN 'Excellent'
       WHEN growth_score >= 3.0 THEN 'Good'
       WHEN growth_score >= 2.5 THEN 'Fair'
       ELSE 'Needs Attention' END as rating,
  'Market expansion and new opportunities' as description

FROM scorecard_metrics
ORDER BY score DESC;