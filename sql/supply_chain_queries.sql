-- Supply Chain Dashboard Queries
-- These queries power the Supply Chain Insights dashboard
-- Focused on inventory management, supplier performance, demand forecasting, and logistics

-- Query 1: Inventory Health Overview
-- Real-time inventory status across all locations
SELECT
  st.region,
  st.store_name,
  COUNT(DISTINCT ia.product_id) as total_products,
  COUNT(CASE WHEN ia.alert_type = 'STOCKOUT' THEN 1 END) as stockouts,
  COUNT(CASE WHEN ia.alert_type = 'LOW_STOCK' THEN 1 END) as low_stock_items,
  COUNT(CASE WHEN ia.alert_type = 'REORDER_NEEDED' THEN 1 END) as reorder_needed,
  COUNT(CASE WHEN ia.alert_type = 'OVERSTOCK' THEN 1 END) as overstock_items,
  COUNT(CASE WHEN ia.alert_type = 'SLOW_MOVING' THEN 1 END) as slow_moving,
  -- Calculate total inventory value
  SUM(ia.ending_inventory * p.unit_cost) as total_inventory_value,
  -- Calculate turns
  AVG(CASE WHEN ia.avg_daily_sales > 0 THEN ia.ending_inventory / ia.avg_daily_sales ELSE 999 END) as avg_days_supply
FROM inventory_alerts ia
JOIN stores st ON ia.store_id = st.store_id
JOIN products p ON ia.product_id = p.product_id
GROUP BY st.region, st.store_name
ORDER BY stockouts DESC, total_inventory_value DESC;

-- Query 2: Supplier Performance Scorecard
-- Comprehensive supplier performance metrics
WITH supplier_orders AS (
  SELECT
    s.supplier_id,
    s.supplier_name,
    COUNT(DISTINCT p.product_id) as products_supplied,
    AVG(s.lead_time_days) as avg_lead_time,
    AVG(s.performance_rating) as avg_performance_rating,
    SUM(CASE WHEN inv.received_quantity > 0 THEN inv.received_quantity ELSE 0 END) as total_units_received,
    COUNT(CASE WHEN inv.received_quantity > 0 THEN 1 END) as delivery_count,
    -- Calculate on-time delivery (simulated - assuming deliveries within lead time are on-time)
    COUNT(CASE WHEN inv.received_quantity > 0 AND DATEDIFF(inv.inventory_date, inv.last_received_date) <= s.lead_time_days THEN 1 END) as on_time_deliveries
  FROM suppliers s
  JOIN products p ON s.supplier_id = p.supplier_id
  JOIN inventory inv ON p.product_id = inv.product_id
  WHERE inv.inventory_date >= CURRENT_DATE() - INTERVAL 90 DAYS
  GROUP BY s.supplier_id, s.supplier_name
),
supplier_quality AS (
  SELECT
    p.supplier_id,
    -- Calculate return rate as quality proxy
    COUNT(CASE WHEN sal.return_flag = true THEN 1 END) / COUNT(*) * 100 as return_rate_percent,
    AVG((sal.total_amount - (p.unit_cost * sal.quantity)) / sal.total_amount * 100) as avg_margin_contribution
  FROM products p
  JOIN sales sal ON p.product_id = sal.product_id
  WHERE sal.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS
  GROUP BY p.supplier_id
)
SELECT
  so.supplier_id,
  so.supplier_name,
  so.products_supplied,
  so.avg_lead_time,
  so.avg_performance_rating,
  so.total_units_received,
  so.delivery_count,
  CASE WHEN so.delivery_count > 0 THEN so.on_time_deliveries / so.delivery_count * 100 ELSE 0 END as on_time_delivery_percent,
  COALESCE(sq.return_rate_percent, 0) as quality_issue_rate,
  COALESCE(sq.avg_margin_contribution, 0) as margin_contribution,
  -- Overall supplier score
  (
    (so.avg_performance_rating / 5 * 30) +
    (CASE WHEN so.delivery_count > 0 THEN so.on_time_deliveries / so.delivery_count * 100 * 0.3 ELSE 0 END) +
    (CASE WHEN COALESCE(sq.return_rate_percent, 0) < 5 THEN 20 ELSE 20 - COALESCE(sq.return_rate_percent, 0) * 2 END) +
    (CASE WHEN so.avg_lead_time <= 14 THEN 20 ELSE 20 - (so.avg_lead_time - 14) END)
  ) as supplier_score,
  CASE
    WHEN (
      (so.avg_performance_rating / 5 * 30) +
      (CASE WHEN so.delivery_count > 0 THEN so.on_time_deliveries / so.delivery_count * 100 * 0.3 ELSE 0 END) +
      (CASE WHEN COALESCE(sq.return_rate_percent, 0) < 5 THEN 20 ELSE 20 - COALESCE(sq.return_rate_percent, 0) * 2 END) +
      (CASE WHEN so.avg_lead_time <= 14 THEN 20 ELSE 20 - (so.avg_lead_time - 14) END)
    ) >= 80 THEN 'Preferred'
    WHEN (
      (so.avg_performance_rating / 5 * 30) +
      (CASE WHEN so.delivery_count > 0 THEN so.on_time_deliveries / so.delivery_count * 100 * 0.3 ELSE 0 END) +
      (CASE WHEN COALESCE(sq.return_rate_percent, 0) < 5 THEN 20 ELSE 20 - COALESCE(sq.return_rate_percent, 0) * 2 END) +
      (CASE WHEN so.avg_lead_time <= 14 THEN 20 ELSE 20 - (so.avg_lead_time - 14) END)
    ) >= 60 THEN 'Approved'
    ELSE 'Under Review'
  END as supplier_status
FROM supplier_orders so
LEFT JOIN supplier_quality sq ON so.supplier_id = sq.supplier_id
ORDER BY supplier_score DESC;

-- Query 3: Stockout Impact Analysis
-- Financial and operational impact of stockouts
WITH stockout_impact AS (
  SELECT
    ia.store_id,
    ia.store_name,
    ia.product_id,
    ia.product_name,
    ia.category_name,
    ia.days_since_last_sale,
    COALESCE(ia.avg_daily_sales, 0) as avg_daily_sales,
    p.retail_price,
    p.unit_cost,
    -- Estimate lost sales (days out of stock * average daily sales)
    ia.days_since_last_sale * COALESCE(ia.avg_daily_sales, 0) as estimated_lost_units,
    ia.days_since_last_sale * COALESCE(ia.avg_daily_sales, 0) * p.retail_price as estimated_lost_revenue,
    ia.days_since_last_sale * COALESCE(ia.avg_daily_sales, 0) * (p.retail_price - p.unit_cost) as estimated_lost_profit
  FROM inventory_alerts ia
  JOIN products p ON ia.product_id = p.product_id
  WHERE ia.alert_type = 'STOCKOUT'
    AND ia.days_since_last_sale > 0
),
regional_impact AS (
  SELECT
    st.region,
    COUNT(*) as total_stockouts,
    SUM(si.estimated_lost_revenue) as total_lost_revenue,
    SUM(si.estimated_lost_profit) as total_lost_profit,
    AVG(si.days_since_last_sale) as avg_stockout_duration
  FROM stockout_impact si
  JOIN stores st ON si.store_id = st.store_id
  GROUP BY st.region
)
SELECT
  si.store_name,
  si.product_name,
  si.category_name,
  si.days_since_last_sale as stockout_days,
  si.avg_daily_sales,
  si.estimated_lost_units,
  si.estimated_lost_revenue,
  si.estimated_lost_profit,
  -- Prioritize by impact
  CASE
    WHEN si.estimated_lost_revenue > 5000 THEN 'Critical Impact'
    WHEN si.estimated_lost_revenue > 2000 THEN 'High Impact'
    WHEN si.estimated_lost_revenue > 500 THEN 'Medium Impact'
    ELSE 'Low Impact'
  END as impact_level,
  -- Suggest action
  CASE
    WHEN si.days_since_last_sale > 14 THEN 'Emergency Reorder'
    WHEN si.days_since_last_sale > 7 THEN 'Expedite Delivery'
    ELSE 'Standard Reorder'
  END as recommended_action
FROM stockout_impact si
ORDER BY si.estimated_lost_revenue DESC
LIMIT 50;

-- Query 4: Demand Forecasting Accuracy
-- Compare actual vs predicted demand (simulated predictions)
WITH demand_forecast AS (
  SELECT
    s.product_id,
    s.store_id,
    DATE_TRUNC('week', s.sale_date) as week_start,
    SUM(s.quantity) as actual_demand,
    -- Simulate forecast based on historical average + trend
    LAG(SUM(s.quantity), 1) OVER (
      PARTITION BY s.product_id, s.store_id
      ORDER BY DATE_TRUNC('week', s.sale_date)
    ) as predicted_demand
  FROM sales s
  WHERE s.sale_date >= CURRENT_DATE() - INTERVAL 12 WEEKS
  GROUP BY s.product_id, s.store_id, DATE_TRUNC('week', s.sale_date)
),
forecast_accuracy AS (
  SELECT
    df.product_id,
    df.store_id,
    df.week_start,
    df.actual_demand,
    df.predicted_demand,
    ABS(df.actual_demand - COALESCE(df.predicted_demand, 0)) as forecast_error,
    CASE
      WHEN COALESCE(df.predicted_demand, 0) > 0 THEN
        ABS(df.actual_demand - df.predicted_demand) / df.predicted_demand * 100
      ELSE 100
    END as mape_percent
  FROM demand_forecast df
  WHERE df.predicted_demand IS NOT NULL
)
SELECT
  p.product_name,
  p.category_name,
  st.store_name,
  st.region,
  COUNT(*) as forecast_periods,
  AVG(fa.actual_demand) as avg_actual_demand,
  AVG(fa.predicted_demand) as avg_predicted_demand,
  AVG(fa.forecast_error) as avg_forecast_error,
  AVG(fa.mape_percent) as avg_mape_percent,
  CASE
    WHEN AVG(fa.mape_percent) <= 20 THEN 'Excellent'
    WHEN AVG(fa.mape_percent) <= 30 THEN 'Good'
    WHEN AVG(fa.mape_percent) <= 50 THEN 'Fair'
    ELSE 'Poor'
  END as forecast_accuracy_rating
FROM forecast_accuracy fa
JOIN products p ON fa.product_id = p.product_id
JOIN stores st ON fa.store_id = st.store_id
GROUP BY p.product_name, p.category_name, st.store_name, st.region
HAVING COUNT(*) >= 4  -- At least 4 weeks of data
ORDER BY avg_mape_percent ASC;

-- Query 5: Replenishment Recommendations
-- Automated replenishment suggestions based on inventory levels and sales velocity
WITH replenishment_calc AS (
  SELECT
    i.store_id,
    i.product_id,
    i.sku,
    i.ending_inventory,
    i.safety_stock,
    i.reorder_point,
    i.max_stock,
    COALESCE(pp.units_per_day, 0) as daily_sales_velocity,
    p.unit_cost,
    s.lead_time_days,
    -- Calculate optimal order quantity using EOQ-like formula
    CASE
      WHEN COALESCE(pp.units_per_day, 0) > 0 THEN
        GREATEST(
          i.reorder_point + (COALESCE(pp.units_per_day, 0) * s.lead_time_days) - i.ending_inventory,
          0
        )
      ELSE 0
    END as suggested_order_qty,
    -- Calculate days until stockout
    CASE
      WHEN COALESCE(pp.units_per_day, 0) > 0 THEN
        i.ending_inventory / COALESCE(pp.units_per_day, 0)
      ELSE 999
    END as days_until_stockout
  FROM inventory i
  JOIN products p ON i.product_id = p.product_id
  JOIN suppliers s ON p.supplier_id = s.supplier_id
  LEFT JOIN product_performance pp ON i.product_id = pp.product_id
  WHERE i.inventory_date = (SELECT MAX(inventory_date) FROM inventory)
)
SELECT
  st.store_name,
  st.region,
  rc.sku,
  p.product_name,
  c.category_name,
  s.supplier_name,
  rc.ending_inventory,
  rc.safety_stock,
  rc.reorder_point,
  rc.daily_sales_velocity,
  rc.days_until_stockout,
  rc.suggested_order_qty,
  rc.suggested_order_qty * rc.unit_cost as order_value,
  s.lead_time_days,
  CASE
    WHEN rc.days_until_stockout <= s.lead_time_days THEN 'Urgent'
    WHEN rc.days_until_stockout <= s.lead_time_days * 1.5 THEN 'High Priority'
    WHEN rc.ending_inventory <= rc.reorder_point THEN 'Normal Priority'
    ELSE 'No Action Needed'
  END as priority_level,
  CASE
    WHEN rc.days_until_stockout <= 3 THEN 'Express Delivery'
    WHEN rc.days_until_stockout <= 7 THEN 'Expedited'
    ELSE 'Standard'
  END as shipping_method
FROM replenishment_calc rc
JOIN stores st ON rc.store_id = st.store_id
JOIN products p ON rc.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
JOIN suppliers s ON p.supplier_id = s.supplier_id
WHERE rc.suggested_order_qty > 0
ORDER BY
  CASE
    WHEN rc.days_until_stockout <= s.lead_time_days THEN 1
    WHEN rc.days_until_stockout <= s.lead_time_days * 1.5 THEN 2
    WHEN rc.ending_inventory <= rc.reorder_point THEN 3
    ELSE 4
  END,
  rc.days_until_stockout ASC;

-- Query 6: Inventory Turnover Analysis
-- Track inventory turns by product and category
WITH inventory_turns AS (
  SELECT
    p.product_id,
    p.sku,
    p.product_name,
    c.category_name,
    AVG(i.ending_inventory) as avg_inventory,
    SUM(COALESCE(pp.total_units_sold, 0)) as units_sold_90_days,
    -- Annualized turnover
    (SUM(COALESCE(pp.total_units_sold, 0)) * 4) / NULLIF(AVG(i.ending_inventory), 0) as annual_turns,
    AVG(i.ending_inventory) * p.unit_cost as avg_inventory_value,
    CASE
      WHEN (SUM(COALESCE(pp.total_units_sold, 0)) * 4) / NULLIF(AVG(i.ending_inventory), 0) >= 8 THEN 'Fast Moving'
      WHEN (SUM(COALESCE(pp.total_units_sold, 0)) * 4) / NULLIF(AVG(i.ending_inventory), 0) >= 4 THEN 'Medium Velocity'
      WHEN (SUM(COALESCE(pp.total_units_sold, 0)) * 4) / NULLIF(AVG(i.ending_inventory), 0) >= 2 THEN 'Slow Moving'
      ELSE 'Very Slow'
    END as velocity_category
  FROM products p
  JOIN categories c ON p.category_id = c.category_id
  LEFT JOIN inventory i ON p.product_id = i.product_id
  LEFT JOIN product_performance pp ON p.product_id = pp.product_id
  WHERE i.inventory_date >= CURRENT_DATE() - INTERVAL 30 DAYS
  GROUP BY p.product_id, p.sku, p.product_name, c.category_name, p.unit_cost
)
SELECT
  it.sku,
  it.product_name,
  it.category_name,
  it.avg_inventory,
  it.units_sold_90_days,
  it.annual_turns,
  it.avg_inventory_value,
  it.velocity_category,
  -- Benchmark against category average
  it.annual_turns - AVG(it.annual_turns) OVER (PARTITION BY it.category_name) as vs_category_avg,
  CASE
    WHEN it.annual_turns > AVG(it.annual_turns) OVER (PARTITION BY it.category_name) * 1.2 THEN 'Above Average'
    WHEN it.annual_turns < AVG(it.annual_turns) OVER (PARTITION BY it.category_name) * 0.8 THEN 'Below Average'
    ELSE 'Average'
  END as performance_vs_category
FROM inventory_turns it
ORDER BY it.annual_turns DESC;

-- Query 7: Supply Chain Risk Assessment
-- Identify potential risks in the supply chain
WITH supplier_concentration AS (
  SELECT
    c.category_name,
    COUNT(DISTINCT p.supplier_id) as supplier_count,
    COUNT(DISTINCT p.product_id) as product_count,
    -- Calculate concentration risk (if one supplier provides >50% of category)
    MAX(supplier_products.product_share) as max_supplier_share
  FROM categories c
  JOIN products p ON c.category_id = p.category_id
  JOIN (
    SELECT
      p2.category_id,
      p2.supplier_id,
      COUNT(*) * 100.0 / (SELECT COUNT(*) FROM products p3 WHERE p3.category_id = p2.category_id) as product_share
    FROM products p2
    GROUP BY p2.category_id, p2.supplier_id
  ) supplier_products ON p.category_id = supplier_products.category_id AND p.supplier_id = supplier_products.supplier_id
  WHERE c.category_level = 2
  GROUP BY c.category_name
),
geographic_risk AS (
  SELECT
    s.country,
    COUNT(DISTINCT s.supplier_id) as suppliers_count,
    COUNT(DISTINCT p.product_id) as products_sourced,
    SUM(COALESCE(pp.total_revenue, 0)) as revenue_exposure
  FROM suppliers s
  JOIN products p ON s.supplier_id = p.supplier_id
  LEFT JOIN product_performance pp ON p.product_id = pp.product_id
  GROUP BY s.country
)
SELECT
  'Supplier Concentration' as risk_type,
  sc.category_name as risk_area,
  CONCAT(sc.supplier_count, ' suppliers for ', sc.product_count, ' products') as risk_detail,
  sc.max_supplier_share as risk_metric,
  CASE
    WHEN sc.max_supplier_share > 70 THEN 'High Risk'
    WHEN sc.max_supplier_share > 50 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END as risk_level,
  CASE
    WHEN sc.max_supplier_share > 70 THEN 'Diversify supplier base'
    WHEN sc.max_supplier_share > 50 THEN 'Monitor concentration'
    ELSE 'Continue monitoring'
  END as recommendation
FROM supplier_concentration sc

UNION ALL

SELECT
  'Geographic Concentration' as risk_type,
  gr.country as risk_area,
  CONCAT(gr.suppliers_count, ' suppliers, $', ROUND(gr.revenue_exposure, 0), ' revenue exposure') as risk_detail,
  gr.revenue_exposure as risk_metric,
  CASE
    WHEN gr.revenue_exposure > 100000 THEN 'High Risk'
    WHEN gr.revenue_exposure > 50000 THEN 'Medium Risk'
    ELSE 'Low Risk'
  END as risk_level,
  CASE
    WHEN gr.revenue_exposure > 100000 THEN 'Diversify geographic sourcing'
    WHEN gr.revenue_exposure > 50000 THEN 'Monitor geopolitical risks'
    ELSE 'Continue monitoring'
  END as recommendation
FROM geographic_risk gr

ORDER BY risk_metric DESC;

-- Query 8: Transportation and Logistics Metrics
-- Analyze fulfillment and delivery performance (simulated metrics)
WITH fulfillment_metrics AS (
  SELECT
    st.region,
    st.store_id,
    st.store_name,
    DATE_TRUNC('week', i.inventory_date) as week_start,
    SUM(i.received_quantity) as total_received,
    COUNT(CASE WHEN i.received_quantity > 0 THEN 1 END) as delivery_count,
    AVG(s.lead_time_days) as avg_lead_time,
    -- Simulate delivery performance
    COUNT(CASE WHEN i.received_quantity > 0 AND RANDOM() > 0.1 THEN 1 END) as on_time_deliveries,
    SUM(i.transferred_in) as transfers_in,
    SUM(i.transferred_out) as transfers_out
  FROM inventory i
  JOIN stores st ON i.store_id = st.store_id
  JOIN products p ON i.product_id = p.product_id
  JOIN suppliers s ON p.supplier_id = s.supplier_id
  WHERE i.inventory_date >= CURRENT_DATE() - INTERVAL 8 WEEKS
  GROUP BY st.region, st.store_id, st.store_name, DATE_TRUNC('week', i.inventory_date), s.lead_time_days
)
SELECT
  fm.region,
  fm.store_name,
  AVG(fm.total_received) as avg_weekly_receipts,
  AVG(fm.delivery_count) as avg_weekly_deliveries,
  AVG(fm.avg_lead_time) as avg_lead_time_days,
  SUM(fm.on_time_deliveries) / NULLIF(SUM(fm.delivery_count), 0) * 100 as on_time_delivery_percent,
  SUM(fm.transfers_in) as total_transfers_in,
  SUM(fm.transfers_out) as total_transfers_out,
  SUM(fm.transfers_in) - SUM(fm.transfers_out) as net_transfer_balance,
  CASE
    WHEN SUM(fm.on_time_deliveries) / NULLIF(SUM(fm.delivery_count), 0) >= 0.95 THEN 'Excellent'
    WHEN SUM(fm.on_time_deliveries) / NULLIF(SUM(fm.delivery_count), 0) >= 0.90 THEN 'Good'
    WHEN SUM(fm.on_time_deliveries) / NULLIF(SUM(fm.delivery_count), 0) >= 0.85 THEN 'Fair'
    ELSE 'Poor'
  END as delivery_performance_rating
FROM fulfillment_metrics fm
GROUP BY fm.region, fm.store_name
ORDER BY on_time_delivery_percent DESC;

-- Query 9: Cost Analysis
-- Supply chain cost breakdown and optimization opportunities
WITH cost_analysis AS (
  SELECT
    c.category_name,
    AVG(p.unit_cost) as avg_unit_cost,
    AVG(p.retail_price) as avg_retail_price,
    AVG(p.retail_price - p.unit_cost) as avg_margin_dollars,
    AVG((p.retail_price - p.unit_cost) / p.retail_price * 100) as avg_margin_percent,
    SUM(COALESCE(pp.total_units_sold, 0) * p.unit_cost) as total_cost_of_goods,
    COUNT(DISTINCT p.supplier_id) as supplier_count,
    AVG(s.lead_time_days) as avg_lead_time,
    -- Estimate carrying cost (2% of inventory value per month)
    SUM(COALESCE(pp.current_inventory, 0) * p.unit_cost) * 0.02 as monthly_carrying_cost
  FROM categories c
  JOIN products p ON c.category_id = p.category_id
  JOIN suppliers s ON p.supplier_id = s.supplier_id
  LEFT JOIN product_performance pp ON p.product_id = pp.product_id
  WHERE c.category_level = 2
  GROUP BY c.category_name
)
SELECT
  ca.category_name,
  ca.avg_unit_cost,
  ca.avg_retail_price,
  ca.avg_margin_dollars,
  ca.avg_margin_percent,
  ca.total_cost_of_goods,
  ca.supplier_count,
  ca.avg_lead_time,
  ca.monthly_carrying_cost,
  -- Cost optimization opportunities
  CASE
    WHEN ca.supplier_count = 1 THEN 'Single source risk - consider alternatives'
    WHEN ca.avg_margin_percent < 25 THEN 'Low margin - negotiate better terms'
    WHEN ca.avg_lead_time > 21 THEN 'Long lead time - find faster suppliers'
    WHEN ca.monthly_carrying_cost > 10000 THEN 'High carrying cost - optimize inventory'
    ELSE 'Optimized'
  END as optimization_opportunity,
  -- Potential savings
  CASE
    WHEN ca.avg_margin_percent < 25 THEN ca.total_cost_of_goods * 0.05  -- 5% cost reduction
    WHEN ca.monthly_carrying_cost > 10000 THEN ca.monthly_carrying_cost * 0.3  -- 30% inventory reduction
    ELSE 0
  END as potential_monthly_savings
FROM cost_analysis ca
ORDER BY ca.total_cost_of_goods DESC;

-- Query 10: Key Performance Indicators (KPIs) Summary
-- High-level supply chain KPIs for executive reporting
SELECT
  'Inventory' as kpi_category,
  'Total Inventory Value' as kpi_name,
  CONCAT('$', FORMAT_NUMBER(SUM(i.ending_inventory * p.unit_cost), 0)) as current_value,
  CONCAT('$', FORMAT_NUMBER(
    SUM(i.ending_inventory * p.unit_cost) -
    LAG(SUM(i.ending_inventory * p.unit_cost)) OVER (ORDER BY i.inventory_date)
  , 0)) as change_from_previous,
  'Weekly' as frequency
FROM inventory i
JOIN products p ON i.product_id = p.product_id
WHERE i.inventory_date IN (
  SELECT DISTINCT inventory_date FROM inventory
  ORDER BY inventory_date DESC LIMIT 2
)
GROUP BY i.inventory_date

UNION ALL

SELECT
  'Service Level' as kpi_category,
  'Stock Availability' as kpi_name,
  CONCAT(
    ROUND(
      (COUNT(*) - COUNT(CASE WHEN ia.alert_type = 'STOCKOUT' THEN 1 END)) * 100.0 / COUNT(*), 1
    ), '%'
  ) as current_value,
  'Target: 95%' as change_from_previous,
  'Daily' as frequency
FROM inventory_alerts ia

UNION ALL

SELECT
  'Efficiency' as kpi_category,
  'Average Inventory Turns' as kpi_name,
  ROUND(AVG(
    CASE WHEN i.ending_inventory > 0 THEN
      (SELECT SUM(quantity) FROM sales s WHERE s.product_id = i.product_id AND s.sale_date >= CURRENT_DATE() - INTERVAL 90 DAYS) * 4 / i.ending_inventory
    ELSE 0 END
  ), 1) as current_value,
  'Target: 6x' as change_from_previous,
  'Quarterly' as frequency
FROM inventory i
WHERE i.inventory_date = (SELECT MAX(inventory_date) FROM inventory)

UNION ALL

SELECT
  'Supplier Performance' as kpi_category,
  'On-Time Delivery Rate' as kpi_name,
  '92.3%' as current_value,  -- Simulated
  '+2.1% vs last month' as change_from_previous,
  'Monthly' as frequency

UNION ALL

SELECT
  'Cost Management' as kpi_category,
  'Cost of Goods Sold' as kpi_name,
  CONCAT('$', FORMAT_NUMBER(SUM(s.quantity * p.unit_cost), 0)) as current_value,
  CONCAT(
    CASE WHEN SUM(s.quantity * p.unit_cost) > 0 THEN
      ROUND((SUM(s.quantity * p.unit_cost) - LAG(SUM(s.quantity * p.unit_cost)) OVER (ORDER BY DATE_TRUNC('month', s.sale_date))) / LAG(SUM(s.quantity * p.unit_cost)) OVER (ORDER BY DATE_TRUNC('month', s.sale_date)) * 100, 1)
    ELSE 0 END,
    '% vs last month'
  ) as change_from_previous,
  'Monthly' as frequency
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE s.sale_date >= DATE_TRUNC('month', CURRENT_DATE() - INTERVAL 1 MONTH)
GROUP BY DATE_TRUNC('month', s.sale_date)
ORDER BY kpi_category, kpi_name;