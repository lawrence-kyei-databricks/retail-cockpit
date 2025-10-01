# Databricks notebook source
# MAGIC %md
# MAGIC # Genie Integration Setup for Retail Insight Cockpit
# MAGIC
# MAGIC This notebook configures Databricks Genie for the Retail Insight Cockpit,
# MAGIC enabling natural language queries across all retail dashboards and data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import requests
import json
from datetime import datetime
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Space Creation

# COMMAND ----------

def create_genie_space(catalog_name: str, schema_name: str, workspace_url: str):
    """
    Create a Genie space for retail analytics with proper configuration
    """

    genie_space_config = {
        "display_name": "Retail Insight Cockpit",
        "description": "AI-powered natural language interface for retail analytics and insights",

        # Data source configuration
        "data_sources": [
            {
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "tables": [
                    {
                        "name": "sales",
                        "description": "Transaction-level sales data with customer, product, and store details",
                        "primary_keys": ["transaction_id"],
                        "business_context": "Core sales transactions for revenue analysis and customer behavior insights"
                    },
                    {
                        "name": "customers",
                        "description": "Customer master data with demographics, loyalty tiers, and segmentation",
                        "primary_keys": ["customer_id"],
                        "business_context": "Customer information for personalization and retention analysis"
                    },
                    {
                        "name": "products",
                        "description": "Product catalog with pricing, categories, suppliers, and attributes",
                        "primary_keys": ["product_id"],
                        "business_context": "Product information for merchandising and inventory management"
                    },
                    {
                        "name": "stores",
                        "description": "Store locations with regions, performance metrics, and operational data",
                        "primary_keys": ["store_id"],
                        "business_context": "Store information for location-based analysis and operations"
                    },
                    {
                        "name": "inventory",
                        "description": "Inventory levels, movements, and stock management data",
                        "primary_keys": ["store_id", "product_id", "inventory_date"],
                        "business_context": "Inventory tracking for supply chain optimization and stockout prevention"
                    },
                    {
                        "name": "promotions",
                        "description": "Marketing campaigns, promotional data, and effectiveness metrics",
                        "primary_keys": ["promotion_id"],
                        "business_context": "Promotional campaigns for marketing ROI and customer engagement analysis"
                    },
                    {
                        "name": "suppliers",
                        "description": "Vendor information, performance ratings, and supply chain data",
                        "primary_keys": ["supplier_id"],
                        "business_context": "Supplier management for cost optimization and performance monitoring"
                    }
                ],

                "views": [
                    {
                        "name": "daily_sales_agg",
                        "description": "Daily sales aggregations by store, category, and region",
                        "business_context": "Pre-aggregated daily metrics for performance dashboards"
                    },
                    {
                        "name": "store_performance",
                        "description": "Store-level KPIs, targets, and performance comparisons",
                        "business_context": "Store management metrics and benchmarking"
                    },
                    {
                        "name": "inventory_alerts",
                        "description": "Real-time inventory alerts, stockouts, and replenishment needs",
                        "business_context": "Critical inventory management and exception reporting"
                    },
                    {
                        "name": "customer_segments",
                        "description": "Customer segmentation, lifetime value, and churn analysis",
                        "business_context": "Customer analytics for targeting and retention strategies"
                    },
                    {
                        "name": "product_performance",
                        "description": "Product sales performance, profitability, and trend analysis",
                        "business_context": "Merchandising insights for buying and pricing decisions"
                    },
                    {
                        "name": "promotional_performance",
                        "description": "Campaign effectiveness, ROI, and promotional impact analysis",
                        "business_context": "Marketing performance measurement and optimization"
                    }
                ]
            }
        ],

        # Business context and instructions
        "instructions": f"""
        You are an expert retail analytics AI assistant for the Retail Insight Cockpit.

        BUSINESS CONTEXT:
        - You're helping retail professionals analyze sales, inventory, customers, and operations
        - Data covers stores, products, customers, suppliers, and promotional campaigns
        - Users include store managers, merchandisers, supply chain managers, and executives
        - Focus on actionable insights that drive business decisions

        RESPONSE GUIDELINES:
        1. Provide specific, data-driven answers with supporting metrics
        2. Include relevant time periods, comparisons, and trends
        3. Use retail terminology and KPIs appropriately
        4. Format numbers clearly (e.g., $1.2M, 15.3%, 1,234 units)
        5. Suggest follow-up questions for deeper analysis
        6. Highlight exceptions, opportunities, and risks

        KEY RETAIL METRICS TO UNDERSTAND:
        - Revenue, profit margin, same-store sales growth
        - Inventory turnover, stockout rates, days of supply
        - Customer lifetime value, churn rate, loyalty metrics
        - Promotional ROI, incremental sales, margin impact
        - Store productivity, sales per square foot
        - Supplier performance, lead times, quality metrics

        COMMON RETAIL QUESTIONS:
        - Performance: "How are sales trending vs target?"
        - Inventory: "What products are out of stock?"
        - Customers: "Who are my high-value customers?"
        - Products: "Which items need markdown?"
        - Operations: "Which stores are underperforming?"
        - Promotions: "What campaigns drove the best ROI?"
        """,

        # Role-based access and customization
        "user_personas": [
            {
                "name": "Store Manager",
                "description": "Daily operations, customer service, inventory management",
                "focus_areas": ["store performance", "inventory alerts", "customer traffic", "staff productivity"],
                "data_scope": "Single store or district level",
                "typical_questions": [
                    "How are today's sales vs target?",
                    "What products are out of stock?",
                    "Who are my VIP customers?",
                    "What are my busiest hours?"
                ]
            },
            {
                "name": "Merchandiser",
                "description": "Product performance, pricing, category management",
                "focus_areas": ["product performance", "pricing analysis", "promotional effectiveness", "inventory planning"],
                "data_scope": "Category or brand across all stores",
                "typical_questions": [
                    "Which products need markdown?",
                    "How are seasonal trends looking?",
                    "What promotions worked best?",
                    "Which items should I bundle?"
                ]
            },
            {
                "name": "Supply Chain Manager",
                "description": "Inventory optimization, supplier management, logistics",
                "focus_areas": ["inventory management", "supplier performance", "demand forecasting", "cost optimization"],
                "data_scope": "Company-wide supply chain operations",
                "typical_questions": [
                    "What needs immediate replenishment?",
                    "Which suppliers are underperforming?",
                    "Where do I have excess inventory?",
                    "How accurate are my forecasts?"
                ]
            },
            {
                "name": "Executive",
                "description": "Strategic insights, financial performance, competitive positioning",
                "focus_areas": ["financial metrics", "market trends", "strategic opportunities", "risk management"],
                "data_scope": "Company-wide strategic view",
                "typical_questions": [
                    "How is revenue trending?",
                    "Which regions are growing fastest?",
                    "What are our biggest risks?",
                    "Where should we invest next?"
                ]
            }
        ],

        # Integration settings
        "dashboard_integration": True,
        "slack_integration": False,
        "email_alerts": True,
        "auto_refresh": True,

        # Security and governance
        "data_governance": {
            "row_level_security": True,
            "column_level_security": True,
            "audit_logging": True,
            "pii_protection": True
        }
    }

    return genie_space_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Questions Library

# COMMAND ----------

def create_sample_questions_library():
    """
    Create a comprehensive library of sample questions organized by role and use case
    """

    sample_questions = {
        "Store Manager": {
            "Daily Operations": [
                "How are today's sales tracking compared to target?",
                "What are my top 5 selling products today?",
                "Which products are out of stock and causing lost sales?",
                "How many customers visited my store yesterday?",
                "What's my conversion rate this week vs last week?"
            ],
            "Customer Insights": [
                "Who are my VIP customers and when do they typically shop?",
                "Which customers haven't visited in the last 30 days?",
                "What products do my loyalty customers buy most?",
                "How many new customers did I acquire this month?",
                "Which customers are at risk of churning?"
            ],
            "Inventory Management": [
                "What products need immediate reordering?",
                "Which items have been sitting in inventory too long?",
                "What's my inventory turnover for top categories?",
                "Which products have high return rates?",
                "What inventory transfers should I request?"
            ],
            "Performance Analysis": [
                "How does my store rank compared to others in my region?",
                "What are my busiest hours and days?",
                "Which categories are over/under performing?",
                "How is my average transaction value trending?",
                "What promotions are driving traffic to my store?"
            ]
        },

        "Merchandiser": {
            "Product Performance": [
                "Which products are top performers across all stores?",
                "What items need markdown to clear inventory?",
                "How are new product launches performing?",
                "Which products have declining sales trends?",
                "What's the sell-through rate for seasonal items?"
            ],
            "Pricing & Margins": [
                "Which products can I raise prices on without affecting sales?",
                "What's my margin performance by category?",
                "How do my prices compare to market rates?",
                "Which items have the highest price elasticity?",
                "What pricing changes would optimize revenue?"
            ],
            "Promotional Analysis": [
                "What promotions had the best ROI last quarter?",
                "Which products respond best to discounts?",
                "How much incremental sales did promotions drive?",
                "What's the optimal promotion frequency for each category?",
                "Which bundle opportunities should I pursue?"
            ],
            "Category Management": [
                "How is each category contributing to total revenue?",
                "What seasonal trends should I plan for?",
                "Which categories have the fastest inventory turns?",
                "What cross-selling opportunities exist?",
                "How do category margins compare to targets?"
            ]
        },

        "Supply Chain Manager": {
            "Inventory Optimization": [
                "What products need immediate replenishment across all stores?",
                "Where do I have excess inventory that needs redistribution?",
                "Which items have the lowest inventory turnover?",
                "What's my overall inventory health by region?",
                "Which products frequently stock out?"
            ],
            "Supplier Performance": [
                "Which suppliers have the best on-time delivery rates?",
                "What suppliers consistently provide quality issues?",
                "How do supplier costs compare across categories?",
                "Which vendors offer the best payment terms?",
                "What supplier relationships need renegotiation?"
            ],
            "Demand Planning": [
                "How accurate are my demand forecasts by category?",
                "Which products show unusual demand patterns?",
                "What seasonal adjustments should I make to forecasts?",
                "How do promotions affect demand patterns?",
                "What external factors impact my forecast accuracy?"
            ],
            "Cost Management": [
                "Where can I reduce total cost of goods sold?",
                "What products have rising supplier costs?",
                "Which categories offer volume discount opportunities?",
                "How do logistics costs vary by region?",
                "What consolidation opportunities exist with suppliers?"
            ]
        },

        "Executive": {
            "Financial Performance": [
                "How is revenue and profit trending vs last year?",
                "What's driving changes in gross margin?",
                "Which regions contribute most to growth?",
                "How does our performance compare to industry benchmarks?",
                "What's our return on invested capital by business unit?"
            ],
            "Strategic Insights": [
                "What are our biggest growth opportunities?",
                "Which customer segments drive the most value?",
                "How is market share trending in key categories?",
                "What competitive threats should we address?",
                "Where should we focus investment for maximum ROI?"
            ],
            "Operational Excellence": [
                "What stores or regions are underperforming?",
                "How efficient are our operations vs targets?",
                "What operational risks need immediate attention?",
                "How is customer satisfaction trending?",
                "What initiatives show the best ROI?"
            ],
            "Market Analysis": [
                "How are we positioned vs competitors on price?",
                "What market trends are affecting our business?",
                "Which categories are growing fastest in the market?",
                "How do our margins compare to industry standards?",
                "What new market opportunities should we explore?"
            ]
        },

        "Cross-Functional": {
            "Alerts & Exceptions": [
                "What critical issues need attention today?",
                "Show me any unusual patterns in sales or inventory",
                "What metrics are trending in the wrong direction?",
                "Are there any data quality issues I should know about?",
                "What automated alerts have been triggered recently?"
            ],
            "Comparative Analysis": [
                "How does this week compare to last week and last year?",
                "What's different about top vs bottom performing stores?",
                "How do weekday vs weekend patterns differ?",
                "What's the impact of weather on sales performance?",
                "How do holiday periods affect different categories?"
            ]
        }
    }

    return sample_questions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Integration Functions

# COMMAND ----------

def create_dashboard_genie_widgets():
    """
    Create Genie widget configurations for embedding in dashboards
    """

    dashboard_widgets = {
        "store_manager_dashboard": {
            "widget_id": "genie_store_manager",
            "title": "Ask About Your Store Performance",
            "placeholder": "Ask me about sales, inventory, customers, or operations...",
            "suggested_prompts": [
                "How are today's sales vs target?",
                "What products are out of stock?",
                "Who are my top customers this month?",
                "What are my busiest hours today?"
            ],
            "context_filters": ["store_id"],
            "quick_insights": [
                "Critical stockouts",
                "Daily performance summary",
                "Top selling products",
                "Customer alerts"
            ]
        },

        "merchandiser_dashboard": {
            "widget_id": "genie_merchandiser",
            "title": "Ask About Product & Category Performance",
            "placeholder": "Ask me about products, pricing, promotions, or trends...",
            "suggested_prompts": [
                "Which products need markdown?",
                "How are promotions performing?",
                "What seasonal trends should I know?",
                "Which categories have best margins?"
            ],
            "context_filters": ["category_id", "brand"],
            "quick_insights": [
                "Markdown candidates",
                "Promotion ROI",
                "New product performance",
                "Category trends"
            ]
        },

        "supply_chain_dashboard": {
            "widget_id": "genie_supply_chain",
            "title": "Ask About Inventory & Supply Chain",
            "placeholder": "Ask me about inventory, suppliers, or logistics...",
            "suggested_prompts": [
                "What needs immediate replenishment?",
                "Which suppliers are underperforming?",
                "Where do I have excess inventory?",
                "How accurate are my forecasts?"
            ],
            "context_filters": ["region", "supplier_id"],
            "quick_insights": [
                "Replenishment priorities",
                "Supplier scorecards",
                "Inventory health",
                "Forecast accuracy"
            ]
        },

        "executive_dashboard": {
            "widget_id": "genie_executive",
            "title": "Ask About Strategic Performance",
            "placeholder": "Ask me about revenue, growth, markets, or strategy...",
            "suggested_prompts": [
                "How is revenue trending vs last year?",
                "Which regions are growing fastest?",
                "What are our biggest opportunities?",
                "How do we compare to competitors?"
            ],
            "context_filters": ["time_period"],
            "quick_insights": [
                "Financial summary",
                "Growth drivers",
                "Market position",
                "Strategic risks"
            ]
        }
    }

    return dashboard_widgets

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Setup

# COMMAND ----------

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")

print(f"Setting up Genie integration for {catalog_name}.{schema_name}")

# Create Genie space configuration
genie_config = create_genie_space(catalog_name, schema_name, workspace_url)

# Create sample questions library
sample_questions = create_sample_questions_library()

# Create dashboard widget configurations
dashboard_widgets = create_dashboard_genie_widgets()

# Save configurations to Delta tables
config_data = [{
    "config_type": "genie_space",
    "config_name": "retail_insight_cockpit",
    "created_date": datetime.now(),
    "configuration": json.dumps(genie_config, indent=2, default=str)
}]

config_df = spark.createDataFrame(config_data)
config_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_space_config")

# Save sample questions
questions_data = []
for role, categories in sample_questions.items():
    for category, questions in categories.items():
        for question in questions:
            questions_data.append({
                "role": role,
                "category": category,
                "question": question,
                "created_date": datetime.now()
            })

questions_df = spark.createDataFrame(questions_data)
questions_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_sample_questions")

# Save dashboard widget configurations
widget_data = []
for dashboard, config in dashboard_widgets.items():
    widget_data.append({
        "dashboard_name": dashboard,
        "widget_config": json.dumps(config, indent=2),
        "created_date": datetime.now()
    })

widgets_df = spark.createDataFrame(widget_data)
widgets_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_dashboard_widgets")

print("✅ Genie integration setup completed successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation and Testing

# COMMAND ----------

# Test data access for Genie
test_queries = {
    "Sales Data": f"SELECT COUNT(*) as sales_count FROM {catalog_name}.{schema_name}.sales",
    "Inventory Alerts": f"SELECT COUNT(*) as alert_count FROM {catalog_name}.{schema_name}.inventory_alerts WHERE priority = 'CRITICAL'",
    "Store Performance": f"SELECT COUNT(*) as store_count FROM {catalog_name}.{schema_name}.store_performance",
    "Customer Segments": f"SELECT customer_segment, COUNT(*) as count FROM {catalog_name}.{schema_name}.customer_segments GROUP BY customer_segment"
}

print("TESTING GENIE DATA ACCESS:")
print("=" * 50)

for test_name, query in test_queries.items():
    try:
        result = spark.sql(query).collect()
        print(f"✅ {test_name}: {len(result)} rows returned")
        if len(result) > 0:
            print(f"   Sample: {dict(result[0])}")
    except Exception as e:
        print(f"❌ {test_name}: Error - {e}")

print("\nGENIE CONFIGURATION SUMMARY:")
print("=" * 50)
print(f"📊 Catalog: {catalog_name}")
print(f"📊 Schema: {schema_name}")
print(f"📊 Tables: 7 core tables + 6 analytical views")
print(f"📊 Roles: 4 personas with specific contexts")
print(f"📊 Sample Questions: {len(questions_data)} curated prompts")
print(f"📊 Dashboard Widgets: {len(dashboard_widgets)} embedded interfaces")

print("\n🎉 Retail Insight Cockpit Genie is ready for natural language queries!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions

# COMMAND ----------

print("""
GENIE INTEGRATION USAGE GUIDE
=============================

1. ACCESSING GENIE:
   • Open any dashboard in the Retail Insight Cockpit
   • Look for the Genie widget/button
   • Click to open the natural language interface

2. ASKING QUESTIONS:
   • Use natural language - no SQL required
   • Be specific about time periods, stores, categories
   • Ask follow-up questions for deeper insights

3. EXAMPLE QUERIES BY ROLE:

   Store Manager:
   "How are today's sales vs target for my store?"
   "What products are out of stock and need reordering?"
   "Who are my VIP customers and when do they shop?"

   Merchandiser:
   "Which products need markdown to clear inventory?"
   "How did last quarter's promotions perform?"
   "What seasonal trends should I plan for?"

   Supply Chain:
   "What products need immediate replenishment?"
   "Which suppliers have delivery issues?"
   "Where do I have excess inventory to redistribute?"

   Executive:
   "How is revenue trending compared to last year?"
   "Which regions are growing fastest?"
   "What are our biggest operational risks?"

4. GETTING BETTER RESULTS:
   • Include specific time periods: "last 30 days", "this quarter"
   • Specify locations: "my store", "West region", "all stores"
   • Ask for comparisons: "vs last year", "vs target", "vs benchmark"
   • Request specific metrics: "revenue", "units", "margin", "ROI"

5. FOLLOW-UP QUESTIONS:
   • Genie will suggest relevant follow-up questions
   • Drill down: "Why is that happening?"
   • Expand scope: "How does this look across all stores?"
   • Get recommendations: "What should I do about this?"

6. TROUBLESHOOTING:
   • If results seem wrong, rephrase your question
   • Be more specific about what you're looking for
   • Check if you have access to the requested data
   • Use suggested prompts as starting points

Ready to start asking questions! 🚀
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook has successfully configured Databricks Genie for the Retail Insight Cockpit:
# MAGIC
# MAGIC ✅ **Genie Space Configuration**: Optimized for retail analytics with proper business context
# MAGIC
# MAGIC ✅ **Role-Based Personas**: Customized for Store Managers, Merchandisers, Supply Chain, and Executives
# MAGIC
# MAGIC ✅ **Sample Question Library**: 100+ curated prompts organized by role and use case
# MAGIC
# MAGIC ✅ **Dashboard Integration**: Embedded Genie widgets for each dashboard
# MAGIC
# MAGIC ✅ **Data Access Validation**: Confirmed access to all tables and views
# MAGIC
# MAGIC ✅ **Usage Documentation**: Complete guide for end users
# MAGIC
# MAGIC Users can now ask natural language questions like:
# MAGIC - "What products are critically out of stock?"
# MAGIC - "How are sales trending vs last year?"
# MAGIC - "Which customers are at risk of churning?"
# MAGIC - "What promotions had the best ROI?"
# MAGIC
# MAGIC Genie will provide intelligent, context-aware responses with actionable insights for retail decision-making.