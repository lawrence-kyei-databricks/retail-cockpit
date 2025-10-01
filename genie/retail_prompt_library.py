# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Insight Cockpit - Genie Prompt Library
# MAGIC
# MAGIC This notebook creates and manages curated prompt libraries for Genie AI integration.
# MAGIC It provides role-specific prompts optimized for retail analytics use cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Setup

# COMMAND ----------

import json
from datetime import datetime, date
from typing import Dict, List, Any

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Library Configuration

# COMMAND ----------

class RetailPromptLibrary:
    """
    Retail Insight Cockpit Genie Prompt Library

    Provides curated, role-specific prompts for natural language queries
    across different retail personas and use cases.
    """

    def __init__(self, catalog_name: str, schema_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.base_context = f"""
        You are a retail analytics AI assistant with access to comprehensive retail data in {catalog_name}.{schema_name}.

        Available tables:
        - sales: Transaction-level sales data with customer, product, store details
        - customers: Customer master data with loyalty tiers and segments
        - products: Product catalog with pricing, categories, suppliers
        - stores: Store locations with regions, types, performance metrics
        - inventory: Inventory levels, movements, and alerts
        - promotions: Marketing campaigns and promotional data
        - suppliers: Vendor information and performance data

        Available views:
        - daily_sales_agg: Daily sales aggregations by store and category
        - store_performance: Store-level KPIs and benchmarks
        - inventory_alerts: Real-time inventory alerts and priorities
        - customer_segments: Customer segmentation and churn analysis
        - product_performance: Product sales and profitability metrics
        - promotional_performance: Campaign effectiveness and ROI

        Always provide specific, actionable insights with relevant data points.
        Include time periods, store/region context, and performance comparisons when relevant.
        """

    def get_store_manager_prompts(self) -> List[Dict[str, Any]]:
        """Store Manager focused prompts for daily operations"""
        return [
            {
                "category": "Daily Performance",
                "prompts": [
                    {
                        "question": "How are today's sales tracking vs target?",
                        "context": "Store manager needs immediate performance update",
                        "expected_data": ["current sales", "target", "variance", "trend"],
                        "sample_followup": ["Which departments are overperforming?", "What's driving the variance?"]
                    },
                    {
                        "question": "What are my top selling products today?",
                        "context": "Identify high-performing items for staff focus",
                        "expected_data": ["product names", "units sold", "revenue", "margin"],
                        "sample_followup": ["Are these seasonal trends?", "Do we have enough inventory?"]
                    },
                    {
                        "question": "Show me products that are out of stock with high demand",
                        "context": "Critical stockout management",
                        "expected_data": ["product names", "stockout duration", "lost sales estimate"],
                        "sample_followup": ["When will we receive new inventory?", "Can we substitute with similar products?"]
                    }
                ]
            },
            {
                "category": "Customer Insights",
                "prompts": [
                    {
                        "question": "Which customers haven't visited in the last 30 days?",
                        "context": "Customer retention and win-back opportunities",
                        "expected_data": ["customer names", "last visit", "typical spend", "contact info"],
                        "sample_followup": ["What was their purchase history?", "Should we send them a promotion?"]
                    },
                    {
                        "question": "Who are my VIP customers and how often do they shop?",
                        "context": "High-value customer relationship management",
                        "expected_data": ["customer names", "loyalty tier", "frequency", "average spend"],
                        "sample_followup": ["What do they typically buy?", "When do they usually visit?"]
                    }
                ]
            },
            {
                "category": "Operational Efficiency",
                "prompts": [
                    {
                        "question": "What are my busiest hours and how should I staff them?",
                        "context": "Staffing optimization based on traffic patterns",
                        "expected_data": ["hourly traffic", "transaction volume", "staffing recommendations"],
                        "sample_followup": ["How does this compare to last week?", "What about seasonal variations?"]
                    },
                    {
                        "question": "Which products have high return rates and why?",
                        "context": "Quality and customer satisfaction monitoring",
                        "expected_data": ["product names", "return rates", "return reasons", "impact"],
                        "sample_followup": ["Is this a supplier issue?", "Should we stop carrying these items?"]
                    }
                ]
            }
        ]

    def get_merchandiser_prompts(self) -> List[Dict[str, Any]]:
        """Merchandiser focused prompts for product and category management"""
        return [
            {
                "category": "Product Performance",
                "prompts": [
                    {
                        "question": "Which products are underperforming and need markdown?",
                        "context": "Inventory liquidation and margin optimization",
                        "expected_data": ["product names", "current inventory", "sales velocity", "markdown recommendations"],
                        "sample_followup": ["What markdown percentage would clear inventory in 30 days?", "What's the margin impact?"]
                    },
                    {
                        "question": "Show me seasonal trends for my top categories",
                        "context": "Seasonal buying and inventory planning",
                        "expected_data": ["category names", "monthly trends", "peak seasons", "planning insights"],
                        "sample_followup": ["How do weather patterns affect sales?", "When should I start seasonal buying?"]
                    },
                    {
                        "question": "What new products are performing well since launch?",
                        "context": "New product introduction success tracking",
                        "expected_data": ["product names", "launch date", "sales performance", "customer adoption"],
                        "sample_followup": ["Should we expand distribution?", "What similar products should we consider?"]
                    }
                ]
            },
            {
                "category": "Pricing Strategy",
                "prompts": [
                    {
                        "question": "Which products can I raise prices on without affecting sales?",
                        "context": "Price elasticity and margin optimization",
                        "expected_data": ["product names", "current prices", "elasticity analysis", "recommendations"],
                        "sample_followup": ["How much can I increase without customer pushback?", "What's the profit impact?"]
                    },
                    {
                        "question": "How do my prices compare to market rates?",
                        "context": "Competitive pricing analysis",
                        "expected_data": ["product comparisons", "price positioning", "competitive gaps"],
                        "sample_followup": ["Where am I overpriced?", "What opportunities exist to capture market share?"]
                    }
                ]
            },
            {
                "category": "Promotional Analysis",
                "prompts": [
                    {
                        "question": "Which promotions drove the best ROI last quarter?",
                        "context": "Campaign effectiveness and future planning",
                        "expected_data": ["promotion names", "ROI metrics", "incremental sales", "customer response"],
                        "sample_followup": ["Can we repeat successful promotions?", "What made them effective?"]
                    },
                    {
                        "question": "What products should I bundle together for cross-selling?",
                        "context": "Market basket analysis and bundling strategies",
                        "expected_data": ["product pairs", "co-purchase frequency", "bundle opportunities"],
                        "sample_followup": ["What discount would make bundles attractive?", "How often do customers buy these together?"]
                    }
                ]
            }
        ]

    def get_supply_chain_prompts(self) -> List[Dict[str, Any]]:
        """Supply Chain focused prompts for inventory and logistics optimization"""
        return [
            {
                "category": "Inventory Management",
                "prompts": [
                    {
                        "question": "Which products need immediate replenishment across all stores?",
                        "context": "Critical stockout prevention and ordering",
                        "expected_data": ["product names", "current stock", "reorder points", "urgency levels"],
                        "sample_followup": ["Can we expedite delivery?", "Which suppliers have fastest lead times?"]
                    },
                    {
                        "question": "Where do I have excess inventory that needs redistribution?",
                        "context": "Inventory balancing and transfer optimization",
                        "expected_data": ["store locations", "product names", "excess quantities", "transfer recommendations"],
                        "sample_followup": ["What's the cost of transferring vs markdowns?", "Which stores need these products most?"]
                    },
                    {
                        "question": "What's my inventory turnover by category and how can I improve it?",
                        "context": "Working capital optimization and efficiency",
                        "expected_data": ["category names", "turnover rates", "benchmarks", "improvement opportunities"],
                        "sample_followup": ["Which categories tie up too much cash?", "What's the optimal inventory level?"]
                    }
                ]
            },
            {
                "category": "Supplier Performance",
                "prompts": [
                    {
                        "question": "Which suppliers are consistently late with deliveries?",
                        "context": "Supplier performance management and risk mitigation",
                        "expected_data": ["supplier names", "on-time rates", "delay patterns", "impact assessment"],
                        "sample_followup": ["Should we find alternative suppliers?", "What penalties should we enforce?"]
                    },
                    {
                        "question": "How can I reduce my total cost of goods sold?",
                        "context": "Cost optimization and negotiation opportunities",
                        "expected_data": ["cost breakdown", "supplier comparison", "negotiation targets"],
                        "sample_followup": ["Which suppliers offer volume discounts?", "Where can I consolidate orders?"]
                    }
                ]
            },
            {
                "category": "Demand Forecasting",
                "prompts": [
                    {
                        "question": "What's my demand forecast accuracy and where can I improve?",
                        "context": "Forecasting performance and methodology improvement",
                        "expected_data": ["accuracy metrics", "error analysis", "improvement recommendations"],
                        "sample_followup": ["Which products are hardest to forecast?", "How do external factors affect accuracy?"]
                    },
                    {
                        "question": "Which products show unusual demand patterns that need investigation?",
                        "context": "Anomaly detection and trend analysis",
                        "expected_data": ["product names", "demand patterns", "anomalies", "potential causes"],
                        "sample_followup": ["Is this a market trend or data issue?", "Should I adjust my forecasting model?"]
                    }
                ]
            }
        ]

    def get_executive_prompts(self) -> List[Dict[str, Any]]:
        """Executive focused prompts for strategic insights and performance overview"""
        return [
            {
                "category": "Financial Performance",
                "prompts": [
                    {
                        "question": "How is our revenue and profit trending compared to last year?",
                        "context": "High-level financial performance and growth trends",
                        "expected_data": ["revenue trends", "profit margins", "year-over-year comparison", "forecasts"],
                        "sample_followup": ["What's driving the changes?", "Are we meeting our annual targets?"]
                    },
                    {
                        "question": "Which regions and stores are underperforming and need attention?",
                        "context": "Performance management and resource allocation",
                        "expected_data": ["region rankings", "store performance", "improvement opportunities"],
                        "sample_followup": ["What support do underperforming locations need?", "Should we consider closures?"]
                    },
                    {
                        "question": "What's our return on marketing investment across channels?",
                        "context": "Marketing effectiveness and budget allocation",
                        "expected_data": ["channel ROI", "campaign performance", "budget recommendations"],
                        "sample_followup": ["Where should we increase spending?", "Which channels should we reduce?"]
                    }
                ]
            },
            {
                "category": "Strategic Insights",
                "prompts": [
                    {
                        "question": "What are our biggest growth opportunities in the next quarter?",
                        "context": "Strategic planning and opportunity identification",
                        "expected_data": ["growth drivers", "market opportunities", "resource requirements"],
                        "sample_followup": ["What investment is needed?", "What are the risks?"]
                    },
                    {
                        "question": "How is customer loyalty trending and what impacts retention?",
                        "context": "Customer experience and lifetime value management",
                        "expected_data": ["loyalty metrics", "retention trends", "satisfaction drivers"],
                        "sample_followup": ["What initiatives improve loyalty most?", "How do we compare to competitors?"]
                    },
                    {
                        "question": "What operational risks need immediate attention?",
                        "context": "Risk management and business continuity",
                        "expected_data": ["risk assessment", "critical issues", "mitigation strategies"],
                        "sample_followup": ["What's the financial impact?", "How quickly can we resolve these?"]
                    }
                ]
            },
            {
                "category": "Competitive Analysis",
                "prompts": [
                    {
                        "question": "Where are we gaining or losing market share?",
                        "context": "Competitive positioning and market dynamics",
                        "expected_data": ["market share trends", "competitive analysis", "positioning insights"],
                        "sample_followup": ["What competitive advantages do we have?", "Where should we invest to compete better?"]
                    },
                    {
                        "question": "How do our margins compare to industry benchmarks?",
                        "context": "Profitability benchmarking and improvement opportunities",
                        "expected_data": ["margin comparison", "industry benchmarks", "improvement areas"],
                        "sample_followup": ["What drives margin differences?", "How can we improve profitability?"]
                    }
                ]
            }
        ]

    def get_cross_functional_prompts(self) -> List[Dict[str, Any]]:
        """Cross-functional prompts useful across roles"""
        return [
            {
                "category": "Alerts & Exceptions",
                "prompts": [
                    {
                        "question": "What critical issues need my attention today?",
                        "context": "Daily exception reporting and priority management",
                        "expected_data": ["critical alerts", "priority ranking", "recommended actions"],
                        "sample_followup": ["What's the timeline to resolve?", "Who else needs to be involved?"]
                    },
                    {
                        "question": "Show me unusual patterns in sales or inventory",
                        "context": "Anomaly detection and investigation",
                        "expected_data": ["anomalies detected", "pattern analysis", "potential causes"],
                        "sample_followup": ["Is this a data quality issue?", "What's the business impact?"]
                    }
                ]
            },
            {
                "category": "Performance Comparisons",
                "prompts": [
                    {
                        "question": "How does this week compare to last week and last year?",
                        "context": "Trend analysis and performance context",
                        "expected_data": ["week-over-week trends", "year-over-year comparison", "variance analysis"],
                        "sample_followup": ["What's driving the changes?", "Is this seasonal or structural?"]
                    },
                    {
                        "question": "Which metrics are trending in the wrong direction?",
                        "context": "Early warning system and course correction",
                        "expected_data": ["declining metrics", "trend analysis", "correction opportunities"],
                        "sample_followup": ["What's causing the decline?", "How quickly can we intervene?"]
                    }
                ]
            }
        ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Integration Functions

# COMMAND ----------

def create_genie_prompt_datasets(prompt_library: RetailPromptLibrary, catalog_name: str, schema_name: str):
    """
    Create Genie-compatible prompt datasets for each role
    """

    # Store Manager Prompts
    store_mgr_prompts = []
    for category in prompt_library.get_store_manager_prompts():
        for prompt in category["prompts"]:
            store_mgr_prompts.append({
                "role": "Store Manager",
                "category": category["category"],
                "question": prompt["question"],
                "context": prompt["context"],
                "expected_data": ", ".join(prompt["expected_data"]),
                "sample_followup": ", ".join(prompt["sample_followup"]),
                "persona_context": f"You are analyzing data for a store manager who needs {prompt['context'].lower()}",
                "data_scope": "Store-level and product-level details",
                "urgency": "High" if "immediate" in prompt["context"].lower() or "critical" in prompt["context"].lower() else "Medium"
            })

    # Create DataFrame and save as Delta table
    store_mgr_df = spark.createDataFrame(store_mgr_prompts)
    store_mgr_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_prompts_store_manager")

    # Merchandiser Prompts
    merchandiser_prompts = []
    for category in prompt_library.get_merchandiser_prompts():
        for prompt in category["prompts"]:
            merchandiser_prompts.append({
                "role": "Merchandiser",
                "category": category["category"],
                "question": prompt["question"],
                "context": prompt["context"],
                "expected_data": ", ".join(prompt["expected_data"]),
                "sample_followup": ", ".join(prompt["sample_followup"]),
                "persona_context": f"You are analyzing data for a merchandiser who needs {prompt['context'].lower()}",
                "data_scope": "Product and category performance across all stores",
                "urgency": "Medium"
            })

    merchandiser_df = spark.createDataFrame(merchandiser_prompts)
    merchandiser_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_prompts_merchandiser")

    # Supply Chain Prompts
    supply_chain_prompts = []
    for category in prompt_library.get_supply_chain_prompts():
        for prompt in category["prompts"]:
            supply_chain_prompts.append({
                "role": "Supply Chain",
                "category": category["category"],
                "question": prompt["question"],
                "context": prompt["context"],
                "expected_data": ", ".join(prompt["expected_data"]),
                "sample_followup": ", ".join(prompt["sample_followup"]),
                "persona_context": f"You are analyzing data for a supply chain manager who needs {prompt['context'].lower()}",
                "data_scope": "Inventory, suppliers, and logistics across all locations",
                "urgency": "High" if "immediate" in prompt["context"].lower() else "Medium"
            })

    supply_chain_df = spark.createDataFrame(supply_chain_prompts)
    supply_chain_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_prompts_supply_chain")

    # Executive Prompts
    executive_prompts = []
    for category in prompt_library.get_executive_prompts():
        for prompt in category["prompts"]:
            executive_prompts.append({
                "role": "Executive",
                "category": category["category"],
                "question": prompt["question"],
                "context": prompt["context"],
                "expected_data": ", ".join(prompt["expected_data"]),
                "sample_followup": ", ".join(prompt["sample_followup"]),
                "persona_context": f"You are providing executive-level insights for {prompt['context'].lower()}",
                "data_scope": "Company-wide strategic metrics and trends",
                "urgency": "Low"
            })

    executive_df = spark.createDataFrame(executive_prompts)
    executive_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_prompts_executive")

    print("Created Genie prompt datasets for all roles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genie Configuration and Setup

# COMMAND ----------

def setup_genie_configuration(catalog_name: str, schema_name: str):
    """
    Create Genie configuration for retail analytics
    """

    genie_config = {
        "name": "Retail Insight Cockpit Genie",
        "description": "AI assistant for retail analytics and insights",
        "instructions": f"""
        You are an expert retail analytics AI assistant with access to comprehensive retail data.

        CONTEXT:
        - You have access to sales, inventory, customer, product, and operational data
        - Data is stored in Unity Catalog at {catalog_name}.{schema_name}
        - Users are retail professionals: store managers, merchandisers, supply chain managers, and executives

        BEHAVIOR:
        - Provide specific, actionable insights with data to support recommendations
        - Always include relevant time periods, comparisons, and context
        - Suggest follow-up questions to deepen analysis
        - Use retail terminology and KPIs appropriately
        - Format numbers clearly (e.g., $1.2M, 15.3%, 1,234 units)
        - Highlight exceptions, trends, and opportunities

        RESPONSE FORMAT:
        1. Direct answer to the question
        2. Supporting data and metrics
        3. Context and comparisons (vs targets, previous periods, benchmarks)
        4. Actionable recommendations
        5. Suggested follow-up questions

        RETAIL FOCUS AREAS:
        - Sales performance and trends
        - Inventory management and optimization
        - Customer behavior and segmentation
        - Product performance and merchandising
        - Operational efficiency
        - Promotional effectiveness
        - Supplier management
        - Financial metrics and profitability
        """,

        "data_sources": {
            "primary_catalog": catalog_name,
            "primary_schema": schema_name,
            "key_tables": [
                "sales", "customers", "products", "stores", "inventory",
                "promotions", "suppliers", "daily_sales_agg", "store_performance",
                "inventory_alerts", "customer_segments", "product_performance"
            ]
        },

        "role_customization": {
            "store_manager": {
                "focus": "Daily operations, customer service, inventory alerts, staff performance",
                "scope": "Single store or district",
                "urgency": "Real-time to daily"
            },
            "merchandiser": {
                "focus": "Product performance, pricing, promotions, category management",
                "scope": "Category or brand across all stores",
                "urgency": "Weekly to monthly"
            },
            "supply_chain": {
                "focus": "Inventory optimization, supplier performance, demand forecasting",
                "scope": "Company-wide logistics and operations",
                "urgency": "Daily to weekly"
            },
            "executive": {
                "focus": "Strategic insights, financial performance, competitive positioning",
                "scope": "Company-wide strategic metrics",
                "urgency": "Weekly to quarterly"
            }
        },

        "prompt_examples": [
            "What products are out of stock in my store?",
            "How are sales trending compared to last year?",
            "Which customers are at risk of churning?",
            "What promotions had the best ROI?",
            "Where should I focus my inventory investment?",
            "How do my margins compare to targets?"
        ]
    }

    # Save configuration as JSON
    config_json = json.dumps(genie_config, indent=2)

    # Create configuration table
    config_data = [{
        "config_name": "retail_genie_setup",
        "config_version": "1.0",
        "created_date": datetime.now(),
        "configuration": config_json
    }]

    config_df = spark.createDataFrame(config_data)
    config_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.genie_configuration")

    print("Created Genie configuration")
    return genie_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Setup

# COMMAND ----------

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Setting up Genie integration for {catalog_name}.{schema_name}")

# Initialize prompt library
prompt_library = RetailPromptLibrary(catalog_name, schema_name)

# Create prompt datasets
create_genie_prompt_datasets(prompt_library, catalog_name, schema_name)

# Setup Genie configuration
genie_config = setup_genie_configuration(catalog_name, schema_name)

print("\nGenie integration setup complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Display sample prompts for each role
print("SAMPLE RETAIL GENIE PROMPTS BY ROLE")
print("=" * 50)

roles = [
    ("Store Manager", prompt_library.get_store_manager_prompts()),
    ("Merchandiser", prompt_library.get_merchandiser_prompts()),
    ("Supply Chain", prompt_library.get_supply_chain_prompts()),
    ("Executive", prompt_library.get_executive_prompts())
]

for role_name, role_prompts in roles:
    print(f"\n{role_name.upper()} PROMPTS:")
    print("-" * 30)

    for category in role_prompts[:2]:  # Show first 2 categories
        print(f"\n{category['category']}:")
        for prompt in category['prompts'][:2]:  # Show first 2 prompts
            print(f"  • {prompt['question']}")

    print(f"\n  ... and more {role_name.lower()} specific prompts")

print(f"\n\nPROMPT DATASETS CREATED:")
print(f"• {catalog_name}.{schema_name}.genie_prompts_store_manager")
print(f"• {catalog_name}.{schema_name}.genie_prompts_merchandiser")
print(f"• {catalog_name}.{schema_name}.genie_prompts_supply_chain")
print(f"• {catalog_name}.{schema_name}.genie_prompts_executive")
print(f"• {catalog_name}.{schema_name}.genie_configuration")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Genie Integration

# COMMAND ----------

# Test queries to verify Genie can access the data
test_queries = [
    "SELECT COUNT(*) as total_products FROM products",
    "SELECT COUNT(*) as total_sales FROM sales WHERE sale_date >= CURRENT_DATE() - INTERVAL 7 DAYS",
    "SELECT COUNT(*) as critical_alerts FROM inventory_alerts WHERE priority = 'CRITICAL'",
    "SELECT region, COUNT(*) as store_count FROM stores GROUP BY region"
]

print("TESTING DATA ACCESS FOR GENIE:")
print("=" * 40)

for i, query in enumerate(test_queries, 1):
    try:
        result = spark.sql(query).collect()[0]
        print(f"{i}. ✅ {query}")
        print(f"   Result: {dict(result)}")
    except Exception as e:
        print(f"{i}. ❌ {query}")
        print(f"   Error: {e}")

print("\nGenie integration ready for natural language queries! 🤖")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC This notebook has successfully created:
# MAGIC
# MAGIC 1. **Role-specific prompt libraries** with curated questions for:
# MAGIC    - Store Managers (operational focus)
# MAGIC    - Merchandisers (product/category focus)
# MAGIC    - Supply Chain (inventory/logistics focus)
# MAGIC    - Executives (strategic focus)
# MAGIC
# MAGIC 2. **Genie configuration** optimized for retail analytics
# MAGIC
# MAGIC 3. **Prompt datasets** stored as Delta tables for easy management
# MAGIC
# MAGIC 4. **Context-aware responses** that understand retail terminology and KPIs
# MAGIC
# MAGIC Users can now ask natural language questions like:
# MAGIC - "What products are critically out of stock?"
# MAGIC - "How are my sales trending vs last year?"
# MAGIC - "Which customers are at risk of churning?"
# MAGIC - "What promotions had the best ROI?"
# MAGIC
# MAGIC The Genie assistant will provide role-appropriate, data-driven insights with actionable recommendations.