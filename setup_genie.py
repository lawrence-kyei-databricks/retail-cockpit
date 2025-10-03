# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Insight Cockpit - Genie AI Setup
# MAGIC
# MAGIC This notebook configures Genie AI for natural language queries across the retail dataset.
# MAGIC It sets up business context, sample questions, and semantic understanding.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters and Configuration

# COMMAND ----------

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Setting up Genie AI for {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Business Context and Semantic Layer

# COMMAND ----------

# Define business context for Genie
business_context = f"""
# Retail Insight Cockpit - Business Context

## Domain: Retail Analytics
This is a comprehensive retail analytics platform with the following key entities:

### Core Business Entities:
- **Stores**: Physical retail locations with performance metrics
- **Products**: Items sold across stores with category hierarchy
- **Customers**: Shoppers with loyalty tiers and purchase history
- **Sales**: Transaction records with detailed product and customer information
- **Inventory**: Stock levels and movement tracking
- **Suppliers**: Product suppliers with performance ratings
- **Promotions**: Marketing campaigns and discount programs

### Key Business Metrics:
- **Revenue**: Total sales amount (total_amount in sales table)
- **Units Sold**: Quantity of products sold
- **Margin**: Difference between selling price and cost
- **Inventory Turnover**: How quickly products sell (days_on_hand)
- **Customer Segments**: VIP, High Value, Frequent Buyer, etc.
- **Store Performance**: Sales vs targets and regional comparisons

### Common Business Questions:
1. "How are sales performing today vs last week?"
2. "Which products are out of stock and need reordering?"
3. "Who are our top customers and what do they buy?"
4. "Which stores are underperforming vs target?"
5. "What promotions are driving the most incremental sales?"

### Data Relationships:
- Sales connects to Products, Stores, Customers, and Promotions
- Products belong to Categories and are supplied by Suppliers
- Inventory tracks product levels at each Store
- Customers have loyalty tiers and preferred stores

### Time Periods:
- Use CURRENT_DATE() for "today"
- "Last 30 days" = CURRENT_DATE() - INTERVAL 30 DAYS
- "This quarter" = current quarter based on CURRENT_DATE()
- "Year to date" = January 1st to CURRENT_DATE()

### Business Rules:
- Active products have is_active = true
- Store performance measured against daily_sales_target
- Critical alerts have priority = 'CRITICAL'
- Returns are flagged with return_flag = true
"""

print("Business context defined for Genie AI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample Questions by Role

# COMMAND ----------

# Store Manager Sample Questions
store_manager_questions = [
    "How are today's sales compared to my target?",
    "Which products are out of stock in my store?",
    "What are my top selling products this week?",
    "How many customers visited my store today?",
    "What time of day do I get the most sales?",
    "Which products have the highest returns?",
    "How does my store compare to others in my region?",
    "What inventory alerts do I need to address today?",
    "How is my conversion rate trending?",
    "What promotions are most effective in my store?"
]

# Merchandiser Sample Questions
merchandiser_questions = [
    "Which product categories are growing fastest?",
    "What products need markdown to clear inventory?",
    "How did our summer sale promotion perform?",
    "Which brands have the highest margins?",
    "What seasonal products should we discontinue?",
    "Which suppliers have the best performing products?",
    "How does pricing affect product velocity?",
    "What categories have the highest inventory turnover?",
    "Which new products are exceeding sales expectations?",
    "What products should we bundle for the holiday season?"
]

# Supply Chain Sample Questions
supply_chain_questions = [
    "Which products across all stores need immediate replenishment?",
    "What suppliers are consistently late with deliveries?",
    "Where do I have excess inventory that could be redistributed?",
    "How accurate are our demand forecasts?",
    "Which products have the longest days on hand?",
    "What stores have the most stockout alerts?",
    "How is our inventory turnover trending by category?",
    "Which suppliers have the best performance ratings?",
    "What products are slow moving and need attention?",
    "How much safety stock should we maintain?"
]

# Executive Sample Questions
executive_questions = [
    "How is our revenue trending compared to last year?",
    "Which regions are growing fastest and why?",
    "What are our biggest operational risks right now?",
    "How does our market share compare to competitors?",
    "Which stores are our top performers by sales per square foot?",
    "What's our customer acquisition cost trending?",
    "How effective are our promotional campaigns?",
    "What categories drive the most profitable growth?",
    "How is customer loyalty trending across segments?",
    "What's our inventory investment and turnover?"
]

print("Sample questions defined for all roles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Genie Space Configuration

# COMMAND ----------

# This would typically be done through the Databricks UI, but here's the configuration
genie_config = {
    "space_name": f"Retail Insight Cockpit - {catalog_name.title()}",
    "description": "AI-powered natural language interface for retail analytics",
    "data_sources": [
        f"{catalog_name}.{schema_name}.sales",
        f"{catalog_name}.{schema_name}.products",
        f"{catalog_name}.{schema_name}.stores",
        f"{catalog_name}.{schema_name}.customers",
        f"{catalog_name}.{schema_name}.inventory",
        f"{catalog_name}.{schema_name}.suppliers",
        f"{catalog_name}.{schema_name}.promotions",
        f"{catalog_name}.{schema_name}.categories",
        # Analytical views
        f"{catalog_name}.{schema_name}.daily_sales_agg",
        f"{catalog_name}.{schema_name}.store_performance",
        f"{catalog_name}.{schema_name}.inventory_alerts",
        f"{catalog_name}.{schema_name}.customer_segments",
        f"{catalog_name}.{schema_name}.product_performance",
        f"{catalog_name}.{schema_name}.promotional_performance"
    ],
    "business_context": business_context,
    "sample_questions": {
        "Store Manager": store_manager_questions,
        "Merchandiser": merchandiser_questions,
        "Supply Chain": supply_chain_questions,
        "Executive": executive_questions
    }
}

print("Genie configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Data Access

# COMMAND ----------

# Test queries to ensure Genie can access the data
test_queries = [
    f"SELECT COUNT(*) as total_sales FROM {catalog_name}.{schema_name}.sales",
    f"SELECT COUNT(*) as total_products FROM {catalog_name}.{schema_name}.products",
    f"SELECT COUNT(*) as total_stores FROM {catalog_name}.{schema_name}.stores",
    f"SELECT COUNT(*) as total_customers FROM {catalog_name}.{schema_name}.customers"
]

print("Testing data access for Genie...")
for query in test_queries:
    try:
        result = spark.sql(query).collect()[0][0]
        table_name = query.split("FROM ")[1].split(".")[-1]
        print(f"[OK] {table_name}: {result:,} records")
    except Exception as e:
        print(f"[ERROR] Error accessing data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Instructions

# COMMAND ----------

print("""
Genie AI Setup Complete!

Manual Setup Steps (Complete in Databricks UI):

1. **Create Genie Space:**
   - Go to Databricks workspace
   - Navigate to "Genie" in the sidebar
   - Click "Create Space"
   - Use configuration from this notebook

2. **Configure Data Sources:**
   - Add all tables from the retail cockpit schema
   - Include both raw tables and analytical views
   - Set appropriate permissions

3. **Add Business Context:**
   - Copy the business context from this notebook
   - Add to Genie space configuration
   - Include sample questions for each role

4. **Test Natural Language Queries:**
   - Try sample questions from each role
   - Refine responses based on results
   - Train Genie with additional examples

5. **Share with Users:**
   - Grant access to appropriate user groups
   - Provide training on natural language queries
   - Share sample questions as examples

Sample Questions to Test:
""")

print("\nStore Manager:")
for q in store_manager_questions[:3]:
    print(f"   - {q}")

print("\nMerchandiser:")
for q in merchandiser_questions[:3]:
    print(f"   - {q}")

print("\nSupply Chain:")
for q in supply_chain_questions[:3]:
    print(f"   - {q}")

print("\nExecutive:")
for q in executive_questions[:3]:
    print(f"   - {q}")

print(f"""

Your Retail Insight Cockpit is now ready with:
   - All data tables and views
   - Role-based dashboards
   - Genie AI configuration
   - Sample questions and business context

Start asking questions in natural language!
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Export Sample Questions

# COMMAND ----------

# Create a summary of all sample questions for documentation
all_questions = {
    "Store Manager": store_manager_questions,
    "Merchandiser": merchandiser_questions,
    "Supply Chain": supply_chain_questions,
    "Executive": executive_questions
}

# This could be exported to a file or documentation system
print("Sample Questions by Role:")
print("=" * 50)

for role, questions in all_questions.items():
    print(f"\n{role.upper()}:")
    print("-" * len(role))
    for i, question in enumerate(questions, 1):
        print(f"{i:2d}. {question}")

print(f"\nTotal sample questions: {sum(len(q) for q in all_questions.values())}")
print("Genie AI setup complete!")