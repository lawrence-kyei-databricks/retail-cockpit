# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Insight Cockpit - Table Creation
# MAGIC
# MAGIC This notebook creates the core Unity Catalog tables for the Retail Insight Cockpit.
# MAGIC It defines the schema and creates Delta tables optimized for retail analytics.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters and Configuration

# COMMAND ----------

# Get parameters from Asset Bundle
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Creating tables in {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog and Schema

# COMMAND ----------

# Create catalog if it doesn't exist
spark.sql(f"""
CREATE CATALOG IF NOT EXISTS {catalog_name}
COMMENT 'Retail Analytics Catalog for Insight Cockpit'
""")

# Create schema
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
COMMENT 'Retail Insight Cockpit schema containing all retail analytics tables'
""")

# Use the schema
spark.sql(f"USE {catalog_name}.{schema_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Tables Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Stores table: Physical store locations and metadata
# MAGIC CREATE TABLE IF NOT EXISTS stores (
# MAGIC   store_id STRING NOT NULL,
# MAGIC   store_name STRING NOT NULL,
# MAGIC   region STRING NOT NULL,
# MAGIC   district STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   country STRING,
# MAGIC   postal_code STRING,
# MAGIC   latitude DECIMAL(10,8),
# MAGIC   longitude DECIMAL(11,8),
# MAGIC   store_type STRING, -- Mall, Standalone, Outlet
# MAGIC   square_footage INT,
# MAGIC   opening_date DATE,
# MAGIC   status STRING, -- Active, Closed, Renovation
# MAGIC   manager_id STRING,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Categories table: Product category hierarchy
# MAGIC CREATE TABLE IF NOT EXISTS categories (
# MAGIC   category_id STRING NOT NULL,
# MAGIC   category_name STRING NOT NULL,
# MAGIC   parent_category_id STRING,
# MAGIC   category_level INT, -- 1=Department, 2=Category, 3=Subcategory
# MAGIC   description STRING,
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Suppliers table: Vendor and supplier information
# MAGIC CREATE TABLE IF NOT EXISTS suppliers (
# MAGIC   supplier_id STRING NOT NULL,
# MAGIC   supplier_name STRING NOT NULL,
# MAGIC   contact_person STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   country STRING,
# MAGIC   lead_time_days INT,
# MAGIC   performance_rating DECIMAL(3,2), -- 1.00 to 5.00
# MAGIC   payment_terms STRING,
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Products table: Product master data
# MAGIC CREATE TABLE IF NOT EXISTS products (
# MAGIC   product_id STRING NOT NULL,
# MAGIC   sku STRING NOT NULL,
# MAGIC   product_name STRING NOT NULL,
# MAGIC   description STRING,
# MAGIC   category_id STRING NOT NULL,
# MAGIC   supplier_id STRING,
# MAGIC   brand STRING,
# MAGIC   size STRING,
# MAGIC   color STRING,
# MAGIC   weight DECIMAL(10,2),
# MAGIC   unit_cost DECIMAL(10,2),
# MAGIC   retail_price DECIMAL(10,2),
# MAGIC   wholesale_price DECIMAL(10,2),
# MAGIC   launch_date DATE,
# MAGIC   discontinue_date DATE,
# MAGIC   is_seasonal BOOLEAN DEFAULT FALSE,
# MAGIC   season STRING, -- Spring, Summer, Fall, Winter
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (category_id)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers table: Customer master data
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC   customer_id STRING NOT NULL,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   phone STRING,
# MAGIC   date_of_birth DATE,
# MAGIC   gender STRING,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   postal_code STRING,
# MAGIC   country STRING,
# MAGIC   registration_date DATE,
# MAGIC   loyalty_tier STRING, -- Bronze, Silver, Gold, Platinum
# MAGIC   preferred_store_id STRING,
# MAGIC   email_opt_in BOOLEAN DEFAULT FALSE,
# MAGIC   sms_opt_in BOOLEAN DEFAULT FALSE,
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sales table: Transaction-level sales data
# MAGIC CREATE TABLE IF NOT EXISTS sales (
# MAGIC   transaction_id STRING NOT NULL,
# MAGIC   sale_date DATE NOT NULL,
# MAGIC   sale_timestamp TIMESTAMP NOT NULL,
# MAGIC   store_id STRING NOT NULL,
# MAGIC   customer_id STRING,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   sku STRING NOT NULL,
# MAGIC   quantity INT NOT NULL,
# MAGIC   unit_price DECIMAL(10,2) NOT NULL,
# MAGIC   discount_amount DECIMAL(10,2) DEFAULT 0,
# MAGIC   tax_amount DECIMAL(10,2) DEFAULT 0,
# MAGIC   total_amount DECIMAL(10,2) NOT NULL,
# MAGIC   payment_method STRING, -- Cash, Card, Mobile, Gift Card
# MAGIC   promotion_id STRING,
# MAGIC   sales_associate_id STRING,
# MAGIC   channel STRING DEFAULT 'In-Store', -- In-Store, Online, Mobile
# MAGIC   return_flag BOOLEAN DEFAULT FALSE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (sale_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inventory table: Current inventory levels and movement
# MAGIC CREATE TABLE IF NOT EXISTS inventory (
# MAGIC   inventory_date DATE NOT NULL,
# MAGIC   store_id STRING NOT NULL,
# MAGIC   product_id STRING NOT NULL,
# MAGIC   sku STRING NOT NULL,
# MAGIC   beginning_inventory INT DEFAULT 0,
# MAGIC   received_quantity INT DEFAULT 0,
# MAGIC   sold_quantity INT DEFAULT 0,
# MAGIC   transferred_out INT DEFAULT 0,
# MAGIC   transferred_in INT DEFAULT 0,
# MAGIC   shrinkage INT DEFAULT 0,
# MAGIC   ending_inventory INT DEFAULT 0,
# MAGIC   safety_stock INT DEFAULT 0,
# MAGIC   reorder_point INT DEFAULT 0,
# MAGIC   max_stock INT DEFAULT 0,
# MAGIC   last_received_date DATE,
# MAGIC   last_sold_date DATE,
# MAGIC   days_on_hand INT,
# MAGIC   stockout_flag BOOLEAN DEFAULT FALSE,
# MAGIC   overstock_flag BOOLEAN DEFAULT FALSE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (inventory_date)
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Promotions table: Marketing campaigns and promotions
# MAGIC CREATE TABLE IF NOT EXISTS promotions (
# MAGIC   promotion_id STRING NOT NULL,
# MAGIC   promotion_name STRING NOT NULL,
# MAGIC   promotion_type STRING, -- Discount, BOGO, Bundle, Clearance
# MAGIC   description STRING,
# MAGIC   start_date DATE NOT NULL,
# MAGIC   end_date DATE NOT NULL,
# MAGIC   discount_percentage DECIMAL(5,2),
# MAGIC   discount_amount DECIMAL(10,2),
# MAGIC   min_purchase_amount DECIMAL(10,2),
# MAGIC   applicable_products STRING, -- JSON array or comma-separated
# MAGIC   applicable_categories STRING,
# MAGIC   applicable_stores STRING,
# MAGIC   channel STRING, -- All, In-Store, Online
# MAGIC   budget DECIMAL(15,2),
# MAGIC   target_revenue DECIMAL(15,2),
# MAGIC   is_active BOOLEAN DEFAULT TRUE,
# MAGIC   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
# MAGIC   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Indexes and Constraints

# COMMAND ----------

# Create primary key constraints and optimize tables
spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.stores
ADD CONSTRAINT stores_pk PRIMARY KEY(store_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.categories
ADD CONSTRAINT categories_pk PRIMARY KEY(category_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.suppliers
ADD CONSTRAINT suppliers_pk PRIMARY KEY(supplier_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.products
ADD CONSTRAINT products_pk PRIMARY KEY(product_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.customers
ADD CONSTRAINT customers_pk PRIMARY KEY(customer_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.sales
ADD CONSTRAINT sales_pk PRIMARY KEY(transaction_id)
""")

spark.sql(f"""
ALTER TABLE {catalog_name}.{schema_name}.promotions
ADD CONSTRAINT promotions_pk PRIMARY KEY(promotion_id)
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant Permissions

# COMMAND ----------

# Grant permissions to different user groups
permission_groups = [
    "retail_analysts",
    "store_managers",
    "merchandisers",
    "executives"
]

for group in permission_groups:
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog_name} TO `{group}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog_name}.{schema_name} TO `{group}`")
        spark.sql(f"GRANT SELECT ON SCHEMA {catalog_name}.{schema_name} TO `{group}`")
        print(f"Granted permissions to {group}")
    except Exception as e:
        print(f"Note: Could not grant permissions to {group}. Group may not exist yet: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display table information
tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema_name}").collect()

print("Created tables:")
for table in tables:
    table_name = table['tableName']
    row_count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table_name}").collect()[0]['count']
    print(f"  - {table_name}: {row_count} rows")

print(f"\nTables created successfully in {catalog_name}.{schema_name}")
print("Ready for sample data generation and analytical view creation.")