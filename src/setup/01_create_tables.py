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
# MAGIC Note: Tables are created without DEFAULT values in DDL to avoid Delta feature conflicts.
# MAGIC Default values can be handled at the application layer during data insertion.

# COMMAND ----------

# Create stores table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.stores (
  store_id STRING NOT NULL,
  store_name STRING NOT NULL,
  region STRING NOT NULL,
  district STRING,
  address STRING,
  city STRING,
  state STRING,
  country STRING,
  postal_code STRING,
  latitude DECIMAL(10,8),
  longitude DECIMAL(11,8),
  store_type STRING,
  square_footage INT,
  opening_date DATE,
  status STRING,
  manager_id STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created stores table")

# COMMAND ----------

# Create categories table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.categories (
  category_id STRING NOT NULL,
  category_name STRING NOT NULL,
  parent_category_id STRING,
  category_level INT,
  description STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created categories table")

# COMMAND ----------

# Create suppliers table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.suppliers (
  supplier_id STRING NOT NULL,
  supplier_name STRING NOT NULL,
  contact_person STRING,
  email STRING,
  phone STRING,
  address STRING,
  city STRING,
  country STRING,
  lead_time_days INT,
  performance_rating DECIMAL(3,2),
  payment_terms STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created suppliers table")

# COMMAND ----------

# Create products table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.products (
  product_id STRING NOT NULL,
  sku STRING NOT NULL,
  product_name STRING NOT NULL,
  description STRING,
  category_id STRING NOT NULL,
  supplier_id STRING,
  brand STRING,
  size STRING,
  color STRING,
  weight DECIMAL(10,2),
  unit_cost DECIMAL(10,2),
  retail_price DECIMAL(10,2),
  wholesale_price DECIMAL(10,2),
  launch_date DATE,
  discontinue_date DATE,
  is_seasonal BOOLEAN,
  season STRING,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (category_id)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created products table")

# COMMAND ----------

# Create customers table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.customers (
  customer_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  date_of_birth DATE,
  gender STRING,
  address STRING,
  city STRING,
  state STRING,
  postal_code STRING,
  country STRING,
  registration_date DATE,
  loyalty_tier STRING,
  preferred_store_id STRING,
  email_opt_in BOOLEAN,
  sms_opt_in BOOLEAN,
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created customers table")

# COMMAND ----------

# Create sales table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.sales (
  transaction_id STRING NOT NULL,
  sale_date DATE NOT NULL,
  sale_timestamp TIMESTAMP NOT NULL,
  store_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING NOT NULL,
  sku STRING NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL,
  discount_amount DECIMAL(10,2),
  tax_amount DECIMAL(10,2),
  total_amount DECIMAL(10,2) NOT NULL,
  payment_method STRING,
  promotion_id STRING,
  sales_associate_id STRING,
  channel STRING,
  return_flag BOOLEAN,
  created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (sale_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created sales table")

# COMMAND ----------

# Create inventory table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.inventory (
  inventory_date DATE NOT NULL,
  store_id STRING NOT NULL,
  product_id STRING NOT NULL,
  sku STRING NOT NULL,
  beginning_inventory INT,
  received_quantity INT,
  sold_quantity INT,
  transferred_out INT,
  transferred_in INT,
  shrinkage INT,
  ending_inventory INT,
  safety_stock INT,
  reorder_point INT,
  max_stock INT,
  last_received_date DATE,
  last_sold_date DATE,
  days_on_hand INT,
  stockout_flag BOOLEAN,
  overstock_flag BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
PARTITIONED BY (inventory_date)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created inventory table")

# COMMAND ----------

# Create promotions table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.promotions (
  promotion_id STRING NOT NULL,
  promotion_name STRING NOT NULL,
  promotion_type STRING,
  description STRING,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  discount_percentage DECIMAL(5,2),
  discount_amount DECIMAL(10,2),
  min_purchase_amount DECIMAL(10,2),
  applicable_products STRING,
  applicable_categories STRING,
  applicable_stores STRING,
  channel STRING,
  budget DECIMAL(15,2),
  target_revenue DECIMAL(15,2),
  is_active BOOLEAN,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
) USING DELTA
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
)
""")

print("Created promotions table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Indexes and Constraints

# COMMAND ----------

# Create primary key constraints and optimize tables
tables_and_keys = [
    ("stores", "store_id"),
    ("categories", "category_id"),
    ("suppliers", "supplier_id"),
    ("products", "product_id"),
    ("customers", "customer_id"),
    ("sales", "transaction_id"),
    ("promotions", "promotion_id")
]

for table_name, pk_column in tables_and_keys:
    try:
        spark.sql(f"""
        ALTER TABLE {catalog_name}.{schema_name}.{table_name}
        ADD CONSTRAINT {table_name}_pk PRIMARY KEY({pk_column})
        """)
        print(f"Added primary key constraint to {table_name}")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Primary key constraint already exists for {table_name}")
        else:
            print(f"Note: Could not add primary key to {table_name}: {e}")

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
    try:
        row_count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table_name}").collect()[0]['count']
        print(f"  - {table_name}: {row_count} rows")
    except:
        print(f"  - {table_name}: table exists")

print(f"\nTables created successfully in {catalog_name}.{schema_name}")
print("Ready for sample data generation and analytical view creation.")