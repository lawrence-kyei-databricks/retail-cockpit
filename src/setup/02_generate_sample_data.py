# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Insight Cockpit - Sample Data Generation
# MAGIC
# MAGIC This notebook generates realistic sample data for the Retail Insight Cockpit.
# MAGIC The data includes stores, products, customers, sales transactions, and inventory.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries and Setup

# COMMAND ----------

import random
from datetime import datetime, timedelta, date
from pyspark.sql import functions as F
from pyspark.sql.types import *
import uuid
from decimal import Decimal

# Get parameters
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")

print(f"Generating sample data for {catalog_name}.{schema_name}")

# Use the schema
spark.sql(f"USE {catalog_name}.{schema_name}")

# Set seed for reproducible results
random.seed(42)
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Stores Data

# COMMAND ----------

# Clear existing data from stores table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.stores")

# Store data with proper decimal types
stores_data = [
    ("STORE_001", "Downtown Flagship", "West", "Metro West", "123 Main St", "San Francisco", "CA", "USA", "94105", Decimal("37.77490000"), Decimal("-122.41940000"), "Flagship", 15000, date(2018, 3, 15), "Active", "MGR_001"),
    ("STORE_002", "Mall Central", "West", "Metro West", "456 Shopping Center Dr", "Los Angeles", "CA", "USA", "90210", Decimal("34.05220000"), Decimal("-118.24370000"), "Mall", 12000, date(2019, 6, 1), "Active", "MGR_002"),
    ("STORE_003", "Suburban Plaza", "West", "Metro South", "789 Plaza Blvd", "San Diego", "CA", "USA", "92101", Decimal("32.71570000"), Decimal("-117.16110000"), "Plaza", 8000, date(2020, 1, 20), "Active", "MGR_003"),
    ("STORE_004", "Outlet Center", "West", "Metro North", "321 Outlet Way", "Sacramento", "CA", "USA", "95814", Decimal("38.58160000"), Decimal("-121.49440000"), "Outlet", 10000, date(2017, 11, 12), "Active", "MGR_004"),
    ("STORE_005", "Metro East", "East", "Metro East", "555 Commerce St", "New York", "NY", "USA", "10001", Decimal("40.71280000"), Decimal("-74.00600000"), "Standalone", 11000, date(2019, 9, 5), "Active", "MGR_005"),
    ("STORE_006", "Boston Commons", "East", "Metro East", "777 Commonwealth Ave", "Boston", "MA", "USA", "02116", Decimal("42.36010000"), Decimal("-71.05890000"), "Mall", 9500, date(2020, 4, 18), "Active", "MGR_006"),
    ("STORE_007", "Chicago Loop", "Central", "Metro Central", "999 State St", "Chicago", "IL", "USA", "60601", Decimal("41.87810000"), Decimal("-87.62980000"), "Standalone", 13000, date(2018, 8, 25), "Active", "MGR_007"),
    ("STORE_008", "Dallas Galleria", "Central", "Metro South", "111 Galleria Pkwy", "Dallas", "TX", "USA", "75240", Decimal("32.77670000"), Decimal("-96.79700000"), "Mall", 14000, date(2019, 12, 10), "Active", "MGR_008"),
    ("STORE_009", "Miami Beach", "South", "Metro South", "222 Ocean Dr", "Miami", "FL", "USA", "33139", Decimal("25.76170000"), Decimal("-80.19180000"), "Standalone", 7500, date(2021, 2, 14), "Active", "MGR_009"),
    ("STORE_010", "Atlanta Perimeter", "South", "Metro South", "333 Perimeter Mall Dr", "Atlanta", "GA", "USA", "30346", Decimal("33.93040000"), Decimal("-84.34130000"), "Mall", 12500, date(2018, 7, 7), "Active", "MGR_010")
]

stores_schema = StructType([
    StructField("store_id", StringType(), False),
    StructField("store_name", StringType(), False),
    StructField("region", StringType(), False),
    StructField("district", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("latitude", DecimalType(10,8), True),
    StructField("longitude", DecimalType(11,8), True),
    StructField("store_type", StringType(), True),
    StructField("square_footage", IntegerType(), True),
    StructField("opening_date", DateType(), True),
    StructField("status", StringType(), True),
    StructField("manager_id", StringType(), True)
])

stores_df = spark.createDataFrame(stores_data, stores_schema)
stores_df = stores_df.withColumn("created_at", F.current_timestamp()) \
                   .withColumn("updated_at", F.current_timestamp())

stores_df.createOrReplaceTempView("temp_stores")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.stores SELECT * FROM temp_stores")
print(f"Generated {stores_df.count()} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Categories Data

# COMMAND ----------

# Clear existing data from categories table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.categories")

# Category hierarchy data
categories_data = [
    # Level 1 - Departments
    ("CAT_001", "Women's Apparel", None, 1, "Women's clothing and accessories", True),
    ("CAT_002", "Men's Apparel", None, 1, "Men's clothing and accessories", True),
    ("CAT_003", "Children's", None, 1, "Children's clothing and toys", True),
    ("CAT_004", "Home & Garden", None, 1, "Home furnishings and garden supplies", True),
    ("CAT_005", "Electronics", None, 1, "Consumer electronics and gadgets", True),

    # Level 2 - Categories
    ("CAT_011", "Dresses", "CAT_001", 2, "Women's dresses", True),
    ("CAT_012", "Tops & Blouses", "CAT_001", 2, "Women's tops and blouses", True),
    ("CAT_013", "Shoes", "CAT_001", 2, "Women's footwear", True),
    ("CAT_021", "Shirts", "CAT_002", 2, "Men's shirts", True),
    ("CAT_022", "Pants", "CAT_002", 2, "Men's trousers and jeans", True),
    ("CAT_023", "Shoes", "CAT_002", 2, "Men's footwear", True),
    ("CAT_031", "Girls Clothing", "CAT_003", 2, "Girls' apparel", True),
    ("CAT_032", "Boys Clothing", "CAT_003", 2, "Boys' apparel", True),
    ("CAT_033", "Toys", "CAT_003", 2, "Children's toys and games", True),
    ("CAT_041", "Furniture", "CAT_004", 2, "Home furniture", True),
    ("CAT_042", "Decor", "CAT_004", 2, "Home decoration items", True),
    ("CAT_051", "Smartphones", "CAT_005", 2, "Mobile phones and accessories", True),
    ("CAT_052", "Laptops", "CAT_005", 2, "Portable computers", True),

    # Level 3 - Subcategories
    ("CAT_111", "Casual Dresses", "CAT_011", 3, "Everyday dresses", True),
    ("CAT_112", "Formal Dresses", "CAT_011", 3, "Evening and formal wear", True),
    ("CAT_121", "T-Shirts", "CAT_012", 3, "Casual t-shirts", True),
    ("CAT_122", "Blouses", "CAT_012", 3, "Formal blouses", True),
    ("CAT_211", "Dress Shirts", "CAT_021", 3, "Formal dress shirts", True),
    ("CAT_212", "Casual Shirts", "CAT_021", 3, "Casual button-down shirts", True),
    ("CAT_221", "Jeans", "CAT_022", 3, "Denim jeans", True),
    ("CAT_222", "Dress Pants", "CAT_022", 3, "Formal trousers", True),
]

categories_schema = StructType([
    StructField("category_id", StringType(), False),
    StructField("category_name", StringType(), False),
    StructField("parent_category_id", StringType(), True),
    StructField("category_level", IntegerType(), True),
    StructField("description", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

categories_df = spark.createDataFrame(categories_data, categories_schema)
categories_df = categories_df.withColumn("is_active", F.when(F.col("is_active").isNull(), F.lit(True)).otherwise(F.col("is_active"))) \
                           .withColumn("created_at", F.current_timestamp()) \
                           .withColumn("updated_at", F.current_timestamp())

categories_df.createOrReplaceTempView("temp_categories")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.categories SELECT * FROM temp_categories")
print(f"Generated {categories_df.count()} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Suppliers Data

# COMMAND ----------

# Clear existing data from suppliers table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.suppliers")

# Supplier data
suppliers_data = [
    ("SUP_001", "Fashion Forward Inc", "Jane Smith", "jane@fashionforward.com", "+1-555-0101", "100 Fashion Ave", "New York", "USA", 14, Decimal("4.20"), "Net 30", True),
    ("SUP_002", "Global Textiles Ltd", "Mark Johnson", "mark@globaltextiles.com", "+1-555-0102", "200 Textile Blvd", "Los Angeles", "USA", 21, Decimal("3.80"), "Net 45", True),
    ("SUP_003", "Trendy Shoes Co", "Sarah Wilson", "sarah@trendyshoes.com", "+1-555-0103", "300 Shoe Street", "Chicago", "USA", 10, Decimal("4.50"), "Net 30", True),
    ("SUP_004", "Electronics Plus", "David Chen", "david@electronicsplus.com", "+1-555-0104", "400 Tech Park Dr", "San Francisco", "USA", 7, Decimal("4.10"), "Net 15", True),
    ("SUP_005", "Home Comforts", "Lisa Brown", "lisa@homecomforts.com", "+1-555-0105", "500 Comfort Lane", "Atlanta", "USA", 18, Decimal("3.90"), "Net 60", True),
    ("SUP_006", "Kids World", "Tom Davis", "tom@kidsworld.com", "+1-555-0106", "600 Playground St", "Miami", "USA", 12, Decimal("4.30"), "Net 30", True),
]

suppliers_schema = StructType([
    StructField("supplier_id", StringType(), False),
    StructField("supplier_name", StringType(), False),
    StructField("contact_person", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lead_time_days", IntegerType(), True),
    StructField("performance_rating", DecimalType(3,2), True),
    StructField("payment_terms", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

suppliers_df = spark.createDataFrame(suppliers_data, suppliers_schema)
suppliers_df = suppliers_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

suppliers_df.createOrReplaceTempView("temp_suppliers")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.suppliers SELECT * FROM temp_suppliers")
print(f"Generated {suppliers_df.count()} suppliers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Data

# COMMAND ----------

# Clear existing data from products table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.products")

# Product generation function
def generate_products(num_products=500):
    products = []

    # Define product templates by category
    product_templates = {
        "CAT_111": [  # Casual Dresses
            ("Casual Summer Dress", ["Navy", "Red", "Black", "White"], ["XS", "S", "M", "L", "XL"], "SUP_001", "StyleCo", Decimal("25.99"), Decimal("49.99"), Decimal("35.99")),
            ("Cotton Midi Dress", ["Blue", "Green", "Pink"], ["S", "M", "L", "XL"], "SUP_001", "ComfortWear", Decimal("22.50"), Decimal("44.99"), Decimal("32.99")),
            ("Floral Print Dress", ["Multicolor"], ["XS", "S", "M", "L"], "SUP_001", "FlowerPower", Decimal("28.00"), Decimal("54.99"), Decimal("39.99")),
        ],
        "CAT_112": [  # Formal Dresses
            ("Evening Gown", ["Black", "Navy", "Burgundy"], ["XS", "S", "M", "L", "XL"], "SUP_001", "Elegance", Decimal("75.00"), Decimal("149.99"), Decimal("99.99")),
            ("Cocktail Dress", ["Black", "Red", "Gold"], ["XS", "S", "M", "L"], "SUP_001", "PartyTime", Decimal("45.00"), Decimal("89.99"), Decimal("64.99")),
        ],
        "CAT_121": [  # T-Shirts
            ("Basic Cotton Tee", ["White", "Black", "Gray", "Navy"], ["XS", "S", "M", "L", "XL", "XXL"], "SUP_002", "BasicBrand", Decimal("8.50"), Decimal("19.99"), Decimal("14.99")),
            ("Graphic Print Tee", ["Various"], ["S", "M", "L", "XL"], "SUP_002", "GraphicTees", Decimal("12.00"), Decimal("24.99"), Decimal("18.99")),
        ],
        "CAT_211": [  # Dress Shirts
            ("Business Dress Shirt", ["White", "Blue", "Light Blue"], ["S", "M", "L", "XL", "XXL"], "SUP_002", "Professional", Decimal("22.00"), Decimal("49.99"), Decimal("34.99")),
            ("Oxford Button Down", ["White", "Blue", "Pink"], ["S", "M", "L", "XL"], "SUP_002", "Oxford Co", Decimal("25.00"), Decimal("54.99"), Decimal("39.99")),
        ],
        "CAT_221": [  # Jeans
            ("Classic Blue Jeans", ["Light Blue", "Dark Blue", "Black"], ["28", "30", "32", "34", "36", "38"], "SUP_002", "DenimCo", Decimal("18.00"), Decimal("59.99"), Decimal("39.99")),
            ("Skinny Fit Jeans", ["Blue", "Black", "Gray"], ["28", "30", "32", "34", "36"], "SUP_002", "SlimFit", Decimal("20.00"), Decimal("64.99"), Decimal("44.99")),
        ],
        "CAT_013": [  # Women's Shoes
            ("Running Sneakers", ["White", "Black", "Pink"], ["6", "7", "8", "9", "10"], "SUP_003", "SportFeet", Decimal("35.00"), Decimal("89.99"), Decimal("64.99")),
            ("High Heels", ["Black", "Nude", "Red"], ["6", "7", "8", "9"], "SUP_003", "Elegance", Decimal("28.00"), Decimal("79.99"), Decimal("54.99")),
        ],
        "CAT_051": [  # Smartphones
            ("Smartphone Pro", ["Black", "White", "Gold"], ["128GB", "256GB"], "SUP_004", "TechBrand", Decimal("299.00"), Decimal("799.99"), Decimal("599.99")),
            ("Budget Phone", ["Black", "Blue"], ["64GB"], "SUP_004", "ValueTech", Decimal("89.00"), Decimal("199.99"), Decimal("149.99")),
        ]
    }

    product_id = 1
    for category_id, templates in product_templates.items():
        for template in templates:
            name, colors, sizes, supplier_id, brand, cost, retail, wholesale = template

            for color in colors:
                for size in sizes:
                    if product_id > num_products:
                        break

                    sku = f"SKU{product_id:06d}"
                    product_name = f"{name} - {color} - {size}"
                    launch_date = date(2022, random.randint(1, 12), random.randint(1, 28))

                    # Add some seasonal variation
                    season = "Spring" if launch_date.month in [3,4,5] else \
                            "Summer" if launch_date.month in [6,7,8] else \
                            "Fall" if launch_date.month in [9,10,11] else "Winter"

                    products.append((
                        f"PROD_{product_id:06d}",
                        sku,
                        product_name,
                        f"High-quality {name.lower()} in {color.lower()}",
                        category_id,
                        supplier_id,
                        brand,
                        size,
                        color,
                        Decimal(str(round(random.uniform(0.1, 5.0), 2))),  # weight
                        cost,
                        retail,
                        wholesale,
                        launch_date,
                        None,  # discontinue_date
                        random.choice([True, False]),  # is_seasonal
                        season,
                        True  # is_active
                    ))
                    product_id += 1

    return products

# Generate products
products_data = generate_products(300)

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("description", StringType(), True),
    StructField("category_id", StringType(), False),
    StructField("supplier_id", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("size", StringType(), True),
    StructField("color", StringType(), True),
    StructField("weight", DecimalType(10,2), True),
    StructField("unit_cost", DecimalType(10,2), True),
    StructField("retail_price", DecimalType(10,2), True),
    StructField("wholesale_price", DecimalType(10,2), True),
    StructField("launch_date", DateType(), True),
    StructField("discontinue_date", DateType(), True),
    StructField("is_seasonal", BooleanType(), True),
    StructField("season", StringType(), True),
    StructField("is_active", BooleanType(), True)
])

products_df = spark.createDataFrame(products_data, products_schema)
products_df = products_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

products_df.createOrReplaceTempView("temp_products")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.products SELECT * FROM temp_products")
print(f"Generated {products_df.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers Data

# COMMAND ----------

# Clear existing data from customers table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.customers")

# Customer generation function
def generate_customers(num_customers=1000):
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
                   "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah", "Christopher", "Karen",
                   "Charles", "Nancy", "Daniel", "Lisa", "Matthew", "Betty", "Anthony", "Helen", "Mark", "Sandra"]

    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                  "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                  "Lee", "Perez", "Thompson", "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson"]

    cities_states = [
        ("San Francisco", "CA"), ("Los Angeles", "CA"), ("San Diego", "CA"), ("Sacramento", "CA"),
        ("New York", "NY"), ("Boston", "MA"), ("Chicago", "IL"), ("Dallas", "TX"), ("Miami", "FL"), ("Atlanta", "GA")
    ]

    customers = []

    for i in range(1, num_customers + 1):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        city, state = random.choice(cities_states)

        # Generate realistic email
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"

        # Generate birth date (18-80 years old)
        birth_year = datetime.now().year - random.randint(18, 80)
        birth_date = date(birth_year, random.randint(1, 12), random.randint(1, 28))

        # Registration date (within last 5 years)
        reg_year = random.randint(2019, 2024)
        reg_date = date(reg_year, random.randint(1, 12), random.randint(1, 28))

        customers.append((
            f"CUST_{i:06d}",
            first_name,
            last_name,
            email,
            f"+1-555-{random.randint(1000, 9999)}",
            birth_date,
            random.choice(["M", "F", "Other"]),
            f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine', 'Elm', 'Cedar'])} St",
            city,
            state,
            f"{random.randint(10000, 99999)}",
            "USA",
            reg_date,
            random.choice(["Bronze", "Silver", "Gold", "Platinum"]),
            f"STORE_{random.randint(1, 10):03d}",
            random.choice([True, False]),
            random.choice([True, False]),
            True
        ))

    return customers

# Generate customers
customers_data = generate_customers(1000)

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_of_birth", DateType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("registration_date", DateType(), True),
    StructField("loyalty_tier", StringType(), True),
    StructField("preferred_store_id", StringType(), True),
    StructField("email_opt_in", BooleanType(), True),
    StructField("sms_opt_in", BooleanType(), True),
    StructField("is_active", BooleanType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)
customers_df = customers_df.withColumn("email_opt_in", F.when(F.col("email_opt_in").isNull(), F.lit(False)).otherwise(F.col("email_opt_in"))) \
                         .withColumn("sms_opt_in", F.when(F.col("sms_opt_in").isNull(), F.lit(False)).otherwise(F.col("sms_opt_in"))) \
                         .withColumn("is_active", F.when(F.col("is_active").isNull(), F.lit(True)).otherwise(F.col("is_active"))) \
                         .withColumn("created_at", F.current_timestamp()) \
                         .withColumn("updated_at", F.current_timestamp())

customers_df.createOrReplaceTempView("temp_customers")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.customers SELECT * FROM temp_customers")
print(f"Generated {customers_df.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Promotions Data

# COMMAND ----------

# Clear existing data from promotions table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.promotions")

# Promotions data
promotions_data = [
    ("PROMO_001", "Summer Sale 2024", "Discount", "25% off summer collection", date(2024, 6, 1), date(2024, 8, 31), Decimal("25.00"), None, Decimal("50.00"), "CAT_111,CAT_121", None, "All", "All", Decimal("50000.00"), Decimal("200000.00"), True),
    ("PROMO_002", "Back to School", "Discount", "15% off children's items", date(2024, 8, 1), date(2024, 9, 15), Decimal("15.00"), None, None, None, "CAT_003", "All", "All", Decimal("30000.00"), Decimal("150000.00"), True),
    ("PROMO_003", "Holiday Special", "Discount", "30% off electronics", date(2024, 11, 15), date(2024, 12, 31), Decimal("30.00"), None, Decimal("100.00"), None, "CAT_005", "All", "All", Decimal("75000.00"), Decimal("300000.00"), True),
    ("PROMO_004", "Buy One Get One", "BOGO", "Buy one dress, get one 50% off", date(2024, 3, 1), date(2024, 3, 31), None, None, None, None, "CAT_011", "All", "In-Store", Decimal("25000.00"), Decimal("100000.00"), True),
    ("PROMO_005", "Weekend Flash Sale", "Clearance", "40% off select items", date(2024, 5, 10), date(2024, 5, 12), Decimal("40.00"), None, None, None, None, "All", "Online", Decimal("15000.00"), Decimal("60000.00"), False),
]

promotions_schema = StructType([
    StructField("promotion_id", StringType(), False),
    StructField("promotion_name", StringType(), False),
    StructField("promotion_type", StringType(), True),
    StructField("description", StringType(), True),
    StructField("start_date", DateType(), False),
    StructField("end_date", DateType(), False),
    StructField("discount_percentage", DecimalType(5,2), True),
    StructField("discount_amount", DecimalType(10,2), True),
    StructField("min_purchase_amount", DecimalType(10,2), True),
    StructField("applicable_products", StringType(), True),
    StructField("applicable_categories", StringType(), True),
    StructField("applicable_stores", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("budget", DecimalType(15,2), True),
    StructField("target_revenue", DecimalType(15,2), True),
    StructField("is_active", BooleanType(), True)
])

promotions_df = spark.createDataFrame(promotions_data, promotions_schema)
promotions_df = promotions_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

promotions_df.createOrReplaceTempView("temp_promotions")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.promotions SELECT * FROM temp_promotions")
print(f"Generated {promotions_df.count()} promotions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sales Data

# COMMAND ----------

# Clear existing data from sales table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.sales")

# Generate sales transactions
def generate_sales_transactions(num_transactions=10000):
    # Get products and stores for reference
    products = spark.sql(f"SELECT product_id, sku, retail_price, category_id FROM {catalog_name}.{schema_name}.products").collect()
    stores = spark.sql(f"SELECT store_id FROM {catalog_name}.{schema_name}.stores").collect()
    customers = spark.sql(f"SELECT customer_id FROM {catalog_name}.{schema_name}.customers").collect()
    promotions = spark.sql(f"SELECT promotion_id, discount_percentage FROM {catalog_name}.{schema_name}.promotions WHERE is_active = true").collect()

    products_list = [(p.product_id, p.sku, float(p.retail_price), p.category_id) for p in products]
    stores_list = [s.store_id for s in stores]
    customers_list = [c.customer_id for c in customers]
    promotions_list = [(p.promotion_id, float(p.discount_percentage) if p.discount_percentage else 0) for p in promotions]

    transactions = []

    # Generate transactions over the last 90 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    for i in range(1, num_transactions + 1):
        # Random transaction date
        random_days = random.randint(0, 90)
        transaction_date = start_date + timedelta(days=random_days)

        # Random time during business hours (9 AM - 9 PM)
        hour = random.randint(9, 21)
        minute = random.randint(0, 59)
        transaction_datetime = transaction_date.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # Select random product and store
        product_id, sku, retail_price, category_id = random.choice(products_list)
        store_id = random.choice(stores_list)

        # 70% chance of having a customer ID (30% are anonymous)
        customer_id = random.choice(customers_list) if random.random() < 0.7 else None

        # Random quantity (mostly 1-3 items)
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3])[0]

        # Apply promotion discount (20% chance)
        discount_amount = 0.0
        promotion_id = None
        if promotions_list and random.random() < 0.2:
            promotion_id, discount_percentage = random.choice(promotions_list)
            discount_amount = retail_price * quantity * (discount_percentage / 100)

        # Calculate amounts
        subtotal = retail_price * quantity
        tax_rate = 0.08  # 8% tax
        tax_amount = (subtotal - discount_amount) * tax_rate
        total_amount = subtotal - discount_amount + tax_amount

        # Payment method
        payment_method = random.choices(
            ["Card", "Cash", "Mobile", "Gift Card"],
            weights=[60, 20, 15, 5]
        )[0]

        # Channel (mostly in-store for this demo)
        channel = random.choices(
            ["In-Store", "Online", "Mobile"],
            weights=[80, 15, 5]
        )[0]

        # Small chance of returns
        return_flag = random.random() < 0.05

        transactions.append((
            f"TXN_{i:08d}",
            transaction_date.date(),
            transaction_datetime,
            store_id,
            customer_id,
            product_id,
            sku,
            quantity,
            Decimal(str(round(retail_price, 2))),
            Decimal(str(round(discount_amount, 2))),
            Decimal(str(round(tax_amount, 2))),
            Decimal(str(round(total_amount, 2))),
            payment_method,
            promotion_id,
            f"EMP_{random.randint(1, 50):03d}",
            channel,
            return_flag
        ))

    return transactions

# Generate sales data
print("Generating sales transactions...")
sales_data = generate_sales_transactions(10000)

sales_schema = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("sale_date", DateType(), False),
    StructField("sale_timestamp", TimestampType(), False),
    StructField("store_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DecimalType(10,2), False),
    StructField("discount_amount", DecimalType(10,2), True),
    StructField("tax_amount", DecimalType(10,2), True),
    StructField("total_amount", DecimalType(10,2), False),
    StructField("payment_method", StringType(), True),
    StructField("promotion_id", StringType(), True),
    StructField("sales_associate_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("return_flag", BooleanType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)
sales_df = sales_df.withColumn("discount_amount", F.when(F.col("discount_amount").isNull(), F.lit(Decimal("0.00"))).otherwise(F.col("discount_amount"))) \
                 .withColumn("tax_amount", F.when(F.col("tax_amount").isNull(), F.lit(Decimal("0.00"))).otherwise(F.col("tax_amount"))) \
                 .withColumn("channel", F.when(F.col("channel").isNull(), F.lit("In-Store")).otherwise(F.col("channel"))) \
                 .withColumn("return_flag", F.when(F.col("return_flag").isNull(), F.lit(False)).otherwise(F.col("return_flag"))) \
                 .withColumn("created_at", F.current_timestamp())

sales_df.createOrReplaceTempView("temp_sales")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.sales SELECT * FROM temp_sales")
print(f"Generated {sales_df.count()} sales transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Inventory Data

# COMMAND ----------

# Clear existing data from inventory table
spark.sql(f"TRUNCATE TABLE {catalog_name}.{schema_name}.inventory")

# Generate inventory data for the last 30 days
def generate_inventory_data():
    # Get products and stores
    products = spark.sql(f"SELECT product_id, sku FROM {catalog_name}.{schema_name}.products").collect()
    stores = spark.sql(f"SELECT store_id FROM {catalog_name}.{schema_name}.stores").collect()

    inventory_records = []

    # Generate for last 30 days
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)

    for single_date in [start_date + timedelta(n) for n in range(31)]:
        for store in stores:
            # Sample products for each store (not all products in all stores)
            store_products = random.sample(products, min(len(products), random.randint(50, 150)))

            for product in store_products:
                # Initial inventory levels
                if single_date == start_date:
                    beginning_inventory = random.randint(10, 100)
                else:
                    # For subsequent days, beginning = previous day's ending
                    beginning_inventory = random.randint(5, 95)

                # Daily movements
                received_quantity = random.choices([0, 5, 10, 20, 50], weights=[70, 10, 10, 7, 3])[0]
                sold_quantity = random.randint(0, min(beginning_inventory + received_quantity, 15))
                transferred_out = random.choices([0, 2, 5], weights=[85, 10, 5])[0]
                transferred_in = random.choices([0, 2, 5], weights=[85, 10, 5])[0]
                shrinkage = random.choices([0, 1, 2], weights=[90, 7, 3])[0]

                ending_inventory = beginning_inventory + received_quantity - sold_quantity - transferred_out + transferred_in - shrinkage
                ending_inventory = max(0, ending_inventory)  # Can't be negative

                # Safety stock and reorder points
                safety_stock = random.randint(5, 20)
                reorder_point = random.randint(10, 30)
                max_stock = random.randint(50, 200)

                # Calculate days on hand
                avg_daily_sales = max(1, sold_quantity)
                days_on_hand = ending_inventory // avg_daily_sales if avg_daily_sales > 0 else 99

                # Flags
                stockout_flag = ending_inventory == 0
                overstock_flag = ending_inventory > max_stock

                # Last dates
                last_received_date = single_date if received_quantity > 0 else None
                last_sold_date = single_date if sold_quantity > 0 else None

                inventory_records.append((
                    single_date,
                    store.store_id,
                    product.product_id,
                    product.sku,
                    beginning_inventory,
                    received_quantity,
                    sold_quantity,
                    transferred_out,
                    transferred_in,
                    shrinkage,
                    ending_inventory,
                    safety_stock,
                    reorder_point,
                    max_stock,
                    last_received_date,
                    last_sold_date,
                    days_on_hand,
                    stockout_flag,
                    overstock_flag
                ))

    return inventory_records

# Generate inventory data
print("Generating inventory data...")
inventory_data = generate_inventory_data()

inventory_schema = StructType([
    StructField("inventory_date", DateType(), False),
    StructField("store_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("sku", StringType(), False),
    StructField("beginning_inventory", IntegerType(), True),
    StructField("received_quantity", IntegerType(), True),
    StructField("sold_quantity", IntegerType(), True),
    StructField("transferred_out", IntegerType(), True),
    StructField("transferred_in", IntegerType(), True),
    StructField("shrinkage", IntegerType(), True),
    StructField("ending_inventory", IntegerType(), True),
    StructField("safety_stock", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("max_stock", IntegerType(), True),
    StructField("last_received_date", DateType(), True),
    StructField("last_sold_date", DateType(), True),
    StructField("days_on_hand", IntegerType(), True),
    StructField("stockout_flag", BooleanType(), True),
    StructField("overstock_flag", BooleanType(), True)
])

inventory_df = spark.createDataFrame(inventory_data, inventory_schema)
# Handle defaults at application layer for nullable integer columns
default_zero_cols = ["beginning_inventory", "received_quantity", "sold_quantity", "transferred_out",
                    "transferred_in", "shrinkage", "ending_inventory", "safety_stock", "reorder_point", "max_stock"]
for col_name in default_zero_cols:
    inventory_df = inventory_df.withColumn(col_name, F.when(F.col(col_name).isNull(), F.lit(0)).otherwise(F.col(col_name)))

inventory_df = inventory_df.withColumn("stockout_flag", F.when(F.col("stockout_flag").isNull(), F.lit(False)).otherwise(F.col("stockout_flag"))) \
                         .withColumn("overstock_flag", F.when(F.col("overstock_flag").isNull(), F.lit(False)).otherwise(F.col("overstock_flag"))) \
                         .withColumn("created_at", F.current_timestamp()) \
                         .withColumn("updated_at", F.current_timestamp())

inventory_df.createOrReplaceTempView("temp_inventory")
spark.sql(f"INSERT INTO {catalog_name}.{schema_name}.inventory SELECT * FROM temp_inventory")
print(f"Generated {inventory_df.count()} inventory records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Display summary of generated data
tables = [
    "stores", "categories", "suppliers", "products",
    "customers", "promotions", "sales", "inventory"
]

print("Sample data generation complete!\n")
print("Generated data summary:")
print("-" * 40)

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table}").collect()[0]['count']
    print(f"{table:15}: {count:,} records")

print("\nSample data is ready for dashboard creation and analytics!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("Running data quality checks...\n")

# Check for any null primary keys
for table, pk_col in [("stores", "store_id"), ("products", "product_id"), ("customers", "customer_id"), ("sales", "transaction_id")]:
    null_count = spark.sql(f"SELECT COUNT(*) as count FROM {catalog_name}.{schema_name}.{table} WHERE {pk_col} IS NULL").collect()[0]['count']
    print(f"{table} null {pk_col}: {null_count}")

# Check sales data integrity
sales_checks = spark.sql(f"""
SELECT
    COUNT(*) as total_transactions,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT store_id) as stores_with_sales,
    COUNT(DISTINCT product_id) as products_sold
FROM {catalog_name}.{schema_name}.sales
""").collect()[0]

print(f"\nSales Summary:")
print(f"Total Transactions: {sales_checks['total_transactions']:,}")
print(f"Total Revenue: ${sales_checks['total_revenue']:,.2f}")
print(f"Average Transaction: ${sales_checks['avg_transaction']:.2f}")
print(f"Unique Customers: {sales_checks['unique_customers']:,}")
print(f"Stores with Sales: {sales_checks['stores_with_sales']}")
print(f"Products Sold: {sales_checks['products_sold']}")

print("\nData quality checks passed!")