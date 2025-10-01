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

# Store data
stores_data = [
    ("STORE_001", "Downtown Flagship", "West", "Metro West", "123 Main St", "San Francisco", "CA", "USA", "94105", 37.7749, -122.4194, "Flagship", 15000, "2018-03-15", "Active", "MGR_001"),
    ("STORE_002", "Mall Central", "West", "Metro West", "456 Shopping Center Dr", "Los Angeles", "CA", "USA", "90210", 34.0522, -118.2437, "Mall", 12000, "2019-06-01", "Active", "MGR_002"),
    ("STORE_003", "Suburban Plaza", "West", "Metro South", "789 Plaza Blvd", "San Diego", "CA", "USA", "92101", 32.7157, -117.1611, "Plaza", 8000, "2020-01-20", "Active", "MGR_003"),
    ("STORE_004", "Outlet Center", "West", "Metro North", "321 Outlet Way", "Sacramento", "CA", "USA", "95814", 38.5816, -121.4944, "Outlet", 10000, "2017-11-12", "Active", "MGR_004"),
    ("STORE_005", "Metro East", "East", "Metro East", "555 Commerce St", "New York", "NY", "USA", "10001", 40.7128, -74.0060, "Standalone", 11000, "2019-09-05", "Active", "MGR_005"),
    ("STORE_006", "Boston Commons", "East", "Metro East", "777 Commonwealth Ave", "Boston", "MA", "USA", "02116", 42.3601, -71.0589, "Mall", 9500, "2020-04-18", "Active", "MGR_006"),
    ("STORE_007", "Chicago Loop", "Central", "Metro Central", "999 State St", "Chicago", "IL", "USA", "60601", 41.8781, -87.6298, "Standalone", 13000, "2018-08-25", "Active", "MGR_007"),
    ("STORE_008", "Dallas Galleria", "Central", "Metro South", "111 Galleria Pkwy", "Dallas", "TX", "USA", "75240", 32.7767, -96.7970, "Mall", 14000, "2019-12-10", "Active", "MGR_008"),
    ("STORE_009", "Miami Beach", "South", "Metro South", "222 Ocean Dr", "Miami", "FL", "USA", "33139", 25.7617, -80.1918, "Standalone", 7500, "2021-02-14", "Active", "MGR_009"),
    ("STORE_010", "Atlanta Perimeter", "South", "Metro South", "333 Perimeter Mall Dr", "Atlanta", "GA", "USA", "30346", 33.9304, -84.3413, "Mall", 12500, "2018-07-07", "Active", "MGR_010")
]

stores_columns = ["store_id", "store_name", "region", "district", "address", "city", "state", "country",
                 "postal_code", "latitude", "longitude", "store_type", "square_footage", "opening_date",
                 "status", "manager_id"]

stores_df = spark.createDataFrame(stores_data, stores_columns)
stores_df = stores_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

stores_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.stores")
print(f"Generated {stores_df.count()} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Categories Data

# COMMAND ----------

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

categories_columns = ["category_id", "category_name", "parent_category_id", "category_level", "description", "is_active"]

categories_df = spark.createDataFrame(categories_data, categories_columns)
categories_df = categories_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

categories_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.categories")
print(f"Generated {categories_df.count()} categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Suppliers Data

# COMMAND ----------

# Supplier data
suppliers_data = [
    ("SUP_001", "Fashion Forward Inc", "Jane Smith", "jane@fashionforward.com", "+1-555-0101", "100 Fashion Ave", "New York", "USA", 14, 4.2, "Net 30", True),
    ("SUP_002", "Global Textiles Ltd", "Mark Johnson", "mark@globaltextiles.com", "+1-555-0102", "200 Textile Blvd", "Los Angeles", "USA", 21, 3.8, "Net 45", True),
    ("SUP_003", "Trendy Shoes Co", "Sarah Wilson", "sarah@trendyshoes.com", "+1-555-0103", "300 Shoe Street", "Chicago", "USA", 10, 4.5, "Net 30", True),
    ("SUP_004", "Electronics Plus", "David Chen", "david@electronicsplus.com", "+1-555-0104", "400 Tech Park Dr", "San Francisco", "USA", 7, 4.1, "Net 15", True),
    ("SUP_005", "Home Comforts", "Lisa Brown", "lisa@homecomforts.com", "+1-555-0105", "500 Comfort Lane", "Atlanta", "USA", 18, 3.9, "Net 60", True),
    ("SUP_006", "Kids World", "Tom Davis", "tom@kidsworld.com", "+1-555-0106", "600 Playground St", "Miami", "USA", 12, 4.3, "Net 30", True),
]

suppliers_columns = ["supplier_id", "supplier_name", "contact_person", "email", "phone", "address",
                    "city", "country", "lead_time_days", "performance_rating", "payment_terms", "is_active"]

suppliers_df = spark.createDataFrame(suppliers_data, suppliers_columns)
suppliers_df = suppliers_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

suppliers_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.suppliers")
print(f"Generated {suppliers_df.count()} suppliers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Products Data

# COMMAND ----------

# Product generation function
def generate_products(num_products=500):
    products = []

    # Define product templates by category
    product_templates = {
        "CAT_111": [  # Casual Dresses
            ("Casual Summer Dress", ["Navy", "Red", "Black", "White"], ["XS", "S", "M", "L", "XL"], "SUP_001", "StyleCo", 25.99, 49.99, 35.99),
            ("Cotton Midi Dress", ["Blue", "Green", "Pink"], ["S", "M", "L", "XL"], "SUP_001", "ComfortWear", 22.50, 44.99, 32.99),
            ("Floral Print Dress", ["Multicolor"], ["XS", "S", "M", "L"], "SUP_001", "FlowerPower", 28.00, 54.99, 39.99),
        ],
        "CAT_112": [  # Formal Dresses
            ("Evening Gown", ["Black", "Navy", "Burgundy"], ["XS", "S", "M", "L", "XL"], "SUP_001", "Elegance", 75.00, 149.99, 99.99),
            ("Cocktail Dress", ["Black", "Red", "Gold"], ["XS", "S", "M", "L"], "SUP_001", "PartyTime", 45.00, 89.99, 64.99),
        ],
        "CAT_121": [  # T-Shirts
            ("Basic Cotton Tee", ["White", "Black", "Gray", "Navy"], ["XS", "S", "M", "L", "XL", "XXL"], "SUP_002", "BasicBrand", 8.50, 19.99, 14.99),
            ("Graphic Print Tee", ["Various"], ["S", "M", "L", "XL"], "SUP_002", "GraphicTees", 12.00, 24.99, 18.99),
        ],
        "CAT_211": [  # Dress Shirts
            ("Business Dress Shirt", ["White", "Blue", "Light Blue"], ["S", "M", "L", "XL", "XXL"], "SUP_002", "Professional", 22.00, 49.99, 34.99),
            ("Oxford Button Down", ["White", "Blue", "Pink"], ["S", "M", "L", "XL"], "SUP_002", "Oxford Co", 25.00, 54.99, 39.99),
        ],
        "CAT_221": [  # Jeans
            ("Classic Blue Jeans", ["Light Blue", "Dark Blue", "Black"], ["28", "30", "32", "34", "36", "38"], "SUP_002", "DenimCo", 18.00, 59.99, 39.99),
            ("Skinny Fit Jeans", ["Blue", "Black", "Gray"], ["28", "30", "32", "34", "36"], "SUP_002", "SlimFit", 20.00, 64.99, 44.99),
        ],
        "CAT_131": [  # Women's Shoes
            ("Running Sneakers", ["White", "Black", "Pink"], ["6", "7", "8", "9", "10"], "SUP_003", "SportFeet", 35.00, 89.99, 64.99),
            ("High Heels", ["Black", "Nude", "Red"], ["6", "7", "8", "9"], "SUP_003", "Elegance", 28.00, 79.99, 54.99),
        ],
        "CAT_051": [  # Smartphones
            ("Smartphone Pro", ["Black", "White", "Gold"], ["128GB", "256GB"], "SUP_004", "TechBrand", 299.00, 799.99, 599.99),
            ("Budget Phone", ["Black", "Blue"], ["64GB"], "SUP_004", "ValueTech", 89.00, 199.99, 149.99),
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
                        round(random.uniform(0.1, 5.0), 2),  # weight
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
products_columns = ["product_id", "sku", "product_name", "description", "category_id", "supplier_id",
                   "brand", "size", "color", "weight", "unit_cost", "retail_price", "wholesale_price",
                   "launch_date", "discontinue_date", "is_seasonal", "season", "is_active"]

products_df = spark.createDataFrame(products_data, products_columns)
products_df = products_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

products_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.products")
print(f"Generated {products_df.count()} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Customers Data

# COMMAND ----------

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
customers_columns = ["customer_id", "first_name", "last_name", "email", "phone", "date_of_birth", "gender",
                    "address", "city", "state", "postal_code", "country", "registration_date", "loyalty_tier",
                    "preferred_store_id", "email_opt_in", "sms_opt_in", "is_active"]

customers_df = spark.createDataFrame(customers_data, customers_columns)
customers_df = customers_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

customers_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.customers")
print(f"Generated {customers_df.count()} customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Promotions Data

# COMMAND ----------

# Promotions data
start_date = date(2024, 1, 1)
promotions_data = [
    ("PROMO_001", "Summer Sale 2024", "Discount", "25% off summer collection", date(2024, 6, 1), date(2024, 8, 31), 25.0, None, 50.0, "CAT_111,CAT_121", None, "All", "All", 50000.0, 200000.0, True),
    ("PROMO_002", "Back to School", "Discount", "15% off children's items", date(2024, 8, 1), date(2024, 9, 15), 15.0, None, None, None, "CAT_003", "All", "All", 30000.0, 150000.0, True),
    ("PROMO_003", "Holiday Special", "Discount", "30% off electronics", date(2024, 11, 15), date(2024, 12, 31), 30.0, None, 100.0, None, "CAT_005", "All", "All", 75000.0, 300000.0, True),
    ("PROMO_004", "Buy One Get One", "BOGO", "Buy one dress, get one 50% off", date(2024, 3, 1), date(2024, 3, 31), None, None, None, None, "CAT_011", "All", "In-Store", 25000.0, 100000.0, True),
    ("PROMO_005", "Weekend Flash Sale", "Clearance", "40% off select items", date(2024, 5, 10), date(2024, 5, 12), 40.0, None, None, None, None, "All", "Online", 15000.0, 60000.0, False),
]

promotions_columns = ["promotion_id", "promotion_name", "promotion_type", "description", "start_date", "end_date",
                     "discount_percentage", "discount_amount", "min_purchase_amount", "applicable_products",
                     "applicable_categories", "applicable_stores", "channel", "budget", "target_revenue", "is_active"]

promotions_df = spark.createDataFrame(promotions_data, promotions_columns)
promotions_df = promotions_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

promotions_df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name}.promotions")
print(f"Generated {promotions_df.count()} promotions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sales Data

# COMMAND ----------

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
            retail_price,
            round(discount_amount, 2),
            round(tax_amount, 2),
            round(total_amount, 2),
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
sales_columns = ["transaction_id", "sale_date", "sale_timestamp", "store_id", "customer_id", "product_id", "sku",
                "quantity", "unit_price", "discount_amount", "tax_amount", "total_amount", "payment_method",
                "promotion_id", "sales_associate_id", "channel", "return_flag"]

sales_df = spark.createDataFrame(sales_data, sales_columns)
sales_df = sales_df.withColumn("created_at", F.current_timestamp())

sales_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.sales")
print(f"Generated {sales_df.count()} sales transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Inventory Data

# COMMAND ----------

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
inventory_columns = ["inventory_date", "store_id", "product_id", "sku", "beginning_inventory", "received_quantity",
                    "sold_quantity", "transferred_out", "transferred_in", "shrinkage", "ending_inventory",
                    "safety_stock", "reorder_point", "max_stock", "last_received_date", "last_sold_date",
                    "days_on_hand", "stockout_flag", "overstock_flag"]

inventory_df = spark.createDataFrame(inventory_data, inventory_columns)
inventory_df = inventory_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

inventory_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog_name}.{schema_name}.inventory")
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

print("\nData quality checks passed! ✅")