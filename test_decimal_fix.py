# Test script to verify decimal type fix
from decimal import Decimal
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark
spark = SparkSession.builder.appName("DecimalTest").getOrCreate()

# Test data with proper decimal types
test_stores_data = [
    ("STORE_001", "Test Store", "West", "Metro", "123 Main St", "San Francisco", "CA", "USA", "94105",
     Decimal("37.77490000"), Decimal("-122.41940000"), "Mall", 10000, "2020-01-01", "Active", "MGR_001")
]

stores_columns = ["store_id", "store_name", "region", "district", "address", "city", "state", "country",
                 "postal_code", "latitude", "longitude", "store_type", "square_footage", "opening_date",
                 "status", "manager_id"]

# Create DataFrame
test_df = spark.createDataFrame(test_stores_data, stores_columns)
test_df = test_df.withColumn("created_at", F.current_timestamp()).withColumn("updated_at", F.current_timestamp())

# Show schema to verify types
print("DataFrame Schema:")
test_df.printSchema()

# Show data
print("\nDataFrame Content:")
test_df.show()

# Try to create a temporary table to test compatibility
try:
    test_df.createOrReplaceTempView("test_stores")
    result = spark.sql("SELECT latitude, longitude FROM test_stores")
    result.show()
    print("✅ Decimal types working correctly!")
except Exception as e:
    print(f"❌ Error: {e}")

spark.stop()