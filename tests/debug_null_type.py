#!/usr/bin/env python3
"""Debug script to see what type NULL gets in CASE WHEN expressions"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from decimal import Decimal

# Connect to Thunderduck
spark = SparkSession.builder \
    .remote("sc://localhost:15002") \
    .getOrCreate()

# Create a simple DataFrame with a decimal column
data = [(1, Decimal("10.50")), (2, Decimal("20.75"))]
df = spark.createDataFrame(data, ["id", "ss_sales_price"])

# Create CASE WHEN with NULL ELSE (simulating TPC-DS Q43 pattern)
result = df.select(
    F.col("id"),
    F.when(F.col("id") == 1, F.col("ss_sales_price"))
     .otherwise(None)  # NULL literal
     .alias("conditional_price")
)

# Print schema to see types
print("Schema:")
result.printSchema()

# Try to collect to trigger the query
try:
    rows = result.collect()
    for row in rows:
        print(row)
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
