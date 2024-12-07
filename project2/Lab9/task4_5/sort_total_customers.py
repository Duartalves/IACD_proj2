from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SortTotalAmountByCustomerSQL") \
    .getOrCreate()

# Define input and output paths
input_path = "hdfs://localhost:9000/user/hadoop/input/customer-orders.csv"
output_path = "hdfs://localhost:9000/user/hadoop/output/sorted_customer_spending9"

# Load the customer orders data into a DataFrame
# Assume the file format is: CustomerID, OrderAmount
orders_df = spark.read.csv(input_path, header=False, inferSchema=True).toDF("CustomerID","", "OrderAmount")

# Calculate total spending per customer
customer_spending_df = (
    orders_df
    .groupBy("CustomerID")
    .agg(_sum("OrderAmount").alias("TotalSpending"))  # Calculate total spending
)

# Sort customers by total spending (descending) and CustomerID (ascending) for ties
sorted_customer_spending_df = customer_spending_df.orderBy(col("TotalSpending").desc(), col("CustomerID"))

# Save the sorted results to HDFS or local file system as CSV
sorted_customer_spending_df.write.csv(output_path, header=True, mode="overwrite")

print(f"Customer spending sorted by total amount saved to: {output_path}")

# Stop the Spark session
spark.stop()
