from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "SortTotalAmountByCustomer")

# Define input and output paths
input_path = "hdfs://localhost:9000/user/hadoop/input/customer-orders.csv"
output_path = "hdfs://localhost:9000/user/hadoop/output/sorted_customer_spending"

# Load the customer orders data into an RDD
# Assume the file format is: CustomerID,OrderAmount (e.g., "1,200.50")
orders_rdd = sc.textFile(input_path)

# Process the text to calculate total spending per customer
customer_spending_rdd = (
    orders_rdd
    .map(lambda line: line.split(","))  # Split each line into [CustomerID, OrderAmount]
    .map(lambda fields: (int(fields[0]), float(fields[2])))  # Convert to (CustomerID, OrderAmount)
    .reduceByKey(lambda a, b: a + b)  # Sum amounts by CustomerID
)

# Sort customers by total spending (descending) and CustomerID (ascending) for ties
sorted_customer_spending_rdd = customer_spending_rdd.sortBy(lambda pair: (-pair[1], pair[0]))

# Save the sorted results to HDFS or local file system
sorted_customer_spending_rdd.saveAsTextFile(output_path)

# Print confirmation
print(f"Customer spending sorted by total amount saved to: {output_path}")

# Stop the Spark Context
sc.stop()
