from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as sql_min

# Initialize SparkSession
spark = SparkSession.builder.appName("MinimumTemperaturePerCitySQL").getOrCreate()

# Define file paths
input_path = "hdfs://localhost:9000/user/hadoop/input/1800.csv"
output_path = "hdfs://localhost:9000/user/hadoop/output/min_temperature_per_city9"

# Load the input data into a DataFrame
df = spark.read.csv(input_path, header=False, inferSchema=True)

# Rename columns for clarity
df = df.withColumnRenamed("_c0", "city") \
       .withColumnRenamed("_c2", "record_type") \
       .withColumnRenamed("_c3", "temperature")

# Filter for TMIN records and find the minimum temperature per city
min_temp_df = (
    df.filter(col("record_type") == "TMIN")  # Filter for TMIN records
      .groupBy("city")  # Group by city
      .agg(sql_min("temperature").alias("min_temperature"))  # Find the minimum temperature
)

# Save the result to HDFS
min_temp_df.write.csv(output_path, header=True, mode="overwrite")

# Stop the SparkSession
spark.stop()
