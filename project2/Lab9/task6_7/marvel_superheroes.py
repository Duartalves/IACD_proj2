from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, lit, max as sql_max, min as sql_min

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("SuperheroPopularitySQLWithTies") \
    .getOrCreate()

# Define file paths
graph_input_path = "hdfs://localhost:9000/user/hadoop/input/Marvel+Graph"
names_input_path = "hdfs://localhost:9000/user/hadoop/input/Marvel+Names"
output_path = "hdfs://localhost:9000/user/hadoop/output/superhero_popularity9"

# Load the Marvel+Graph file and count occurrences of each superhero ID
graph_df = spark.read.text(graph_input_path).toDF("connections")

# Split the connections into an array and explode into individual rows
hero_counts_df = (
    graph_df
    .withColumn("hero", explode(split(col("connections"), " ")))  # Split and explode into rows
    .withColumn("heroID", col("hero").cast("int"))  # Cast exploded values to integer
    .groupBy("heroID")  # Group by hero ID
    .count()  # Count occurrences of each ID
)

# Load the Marvel+Names file and create a mapping of superhero ID to name
names_df = (
    spark.read.text(names_input_path)
    .select(split(col("value"), " ", 2).alias("parts"))  # Split the line into ID and name
    .withColumn("heroID", col("parts").getItem(0).cast("int"))  # Extract and cast ID
    .withColumn("heroName", col("parts").getItem(1).cast("string"))  # Extract name
    .select("heroID", "heroName")
)

# Join the counts with the names
popularity_df = hero_counts_df.join(names_df, "heroID")

# Find the maximum and minimum counts
max_count = popularity_df.select(sql_max("count").alias("maxCount")).collect()[0]["maxCount"]
min_count = popularity_df.select(sql_min("count").alias("minCount")).collect()[0]["minCount"]

# Filter superheroes with the maximum count
most_popular_df = popularity_df.filter(col("count") == max_count).select(
    lit("Most popular superhero(s)").alias("Description"),
    col("heroName"),
    col("count").alias("Occurrences")
)

# Filter superheroes with the minimum count
least_popular_df = popularity_df.filter(col("count") == min_count).select(
    lit("Least popular superhero(s)").alias("Description"),
    col("heroName"),
    col("count").alias("Occurrences")
)

# Combine the results into a single DataFrame
results_df = most_popular_df.union(least_popular_df)

# Save the results to HDFS as a single file
results_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

# Show the results
results_df.show(truncate=False)

# Stop the Spark session
spark.stop()
