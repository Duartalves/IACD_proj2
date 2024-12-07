from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, lower, split, explode, count

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("WordCountAndSortSQL") \
    .getOrCreate()

# Define input and output paths
input_path = "hdfs://localhost:9000/user/hadoop/input/Book.txt"
output_path = "hdfs://localhost:9000/user/hadoop/output/sorted_word_counts9"

# Load the book text file into a DataFrame
book_df = spark.read.text(input_path).withColumnRenamed("value", "line")

# Process the text:
# 1. Split lines into words
# 2. Normalize to lowercase
# 3. Remove non-alphanumeric characters
words_df = (
    book_df
    .withColumn("word", explode(split(col("line"), "\\s+")))  # Split lines into words
    .withColumn("word", regexp_replace(col("word"), "[^a-zA-Z0-9]", ""))  # Remove non-alphanumeric chars
    .withColumn("word", lower(col("word")))  # Convert to lowercase
    .filter(col("word") != "")  # Remove empty strings
)

# Count the words
word_counts_df = words_df.groupBy("word").agg(count("word").alias("count"))

# Sort by count (descending) and alphabetically for ties
sorted_word_counts_df = word_counts_df.orderBy(col("count").desc(), col("word"))

# Save the result to HDFS as a CSV
sorted_word_counts_df.write.csv(output_path, header=True, mode="overwrite")

print(f"Word counts sorted by frequency saved to: {output_path}")

# Stop the Spark session
spark.stop()
