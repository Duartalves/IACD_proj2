from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "SuperheroPopularityRDD")

# Define file paths
graph_input_path = "hdfs://localhost:9000/user/hadoop/input/Marvel+Graph"
names_input_path = "hdfs://localhost:9000/user/hadoop/input/Marvel+Names"
output_path = "hdfs://localhost:9000/user/hadoop/output/superhero_popularity"

# Load the Marvel+Graph file and count occurrences of each superhero ID
hero_counts_rdd = (
    sc.textFile(graph_input_path)
    .flatMap(lambda line: line.split())  # Split the line into superhero IDs
    .map(lambda hero_id: (int(hero_id), 1))  # Map each ID to (ID, 1)
    .reduceByKey(lambda a, b: a + b)  # Aggregate counts by ID
)

# Find the most and least popular superheroes by reducing the RDD
most_popular = hero_counts_rdd.reduce(lambda x, y: x if x[1] > y[1] else y)
least_popular = hero_counts_rdd.reduce(lambda x, y: x if x[1] < y[1] else y)

# Load the Marvel+Names file and create a mapping of superhero ID to name
hero_names_rdd = (
    sc.textFile(names_input_path)
    .map(lambda line: line.split(" ", 1))  # Split the line into ID and name
    .map(lambda parts: (int(parts[0]), parts[1].strip('"')))  # Map to (ID, name)
)

# Get the names of the most and least popular superheroes
most_popular_name = hero_names_rdd.lookup(most_popular[0])[0]
least_popular_name = hero_names_rdd.lookup(least_popular[0])[0]

# Prepare the result
results = [
    f"Most popular superhero: {most_popular_name} with {most_popular[1]} occurrences",
    f"Least popular superhero: {least_popular_name} with {least_popular[1]} occurrences"
]

# Save the results to a single file
sc.parallelize(results).saveAsTextFile(output_path)

# Print out the results
print("\n".join(results))

# Stop the Spark Context
sc.stop()
