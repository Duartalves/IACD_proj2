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
    .flatMap(lambda line: line.split())   # Split the line into superhero IDs
    .map(lambda hero_id: (int(hero_id), 1))   # Map each ID to (ID, 1)
    .reduceByKey(lambda a, b: a + b)   # Aggregate counts by ID
)

# Aggregate to find max and min counts in a single pass
count_stats = hero_counts_rdd.aggregate(
    (float("inf"), float("-inf")),  # Initialize (min, max)
    lambda acc, value: (min(acc[0], value[1]), max(acc[1], value[1])),  # Compare counts
    lambda acc1, acc2: (min(acc1[0], acc2[0]), max(acc1[1], acc2[1]))   # Merge partitions
)

min_count, max_count = count_stats

# Filter IDs for most and least popular superheroes
most_popular_ids = hero_counts_rdd.filter(lambda x: x[1] == max_count).keys().collect()
least_popular_ids = hero_counts_rdd.filter(lambda x: x[1] == min_count).keys().collect()

# Load superhero names and collect as a local dictionary for fast lookups
hero_names = (
    sc.textFile(names_input_path)
    .map(lambda line: line.split(" ", 1))   # Split into (ID, Name)
    .map(lambda parts: (int(parts[0]), parts[1].strip('"')))
    .collectAsMap()
)

# Map IDs to names for output
most_popular_names = [hero_names.get(hero_id, f"Unknown ID {hero_id}") for hero_id in most_popular_ids]
least_popular_names = [hero_names.get(hero_id, f"Unknown ID {hero_id}") for hero_id in least_popular_ids]

# Prepare the result strings
results = [f"Most popular superheroes (with {max_count} occurrences):"]
results += most_popular_names

results.append(f"\nLeast popular superheroes (with {min_count} occurrences):")
results += least_popular_names

# Save the results to a single file
sc.parallelize(results).saveAsTextFile(output_path)

# Print out the results
print("\n".join(results))

# Stop the Spark Context
sc.stop()
