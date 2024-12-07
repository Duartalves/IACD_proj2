from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "WordCountAndSort")

# Define input and output paths
input_path = "hdfs://localhost:9000/user/hadoop/input/Book.txt"
output_path = "hdfs://localhost:9000/user/hadoop/output/sorted_word_counts"

# Load the book text file into an RDD
book_rdd = sc.textFile(input_path)

# Process the text: Split into words, normalize case, and remove non-alphanumeric characters
words_rdd = (
    book_rdd
    .flatMap(lambda line: line.split())  # Split lines into words
    .map(lambda word: ''.join(c for c in word if c.isalnum()))  # Remove punctuation
    .map(lambda word: word.lower())  # Convert to lowercase
    .filter(lambda word: word != "")  # Remove empty strings
)

# Count the words
word_counts_rdd = (
    words_rdd
    .map(lambda word: (word, 1))  # Create key-value pairs (word, 1)
    .reduceByKey(lambda a, b: a + b)  # Sum counts for each word
)

# Sort the words by count (descending) and alphabetically for ties
sorted_word_counts_rdd = word_counts_rdd.sortBy(lambda pair: (-pair[1], pair[0]))

# Save the result to HDFS
sorted_word_counts_rdd.saveAsTextFile(output_path)

# Print confirmation
print(f"Word counts sorted by frequency saved to: {output_path}")

# Stop the Spark Context
sc.stop()
