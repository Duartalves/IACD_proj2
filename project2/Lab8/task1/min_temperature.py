from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext(appName="Minimum Temperature Per City")

# Define the HDFS input file path
input_path = "hdfs://localhost:9000/user/hadoop/input/1800.csv"
output_path = "hdfs://localhost:9000/user/hadoop/output/min_temperature_per_city"

# Read the input file as an RDD
lines = sc.textFile(input_path)

# Define a function to parse each line
def parse_line(line):
    fields = line.strip().split(",")
    city = fields[0]    # City
    record_type = fields[2]  # Type (TMIN, TMAX, etc.)
    temperature = int(fields[3])  # Temperature (already multiplied by 10)
    return city, record_type, temperature

# Parse the lines and filter only TMIN records
tmin_records = (
    lines.map(parse_line)  # Parse each line
    .filter(lambda x: x[1] == "TMIN")  # Keep only TMIN records
    .map(lambda x: (x[0], x[2]))  # Keep city and temperature
)

# Reduce to find the minimum temperature per city
min_temp_per_city = tmin_records.reduceByKey(min)

# Save the results to HDFS
min_temp_per_city.map(lambda x: f"{x[0]},{x[1]}").saveAsTextFile(output_path)

# Stop the SparkContext
sc.stop()
