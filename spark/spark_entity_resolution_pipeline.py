import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import hash_blocking_spark as hash
import spark.length_blocking_spark as length
csv_folder = r".\CSV-files"
dblp_csv_path = r"\dblp.csv"
acm_csv_path = r"\acm.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("EntityResolution").getOrCreate()
spark.sparkContext._jvm.System.gc()

# Load CSV files into DataFrames
# column names paper_title, author_names, year, publication_venue, index
dblp_df = spark.read.csv(csv_folder+dblp_csv_path, header=True)
acm_df = spark.read.csv(csv_folder+acm_csv_path, header=True)

# Caste die "year"-Spalte zu einem Integer
dblp_df = dblp_df.withColumn("year", col("year").cast("int"))
acm_df = acm_df.withColumn("year", col("year").cast("int"))

selected_columns = ['author_names', 'paper_title', 'year', 'publication_venue']
selected_columns = ['author_names', 'paper_title']
a = hash.initial_hash_parallel(acm_df, selected_columns)
# a.show()
# Check the number of groups when grouping by "initials"
num_init_groups = a.groupBy("initials").count().count()
num_hash_groups = a.groupBy("hash_value").count().count()

# Print the number of groups
print("Number of groups: ", num_init_groups)
print("Number of groups: ", num_hash_groups)

a = length.length_blocking_multi_columns_named_parallel(a, selected_columns)
num_length_groups = a.groupBy("Legths").count().count()
print("Number of groups: ", num_length_groups)

spark.stop()
exit()
# Define a user-defined function (UDF) for similarity calculation
def similarity_udf(title1, title2):
    return jaccard_similarity(title1, title2)  # Change this to your desired similarity function

# Register the UDF
similarity_udf_spark = udf(similarity_udf, StringType())

# Matching: Apply similarity function to determine matches
matches = dblp_blocked.crossJoin(acm_blocked)
matches = matches.withColumn("similarity", similarity_udf_spark(matches["titles"], matches["titles"]))

# Filter matches based on similarity threshold (e.g., 0.7)
matched_pairs = matches.filter(col("similarity") > 0.7).select("year", "similarity")

# Write matched pairs to a CSV file
matched_pairs.write.csv("MatchedEntities.csv", header=True, mode="overwrite")

# Baseline: Apply similarity function on all pairs of the datasets
all_pairs = dblp_df.crossJoin(acm_df)
all_pairs = all_pairs.withColumn("similarity", similarity_udf_spark(all_pairs["title"], all_pairs["title"]))

# Calculate precision, recall, and F-measure
true_positives = matched_pairs.count()
false_positives = all_pairs.filter((col("similarity") > 0.7) & ~col("year_x").isNull()).count()
false_negatives = all_pairs.filter((col("similarity") <= 0.7) & ~col("year_x").isNull()).count()

precision = true_positives / (true_positives + false_positives)
recall = true_positives / (true_positives + false_negatives)
f_measure = 2 * (precision * recall) / (precision + recall)

# Print evaluation metrics
print("Precision: {}".format(precision))
print("Recall: {}".format(recall))
print("F-measure: {}".format(f_measure))

# Clustering: Group together all identified matches in clusters
# Your clustering logic goes here

# Resolve unmatched entities and write them back to disk
# Your resolution logic goes here

# Replicate each dataset 1 to 10 times with minor modifications
# Plot the resulting execution time for each replication factor
# Your replication and plotting logic goes here

# Stop Spark session
spark.stop()
