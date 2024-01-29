import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from pyspark.sql.functions import collect_list, when
from itertools import combinations
import hash_blocking_spark as hash
import length_blocking_spark as length
import matchers_spark as matchers
import time

baselines_folder = r".\baselines"
csv_folder = r".\CSV-files"
dblp_csv_path = r"\dblp.csv"
acm_csv_path = r"\acm.csv"

# Initialize Spark session
spark = SparkSession.builder.appName("EntityResolution").getOrCreate()
# spark.sparkContext._jvm.System.gc()

# Load CSV files into DataFrames
# column names paper_title, author_names, year, publication_venue, index
dblp_df = spark.read.csv(csv_folder+dblp_csv_path, header=True, inferSchema=True)
acm_df = spark.read.csv(csv_folder+acm_csv_path, header=True, inferSchema=True)
dataframes = [dblp_df, acm_df]

dblp_df_datasets = {"full": dblp_df}.update({group_key: group_df for group_key, group_df in matchers.blocking_by_year_and_publisher_column_(dblp_df, ["year", "publication_venue"]).groupBy("blocking_key")})
acm_df_datasets = {"full": acm_df}.update({group_key: group_df for group_key, group_df in matchers.blocking_by_year_and_publisher_column_(acm_df, ["year", "publication_venue"]).groupBy("blocking_key")})
for df in dataframes:
    df = df.withColumn('publication_venue', when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

columns = dblp_df.columns
columns.remove("index")

blocking_functions = [length.length_blocking_parallel, hash.initial_hash_parallel] # Add your blocking functions here
baselines = {0.7:{"jac":spark.read.csv(baselines_folder+r"\base_7_jac_stem.csv", header=True, inferSchema=True),
                  "len": spark.read.csv(baselines_folder+r"\base_7_l_stem.csv", header=True, inferSchema=True)},
#                "lev":spark.read.csv(baselines_folder+r"\base_7_lev_stem.csv", header=True, inferSchema=True)},
             0.85:{"jac":spark.read.csv(baselines_folder+r"\base_7_jac_stem.csv", header=True, inferSchema=True),
                  "len": spark.read.csv(baselines_folder+r"\base_7_l_stem.csv", header=True, inferSchema=True)}}#,
                #"lev":spark.read.csv(baselines_folder+r"\base_7_lev_stem.csv", header=True, inferSchema=True)}}

# Blocking: Apply blocking to both datasets
for r in range(1, len(columns) + 1):
    for comb in combinations(columns, r):
        for blocking in blocking_functions:
            for key in dblp_df_datasets:
                grouped_dfs = []
                for df in [dblp_df_datasets[key], acm_df_datasets[key]]:
                    start_time = time.time()
                    blocked_df = blocking(df, comb)
                    grouped_df = blocked_df.groupBy("blocking_key")
                    end_time = time.time()
                    execution_time = end_time - start_time
                    count = grouped_df.count().count()
                    print(f"Combination {comb}\nwith blocking method '{blocking.__name__}'\ntook {execution_time} seconds and resulted in {count} groups on the {key} dataset.")
                    grouped_dfs.append(grouped_df)
                for threshold in baselines:
                    for similarity_function in baselines[threshold]:
                        print(f"Applying {similarity_function} baseline with threshold {threshold} on {key} dataset.")
                        matches = matchers.apply_similarity_sorted(grouped_dfs[0], grouped_dfs[1], threshold, similarity_function, ["value"])
                        print(f"Found {len(matches)} matches.")
spark.stop()
exit()

a = hash.initial_hash_parallel(acm_df, selected_columns)

# Check the number of groups when grouping by "initials"
num_init_groups = a.groupBy("initials").count().count()
num_hash_groups = a.groupBy("blocking_key").count().count()
grouped_data = a.groupBy("blocking_key").agg(collect_list("index").alias("index_list"))

num_pairs = 0

for row in grouped_data.collect():
    index_list = row["index_list"]
    # Berechne die kombinatorische Vielfalt
    combinations_count = len(list(combinations(index_list, 2)))
    num_pairs += combinations_count

print("Number of pairs: ", num_pairs)

# Print the number of groups
print("Number of groups: ", num_init_groups)
print("Number of groups: ", num_hash_groups)

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

# Print evaluation similarity_functions
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
