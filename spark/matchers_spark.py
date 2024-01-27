from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time
from itertools import product
from pyspark.sql.types import StringType

def apply_similarity_blocks_spark(df1, df2, threshold, similarity_function):
    start_time = time.time()   
    similarity_udf = F.udf(lambda set1, set2: str(similarity_function(set(set1), set(set2))), StringType())

    joined_blocks = df1.alias("block1").crossJoin(df2.alias("block2"))
    similar_pairs_df = joined_blocks.where(
        (F.col("block1.value") == F.col("block2.value"))
    ).select(
        F.col("block1.index").alias("index1"),
        F.col("block2.index").alias("index2"),
        similarity_udf(F.col("block1.value"), F.col("block2.value")).alias("similarity")
    )

    similar_pairs_df = similar_pairs_df.filter(F.col("similarity") >= threshold)
    similar_pairs = similar_pairs_df.collect()

    processed_data = []
   
    for row in similar_pairs:
        index1_values = row['index1'][1:-1].split(', ')
        index2_values = row['index2'][1:-1].split(', ')

        if len(index1_values) > 1 or len(index2_values) > 1:
            index_combinations = list(product(index1_values, index2_values))
            for combination in index_combinations:
                processed_data.append((combination[0], combination[1]))
        else:
            
            processed_data.append(row)
            processed_data = list(set(processed_data))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of index combinations: {len(processed_data)}")

    return processed_data
# for ngram_blocking cause is a list not a dict (similiar_pairs)
def apply_ngram_blocks_spark(df1, df2, threshold, similarity_function):
    start_time = time.time()
 
    similarity_udf = F.udf(lambda set1, set2: str(similarity_function(set(set1), set(set2))), StringType())

    joined_blocks = df1.alias("block1").crossJoin(df2.alias("block2"))
    similar_pairs_df = joined_blocks.where(
        (F.col("block1.value") == F.col("block2.value"))
    ).select(
        F.col("block1.index").alias("index1"),
        F.col("block2.index").alias("index2"),
        similarity_udf(F.col("block1.value"), F.col("block2.value")).alias("similarity")
    )

    similar_pairs_df = similar_pairs_df.filter(F.col("similarity") >= threshold)
    similar_pairs = similar_pairs_df.collect()

    processed_data = []
   
    for row in similar_pairs:
   
        index1_values = row.index1
        index2_values = row.index2
        index1_values = [value for value in index1_values]
        index2_values = [value for value in index2_values]

        if len(index1_values) > 1 or len(index2_values) > 1:
            index_combinations = list(product(index1_values, index2_values))
            for combination in index_combinations:
                processed_data.append((combination[0], combination[1]))
        else:
            processed_data.append(row)
        
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of index combinations: {len(processed_data)}")

    return processed_data




"""
def apply_similarity_blocks_spark(df1, df2, threshold, similarity_function):
    spark = SparkSession.builder.appName("apply_similarity_blocks_spark").getOrCreate()

    start_time = time.time()

    
    data_tuples1 = [(key, value[field_name], value['index']) for key, value in blocks1.items()]
    data_tuples2 = [(key, value[field_name], value['index']) for key, value in blocks2.items()]
    
    schema = StructType([       
        StructField("blocking_key", StringType(), nullable=False),
        StructField(field_name, StringType(), nullable=False),
        StructField("index", ArrayType(StringType()), nullable=False)
    ])
    
    blocks1_df = spark.createDataFrame(data_tuples1, schema=schema)
    blocks2_df = spark.createDataFrame(data_tuples2, schema=schema)
    
   
    similarity_udf = F.udf(lambda set1, set2: str(similarity_function(set(set1), set(set2))), StringType())

    joined_blocks = df1.alias("block1").crossJoin(df2.alias("block2"))
    similar_pairs_df = joined_blocks.where(
        (F.col("block1.value") == F.col("block2.value"))
    ).select(
        F.col("block1.index").alias("index1"),
        F.col("block2.index").alias("index2"),
        similarity_udf(F.col("block1.value"), F.col("block2.value")).alias("similarity")
    )

    similar_pairs_df = similar_pairs_df.filter(F.col("similarity") >= threshold)
    similar_pairs = similar_pairs_df.collect()
    
    index_combinations = [(i, j) for pair in similar_pairs for i in pair['index1'] for j in pair['index2']]
    index_combinations = list(set(index_combinations))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing time: {elapsed_time} seconds. Number of index combinations: {len(index_combinations)}")

    return index_combinations
"""

