from pyspark.sql.functions import concat_ws, udf
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from typing import List, Callable
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from Levenshtein import ratio

group_years = udf(lambda year, year_block, labels: labels[len([y for y in year_block if y <= year])-1], StringType())

def blocking_by_year_and_publisher_column_(df, columns_to_use, labels= ["1995", "1996", "1997", "1998", "1999", "2000", "2001", "2002", "2003", "2004"], year_block=[1995,1996,1997, 1998, 1999,2000,2001, 2002, 2003, 2004,2005]):
    df = df.withColumn("blocking_key", concat_ws("-", group_years(df["year"], year_block, labels), df["publication_venue"]))
    return df

def apply_similarity_sorted(blocks1: DataFrame, blocks2: DataFrame, threshold: float, similarity_function: str, indices: List[str]):
    blocks1 = blocks1.withColumn("value", col("value").cast(StringType()))
    blocks2 = blocks2.withColumn("value", col("value").cast(StringType()))

    joined_blocks = blocks1.crossJoin(blocks2)
    matched_blocks = joined_blocks.filter(ratio(col("blocks1.value"), col("blocks2.value")) > threshold)

    matched_blocks_list = [(row["blocks1.index"], row["blocks2.index"]) for row in matched_blocks.collect()]

    return matched_blocks_list
