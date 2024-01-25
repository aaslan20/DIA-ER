from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, when, udf

def length(x): # length of a string
    return len(str(x))

udf_length = udf(length, StringType())

def length_blocking_multi_columns_named_parallel(df, columns_to_use):
    df = df.withColumn("Legths", concat_ws("_", *(udf_length(df[column_name]) for column_name in columns_to_use)))
    return df

