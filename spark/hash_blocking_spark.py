from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import hashlib
from pyspark.sql import functions as F

def hash_partition(iterator):
    for row in iterator:
        blocking_key = row['blocking_key']
        hash_value = hashlib.md5(blocking_key.encode()).hexdigest()
        yield blocking_key, {'hash_value': hash_value, 'index': row['collect_list(index)']}


def initial_hash_parallel(path, columns_to_use):
    spark = SparkSession.builder.appName("InitialHash").getOrCreate()
    df = spark.read.csv(path, header=True, inferSchema=True)
    
    if 'publication_venue' in columns_to_use:
        df = df.withColumn('publication_venue', F.when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

    transform_author_names_udf = F.udf(lambda value: "".join([name[0] if len(name.split()) == 1 else name.split()[0][0] + name.split()[-1][0] for name in value.split()]), StringType())
    transform_paper_title_udf = F.udf(lambda value: "".join([word[0] for word in value.split()]), StringType())

    for column in columns_to_use:
        if column == 'author_names':
            df = df.withColumn(column, transform_author_names_udf(column))
        elif column == 'paper_title':
            df = df.withColumn(column, transform_paper_title_udf(column))
        elif column == 'year' or column == 'publication_venue':
            df = df.withColumn(column, F.col(column).cast(StringType()))

    df = df.withColumn('blocking_key', F.udf(lambda *args: ''.join(str(arg) for arg in args), StringType())(*columns_to_use))
    blocks = df.groupBy('blocking_key').agg(F.collect_list('index'))

    blocks = blocks.rdd.mapPartitions(hash_partition)

    blocks_dict = dict(blocks.collect())

    spark.stop()
    return blocks_dict