
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, when, udf
import hashlib

func = {'author_names':udf(lambda entry: "".join(name[0] for name in entry.split()), StringType()),
        'year':udf(lambda entry: str(entry), StringType()),
        'publication_venue':udf(lambda entry: str(entry), StringType()),
        'paper_title':udf(lambda entry: "".join(word[0] for word in entry.split()), StringType())}

def initial_hash_parallel(df, columns_to_use):
    # Neue Spalte hinzufügen
    df = df.withColumn("blocking_key", concat_ws("", *(func[column_name](df[column_name]) for column_name in columns_to_use)))
    df = df.withColumn("value",udf(lambda entry: hashlib.md5(entry.encode()).hexdigest(), StringType())(df["initials"]))

    return df