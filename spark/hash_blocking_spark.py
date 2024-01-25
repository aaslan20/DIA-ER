
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, when, udf
import hashlib

func = {'author_names':udf(lambda entry: "".join(name[0] for name in entry.split()), StringType()),
        'year':udf(lambda entry: str(entry), StringType()),
        'publication_venue':udf(lambda entry: str(entry), StringType()),
        'paper_title':udf(lambda entry: "".join(word[0] for word in entry.split()), StringType())}

def initial_hash_parallel(df, columns_to_use):
    # Verbesserung 2: Fehlerbehandlung für unbekannte Spalten hinzufügen
    unknown_columns = set(columns_to_use) - set(df.columns)
    if unknown_columns:
        raise ValueError(f"Unknown column(s): {', '.join(unknown_columns)}")

    # Verbesserung 3: Kommentare hinzufügen
    if 'publication_venue' in columns_to_use:
        # Ändern der 'publication_venue'-Werte basierend auf einer Bedingung
        df = df.withColumn('publication_venue', when(df['publication_venue'].contains('sigmod'), 'sigmod').otherwise('vldb'))

    # Neue Spalte hinzufügen
    df = df.withColumn("initials", concat_ws("", *(func[column_name](df[column_name]) for column_name in columns_to_use)))
    df = df.withColumn("hash_value",udf(lambda entry: hashlib.md5(entry.encode()).hexdigest(), StringType())(df["initials"]))
    return df
if __name__ == "__main__":
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    # Beispiel-Nutzung:
    # SparkSession erstellen (wenn nicht bereits vorhanden)
    spark = SparkSession.builder.appName("Example").getOrCreate()

    # Beispiel DataFrame erstellen
    data = [("A", "John Doe", 2022, "sigmod", "Data Science Paper"),
            ("B", "Alice Smith", 2021, "icde", "Big Data Analytics")]
    columns = ["id", "author_names", "year", "publication_venue", "paper_title"]
    df = spark.createDataFrame(data, columns)

    # Funktion anwenden
    columns_to_use = ['author_names', 'year', 'publication_venue', 'paper_title']

    df_result = initial_hash_parallel(df, columns_to_use)

    # Ergebnisse anzeigen
    df_result.show()