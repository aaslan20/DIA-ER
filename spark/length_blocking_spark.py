
from pyspark.sql.types import StringType
from pyspark.sql.functions import concat_ws, udf

def length(x): # length of a string
    return str(len(str(x)))

udf_length = udf(length, StringType())

def length_blocking(df, columns_to_use):
    df = df.withColumn("Legths", concat_ws("_", *(udf_length(df[column_name]) for column_name in columns_to_use)))
    return df

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
    # SparkSession erstellen
    spark = SparkSession.builder.appName("TestBeispiel").getOrCreate()

    # Beispiel-Daten erstellen
    data = [("Wert1", "Value1"), ("Wert2", "Value2"), ("Wert3", "Value3")]
    columns = ["Spalte1", "Spalte2"]

    # DataFrame erstellen
    df = spark.createDataFrame(data, columns)

    # Spaltenlängen berechnen und neue Spalte hinzufügen
    columns_to_use = ["Spalte1", "Spalte2"]
    df_neu = length_blocking(df, columns_to_use)

    # Ergebnisse anzeigen
    df_neu.show()