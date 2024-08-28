from dependencies.spark_start import spark_start
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    spark = spark_start('data_clean_etl')
    audible_data = r'D:\DataCleaning\Raw-Data-Cleaning\raw_data\audible\audible_uncleaned.csv'
    audible_df = extract_data(spark,audible_data)
    data_cleaning(audible_df)

def extract_data(spark, path):
    df = spark.read.format('csv')\
        .option("header", True)\
        .load(path)
    return df

def data_cleaning(df):
    print("Inside data_cleaning function")
    df_cleaned = df.withColumn("author", regexp_replace("author", r"^Writtenby:",""))\
                    .withColumn("narrator", regexp_replace("narrator", r"^Narratedby:",""))
    df_cleaned.show(truncate=False)

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()


