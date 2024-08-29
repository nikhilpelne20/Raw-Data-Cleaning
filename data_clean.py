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
    df_clean_col_sub_str = df.withColumn("author", regexp_replace("author", r"^Writtenby:",""))\
                    .withColumn("narrator", regexp_replace("narrator", r"^Narratedby:",""))

    df_clean_time = df_clean_col_sub_str.withColumn("time",
                                        (coalesce(regexp_extract("time", r"(\d+)\s*hrs?", 1).cast("int"),lit(0)) * 60) +
                                        coalesce(regexp_extract("time", r"(\d+)\s*mins?", 1).cast("int"),lit(0))
                                        )
    df_clean_stars = df_clean_time.withColumn("star_ratings", regexp_extract("stars", r"(\d+\.?\d*)\s*out\s*of\s*5\s*stars", 1))\
                                    .withColumn("ratings", regexp_extract(col("stars"), r"(\d+)\s*ratings", 1))

   
    df_remove_empty_val = df_clean_stars.withColumn("star_ratings", when(col("star_ratings") == "", "0").otherwise(col("star_ratings")))\
                                        .withColumn("ratings",when(col("ratings") == "", "0").otherwise(col("ratings")))

    df_cleaned_data = df_remove_empty_val.select("name","author","narrator","time", "releasedate","language",col("star_ratings").alias("stars"), "price", "ratings")
    df_cleaned_data.show()

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()


