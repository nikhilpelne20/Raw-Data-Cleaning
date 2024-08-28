import findspark
findspark.init()

from pyspark.sql import SparkSession


def spark_start(app_name):
    spark = SparkSession.builder\
            .master("local[3]")\
            .appName(app_name)\
            .getOrCreate()
    return spark