#test script to check the spark connections

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


spark = SparkSession.builder\
        .master("local[3]")\
        .appName("data_cleaning")\
        .getOrCreate()

data = [
    (1001, "2024-08-01", 101, "P001", 2, 25.50),
    (1002, "2024-08-02", 102, "P002", 1, 40.00),
    (1003, "2024-08-02", 101, "P003", 4, 15.00),
    (1004, "2024-08-03", 103, "P001", 3, 25.50),
    (1005, "2024-08-03", 104, "P004", 1, 30.00),
    (1006, "2024-08-04", 102, "P002", 2, 40.00),
    (1007, "2024-08-04", 103, "P003", 5, 15.00),
    (1008, "2024-08-05", 104, "P005", 2, 50.00),
]

columns = ["TransactionID", "Date", "CustomerID", "ProductID", "Quantity", "Price"]

df = spark.createDataFrame(data, columns)

df.show()


