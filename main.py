from pyspark.sql import SparkSession
if __name__ == "__main__":
    spark = SparkSession.builder.appName("Demo").getOrCreate()
    df = spark.read.format("csv") \
              .option("header","true") \
              .option("inferSchema", "true") \
              .load("data/sales_data.csv")
    df.show(1)