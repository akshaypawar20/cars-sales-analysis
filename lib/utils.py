from pyspark.sql import SparkSession
from conf.Variables import APP_NAME, CONFIG, TIME_CONFIG


# A spark session for our cars-sales-analysis application


def create_spark_session():
    spark = SparkSession.builder.appName(APP_NAME) \
        .config(CONFIG[0], CONFIG[1]) \
        .getOrCreate()
    spark.conf.set(TIME_CONFIG[0], TIME_CONFIG[1])

    return spark


# A function to read the file
# spark -> SparkSession, file_type -> Type of the file to read(CSV,JSON,PARQUET etc)

def read_file(spark, file_type, file_path):
    df = spark.read.format(file_type) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    return df


def write_file(spark, file_type, file_path):
    spark.write.format(file_type) \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .save(file_path)
