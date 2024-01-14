from pyspark.sql.functions import split, col, avg, round, sum
from pyspark.sql.types import IntegerType

def processing_two(input_df):
    df = input_df
    split_col_id = split(df["Customer ID"], "-")
    df = df.select(col("Customer ID"), col("Sales"))
    df = df.withColumn("Customer ID", split_col_id.getItem(1).cast(IntegerType()))
    df = df.withColumn("Sales", col("Sales").cast(IntegerType()))
    #df.select(col("Customer ID"), col("Sales")).printSchema()
    df = df.groupBy(col("Customer ID")).agg(round(sum(col("Sales")), 3).alias("Sum_Sales"),round(avg(col("Sales")), 3).alias("Avg_Sales"))
    df = df.orderBy(col("Sum_Sales").desc())
    filterCondition = (col("Sum_Sales") > 3000)
    df = df.filter(filterCondition)
    return df