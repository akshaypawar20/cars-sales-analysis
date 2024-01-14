from pyspark.sql.functions import col, split, when,count, concat_ws
from pyspark.sql.types import IntegerType
def processing_three(input_df):
    df = input_df
    split_cust_id = split(df["Customer ID"], "-")
    split_cust_name = split(df["Customer ID"], "-")
    nameFilter = ((col("Country") == "United States") & (col("City") == "San Francisco"))
    df = df.filter(nameFilter)
    df = df.withColumn("cust_id", split_cust_id.getItem(1).cast(IntegerType()))
    df = df.withColumn("cust_name", split_cust_name.getItem(0))
    df = df.groupBy(col("cust_id"), col("cust_name")).agg(count("*").alias("Total_Count"))
    df = df.orderBy(col("Total_Count").desc())
    df = df.withColumn("Full_Name", concat_ws("-", col("cust_id"), col("cust_name")))
    df = df.select(col("Full_Name"), col("Total_Count"))
    df = df.withColumn("Top_Orders", when(col("Total_Count") >= 6, "High") \
                   .when(((col("Total_Count") >= 3) & (col("Total_Count") < 6)), "Medium") \
                   .otherwise("Low"))
    return df