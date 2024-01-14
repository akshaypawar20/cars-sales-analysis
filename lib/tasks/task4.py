from pyspark.sql.functions import year, to_date, col, date_add, concat_ws, date_format

def processing_four(input_df):
    df = input_df
    dateFormat = "MM/dd/yyyy"
    df = df.filter(col("Region") == "West")
    df = df.withColumn("Order Date", to_date(col("Order Date"), dateFormat))
    df = df.withColumn("Year", year(col("Order Date")))
    dateFilter = ((col("Year") >= 2016) & (col("Year") <= 2018))
    df = df.filter(dateFilter)
    df = df.filter(col("Segment") == "Consumer")
    df = df.withColumn("Ship Date", to_date(col("Ship Date"), dateFormat))
    df = df.withColumn("Delivered Date", date_add(col("Ship Date"), 4))
    df = df.withColumn("Delivered Date Sample",concat_ws(" ", col("Delivered Date"), date_format(col("Delivered Date"), "E")))
    return df