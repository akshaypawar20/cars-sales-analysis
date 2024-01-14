from pyspark.sql.functions import udf, col, to_date, year, month, day, split

def processing_one(input_df):

    df = input_df
    dateFormat = "MM/dd/yyyy"
    #columnsDropped = ("Order_Date", "Customer_ID", "Customer_Name", "Sub_Category", "Postal_Code", "Row_ID")

    df = df.withColumn("Order Date", to_date(col("Order Date"), dateFormat))

    df = df.withColumn("Ship Date", to_date(col("Ship Date"), dateFormat))

    df = df.withColumn("orders_year", year(col("Order Date"))) \
        .withColumn("orders_month", month(col("Order Date"))) \
        .withColumn("orders_days", day(col("Order Date")))

    split_col_cust = split(df["Customer ID"], "-")
    split_col_product = split(df["Product ID"],  "-")

    df = df.withColumn("Cust_id", split_col_cust.getItem(0)) \
          .withColumn("Cust_name", split_col_cust.getItem(1))

    df = df.withColumn("Product ID", split_col_product.getItem(2))
    #df = df.drop(*columnsDropped)

    return df
