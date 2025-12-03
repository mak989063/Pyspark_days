from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sparkDay2").getOrCreate()

# Dataset
sales_data = [
    ("StoreA", "Mumbai", "2025-11-01", 1000),
    ("StoreA", "Mumbai", "2025-11-02", 1500),
    ("StoreB", "Mumbai", "2025-11-01", 2000),
    ("StoreC", "Delhi",  "2025-11-01", 1200),
    ("StoreC", "Delhi",  "2025-11-02", 1300),
    ("StoreD", "Delhi",  "2025-11-01", 1100),
    ("StoreE", "Chennai","2025-11-01", 900),
]

columns = ["store_name", "city", "sale_date", "sale_amount"]

df = spark.createDataFrame(sales_data, columns)

#Total Sales per City

total_sales_per_city = df.groupBy("city").agg(F.sum("sale_amount").alias("total_sales"))
total_sales_per_city.show()

#Average Daily Sales per Store

average_sales_per_store = df.groupBy("store_name").agg(F.round(F.avg("sale_amount")).cast("integer").alias("avg_daily_sales"))
average_sales_per_store.show()

#Top-Selling Store per City (by total sales)
store_sales_per_city = df.groupBy("city", "store_name").agg(F.sum("sale_amount").alias("total_sales"))

w = Window.partitionBy("city").orderBy(F.col("total_sales").desc())

ranked_stores = store_sales_per_city.withColumn("rank", F.row_number().over(w))
top_selling_store_per_city = ranked_stores.filter(F.col("rank") == 1)
top_selling_store_per_city.select("city", "store_name", "total_sales").orderBy(F.col("total_sales").desc()).show()