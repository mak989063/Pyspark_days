from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import datediff
from pyspark.sql.window import Window

spark = SparkSession .builder .appName("spark_day_1_app") .getOrCreate()

#Dataset:-
data = [
 ("C001", "2024-01-01", 500),
 ("C001", "2024-01-10", 1000),
 ("C001", "2024-01-04", 700),
 ("C002", "2024-01-02", 300),
 ("C002", "2024-01-15", 1200),
 ("C002", "2024-01-20", 500),
 ("C003", "2024-01-05", 900)
]

columns = ["customer_id", "order_date", "amount"]
df = spark.createDataFrame(data, columns)

#convert date column
df = df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))

# window specification
w = Window.partitionBy("customer_id").orderBy("order_date")

#calcuate previous order date
df2 = df.withColumn("prev_order_date", F.lag("order_date").over(w))

#calculate day gap
df2 = df2.withColumn("days_gap", F.date_diff("order_date", "prev_order_date"))

#final aggregation
result = df2.groupBy("customer_id").agg(F.sum("amount").alias("total_amount_spent"),
                                        F.min("order_date").alias("first_order_date"),
                                        F.max("order_date").alias("last_order_date"),
                                        F.avg("days_gap").alias("avg_days_gap"),
                                        )
result.show(truncate=False)







