from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark4 = SparkSession.builder.appName("sparkDay4").getOrCreate()

#Dataset
data = [
 ("U001", "T101", "2024-02-01"),
 ("U001", "T102", "2024-02-02"),
 ("U001", "T103", "2024-02-05"),
 ("U001", "T104", "2024-02-08"),

 ("U002", "T201", "2024-03-10"),
 ("U002", "T202", "2024-03-12"),
 ("U002", "T203", "2024-03-14"),
 ("U002", "T204", "2024-03-18"),

 ("U003", "T301", "2024-01-01"),
 ("U003", "T302", "2024-01-01"),
 ("U003", "T303", "2024-01-04"),
]

columns = ["customer_id", "transaction_id", "transaction_date"]

df = spark4.createDataFrame(data, columns)

#convert transaction to yyyy-MM-dd format
df = df.withColumn("transaction_date", F.to_date("transaction_date", "yyyy-MM-dd"))

#given date range
range_4_df = df.groupBy("customer_id").agg(F.min("transaction_date").alias("min_date"), F.max("transaction_date").alias("max_date"))

#full dates
full_range_4_df = range_4_df.withColumn("full_dates", F.sequence("min_date","max_date",F.expr("interval 1 day"))) \
 .select("customer_id", F.explode("full_dates").alias("transaction_date"))

#missing dates
missing_tran_dates = full_range_4_df.join(df,["customer_id", "transaction_date"],"left_anti")

#idenify gaps

w =  Window.partitionBy("customer_id").orderBy("transaction_date")

missing_tran_dates_df  = missing_tran_dates.withColumn(
    "prev_date",
    F.lag("transaction_date").over(w)
).withColumn(
    "new_group",
    F.when(F.col("prev_date").isNull(), 1)
     .when(F.datediff("transaction_date", F.col("prev_date")) > 1, 1)
     .otherwise(0)
).withColumn(
    "grp",
    F.sum("new_group").over(w))

#final aggregation

result = missing_tran_dates_df.groupBy("customer_id", "grp").agg(F.min("transaction_date").alias("missing_from"), F.max("transaction_date").alias("missing_to")).orderBy("customer_id", "missing_from").drop("grp")
result.show()