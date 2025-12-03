from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("sparkDay2").getOrCreate()

# Dataset
data = [
 ("C001", "2024-01-01"),
 ("C001", "2024-01-02"),
 ("C001", "2024-01-04"),
 ("C001", "2024-01-06"),
 ("C002", "2024-01-03"),
 ("C002", "2024-01-05"),
]

columns = ["customer_id", "billing_date"]
df = spark.createDataFrame(data, columns)

#convert billing date
df = df.withColumn("billing_date", F.to_date("billing_date", "yyyy-MM-dd"))

#given date range
range_df = df.groupBy("customer_id").agg(F.min("billing_date").alias("min_date"), F.max("billing_date").alias("max_date"))

#full dates range expected
full_range_df = range_df.withColumn("full_dates", F.sequence("min_date","max_date",F.expr("interval 1 day"))) \
 .select("customer_id", F.explode("full_dates").alias("billing_date"))

#find missing dates
missing_dates = full_range_df.join(df,["customer_id", "billing_date"], "left_anti")

#Create groups for consecutive missing dates (gaps)
w = Window.partitionBy("customer_id").orderBy("billing_date")

missing_dates = missing_dates \
    .withColumn("rn", F.row_number().over(w)) \
    .withColumn("grp", F.datediff("billing_date", F.lit("1970-01-01")) - F.col("rn"))


#Final aggregation
result = missing_dates.groupBy("customer_id", "grp").agg(
    F.min("billing_date").alias("missing_from"),
    F.max("billing_date").alias("missing_to")
).select(
    "customer_id", "missing_from", "missing_to"
).orderBy("customer_id", "missing_from")

result.show()