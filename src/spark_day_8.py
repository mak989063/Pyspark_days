from pyspark.errors.utils import with_origin_to_class
from pyspark.sql import session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark8 = session.SparkSession.builder.appName("sparkDay8").getOrCreate()

data = [
 ("C001", "2024-03-01 10:00:00", 19000),
 ("C001", "2024-03-01 10:01:30", 15000),
 ("C001", "2024-03-01 10:04:10", 20000),
 ("C002", "2024-03-02 09:00:00", 12000),
 ("C002", "2024-03-02 09:10:00", 13000),
 ("C003", "2024-03-03 11:00:00", 5000)
]

columns = ["customer_id", "txn_ts", "amount"]
#"prev_amount", "prev_ts", "time_diff_min", "fraud_flag"]

df = spark8.createDataFrame(data, columns)

#window spec
w= Window.partitionBy("customer_id").orderBy(df.txn_ts)

#prev_amount
df = df.withColumn("prev_amount", F.lag("amount").over(w))

#"prev_ts"
df = df.withColumn("prev_ts", F.lag(F.to_timestamp("txn_ts")).over(w))

#"time_diff_min"
df = df.withColumn("txn_ts", F.to_timestamp("txn_ts"))
df = df.withColumn("prev_ts", F.to_timestamp("prev_ts"))
df = df.withColumn("time_diff_min", F.round(F.try_subtract(F.to_timestamp("txn_ts").cast("long"), F.to_timestamp("prev_ts").cast("long"))/60, 2))

#fraud_flag
df = df.withColumn("fraud_flag",
                   F.when (
                       (F.col("time_diff_min") <= 2) &
                       (F.col("amount")>10000) &
                       (F.col("prev_amount")>10000), "YES" )
                   .otherwise("NO"))
df.show()