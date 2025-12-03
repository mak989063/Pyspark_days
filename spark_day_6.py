from pyspark.sql import session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark6 = session.SparkSession.builder.appName("sparkDay6").getOrCreate()

data = [
 ("U001", "2025-01-01", 2000, "ADD"),
 ("U001", "2025-01-05",  500, "SPEND"),
 ("U001", "2025-01-03",  200, "CASHBACK"),
 ("U002", "2025-01-02", 3000, "ADD"),
 ("U002", "2025-01-15", 1000, "SPEND"),
 ("U002", "2025-01-10",  300, "CASHBACK"),
 ("U003", "2025-01-08",  900, "ADD")
]

columns = ["user_id", "txn_date", "amount", "txn_type"]

df = spark6.createDataFrame(data, columns)

#1️⃣ Total money credited (only credit_type = "ADD" or “CASHBACK”)
df2 = df.groupBy("user_id").agg(F.sum(F.when(F.col("txn_type").isin("ADD", "CASHBACK"), F.col("amount")).otherwise(0)).alias("total_credit"))

#2️⃣ Total money debited (credit_type = “SPEND”)
df3 = df.groupBy("user_id").agg(F.sum(F.when(F.col("txn_type").isin("SPEND"), F.col("amount")).otherwise(0)).alias("total_debit"))

#3️⃣ First transaction date
df4 = df.groupBy("user_id").agg(F.min(F.col("txn_date")).alias("first transaction date"))

#4️⃣ Most recent transaction date
df5 = df.groupBy("user_id").agg(F.max(F.col("txn_date")).alias("recent transaction date"))

#5️⃣ Average number of days between consecutive transactions (using window functions)
w = Window.partitionBy("user_id").orderBy(F.col("txn_date"))
df = df.withColumn("prev_txn_date", F.lag("txn_date").over(w))
df_avg = df.withColumn("days_gap", F.datediff("txn_date", "prev_txn_date"))
df6 = df_avg.groupBy("user_id").agg(F.avg("days_gap").alias("avg_days_gap"))

#final result
final_df = (
    df2.alias("a")
    .join(df3.alias("b"), "user_id", "left")
    .join(df4.alias("c"), "user_id", "left")
    .join(df5.alias("d"), "user_id", "left")
    .join(df6.alias("e"), "user_id", "left")
)

final_df.show()
