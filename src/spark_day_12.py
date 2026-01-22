from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark12 = SparkSession.builder.appName("sparkDay12").getOrCreate()

data = [
    ("A001", "2024-01-01",  500),
    ("A001", "2024-01-01", -200),
    ("A001", "2024-01-03", -100),
    ("A001", "2024-01-04",  300),

    ("A002", "2024-01-02", 1000),
    ("A002", "2024-01-02", -500),
    ("A002", "2024-01-05", -200),

    ("A003", "2024-01-01",  400),
    ("A003", "2024-01-02",  600),
]

columns = ["account_id", "transaction_date", "amount"]

df = spark12.createDataFrame(data, columns)

#1️⃣ daily_balance → sum of all transactions for that account on that day


df_daily = (df.groupBy("account_id", "transaction_date").agg(F.sum("amount").alias("daily_balance")))

#CREDIT or DEBIT
df = df_daily.withColumn("transaction_type",
                   F.when(F.col("daily_balance")>0, "CREDIT")
                   .when(F.col("daily_balance")<0, "DEBIT")
                   .otherwise("ZERO"))

#Running Balance                   )
w = Window.partitionBy("account_id").orderBy("transaction_date")
df = df.withColumn("running_balance",F.sum("daily_balance").over(w))


df.show()

spark12.stop()





