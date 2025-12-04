from pyspark.sql import session
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark7 = session.SparkSession.builder.appName("sparkDay7").getOrCreate()

data = [
 ("P001", "2025-01-01", 98.5, 80, 120, 98),
 ("P001", "2025-01-03", 99.1, 82, 118, 97),
 ("P001", "2025-01-02", 101.0, 90, 130, 94),

 ("P002", "2025-01-01", 101.8, 78, 122, 99),
 ("P002", "2025-01-10", 102.0, 95, 140, 92),

 ("P003", "2025-01-05", 100.5, 88, 135, 93),
("P003", "2025-01-17", 99.5, 89, 130, 92)
]

columns = ["patient_id", "reading_date", "temperature", "pulse", "bp", "oxygen"]
df = spark7.createDataFrame(data, columns)

#1️⃣ Find the First and Latest Reading for Each Patient
df1 = df.groupBy("patient_id").agg(F.min("reading_date").alias("first_reading_date"),F.max("reading_date").alias("latest_reading_date"))

#2️⃣ Identify Fever Spikes

df2 = df.groupBy("patient_id").agg(F.sum(F.when(F.col("temperature") > 100.4, 1).otherwise(0)).alias("spike_count"))

#3️⃣ Compute the Avg Gap Between Health Readings
w = Window.partitionBy("patient_id").orderBy(F.col("reading_date"))
df_q3 = df.withColumn("prev_reading_date", F.lag("reading_date").over(w))
df_avg = df_q3.withColumn("days_gap", F.datediff("reading_date", "prev_reading_date"))
df3 = df_avg.groupBy("patient_id").agg(F.avg("days_gap").alias("avg_days_gap")).drop("reading_date")

#4️⃣ Find the Highest Pulse Reading AND the Date It Occurred
w1 = Window.partitionBy("patient_id").orderBy(F.col("pulse").desc())

df4 = df.withColumn("rn", F.row_number().over(w1)) \
 .filter("rn=1") \
 .drop("rn").select("patient_id", F.col("pulse").alias("max_pulse"))


#5️⃣ Label Overall Patient Risk Level
w2 = Window.partitionBy("patient_id").orderBy(F.col("reading_date").desc())

df5 = ((df.withColumn("rn", F.row_number().over(w2))
               .filter("rn= 1").withColumn("risk_level", F.when(F.col("temperature") > 100.4, "High Risk")
               .when(F.col("oxygen") < 95, "Medium Risk").otherwise("Normal"))).orderBy(F.col("reading_date").desc())).drop("rn").select("patient_id", "risk_level")

#6️⃣ Detect Missing Daily Records
df = df.withColumn("reading_date", F.to_date("reading_date", "yyyy-MM-dd"))
range_6_df = df.groupBy("patient_id").agg(F.min("reading_date").alias("first_reading_date"),F.max("reading_date").alias("latest_reading_date"))

full_dates_6_df = range_6_df.withColumn("full_dates", F.sequence("first_reading_date", "latest_reading_date", F.expr("interval 1 day"))) \
 .select("patient_id", F.explode("full_dates").alias("reading_date"))

#Identify missing dates
df6 = full_dates_6_df.join(df,["patient_id", "reading_date"],"left_anti")

#7️⃣ Final Combined Output

final_df = (
 df1.alias("a").join(df2.alias("b"), "patient_id", "left").join(df3.alias("c"), "patient_id", "left")
 .join(df4.alias("d"), "patient_id", "left").join(df5.alias("e"), "patient_id", "left")
)

final_df.show()



