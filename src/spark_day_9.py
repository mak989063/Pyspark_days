from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark9 = SparkSession.builder.appName("sparkDay9").getOrCreate()

data = [
    ("2018-01-01 11:00:00", "u1"),
    ("2018-01-01 12:00:00", "u1"),
    ("2018-01-01 13:00:00", "u1"),
    ("2018-01-01 13:00:00", "u1"),
    ("2018-01-01 14:00:00", "u1"),
    ("2018-01-01 15:00:00", "u1"),
    ("2018-01-01 11:00:00", "u2"),
    ("2018-01-02 11:00:00", "u2"),
]

columns = ["click_time", "user_id"]

clickstream_df = spark9.createDataFrame(data, columns)

# Convert click_time to Unix timestamp for easier calculations
clickstream_df = clickstream_df.withColumn("click_timestamp", F.unix_timestamp("click_time"))
session_window = Window.partitionBy("user_id").orderBy("click_timestamp")

clickstream_df = clickstream_df.withColumn("prev_click_timestamp", (F.lag("click_timestamp", 1)).over(session_window))
# Difference between click time and dividing that with 60
clickstream_df = clickstream_df.withColumn("timestamp_diff", (F.col("click_timestamp")-F.col("prev_click_timestamp"))/60)
# Updating null with 0
clickstream_df = clickstream_df.withColumn("timestamp_diff", F.when(F.col("timestamp_diff").isNull(), 0).otherwise(F.col("timestamp_diff")))
# Check for new session
clickstream_df = clickstream_df.withColumn("session_new", F.when(F.col("timestamp_diff") > 30, 1).otherwise(0))
# New session names
clickstream_df = clickstream_df.withColumn("session_new_name", F.concat(F.col("user_id"), F.lit("--S"), F.sum(F.col("session_new")).over(session_window)))
clickstream_df.show()