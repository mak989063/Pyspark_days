from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import current_date
from pyspark.sql.window import Window

spark5 = SparkSession.builder.appName("sparkDay5").getOrCreate()

#Dataset
data = [
 ("C001", "2025-06-01", "Recharge"),
 ("C001", "2025-10-10", "Call"),
 ("C002", "2025-11-15", "Call"),
 ("C002", "2025-12-01", "Recharge"),
 ("C003", "2025-07-01", "SMS"),
 ("C004", "2025-02-27", "Recharge"),
 ("C005", "2025-10-20", "Call")
]

columns = ["customer_id", "activity_date", "activity_type"]
df = spark5.createDataFrame(data, columns)

#convert date column
df = df.withColumn("activity_date", F.to_date("activity_date", "yyyy-MM-dd"))


#1)Finds each customer's latest activity date
df = df.groupBy(["customer_id"]).agg(F.max(F.col("activity_date")).alias("Latest Activity Date"))

#2)Compares it with the current date
# Calculate the number of days since the last activity
df_with_days = df.withColumn("DaysSinceActivity", F.date_diff(F.current_date(), "Latest Activity Date").cast("integer"))

#3)Assigns the correct status label
#add status column based on last_activity
df_final = df_with_days.withColumn("status", F.when(F.col("DaysSinceActivity") <= 30, "Active")
                              .when((F.col("DaysSinceActivity") > 30) & (F.col("DaysSinceActivity") <= 60), "Dormant")
                              .otherwise("Inactive")).drop("DaysSinceActivity")
df_final.show()