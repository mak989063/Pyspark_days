from pyspark.sql.functions import row_number
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark10 = SparkSession.builder.appName("sparkDay10").getOrCreate()

data = [
    ("C001", "Rahul",    "Mumbai",     "2024-01-01 10:00:00"),
    ("C001", "Rahul K",  "Mumbai",     "2024-01-02 09:00:00"),
    ("C002", "Sneha",    "Pune",       "2024-01-05 12:00:00"),
    ("C002", "Sneha",    "Pune",       "2024-01-05 12:00:00"),  # duplicate
    ("C003", "Amit",     "Delhi",      "2024-01-03 08:30:00"),
    ("C003", "Amit",     "New Delhi",  "2024-01-04 11:15:00")
]

columns = ["customer_id", "name", "city", "last_updated_ts"]
df = spark10.createDataFrame(data, columns)

#convert timestamp
df = df.withColumn("last_updated_ts", F.to_timestamp(F.col("last_updated_ts")))

#window spec
w = Window.partitionBy("customer_id").orderBy(F.desc("last_updated_ts"))

#get latest updated timestamp
df = df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
df.show()


