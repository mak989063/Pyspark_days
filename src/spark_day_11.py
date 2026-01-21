from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark11 = SparkSession.builder.appName("sparkDay11").getOrCreate()

data = [
 ("G101", "Team A", 2),
 ("G101", "Team B", 1),
 ("G102", "Team C", 0),
 ("G102", "Team D", 3),
 ("G103", "Team A", 1),
 ("G103", "Team C", 1),
 ("G104", "Team B", 4),
 ("G104", "Team D", 0),
]

columns = ["game_id", "team_name", "goals_scored"]

df = spark11.createDataFrame(data, schema=columns)

df_copy = df

#total goals scored in a game
w = Window.partitionBy("game_id")
total_goals_df = df_copy.groupBy("game_id").agg(F.sum("goals_scored").alias("total_goals")).select("game_id", "total_goals")

#winning_team →
df1 = df.withColumn(
    "max_goals_in_game",
    F.max("goals_scored").over(w)
)

df2 = df1.filter(
    F.col("goals_scored") == F.col("max_goals_in_game")
)

df3 = df2.groupBy("game_id").agg(
    F.count("*").alias("winner_count"),
    F.first("team_name").alias("winner_team")
)

winner_df = df3.withColumn("winning_team",
                     F.when(F.col("winner_count") > 1, F.lit("DRAW"))
                     .otherwise(F.col("winner_team"))).select("game_id", "winning_team")
final_result = (
    winner_df
        .join(total_goals_df, on="game_id", how="inner")
        .select("game_id", "total_goals", "winning_team")
)

final_result.show()

spark11.stop()
