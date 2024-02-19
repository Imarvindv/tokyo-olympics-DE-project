# Databricks notebook source
athlete_df = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympics1/raw-data/athlete/athlete.csv")
coaches_df = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympics1/raw-data/coaches/coaches.csv")
entries_gender_df = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympics1/raw-data/entries_gender/entries_gender.csv")
medals_df = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympics1/raw-data/medals/medals.csv")
teams_df = spark.read.format("csv").option("header","true").load("/mnt/tokyoolympics1/raw-data/teams/teams.csv")

# COMMAND ----------

athlete_df.show()

# COMMAND ----------

athlete_df.printSchema()

# COMMAND ----------

coaches_df.show()

# COMMAND ----------

coaches_df.printSchema()

# COMMAND ----------

entries_gender_df.show()

# COMMAND ----------

entries_gender_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# COMMAND ----------

entries_gender_df = entries_gender_df.withColumn("Female", col("Female").cast(IntegerType())) \
            .withColumn("Male", col("Male").cast(IntegerType())) \
            .withColumn("Total", col("Total").cast(IntegerType())) 

# COMMAND ----------

entries_gender_df.printSchema()

# COMMAND ----------

medals_df.show()

# COMMAND ----------

medals_df = medals_df.withColumn("Gold", col("Gold").cast(IntegerType())) \
            .withColumn("Total", col("Total").cast(IntegerType())) \
            .withColumnRenamed("Rank by Total", "RankbyTotal")

# COMMAND ----------

medals_df.printSchema()

# COMMAND ----------

medals_df.count()

# COMMAND ----------

teams_df.show()

# COMMAND ----------

teams_df.printSchema()

# COMMAND ----------

# Find the top countries with the highest number of gold medals
medals_df.select("Team_Country","Gold").orderBy("Gold", ascending=False).show()

# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
entries_gender_df = entries_gender_df.withColumn("Avg_Female_Entries", entries_gender_df["Female"] / entries_gender_df["Total"]) \
                 .withColumn("Avg_Male_Entries", entries_gender_df["Male"] / entries_gender_df["Total"])
entries_gender_df.show()

# COMMAND ----------

athlete_df.write.mode("overwrite").format("delta").save("/mnt/tokyoplympics1/processed/athletes")
coaches_df.write.mode("overwrite").format("delta").save("/mnt/tokyoplympics1/processed/coaches")
entries_gender_df.write.mode("overwrite").format("delta").save("/mnt/tokyoplympics1/processed/entries_gender")
medals_df.write.mode("overwrite").format("delta").save("/mnt/tokyoplympics1/processed/medals")
teams_df.write.mode("overwrite").format("delta").save("/mnt/tokyoplympics1/processed/teams")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/tokyoplympics1/processed/coaches"))

# COMMAND ----------

display(spark.read.load("/mnt/tokyoplympics1/processed/entries_gender"))