"""
pyspark_transformations.py
Author: Hely Shah
PySpark Bronze → Silver → Gold ELT — runs on AWS EMR, Glue, or Databricks.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SnowflakeELT_PySpark").getOrCreate()

def transform_silver(df):
    w = Window.partitionBy("ORDER_ID").orderBy(F.col("_INGESTED_AT").desc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn")==1).drop("_rn")
    df = df.withColumn("ORDER_DATE", F.to_date("ORDER_DATE","yyyy-MM-dd")) \
           .withColumn("AMOUNT", F.col("AMOUNT").cast("double")) \
           .dropna(subset=["ORDER_ID","ORDER_DATE","AMOUNT"]) \
           .filter((F.col("AMOUNT")>=0)&(F.col("AMOUNT")<=100000)) \
           .withColumn("ORDER_MONTH", F.date_trunc("month","ORDER_DATE")) \
           .withColumn("LAYER", F.lit("SILVER"))
    return df

def transform_gold(df):
    gold = df.groupBy("ORDER_MONTH","CUSTOMER_SEGMENT").agg(
        F.count("ORDER_ID").alias("ORDER_COUNT"),
        F.sum("AMOUNT").alias("TOTAL_REVENUE"),
        F.round(F.avg("AMOUNT"),2).alias("AVG_ORDER_VALUE")
    )
    w = Window.partitionBy("CUSTOMER_SEGMENT").orderBy("ORDER_MONTH")
    gold = gold.withColumn("PREV_MONTH_REVENUE", F.lag("TOTAL_REVENUE").over(w)) \
               .withColumn("MOM_GROWTH_PCT",
                   F.round((F.col("TOTAL_REVENUE")-F.col("PREV_MONTH_REVENUE"))/F.col("PREV_MONTH_REVENUE")*100,2)) \
               .withColumn("LAYER", F.lit("GOLD"))
    return gold

if __name__ == "__main__":
    bronze = spark.read.csv("data/bronze/raw_orders.csv", header=True, inferSchema=True)
    silver = transform_silver(bronze)
    silver.write.mode("overwrite").parquet("data/silver/clean_orders")
    gold = transform_gold(silver)
    gold.write.mode("overwrite").parquet("data/gold/monthly_revenue")
    spark.stop()
