#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import requests
from pyspark.sql.functions import rand

print("=" * 60)
print("STARTING SPARK PROCESSING")
print("=" * 60)

# Configuration
S3_BUCKET = "meu-scalabillity-data-ghcnd"

S3_DATA_PATH = f"s3a://{S3_BUCKET}/ghcnd_data_full/*.dly"   
MONGODB_URI = "mongodb://localhost:"WRITE YOUR LOCAL HOST HERE"
MONGODB_DB = "ghcn_analysis"

# Create Spark Session
print("\nCreating Spark Session...")
spark = SparkSession.builder \
    .appName("GHCN Climate Analysis") \
    .config("spark.mongodb.output.uri", f"{MONGODB_URI}/{MONGODB_DB}") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("Spark Session created")

# Read data from S3
print(f"\nReading data from {S3_DATA_PATH}...")

# .dly file schema (fixed-width format)
# Format: ID(11) YEAR(4) MONTH(2) ELEMENT(4) + 31 days (VALUE(5) MFLAG(1) QFLAG(1) SFLAG(1))
raw_data = spark.read.text(S3_DATA_PATH)

raw_data = raw_data.sample(fraction=0.13, seed=42)

print(f"Total lines read: {raw_data.count():,}")

# Parse fixed-width format
print("\nParsing fixed-width format...")

parsed_data = raw_data.select(
    substring(col("value"), 1, 11).alias("station_id"),
    substring(col("value"), 12, 4).cast("int").alias("year"),
    substring(col("value"), 16, 2).cast("int").alias("month"),
    substring(col("value"), 18, 4).alias("element"),
    # Extract values for the 31 days
    *[substring(col("value"), 22 + (i*8), 5).cast("int").alias(f"day_{i+1}") 
      for i in range(31)]
)

# Transform from wide to long format
print("Transforming to long format...")

# Create one row per day
days_data = []
for day in range(1, 32):
    day_col = parsed_data.select(
        col("station_id"),
        col("year"),
        col("month"),
        lit(day).alias("day"),
        col("element"),
        col(f"day_{day}").alias("value")
    )
    days_data.append(day_col)

# Union all days
long_data = days_data[0]
for df in days_data[1:]:
    long_data = long_data.union(df)

# Filter invalid values (-9999 = missing)
long_data = long_data.filter(col("value") != -9999)

# Convert temperature (tenths of degrees Celsius to degrees)
long_data = long_data.withColumn(
    "temperature",
    when(col("element") == "TMAX", col("value") / 10.0)
    .when(col("element") == "TMIN", col("value") / 10.0)
    .otherwise(None)
).withColumn(
    "precipitation",
    when(col("element") == "PRCP", col("value") / 10.0)
    .otherwise(None)
)

# Create date column
long_data = long_data.withColumn(
    "date",
    to_date(concat_ws("-", col("year"), col("month"), col("day")))
)

# Filter only TMAX, TMIN, PRCP
climate_data = long_data.filter(
    col("element").isin("TMAX", "TMIN", "PRCP")
).select(
    "station_id", "date", "year", "month", "day", 
    "element", "temperature", "precipitation"
)

print(f"Transformed data: {climate_data.count():,} records")

# Calculate historical averages (baseline: 1981–2010)
print("\nCalculating historical averages (baseline: 1981–2010)...")

baseline_data = climate_data.filter(
    (col("year") >= 1981) & (col("year") <= 2010)
)

# Historical average by station, month, day, and element
historical_avg = baseline_data.groupBy(
    "station_id", "month", "day", "element"
).agg(
    avg("temperature").alias("avg_temperature"),
    avg("precipitation").alias("avg_precipitation"),
    stddev("temperature").alias("stddev_temperature")
)

print(f"Historical averages calculated: {historical_avg.count():,} records")

# Calculate anomalies (deviation from historical average)
print("\nCalculating temperature anomalies...")

# Join with historical averages
anomalies = climate_data.alias("current").join(
    historical_avg.alias("hist"),
    (col("current.station_id") == col("hist.station_id")) &
    (col("current.month") == col("hist.month")) &
    (col("current.day") == col("hist.day")) &
    (col("current.element") == col("hist.element")),
    "left"
).select(
    col("current.station_id"),
    col("current.date"),
    col("current.year"),
    col("current.month"),
    col("current.element"),
    col("current.temperature"),
    col("hist.avg_temperature"),
    col("hist.stddev_temperature"),
    (col("current.temperature") - col("hist.avg_temperature")).alias("anomaly"),
    ((col("current.temperature") - col("hist.avg_temperature")) / 
     col("hist.stddev_temperature")).alias("z_score")
)

# Classify anomalies
anomalies = anomalies.withColumn(
    "anomaly_type",
    when(col("z_score") > 2, "extreme_heat")
    .when(col("z_score") > 1, "heat")
    .when(col("z_score") < -2, "extreme_cold")
    .when(col("z_score") < -1, "cold")
    .otherwise("normal")
)

print(f"Anomalies calculated: {anomalies.count():,} records")

# Analysis 1: Anomalies by Decade
print("\nANALYSIS 1:Frequency of anomalies by decade")

anomalies_decade = anomalies.withColumn(
    "decade",
    (floor(col("year") / 10) * 10).cast("int")
).groupBy("decade", "anomaly_type").agg(
    count("*").alias("count")
).orderBy("decade", "anomaly_type")

print("Results by decade:")
anomalies_decade.show(50)

# Analysis 2: Heat vs Cold Comparison over Time
print("\nANALYSIS 2: Heat vs cold anomaly comparison")

heat_cold_comparison = anomalies.withColumn(
    "decade",
    (floor(col("year") / 10) * 10).cast("int")
).filter(
    col("anomaly_type").isin("heat", "extreme_heat", "cold", "extreme_cold")
).groupBy("decade").agg(
    sum(when(col("anomaly_type").isin("heat", "extreme_heat"), 1).otherwise(0)).alias("heat_events"),
    sum(when(col("anomaly_type").isin("cold", "extreme_cold"), 1).otherwise(0)).alias("cold_events"),
    avg(when(col("anomaly_type").isin("heat", "extreme_heat"), col("anomaly"))).alias("avg_heat_anomaly"),
    avg(when(col("anomaly_type").isin("cold", "extreme_cold"), col("anomaly"))).alias("avg_cold_anomaly")
).orderBy("decade")

print("Comparison by decade:")
heat_cold_comparison.show()

# Analysis 3: Temperature vs Precipitation Correlation
print("\nANALYSIS 3: Temperature vs precipitation correlation")

# Prepare data for correlation
temp_precip = climate_data.groupBy("station_id", "year", "month").agg(
    avg(when(col("element") == "TMAX", col("temperature"))).alias("avg_tmax"),
    sum(when(col("element") == "PRCP", col("precipitation"))).alias("total_precip")
).filter(
    col("avg_tmax").isNotNull() & col("total_precip").isNotNull()
)

# Calculate correlation
correlation = temp_precip.stat.corr("avg_tmax", "total_precip")
print(f"Temperature vs Precipitation Correlation: {correlation:.4f}")

# Yearly aggregation
yearly_correlation = temp_precip.groupBy("year").agg(
    avg("avg_tmax").alias("yearly_avg_temp"),
    sum("total_precip").alias("yearly_total_precip")
).orderBy("year")

print("Yearly data:")
yearly_correlation.show(20)

# Save results to MongoDB
print("\nSaving results to MongoDB.")

# Save anomalies by decade
anomalies_decade.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("collection", "anomalies_by_decade") \
    .save()
print("Saved: anomalies_by_decade")

# Save heat vs cold comparison
heat_cold_comparison.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("collection", "heat_cold_comparison") \
    .save()
print("Saved: heat_cold_comparison")

# Save yearly correlation
yearly_correlation.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("collection", "temp_precip_correlation") \
    .save()
print("Saved: temp_precip_correlation")

# Save sample of individual anomalies (last 5 years)
recent_anomalies = anomalies.filter(col("year") >= 2020)
recent_anomalies.write \
    .format("mongo") \
    .mode("overwrite") \
    .option("collection", "recent_anomalies") \
    .save()
print("Saved: recent_anomalies")

print("\n" + "=" * 60)
print("=" * 60)
print(f"\nResults saved in MongoDB:")
print(f"   Database: {MONGODB_DB}")
print(f"   Collections:")
print(f"     - anomalies_by_decade")
print(f"     - heat_cold_comparison")
print(f"     - temp_precip_correlation")
print(f"     - recent_anomalies")

spark.stop()

