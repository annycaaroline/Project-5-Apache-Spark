#!/bin/bash

echo "Executing Spark Job..."

spark-submit \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1,org.apache.hadoop:hadoop-aws:3.3.1 \
  --conf spark.mongodb.output.uri=mongodb://localhost:"WRITE YOUR LOCAL HOST HERE" \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
  --master local[*] \
  scripts/process_with_spark.py
