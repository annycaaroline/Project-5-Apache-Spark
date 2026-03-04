REQUIREMENTS:

boto3==1.35.36
pandas==2.1.4
matplotlib==3.8.2
seaborn==0.13.0
pymongo==4.6.1
pyspark==3.5.1
requests==2.31.

SETUP:
1. Install dependencies: pip install -r requirements.txt
2. Configure AWS credentials: aws configure
3. Start MongoDB: sudo systemctl start mongod

EXECUTION ORDER:
1. Extract data to S3: python3 scripts/extract_all_to_s3.py
2. Process with Spark: ./scripts/run_spark.sh
3. Create mock data (demo): python3 scripts/create_mock_results.py
4. Generate visualizations: python3 scripts/create_visualizations.py


# Orchestration
dagster==1.12.6
dagster-webserver==1.12.6

DATA LOCATIONS:
- S3 Bucket: s3://meu-scalabillity-data-ghcnd/
- MongoDB: mongodb://localhost:"WRITE HERE YOUR LOCAL HOST"/climate_analysis

NOTES:
- +77,136 station files extracted to S3
- Spark processed more than 12 million lines


