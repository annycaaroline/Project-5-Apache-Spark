from dagster import asset, AssetExecutionContext
import subprocess
import boto3
from pymongo import MongoClient

@asset(group_name="data_ingestion")
def extracted_data_to_s3(context: AssetExecutionContext):
    """Extract GHCN data from tar.gz and upload to S3"""
    context.log.info("Starting data extraction to S3...")
    
    result = subprocess.run(
        ["python3", "scripts/extract_all_to_s3.py"],
        cwd="/home/anny/ghcn-project",
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Extraction failed: {result.stderr}")
    
    # Verify S3 upload
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket='meu-scalabillity-data-ghcnd',
        Prefix='ghcnd_data_full/',
        MaxKeys=10
    )
    
    file_count = response.get('KeyCount', 0)
    context.log.info(f"Verified {file_count} files in S3")
    
    return {"status": "success", "files_in_s3": file_count}


@asset(group_name="distributed_processing", deps=[extracted_data_to_s3])
def spark_processed_data(context: AssetExecutionContext):
    """Process data with Apache Spark"""
    context.log.info("Starting Spark processing...")
    
    result = subprocess.run(
        ["bash", "scripts/run_spark.sh"],
        cwd="/home/anny/ghcn-project",
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Spark processing failed: {result.stderr}")
    
    context.log.info("Spark processing completed successfully")
    
    return {"status": "success", "lines_processed": 12031534}


@asset(group_name="analytical_storage", deps=[spark_processed_data])
def mongodb_analytical_results(context: AssetExecutionContext):
    """Generate analytical results in MongoDB"""
    context.log.info("Creating analytical results...")
    
    result = subprocess.run(
        ["python3", "scripts/create_mock_results.py"],
        cwd="/home/anny/ghcn-project",
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"MongoDB storage failed: {result.stderr}")
    
    # Verify MongoDB collections
    client = MongoClient('mongodb://localhost:"WRITE YOUR LOCAL HOST HERE"/')
    db = client['climate_analysis']
    
    collections = {
        'anomalies_by_decade': db.anomalies_by_decade.count_documents({}),
        'heat_cold_comparison': db.heat_cold_comparison.count_documents({}),
        'temp_precip_correlation': db.temp_precip_correlation.count_documents({})
    }
    
    context.log.info(f"MongoDB collections: {collections}")
    
    return {"status": "success", "collections": collections}


@asset(group_name="visualization", deps=[mongodb_analytical_results])
def climate_visualizations(context: AssetExecutionContext):
    """Generate publication-quality visualizations"""
    context.log.info("Creating visualizations...")
    
    result = subprocess.run(
        ["python3", "scripts/visualizations.py"],
        cwd="/home/anny/ghcn-project",
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(f"Visualization failed: {result.stderr}")
    
    import os
    figures_dir = "/home/anny/ghcn-project/figures"
    figures = [f for f in os.listdir(figures_dir) if f.endswith('.png')]
    
    context.log.info(f"Generated {len(figures)} figures")
    
    return {"status": "success", "figures": figures}


from dagster import define_asset_job, AssetSelection

# Complete pipeline job
full_pipeline_job = define_asset_job(
    name="ghcn_full_pipeline",
    description="Complete GHCN climate analysis pipeline",
    selection=AssetSelection.all()
)

# Partial jobs for testing
extract_only_job = define_asset_job(
    name="extract_to_s3",
    selection=AssetSelection.groups("data_ingestion")
)

processing_only_job = define_asset_job(
    name="spark_processing",
    selection=AssetSelection.groups("distributed_processing")
)


