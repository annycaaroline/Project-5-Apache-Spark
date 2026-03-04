from dagster import Definitions
from .ghcn_assets import (
    extracted_data_to_s3,
    spark_processed_data,
    mongodb_analytical_results,
    climate_visualizations,
    full_pipeline_job,
    extract_only_job,
    processing_only_job
)

defs = Definitions(
    assets=[
        extracted_data_to_s3,
        spark_processed_data,
        mongodb_analytical_results,
        climate_visualizations
    ],
    jobs=[
        full_pipeline_job,
        extract_only_job,
        processing_only_job
    ]
)
