"""
This script runs the ETL process for the
Orders dataset.
It extracts the data from a JSON file, transforms it using a metadata file,
and saves the transformed data to a CSV file.

Author: Domenico Vesia
Date: 2023-03-10
"""

import pyspark
import yaml
from extract import extract_data
from transform import transform_data
from load import save_dataframe_to_csv

# Define paths
JSON_FILEPATH = "../data/raw/orders.jsonl"
METADATA_FILEPATH = '../metadata.yaml'
CSV_FILENAME = "orders.csv"

if __name__ == '__main__':
    # Create SparkSession
    spark = pyspark.sql.SparkSession.builder \
        .appName("OrdersETL") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Extract data from JSON file
    raw_df = extract_data(spark, JSON_FILEPATH)

    # Load metadata from YAML file
    metadata = yaml.safe_load(open(METADATA_FILEPATH))

    # Transform data using metadata
    transformed_df = transform_data(raw_df, metadata)

    # Save transformed data to CSV file
    save_dataframe_to_csv(transformed_df, CSV_FILENAME)

    # Stop SparkSession
    spark.stop()
