import os
import random
import csv
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


def generate_orders_data(num_records):
    """
    Generates random order data and saves it to a CSV file.

    :param num_records: The number of records to generate
    """
    # Define the CSV field names
    field_names = ["id", "brand_id", "transaction_value", "created_at"]

    # Generate random data for the orders table
    orders_data = []
    for i in range(1, num_records + 1):
        id = random.randint(1, 10)
        brand_id = random.randint(1, 5)
        transaction_value = round(random.uniform(10.0, 1000.0), 2)
        created_at = datetime.now() - timedelta(days=random.randint(1, 30))
        orders_data.append((id, brand_id, transaction_value, created_at))

    # Create folder if it not exists
    folder_path = "data"
    os.makedirs(folder_path, exist_ok=True)

    # Write the data to a CSV file
    with open(os.path.join(folder_path, "orders.csv"), "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(field_names)
        writer.writerows(orders_data)


def run_sql_and_save_results(
        sql_string: str,
        input_table_name: str,
        csv_input_file: str,
        output_file: str
) -> None:
    """
    Executes a SQL query on a PySpark DataFrame and saves the result to a CSV file.

    :param sql_string: The SQL query to execute. The query can reference the input DataFrame
        using the input_table_name argument.
    :param input_table_name: The name to give the input DataFrame when registering it as a temporary view.
    :param csv_input_file: The path to the input CSV file.
    :param output_file: The path to the output CSV file.
    """

    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("SQL Task") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Read the input CSV file into a DataFrame
    input_df = spark.read.option("header", "true") \
        .option("inferSchema", "true") \
        .csv(csv_input_file)

    # Register the DataFrame as a temporary view
    input_df.createOrReplaceTempView(input_table_name)

    # Execute the SQL query
    output_df = spark.sql(sql_string)

    # Convert the PySpark DataFrame to a Pandas DataFrame
    output_pdf = output_df.toPandas()

    # Save the result to a CSV file
    output_pdf.to_csv(output_file, index=False)
