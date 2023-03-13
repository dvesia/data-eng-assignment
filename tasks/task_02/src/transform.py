"""
This script is responsible for
applies a series of transformations on a PySpark DataFrame,
and returns a new PySpark DataFrame.

Author: Domenico Vesia
Date: 2023-02-09
"""

import pyspark
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as fs
from typing import Tuple
import pgeocode


def get_province_and_region(
        postal_code: str
) -> Tuple[str, str]:
    """
    This function takes an Italian postal code as input and
    returns the corresponding province and region.

    :param postal_code: The Italian postal code (CAP)
    :return: A tuple containing the province and region corresponding to the input postal code
    """
    # Create a Nominatim instance for Italy
    nomi = pgeocode.Nominatim('it')

    # Query the Nominatim database for information about the given postal code
    info = nomi.query_postal_code(postal_code).to_dict()

    # Extract the province and region information from the dictionary
    province = info['county_code']
    region = info['state_name']

    # Return the province and region as a tuple
    return province, region


def get_upper(
        word: str
) -> str:
    """
    This function takes a string as input and returns the same string in uppercase letters.

    :param word: A string to be converted to uppercase
    :return: The input string in uppercase letters
    """
    word = word.upper()
    return word


def transform_data(
        df: pyspark.sql.DataFrame,
        metadata: dict
) -> pyspark.sql.DataFrame:
    """
    This function applies a series of transformations to a PySpark DataFrame
    according to a metadata dictionary.

    :param df: The PySpark DataFrame to transform
    :param metadata: A dictionary containing metadata about the DataFrame
    and the desired transformations
    :return: The transformed PySpark DataFrame
    """
    # Transform nested structure into a flat structure
    df = df.withColumn("StateOrRegion", fs.coalesce(fs.col("ShippingAddress.StateOrRegion"), fs.lit("NA"))) \
        .withColumn("PostalCode", fs.coalesce(fs.col("ShippingAddress.PostalCode"), fs.lit("NA"))) \
        .withColumn("City", fs.coalesce(fs.col("ShippingAddress.City"), fs.lit("NA"))) \
        .withColumn("CountryCode", fs.coalesce(fs.col("ShippingAddress.CountryCode"), fs.lit("NA"))) \
        .withColumn("Amount", fs.col("OrderTotal.Amount")) \
        .withColumn("IsBusinessOrder", fs.when(fs.col("IsBusinessOrder") == 'true', 1).otherwise(0)) \
        .withColumn("IsReplacementOrder", fs.when(fs.col("IsBusinessOrder") == 'true', 1).otherwise(0)) \
        .drop("OrderTotal", "ShippingAddress")

    # Cast columns
    cast_dictionary = metadata['columns_to_cast']

    for data_type, columns in cast_dictionary.items():
        for column in columns:
            df = df.withColumn(column, fs.col(column).cast(data_type))

    # Define the UDF
    get_province_and_region_udf = fs.udf(lambda x: get_province_and_region(x), StructType([
        StructField("province", StringType()),
        StructField("region", StringType())
    ]))

    # Define the UDF
    get_upper_udf = fs.udf(lambda x: get_upper(x), StringType())

    # Apply the UDF to the PostalCode column and assign NA values when PostalCode is null or empty
    cond = ((fs.col("PostalCode").isNull()) | (fs.col("PostalCode") == ""))

    df = df.withColumn(
        "PostalCode",
        fs.when(cond, fs.lit("NA")).otherwise(fs.col("PostalCode"))
    )

    # Apply the UDF to the PostalCode column
    df = df.withColumn("province_region", get_province_and_region_udf("PostalCode"))

    # Split the province_region column into separate columns for province and region
    df = df.withColumn("Province", fs.when(fs.col("PostalCode") == "", "").otherwise(df["province_region"]["province"]))\
        .withColumn("Region", fs.when(fs.col("PostalCode") == "", "").otherwise(df["province_region"]["region"])) \
        .drop("province_region", "StateOrRegion")

    # Apply the UDF to a column
    df = df.withColumn("City", get_upper_udf(fs.col("City"))) \
        .withColumn("Region", get_upper_udf(fs.col("Region")))

    # Extract year, month, and quarter from PurchaseDate
    df = df.withColumn("Year", fs.year("PurchaseDate")) \
        .withColumn("YearMonth", fs.date_format("PurchaseDate", "yyyyMM")) \
        .withColumn("YearQuarter", fs.concat(fs.year("PurchaseDate"), fs.lit("Q"), fs.quarter("PurchaseDate"))) \
        .withColumn("YearWeek", fs.concat(fs.year("PurchaseDate"), fs.lit("W"), fs.weekofyear("PurchaseDate"))) \
        .withColumn("Weekday", fs.date_format(fs.col("PurchaseDate"), "u").cast("integer"))

    # Load columns to fill from yaml file
    columns_to_fill = metadata["columns_to_fill"]

    # Fill categorical columns
    for col in columns_to_fill:
        df = df.fillna({col: 'NA'})

    # Fill Amount column with 0.0 values when OrderStatus is Canceled
    df = df.withColumn(
            "Amount", fs.when(fs.col('OrderStatus') == 'Canceled', 0.0).otherwise(fs.col("Amount"))
        )

    # Reorder columns
    ordered_columns = metadata['ordered_columns']

    # Select ordered columns
    df = df.select(*ordered_columns)

    # Drop duplicates
    df = df.dropDuplicates()

    return df
