"""
This script is responsible for extract data from the
given JSON file and
return it as a PySpark DataFrame.
The returned DataFrame can then be used for further processing,
uch as transformation or analysis.

Author: Domenico Vesia
Date: 2023-02-09
"""

import pyspark


def extract_data(
        spark: pyspark.sql.SparkSession,
        filepath: str
) -> pyspark.sql.DataFrame:
    """
    This function reads the JSON file from the given filepath and returns a PySpark DataFrame.

    :param spark: PySpark session object
    :param filepath: Path to the JSON file to read
    :return: PySpark DataFrame of the JSON data
    """
    # Read JSON file into DataFrame
    df = spark.read.json(filepath)

    return df
