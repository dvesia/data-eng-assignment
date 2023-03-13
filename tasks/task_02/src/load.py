"""
This script is responsible for
loading a CSV file into a PySpark DataFrame.
"""
import os
import pyspark


def save_dataframe_to_csv(
        df: pyspark.sql.DataFrame,
        filename: str
) -> None:
    """
    Saves a PySpark DataFrame to a CSV file.

    :param df: The PySpark DataFrame to save
    :param filename: Name of the CSV file
    """
    # Convert PySpark DataFrame to Pandas DataFrame
    pandas_df = df.toPandas()

    # Create directory if it doesn't exist
    filepath = "../data/prepared"
    os.makedirs(filepath, exist_ok=True)

    # Save Pandas DataFrame to CSV file
    pandas_df.to_csv(os.path.join(filepath, filename), index=False)
