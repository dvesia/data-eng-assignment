from utils import generate_orders_data, run_sql_and_save_results

if __name__ == "__main__":
    # Generate a csv with 500 records
    generate_orders_data(500)

    # Get the last order for each brand for each day.
    query = """
    WITH last_order_cte AS (
        SELECT
            id,
            brand_id,
            transaction_value,
            created_at,
            ROW_NUMBER() OVER (
                PARTITION BY brand_id, DATE(created_at)
                ORDER BY created_at DESC
            ) AS row_num
            FROM input_table
        )
        SELECT id, brand_id, transaction_value, created_at
        FROM last_order_cte
        WHERE row_num = 1
        ORDER BY created_at
        """

    run_sql_and_save_results(
        query,
        input_table_name="input_table",
        csv_input_file="data/orders.csv",
        output_file="data/result.csv"
    )
