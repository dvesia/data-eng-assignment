# Task 3

## Instructions

Use the `Solution` section below to add notes about your implementation.


### Question
Given a table of brand orders with columns id, brand_id, transaction_value, and created_at representing the date and time for each transaction, write a query to get the last order for each brand for each day.

The output should include the id of the transaction, datetime of the transaction, and the transaction amount. Order the transactions by datetime.

**Example**:

Input:

orders table

| Column            | Type     |
|:-------------------|----------|
| id                | INTEGER  |
| brand_id          | INTEGER  |
| transaction_value | FLOAT    |
| created_at        | DATETIME |


Output:

| Column            | Type     |
|:-------------------|----------|
| id                | INTEGER  |
| transaction_value | FLOAT    |
| created_at        | DATETIME |

# Solution

## PySpark SQL Task
This is a Python script that generates random order data,   
loads it into a PySpark DataFrame, executes a SQL query on the DataFrame,   
and saves the result to a CSV file.  

## Prerequisites
- Python 3.x
- PySpark 3.x

## Getting Started
1. Clone the repository and navigate to the project directory.  
2. Run `pip install -r requirements.txt` to install the required Python packages.  
3. Run the main.py file to generate random order data and execute the SQL query.  

The output CSV file will be saved in the data folder.  

## Configuration
The main.py file contains the SQL query that will be executed.   
You can modify this query to perform different analyses on the generated data.

## SQL Query
The following SQL query is executed on the orders data   
and selects the last order for each brand

``` sql
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
```

## Author
__Domenico Vesia__