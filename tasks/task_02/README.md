# Task 2

## Instructions
Implementation: Input data is a jsonl file, for the assignment purpose the desired ouput should be a csv file. Feel free to use any programming language you prefer.

Evaluation: Your work will be evaluated based on the completeness of the ETL process, the accuracy of the data transformation, the efficiency of the ETL code, and the quality of the documentation and test results.

You can use the `Solution` section to add notes about your implementation, add files containing the solution inside the current folder along instructions about the ETL execution.

### Question

Given the dataset inside the folder `data` implement an ETL that clean and flatten data removing duplicates and add the following columns:

year (YYYY)
year-month (YYYYMM) zero filled
year-quarter (YYYYQ#)
year-week (YYYYWXX) ISO week zero filled
weekday (1:mon-7:sun)

# Solution
##  Orders ETL Pipeline
This project demonstrates an ETL process for the Orders dataset.      
The ETL process extracts data from a JSON file, transforms it using a metadata file,   
and saves the transformed data to a CSV file.  

## Project Structure  

```
- data  
    - raw
        - orders.jsonl
    - prepared
        - orders.csv
- src
   - extract.py
   - transform.py
   - load.py
   - run-etl-pipeline.py
- tests
   - test-run-etl-pipeline.py
- metadata.yaml
- requirements.txt
- README.md
```

The data directory contains the raw and prepared data files.  
The metadata.yaml file contains metadata information about the dataset,   
including data types and transformations to apply.   
The execute_etl_pipeline.py script runs the ETL pipeline by extracting data, 
transforming it, and loading it into a CSV file.   
The extract.py, transform.py, and load.py scripts contain functions for each step of the ETL pipeline.
The tests directory contains the test file for check duplicates and null values.  

## Dependencies
- PySpark  
- PyYAML  
- pgeocode  
  
## Installation  
Clone the repository:    
`git clone https://github.com/dvesia/data_eng_assignment.git`    

Install the dependencies:  
`pip install -r requirements.txt  `

## Usage
To run the ETL pipeline, execute the save_dataframe_to_csv function in the etl_pipeline.py   
module using the spark-submit command:

<code>spark-submit execute-etl_pipeline.py</code>

Alternatively, you can run the pipeline using Python by executing the etl_pipeline.py module directly:

<code>python execute-etl_pipeline.py</code>

Note that running the pipeline with spark-submit is recommended for larger datasets,  
as it can take advantage of Spark's distributed computing capabilities to process data more efficiently.

## Continuous Integration
This project uses GitHub Actions for continuous integration. 
The workflow is defined in .github/workflows/ci.yaml.   
Whenever changes are pushed to the repository, GitHub Actions automatically runs   
the tests defined in test_etl_pipeline.py on an Ubuntu virtual machine, using Python 3.8.  
If any of the tests fail, the workflow will fail, and you will receive a notification.  

## Author
- __Domenico Vesia__
