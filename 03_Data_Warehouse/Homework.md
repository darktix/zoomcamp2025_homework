Upload to parquet file from NYC web site into GCS bucket using the provided python script
Create the external tables from the parquet files:
Command:
CREATE OR REPLACE EXTERNAL TABLE zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata
OPTIONS (
  format='Parquet',
  uris = [
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-01.parquet',
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-02.parquet',
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-03.parquet',
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-04.parquet',
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-05.parquet',
    'gs://zoomcamp_hw_2025_data_warehouse/yellow_tripdata_2024-06.parquet'
    ]
);

Create non partition table (regular/materialize table):
CREATE OR REPLACE TABLE zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned AS
SELECT * FROM zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata;

Create partitioned table:
CREATE OR REPLACE TABLE zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_partitioned 
PARTITION BY
  DATE(tpep_dropoff_datetime) AS
SELECT * FROM zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata;

Create partitioned and clustered table:


# Question 1
What is count of records for the 2024 Yellow Taxi Data?  
Answer: 20,332,093
SQL Command:
SELECT COUNT(1) FROM `ny_taxi.external_yellow_tripdata`;

# Question 2:
Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
Answer: 0 MB for the External Table and 155.12 MB for the Materialized Table

SQL Command:
SELECT DISTINCT PULocationID
FROM zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata;

SELECT DISTINCT PULocationID
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned;

# Question 3:
Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
Answer:
BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

SQL Command:
SELECT PULocationID
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned;
SELECT PULocationID, DOLocationID
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned;

# Question 4:
How many records have a fare_amount of 0?
Answer: 8333
SQL Command: 
SELECT COUNT(1)
FROM zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata
WHERE fare_amount = 0;

# Question 5:
What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
Answer: 
Partition by tpep_dropoff_datetime and Cluster on VendorID

SQL Command:
CREATE OR REPLACE TABLE zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime) 
CLUSTER BY VendorID AS
SELECT * FROM zoomcamp-hw-data-warehouse.ny_taxi.external_yellow_tripdata;

# Question 6:
Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?

Answer: 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

SQL Command:
SELECT DISTINCT (VendorID)
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT DISTINCT (VendorID)
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

# Question 7:
Where is the data stored in the External Table you created?
Answer : GCP Bucket

# Question 8:
It is best practice in Big Query to always cluster your data:
Answer: True

# Question 9:
No Points: Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
Answer:
This query will process 0 MB when run. It only reads the number of rows directly from storage info under details tab of the table

SQL Command:
SELECT COUNT(*)
FROM zoomcamp-hw-data-warehouse.ny_taxi.yellow_tripdata_non_partitioned;