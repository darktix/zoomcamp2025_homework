# Question 1: Understanding dbt model resolution    
Provided you've got the following sources.yaml      
    
version: 2      
    
sources:      
  - name: raw_nyc_tripdata      
    database: "{{ env_var('DBT_BIGQUERY_PROJECT', 'dtc_zoomcamp_2025') }}"      
    schema:   "{{ env_var('DBT_BIGQUERY_SOURCE_DATASET', 'raw_nyc_tripdata') }}"      
    tables:      
      - name: ext_green_taxi      
      - name: ext_yellow_taxi      
with the following env variables setup where dbt runs:      
    
export DBT_BIGQUERY_PROJECT=myproject      
export DBT_BIGQUERY_DATASET=my_nyc_tripdata      
What does this .sql model compile to?      
    
select *       
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}      
    
Answer:      
select * from myproject.my_nyc_tripdata.ext_green_taxi      
    
Explanation:      
export set the new value for both of env_var, thus it gives myproject.my_nyc_tripdata.ext_green_taxi      
    
# Question 2: dbt Variables & Dynamic Models    
Say you have to modify the following dbt_model (fct_recent_taxi_trips.sql) to enable Analytics Engineers to dynamically control the date range.      
    
In development, you want to process only the last 7 days of trips      
In production, you need to process the last 30 days for analytics      
select *      
from {{ ref('fact_taxi_trips') }}      
where pickup_datetime >= CURRENT_DATE - INTERVAL '30' DAY      
What would you change to accomplish that in a such way that command line arguments takes precedence over ENV_VARs, which takes precedence over DEFAULT value?      
    
Answer:      
Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ env_var("DAYS_BACK", var("days_back", "30")) }}' DAY      
    
Explanation:     
This nested definition allows the analytics to have acess to the ENV_VAR and set it up (by default 30) when the model is in production, but it also allows the dev to modify it by calling --vars'{'days_back':7}' in CLI in dev environment      
    
# Question 3: dbt Data Lineage and Execution    
Considering the data lineage below and that taxi_zone_lookup is the only materialization build (from a .csv seed file):      
    
image      
    
Select the option that does NOT apply for materializing fct_taxi_monthly_zone_revenue:      
    
Answer:     
dbt run --select models/staging/+      
    
Explanation:      
it will run only the models inside staging which are stg_green, stg_fhv, stg_yellow and thus not materializing fct_taxi_monthly_zone_revenue      
    
# Question 4: dbt Macros and Jinja    
Consider you're dealing with sensitive data (e.g.: PII), that is only available to your team and very selected few individuals, in the raw layer of your DWH (e.g: a specific BigQuery dataset or PostgreSQL schema),      
    
Among other things, you decide to obfuscate/masquerade that data through your staging models, and make it available in a different schema (a staging layer) for other Data/Analytics Engineers to explore      
    
And optionally, yet another layer (service layer), where you'll build your dimension (dim_) and fact (fct_) tables (assuming the Star Schema dimensional modeling) for Dashboarding and for Tech Product Owners/Managers      
    
You decide to make a macro to wrap a logic around it:      
    
{% macro resolve_schema_for(model_type) -%}      
    
    {%- set target_env_var = 'DBT_BIGQUERY_TARGET_DATASET'  -%}      
    {%- set stging_env_var = 'DBT_BIGQUERY_STAGING_DATASET' -%}      
    
    {%- if model_type == 'core' -%} {{- env_var(target_env_var) -}}      
    {%- else -%}                    {{- env_var(stging_env_var, env_var(target_env_var)) -}}      
    {%- endif -%}      
    
{%- endmacro %}      
And use on your staging, dim_ and fact_ models as:      
    
{{ config(      
    schema=resolve_schema_for('core'),      
) }}      
That all being said, regarding macro above, select all statements that are true to the models using it:      
- Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile      
- Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile      
- When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET      
- When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET      
- When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET      
    
Answer:     
Setting a value for DBT_BIGQUERY_TARGET_DATASET env var is mandatory, or it'll fail to compile      
Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile      
When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET      
When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET      
When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET      
    
Explanation:      
First and second statements are needed because set is defined at the begining of the macro and before if condition and ENV_VAR should be defined or else it won't compile      
Third statement correspond to the if condition and thus use target env      
The last two statements works because in the macro only the if model_type =='core' is defined, everything else goes into the else statement      
    
# Question 5: Taxi Quarterly Revenue Growth    
Create a new model fct_taxi_trips_quarterly_revenue.sql      
Compute the Quarterly Revenues for each year for based on total_amount      
Compute the Quarterly YoY (Year-over-Year) revenue growth      
e.g.: In 2020/Q1, Green Taxi had -12.34% revenue growth compared to 2019/Q1      
e.g.: In 2020/Q4, Yellow Taxi had +34.56% revenue growth compared to 2019/Q4      
Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow      
    
Answer:      
green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}  
  
SQL:  
{{  
    config(  
        materialized='table'  
    )  
}}  
  
WITH quarterly_revenue AS (  
    SELECT   
        service_type,  
        year,  
        quarter,  
        year_quarter,  
        SUM(total_amount) as revenue  
    FROM {{ ref('fact_trips_external') }}  
    GROUP BY 1, 2, 3, 4  
),  
  
revenue_growth AS (  
    SELECT   
        current_year.service_type,  
        current_year.year as current_year,  
        current_year.quarter,  
        current_year.year_quarter as current_year_quarter,  
        current_year.revenue as current_revenue,  
        prev_year.revenue as prev_revenue,  
        prev_year.year_quarter as prev_year_quarter,  
        ROUND((current_year.revenue - prev_year.revenue) / prev_year.revenue * 100, 2) as yoy_growth  
    FROM quarterly_revenue current_year  
    LEFT JOIN quarterly_revenue prev_year  
        ON current_year.service_type = prev_year.service_type  
        AND current_year.quarter = prev_year.quarter  
        AND current_year.year = prev_year.year + 1  
    WHERE current_year.year = 2020  
),  
  
ranked_growth AS (  
    SELECT   
        *,  
        ROW_NUMBER() OVER(PARTITION BY service_type ORDER BY yoy_growth DESC) as best_rank,  
        ROW_NUMBER() OVER(PARTITION BY service_type ORDER BY yoy_growth ASC) as worst_rank  
    FROM revenue_growth  
)  
  
SELECT   
    service_type,  
    quarter,  
    current_year_quarter,  
    prev_year_quarter,  
    ROUND(current_revenue, 2) as current_revenue,  
    ROUND(prev_revenue, 2) as prev_revenue,  
    yoy_growth,  
    CASE   
        WHEN best_rank = 1 THEN 'Best performing'  
        WHEN worst_rank = 1 THEN 'Worst performing'  
        ELSE 'Normal'  
    END as performance_label  
FROM ranked_growth  
ORDER BY service_type, quarter  
  
    
# Question 6: P97/P95/P90 Taxi Monthly Fare    
Create a new model fct_taxi_trips_monthly_fare_p95.sql    
Filter out invalid entries (fare_amount > 0, trip_distance > 0, and payment_type_description in ('Cash', 'Credit card'))    
Compute the continous percentile of fare_amount partitioning by service_type, year and and month    
Now, what are the values of p97, p95, p90 for Green Taxi and Yellow Taxi, in April 2020?    
    
Answer:    
green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}    
    
SQL model:    
{{ config(materialized='table') }}    
    
WITH percentiles AS (    
  SELECT     
      service_type,    
      year,    
      month,    
      PERCENTILE_CONT(fare_amount, 0.90) OVER(PARTITION BY service_type, year, month) as p90,    
      PERCENTILE_CONT(fare_amount, 0.95) OVER(PARTITION BY service_type, year, month) as p95,    
      PERCENTILE_CONT(fare_amount, 0.97) OVER(PARTITION BY service_type, year, month) as p97    
  FROM {{ ref('fact_trips_external') }}    
  WHERE fare_amount > 0     
    AND trip_distance > 0     
    AND payment_type_description IN ('Cash', 'Credit card')    
    AND year = 2020    
)    
    
SELECT DISTINCT service_type, year, month, p90, p95, p97    
FROM percentiles    
ORDER by service_type, year, month    
    
    
    
# Question 7: Top #Nth longest P90 travel time Location for FHV    
Prerequisites:    
    
Create a staging model for FHV Data (2019), and DO NOT add a deduplication step, just filter out the entries where where dispatching_base_num is not null    
Create a core model for FHV Data (dim_fhv_trips.sql) joining with dim_zones. Similar to what has been done here    
Add some new dimensions year (e.g.: 2019) and month (e.g.: 1, 2, ..., 12), based on pickup_datetime, to the core model to facilitate filtering for your queries    
Now...    
    
Create a new model fct_fhv_monthly_zone_traveltime_p90.sql    
For each record in dim_fhv_trips.sql, compute the timestamp_diff in seconds between dropoff_datetime and pickup_datetime - we'll call it trip_duration for this exercise    
Compute the continous p90 of trip_duration partitioning by year, month, pickup_location_id, and dropoff_location_id    
For the Trips that respectively started from Newark Airport, SoHo, and Yorkville East, in November 2019, what are dropoff_zones with the 2nd longest p90 trip_duration ?    
    
ANSWER:     
LaGuardia Airport, Chinatown, Garment District    
    
SQL:    
{{  config(materialized='table')  }}    
    
with fhv_zone as (    
    SELECT     
        *,    
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MICROSECOND) as trip_duration,    
    FROM {{  ref('fct_fhv_trips_external')}}    
),    
percentile as (    
    SELECT DISTINCT    
        year,    
        month,    
        pickup_zone,    
        dropoff_zone,    
        trip_duration,    
        PERCENTILE_CONT(trip_duration, 0.90) OVER(PARTITION BY year, month, pickup_locationid, dropoff_locationid) as p90    
    FROM fhv_zone    
)    
SELECT DISTINCT    
    p90,    
    dropoff_zone    
FROM percentile    
WHERE year = 2019     
    AND month = 11    
    AND pickup_zone = 'Yorkville East'    
ORDER BY p90 DESC    
    