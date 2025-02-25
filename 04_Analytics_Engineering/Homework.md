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
green 
best : 2020 Q3  
worst : 2020 Q2  

yellow  
best : 2020 Q3  
worst : 2020 Q2  

--> not in the propositions, figures in the example are not matching, work in progress