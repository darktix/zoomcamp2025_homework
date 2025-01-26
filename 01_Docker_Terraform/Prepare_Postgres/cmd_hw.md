# Prepare Postgres  
Run following comman using Gitbash in venv  
  
docker-compose up -d  
  
For green taxi data:  
  URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz"  
  
  To be run in venv :  
  python ingest_data_hw.py \    
    --user=postgres \    
    --password=postgres \  
    --host=localhost \  
    --port=5433 \  
    --db=ny_taxi \  
    --table_name=green_taxi_trips \  
    --url=${URL}  
  
Testing ingest_data_hw.py with parquet file:  
  URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"  
  
  python ingest_data_hw.py \  
    --user=postgres \  
    --password=postgres \  
    --host=localhost \  
    --port=5433 \  
    --db=ny_taxi \  
    --table_name=yellow_taxi_trips \  
    --url=${URL}  
  
For taxi zone data:  
  Run upload_data_taxi_zone_hw.ipynb with venv python kernel  