# Question 1  
docker run -it --entrypoint=bash python:3.12.8    
pip --version  
--> pip 24.3.1  
  
# Question 2  
hostname: postgres  
port: 5432  
--> postgres:5432  
  
It works also with hostname: pg  
  
# Question 3  
  
How many trips, respectively, happened:  
    Up to 1 mile : 104802  
    In between 1 (exclusive) and 3 miles (inclusive) : 198924   
    In between 3 (exclusive) and 7 miles (inclusive) : 109603  
    In between 7 (exclusive) and 10 miles (inclusive) : 27678  
    Over 10 miles : 35189  
  
Answer : 104,802; 198,924; 109,603; 27,678; 35,189  
  
SQL Command :  
SELECT   
    COUNT(1) as "count"  
FROM   
    green_taxi_trips t  
WHERE   
	CAST(lpep_dropoff_datetime AS DATE) >= '2019-10-01' AND   
	CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'   
    -- AND  
	-- t.trip_distance <= '1';  
    -- t.trip_distance > '1' AND t.trip_distance <= '3';  
    -- t.trip_distance > '3' AND t.trip_distance <= '7';  
    -- t.trip_distance > '7' AND t.trip_distance <= '10';  
    -- t.trip_distance > '10';  
  
  
Bonus:  
Count nb of trips per day during October 2019 :  
    SELECT   
        CAST(lpep_dropoff_datetime AS DATE) AS dropoff_day,  
        COUNT(1) as "count"  
    FROM   
        green_taxi_trips t  
    WHERE   
        CAST(lpep_dropoff_datetime AS DATE) >= '2019-10-01' AND   
        CAST(lpep_dropoff_datetime AS DATE) < '2019-11-01'  
    GROUP BY  
        dropoff_day;  
  
# Question 4  
What was the pick up day with the longest trip distance ?   
Answer: 2019-10-31 with 525.89 miles  
  
SQL command:  
    SELECT   
        CAST(lpep_pickup_datetime AS DATE) AS pickup_day,  
        MAX(t.trip_distance)  
    FROM   
        green_taxi_trips t  
    GROUP BY  
        pickup_day,  
        t.trip_distance  
    ORDER BY   
        t.trip_distance DESC;  
  
# Question 5  
Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?  
Answer: East Harlem North, East Harlem South, Morningside Heights  
  
SQL Command:  
    SELECT   
        zpu."Zone",  
        SUM(total_amount),  
        COUNT(1) as "count"  
    FROM   
        green_taxi_trips t JOIN zones zpu  
            ON t."PULocationID" = zpu."LocationID"  
    WHERE   
        CAST(lpep_pickup_datetime AS DATE) = '2019-10-18'   
    GROUP BY  
        zpu."Zone"  
    ORDER BY  
        SUM(total_amount) DESC;  
  
# Question 6  
For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?  
Answer: JFK Airport  
  
SQL Command:  
    SELECT   
        zdo."Zone",  
        MAX(tip_amount) as "max_tip_amount"  
    FROM   
        green_taxi_trips t JOIN zones zpu  
            ON t."PULocationID" = zpu."LocationID"   
        JOIN zones zdo   
            ON t."DOLocationID" = zdo."LocationID"   
    WHERE   
        CAST(lpep_pickup_datetime AS DATE) >= '2019-10-01' AND   
        CAST(lpep_pickup_datetime AS DATE) < '2019-11-01' AND   
        zpu."Zone" = 'East Harlem North'  
    GROUP BY   
        zdo."Zone"  
    ORDER BY   
        MAX(tip_amount) DESC;  