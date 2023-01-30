# de-zoomcamp
de-zoomcamp homeworks
Week 1
------------
Docker SQL Homeworks
---------------------
**Question 1. Knowing docker tags**
 Run the command to get help on the "docker build" command,Which tag has the following text? - Write the image ID to the file
 
 Answer: --iidfile string

![image](https://user-images.githubusercontent.com/25481135/215389329-f9be9349-96dd-41c2-89e9-691afeb8d150.png)





**Question 2. Understanding docker first run**
Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?

Answer: 3



**Question 3 : How many taxi trips were totally made on January 15?**

Answer: 20530
select count(*) from green_taxi_data gtd
where cast(gtd.lpep_pickup_datetime as date) = '2019-01-15'
and cast(gtd.lpep_dropoff_datetime as date) = '2019-01-15'

![image](https://user-images.githubusercontent.com/25481135/215334936-f54a27d4-f63c-45e9-bb9d-cb90b646fd4f.png)


**Question 4. Largest trip for each day**

Answer: 2019-01-15
select 
distinct(cast(gtd."lpep_pickup_datetime" as date)) as trip_date,
MAX(trip_distance) as trip_distance
from green_taxi_data gtd 
group by (cast(gtd."lpep_pickup_datetime" as date))
order by trip_distance desc
limit 1

![image](https://user-images.githubusercontent.com/25481135/215336529-819e1c59-c636-4b19-a2cc-110aa47adb40.png)

**Question 5. The number of passengers**
In 2019-01-01 how many trips had 2 and 3 passengers?

Answer: 2: 1282 ; 3: 254

select 
passenger_count,
count(*)
from green_taxi_data gtd
where cast(gtd."lpep_pickup_datetime" as date) = '2019-01-01' 
and passenger_count in(2,3)
group by passenger_count

![image](https://user-images.githubusercontent.com/25481135/215338143-d20d474a-9e17-430d-a33e-8de0d550113e.png)

**Question 6. Largest tip**
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.

Answer : Long Island City/Queens Plaza

WITH AP AS
	(SELECT GTD."PULocationID",
			TZL."Zone",
			GTD."DOLocationID",
			GTD.TIP_AMOUNT TIP_AMOUNT
		FROM GREEN_TAXI_DATA GTD
		JOIN TAXI_ZONE_LOOKUP TZL ON GTD."PULocationID" = TZL."LocationID"
		WHERE TZL."Zone" = 'Astoria'
		ORDER BY GTD."tip_amount" DESC
		LIMIT 1) -- passenger details from Astroia 
SELECT AP."PULocationID",
	AP."Zone" PU_ZONE,
	AP."DOLocationID",
	TZL1."Zone" DO_ZONE,
	AP.TIP_AMOUNT
FROM AP
JOIN TAXI_ZONE_LOOKUP TZL1 ON AP."DOLocationID" = TZL1."LocationID"

![image](https://user-images.githubusercontent.com/25481135/215345036-9a62965e-3974-4328-9273-6357d98eee08.png)


