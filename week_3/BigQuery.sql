
-- counting distinc cout for affiliated_base_number from fhv_tripdata_2019 table

SELECT DISTINCT(COUNT(Affiliated_base_number)) FROM `ny-rides-shaheed.dezoomcamp.fhv_tripdata_2019`



-- counting distinc cout for affiliated_base_number from external_fhv_tripdata_2019 table

SELECT DISTINCT(COUNT(Affiliated_base_number)) FROM `ny-rides-shaheed.dezoomcamp.external_fhv_tripdata_2019` 

-- How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

SELECT COUNT(*) FROM `ny-rides-shaheed.dezoomcamp.fhv_tripdata_2019` 
WHERE (PUlocationID IS NULL) 
AND (DOlocationID IS NULL)

-- counting distinct count for affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive)

SELECT DISTINCT(Affiliated_base_number) FROM `ny-rides-shaheed.dezoomcamp.fhv_tripdata_2019`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'


-- Creating a partition and cluster table, to implement the optimized solution

CREATE OR REPLACE TABLE ny-rides-shaheed.dezoomcamp.fhv_tripdata_2019_partitoned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `ny-rides-shaheed.dezoomcamp.external_fhv_tripdata_2019`

-- counting distinct for affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive) from partitioned and clustered table


SELECT DISTINCT(Affiliated_base_number) FROM `ny-rides-shaheed.dezoomcamp.fhv_tripdata_2019_partitoned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31'
