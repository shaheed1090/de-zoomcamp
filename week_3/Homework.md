
**Week 3 Homework**

**Question 1: What is the count for fhv vehicle records for year 2019?**

Answer : 43,244,696

![image](https://user-images.githubusercontent.com/25481135/218498771-df0db86e-2ee9-45e4-b1f1-82ce257cd6e5.png)


**Question 2:**

Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

Answer : 0 MB for the External Table and 317.94MB for the BQ Table

![image](https://user-images.githubusercontent.com/25481135/218501531-f37f7a37-eedd-4546-ba5a-ecdeebc5556c.png)

![image](https://user-images.githubusercontent.com/25481135/218502089-387ce472-78ba-4f82-a38e-11e561436916.png)

**Question 3:**

How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

Answer : 717,748

![image](https://user-images.githubusercontent.com/25481135/218505661-86459f96-d1d4-4edc-a044-be12b41d58e2.png)

**Question 4:**

What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

Answer : Partition by pickup_datetime Cluster on affiliated_base_number

![image](https://user-images.githubusercontent.com/25481135/218538051-6e216fbb-119c-43e5-b00a-edebde160323.png)


**Question 5:**

Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

Answer : 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

Non-partitioned,
![image](https://user-images.githubusercontent.com/25481135/218530325-539094fa-8abf-41f9-be52-d3963bd87573.png)

Partitioned,
![image](https://user-images.githubusercontent.com/25481135/218538954-0c19c962-96f8-4f24-9b86-9ccbd1fad6df.png)



**Question 6:**

Where is the data stored in the External Table you created?

Answer : GCP Bucket


**Question 7:**

It is best practice in Big Query to always cluster your data:

Answer : False
