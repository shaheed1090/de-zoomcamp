
**de-zoomcamp homeworks Week 2**

**Question 1. Load January 2020 data**

Using the etl_web_to_gcs.py flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.How many rows does that dataset have?

Answer: 447,770

![Screenshot 2023-02-03 223629](https://user-images.githubusercontent.com/25481135/216664073-77db1f03-9131-48e1-8a3d-28e84583c8d4.png)

**Question 2. Scheduling with Cron**

Cron is a common scheduling specification for workflows.
Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

Answer: 0 5 1 * *

![image](https://user-images.githubusercontent.com/25481135/216670362-18d8f5fc-0bf3-4c4e-92cb-ee5153f2522f.png)

![image](https://user-images.githubusercontent.com/25481135/216670059-57ec54c4-df9c-4549-afd3-bd84a15db545.png)

**Question 3. Loading data to BigQuery**
Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).
Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

Answer : 14,851,920
