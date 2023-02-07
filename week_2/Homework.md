
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

code flow execution:
![image](https://user-images.githubusercontent.com/25481135/216828290-558ef682-4c26-44e6-b6aa-27054fe3f9b3.png)

Deployment:
![image](https://user-images.githubusercontent.com/25481135/216828672-c62a9f84-a86a-4fad-b7b4-ab7e53ba79c5.png)

*Question 4. Github Storage Block*

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.
How many rows were processed by the script?

Answer : 88,605

![image](https://user-images.githubusercontent.com/25481135/217321313-21c9134e-3dcc-4235-a0d5-e570cb6c70a0.png)

Prefect - Flow Runs

![image](https://user-images.githubusercontent.com/25481135/217357275-fde86fa1-0a51-444f-8794-6acdb93dbd33.png)

deployment code kept under - week_2/github_flow 





