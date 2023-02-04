from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str,year: int,month: int) -> Path:
    """Download trip data from gcs"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path,local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

@task(log_prints=True)
def write_bq(df : pd.DataFrame) -> None:
    """Write dataframe in to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table= "dezoomcamp.rides",
        project_id="ny-rides-shaheed",
        credentials= gcp_credentials_block.get_credentials_from_service_account(),
        chunksize= 500_000,
        if_exists= "append"
    )


@flow(log_prints= True)
def etl_main_flow(months: list[int], year: int,color: str):
    """Main ETL Flow to load data in to Big Query"""
    processed_records = 0
    df_to_BQ = pd.DataFrame()
    for month in months:
        path = extract_from_gcs(color,year,month)
        df = pd.read_parquet(path)
        print(f"No.of records extracted for {month},{year}:{len(df.index)}")
        processed_records += len(df.index)
        df_to_BQ = pd.concat([df_to_BQ,df])

    write_bq(df_to_BQ)
    print(f"Total no.of records extracted from GCS and moved to BQ : {processed_records}")  
    flow.visualize()

    
if __name__ =="__main__":
    color= "yellow"
    months= [2,3]
    year= "2019"
    path=etl_main_flow(months, year,color)   
