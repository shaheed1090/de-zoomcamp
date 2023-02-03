from pathlib import Path
import pandas as pd
from prefect import flow,task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint


@task(log_prints=True,retries =3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """ Read taxi data from web to pandas dataframe"""
    # if randint(0,1)> 0:
    #     raise Exception

    df = pd.read_csv(dataset_url)
    print(f"No.records in green taxi data: {len(df.index)}")
    return df


@task(log_prints=True)
def clean(df = pd.DataFrame) -> pd.DataFrame:
    """Fix some dtype issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    #print(df.head(2))
    #print(f"columns: {df.dtypes}")
    #print(f"rows: {len(df)}")
    return df

@task(log_prints =True)
def write_local(df : pd.DataFrame,color : str, dataset_file: str) -> Path:
    """Write dataframe out as a parquet file"""
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path,compression="gzip")
    return path

@task(log_prints =True)
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs")
    path = Path(path).as_posix()
    gcs_block.upload_from_path(from_path= f"{path}",to_path=path)
    return



@flow()
def etl_web_to_gcs() -> None:
    """ The main ETL function"""
    color = "green"
    year = 2020
    month =1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url =  f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df,color,dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()

         