from datetime import datetime, timedelta
from collections import defaultdict

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from google.cloud import storage

import os
import requests
import json
import ast
import pandas as pd
import sqlalchemy as sa

default_args = {
    'owner': 'Yang Zhang',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': True
}

gcp_conn_id = 'gcs_default'
gcp_bucket = 'pubmed-poc-data'
private_key_path = '/opt/airflow/key_file/pubmed-etl-b62d69ea807b.json'

def init(ds, ti):
    print(f'starting workflow with execution date {ds}')

    local_raw_data = f'/opt/airflow/data/raw_data/{ds}.json'
    local_raw_data_download = f'/opt/airflow/data/raw_data_download/{ds}.json'
    gcp_raw_data = f'raw-data/{ds}.json'

    local_transformed_data = f'/opt/airflow/data/transformed_data/{ds}.csv'
    local_transformed_data_download = f'/opt/airflow/data/transformed_data_download/{ds}.csv'
    gcp_transformed_data = f'transformed-data/{ds}.csv'

    ti.xcom_push(key='local_raw_data', value=local_raw_data)
    ti.xcom_push(key='local_raw_data_download', value=local_raw_data_download)
    ti.xcom_push(key='gcp_raw_data', value=gcp_raw_data)

    ti.xcom_push(key='local_transformed_data', value=local_transformed_data)
    ti.xcom_push(key='local_transformed_data_download', value=local_transformed_data_download)
    ti.xcom_push(key='gcp_transformed_data', value=gcp_transformed_data)

    data_dir_path = '/opt/airflow/data'
    sub_dirs = ['raw_data', 'raw_data_download', 'transformed_data', 'transformed_data_download']
    for sub_dir in sub_dirs:
        data_dir = os.path.join(data_dir_path, sub_dir)
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)


def get_data(ds, ti, query_param):
    db = query_param['db']
    term = query_param['term']
    datetype = query_param['datetype']

    # get the id list
    ds_datetime = datetime.strptime(ds, '%Y-%m-%d')
    mindate = datetime.strftime(ds_datetime - timedelta(2), '%Y/%m/%d')
    maxdate = datetime.strftime(ds_datetime - timedelta(1), '%Y/%m/%d')

    url_base = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
    get_id_list_query = (f'esearch.fcgi?db={db}&term={term}&mindate={mindate}&maxdate={maxdate}'
                         f'&datetype={datetype}&retmax=10000&retmode=json')
    get_id_list_url = url_base + get_id_list_query
    try:
        get_id_list_response = requests.get(get_id_list_url)
        get_id_list_response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise AirflowException(f'HTTP error when getting id list: {e.response.text}')

    response = json.loads(get_id_list_response.text)
    count = response['esearchresult']['count']
    print(f'execution date: {ds}, record count: {count}')
    id_list = response['esearchresult']['idlist']

    # get summary
    chunk_size = 100
    id_chunks = [id_list[i : i + chunk_size] for i in range(0, len(id_list), chunk_size)]

    result = []

    for id_chunk in id_chunks:     
        id = ','.join(id_chunk)
        get_summary_query = (f'esummary.fcgi?db={db}&id={id}&retmax=10000&retmode=json')
        get_summary_url = url_base + get_summary_query

        try:
            get_summary_response = requests.post(get_summary_url)
            get_summary_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise AirflowException(f'HTTP error when getting summary: {e.response.text}')

        result.append(get_summary_response.text)
    
    local_raw_data = ti.xcom_pull(key='local_raw_data', task_ids='init')
    # write raw data locally
    with open(local_raw_data, 'w') as f:
        json.dump(result , f)

def transform_data(ti):
    gcp_raw_data = ti.xcom_pull(key='gcp_raw_data', task_ids='init')
    local_raw_data_download = ti.xcom_pull(key='local_raw_data_download', task_ids='init')
    local_transformed_data = ti.xcom_pull(key='local_transformed_data', task_ids='init')

    # download from GCS bucket
    client = storage.Client.from_service_account_json(json_credentials_path=private_key_path)
    bucket = storage.Bucket(client, gcp_bucket)
    blob = bucket.blob(gcp_raw_data)
    blob.download_to_filename(local_raw_data_download)

    # load json and transform
    with open(local_raw_data_download, 'r') as f:
        raw_data = json.load(f)

    summaries = [summary for resp in raw_data for uid, summary in ast.literal_eval(resp)['result'].items() if uid != 'uids']

    df = pd.DataFrame(summaries)

    # drop duplicates based on uid, assuming it's the unique id
    df = df.drop_duplicates(subset='uid', keep='last')

    df.pubdate = pd.to_datetime(df.pubdate, errors='coerce')
    df.epubdate = pd.to_datetime(df.epubdate, errors='coerce')
    df.sortpubdate = pd.to_datetime(df.sortpubdate, errors='coerce')
    df.srcdate = pd.to_datetime(df.srcdate, errors='ignore')
    df.docdate = pd.to_datetime(df.docdate, errors='ignore')

    # remove entry without valid pub and epub date
    df.dropna(subset=['pubdate', 'epubdate', 'sortpubdate'], inplace=True)

    df.to_csv(local_transformed_data, index=False)

def load_data_staging(ti):
    gcp_transformed_data = ti.xcom_pull(key='gcp_transformed_data', task_ids='init')
    local_transformed_data_download = ti.xcom_pull(key='local_transformed_data_download', task_ids='init')

    # download csv from gcs
    client = storage.Client.from_service_account_json(json_credentials_path=private_key_path)
    bucket = storage.Bucket(client, gcp_bucket)
    blob = bucket.blob(gcp_transformed_data)
    blob.download_to_filename(local_transformed_data_download)

    # load csv file to df
    types = defaultdict(str, 
                        uid='int', 
                        nlmuniqueid='str', 
                        pubstatus='str',
                        pmcrefcount='str',
                        booktitle='str',
                        medium='str',
                        edition='str',
                        publisherlocation='str',
                        publishernam='str',
                        reportnumber='str',
                        availablefromurl='str',
                        locationlabel='str',
                        bookname='str',
                        chapter='str'
                        )
    date_col = ['pubdate', 'epubdate', 'sortpubdate', 'srcdate', 'docdate']

    df = pd.read_csv(local_transformed_data_download, dtype=types, parse_dates=date_col)

    local_transformed_data_download_count = len(df)

    print(f'csv {local_transformed_data_download} has {local_transformed_data_download_count} rows')

    # load df to mssql
    mssql_hook = MsSqlHook()
    engine = sa.create_engine(mssql_hook.get_uri())

    with engine.begin() as mssql_conn:
        mssql_conn.exec_driver_sql("TRUNCATE TABLE staging")
        df.to_sql('staging', con=mssql_conn, if_exists='append', index=False)


def load_data_summary():
    mssql_hook = MsSqlHook()
    engine = sa.create_engine(mssql_hook.get_uri())

    with engine.begin() as mssql_conn:
        with open('/opt/airflow/sql/merge.sql', 'r') as merge_query:
            mssql_conn.exec_driver_sql(merge_query.read())
        
            summary_count_results = mssql_conn.execute('SELECT COUNT(1) FROM summary')
            summary_count = summary_count_results.first()[0]
            print(f'before merging, summary has {summary_count} rows')

            summary_count_results = mssql_conn.execute('SELECT COUNT(1) FROM summary')
            summary_count = summary_count_results.first()[0]
            print(f'after merging, summary has {summary_count} rows')


with DAG(
    default_args=default_args,
    dag_id='pubmed_etl',
    description='get esummary data of a keyword from pubmed database',
    start_date=datetime(2023, 3, 17),
    schedule_interval='@daily'
) as dag:
    init = PythonOperator(
        task_id='init',
        python_callable=init,
        provide_context=True
    )

    get_data = PythonOperator(
        task_id='get_data',
        python_callable=get_data,
        op_kwargs={'query_param': {'db': 'pubmed', 'term': 'covid', 'datetype': 'mdat'}},
        provide_context=True
    )

    upload_raw_file = LocalFilesystemToGCSOperator(
        task_id="upload_raw_file",
        src='/opt/airflow/data/raw_data/{{ds}}.json',
        dst='raw-data/{{ds}}.json',
        bucket=gcp_bucket,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    upload_transformed_csv = LocalFilesystemToGCSOperator(
        task_id="upload_transformed_csv",
        src='/opt/airflow/data/transformed_data/{{ds}}.csv',
        dst='transformed-data/{{ds}}.csv',
        bucket=gcp_bucket,
    )

    load_data_staging = PythonOperator(
        task_id='load_data_staging',
        python_callable=load_data_staging
    )

    column_check = SQLColumnCheckOperator(
        task_id="column_check",
        conn_id='mssql_default',
        table='staging',
        column_mapping={
            "uid": {
                "null_check": {
                    "equal_to": 0,
                    "tolerance": 0,
                },
                "unique_check": {
                    "equal_to": 0,
                },
            },
            "pubdate": {
                "null_check": {
                    "equal_to": 0,
                    "tolerance": 0,
                },
            },
            "epubdate": {
                "null_check": {
                    "equal_to": 0,
                    "tolerance": 0,
                },
            },
            "sortpubdate": {
                "null_check": {
                    "equal_to": 0,
                    "tolerance": 0,
                },
            }
        },
    )

    load_data_summary = PythonOperator(
        task_id='load_data_summary',
        python_callable=load_data_summary
    )

    init >> get_data >> upload_raw_file >> transform_data >> upload_transformed_csv >> load_data_staging >> column_check >> load_data_summary
    # init