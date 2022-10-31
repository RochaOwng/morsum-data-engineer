from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
import pandas as pd 
from sqlalchemy import create_engine
import dotenv 

dotenv.load_dotenv(dotenv.find_dotenv())


target_db = of.getenv('db')

def load_mysql_product_prices():
    conn_gcs = GoogleCloudStorageHook(gcp_conn_id='GCSConn').get_conn() #this param is set in the Airflow config
    conn_mysql = MySqlHook(mysql_conn_id='MySqlConn') #this param is set in the Airflow config

    data = conn_gcs.download(bucket_name='product/prices', object_name='2022-07-01.csv')
    conn_mysql.bulk_load('product_prices', data)
    

with DAG(
    config.dag_display_name,
    descriptions="Example DAG",
    schedule_interval="30 1 * * *",
    max_active_runs=1,
    catchup=False
) as dag:

    delete_bucket_poduct_prices = GCSDeleteBucketOperator(
        task_id="delete_bucket_1", 
        bucket_name=BUCKET_1,
        dag=dag
        )

    copy_product_prices = GCSToGCSOperator(
        task_id="copy_gcs_product_prices",
        source_bucket="vendor",
        source_objects=["product/prices"],
        destination_bucket="product",
        destination_object="prices/",
        dag=dag
    )

    load_mysql = PythonOperator(
        task_id='load_mysql',
        python_callable=load_mysql_product_prices,
        dag=dag
    )

    delete_bucket_poduct_prices >> copy_product_prices >> load_mysql
