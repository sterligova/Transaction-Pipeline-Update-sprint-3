import datetime
import time
import psycopg2

import requests
import json
import pandas as pd
import numpy as np
import os
import logging
import io


from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom
from datetime import datetime, timedelta
from airflow.hooks.http_hook import HttpHook

#set api connection from basehook 
api_conn = BaseHook.get_connection('create_files_api')
url = api_conn.host 
api_key = api_conn.password


nickname = 'Irina-Sterligov'
cohort = '16' 

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


#set postgresql connectionfrom basehook
conn = BaseHook.get_connection('pg_connection')

##init test connection
conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
cur = conn.cursor()
cur.close()
conn.close()




def generate_report(ti):
    response = requests.post(f"https://{url}/generate_report", headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')

def get_report(ti):
    print('Request to get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(10):
        response = requests.get(f"https://{url}/get_report?task_id={task_id}", headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')

    return report_id

def get_increment(date, ti):
    report_id = ti.xcom_pull(key='report_id')
    print(f'Report_id={report_id}')
    response = requests.get(
        f"https://{url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00",
        headers=headers)
    response.raise_for_status()
    logging.info(f'Response is {response.content}')


    increment_id = json.loads(response.content)['data']['increment_id']
    if not increment_id:
        raise ValueError(f'Increment is empty. Most probably due to error in API call.')
    

    logging.info(f'increment_id={increment_id}')
    return increment_id


def upload_data_to_staging(filename, date, pg_table, pg_schema, ti):
    increment_id = ti.xcom_pull(key='increment_id')
    s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
 
    local_filename = date.replace('-', '') + '_' + filename
 
    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)
 
    df = pd.read_csv(local_filename)

 
    if 'status' not in df:
        df['status'] = 'shipped' 
    

    #conn = BaseHook.get_connection('pg_connection')
    #conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")

    psql_conn = 'pg_connection'
    postgres_hook = PostgresHook(psql_conn)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)


dag = DAG(
    'project-sprint-3_dag',
    #schedule_interval='0 0 * * *',
    catchup=False,
    start_date=datetime.today() - timedelta(days=7),
    end_date=datetime.today() + timedelta(days=1),
)
 
business_dt = '{{ ds }}'


generate_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        op_kwargs={ "headers" : headers},
        dag=dag)

get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report,
        op_kwargs={ "headers" : headers},
        dag=dag)

get_increment = PythonOperator(
    task_id='get_increment',
    python_callable=get_increment,
    op_kwargs={ 'date': business_dt},
    dag=dag
)

upload_data = PythonOperator(
        task_id='upload_data',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging'},
        dag=dag
)


update_mart_d_tables = PostgresOperator(
        task_id='update_mart_d_tables',
        psql_conn=psql_conn,
        sql="migrations/mart_d_tables.sql",
        dag=dag
        )

update_mart_f_sales = PostgresOperator(
        task_id='update_mart_f_sales',
        psql_conn=psql_conn,
        sql="migrations/mart.f_sales.sql",
        parameters={"date": {business_dt}},
        dag=dag
        )

update_f_customer_retention = PostgresOperator(
        task_id='update_f_customer_retention',
        psql_conn=psql_conn,
        sql="migrations/f_customer_retention.sql",
        dag=dag
        )






generate_report >> get_report >> get_increment >> upload_data >> update_mart_d_tables >> update_mart_f_sales >> update_f_customer_retention