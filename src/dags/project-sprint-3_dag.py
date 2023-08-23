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


url = 'd5dg1j9kt695d30blp03.apigw.yandexcloud.net'
api_key = '5f55e6c0-e9e5-4a9c-b313-63c01fc31460'
nickname = 'Irina-Sterligov'
cohort = '16' 

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}
#set api connection from basehook 
api_conn = BaseHook.get_connection('create_files_api')
api_endpoint = api_conn.host 
api_token = api_conn.password

#set postgresql connectionfrom basehook
psql_conn = BaseHook.get_connection('pg_connection')

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
    #df = df.drop_duplicates(subset='id')
    #df = df.drop('id', axis=1)
 
    if 'status' not in df:
        df['status'] = 'shipped' 
    

    #psql_conn = BaseHook.get_connection('pg_connection')

    #conn = psycopg2.connect(f"dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'")
    psql_conn = 'pg_connection'
    postgres_hook = PostgresHook(psql_conn)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)

def update_mart_d_tables(ti):
 
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    #d_calendar
    cur.execute("""
truncate table mart.d_calendar cascade;
truncate table mart.d_customer cascade;
truncate table mart.d_item cascade;

CREATE SEQUENCE d_calendar_date_id start 1; 
insert into mart.d_calendar (date_id, fact_date, day_num, month_num, month_name, year_num)
with all_dates as (
select date_id as date_time
from stage.customer_research cr 
UNION
select date_time
from stage.user_activity_log ual
UNION
select date_time
from stage.user_order_log uol
)
select 
  nextval('d_calendar_date_id') as date_id
, date_time as fact_date
, extract(DAY from date_time)::INTEGER as day_num
, extract(MONTH from date_time)::INTEGER as month_num
, TO_CHAR(date_time, 'Month') as month_name
, extract(YEAR from date_time)::INTEGER as year_num
from all_dates
order by date_time ASC;
DROP SEQUENCE d_calendar_date_id;
    """)
    conn.commit()


    #d_customer
    cur.execute("""

insert into mart.d_customer (customer_id, city_id, first_name, last_name)
select
  customer_id
, city_id
, first_name
, last_name
from (
select distinct
  customer_id
, city_id
, first_name
, last_name
, row_number() over(partition by customer_id order by city_id DESC) as rn
from stage.user_order_log uol) as u
where rn = 1;
    """)
    conn.commit()


    #d_item
    cur.execute("""

insert into mart.d_item (item_id, item_name)
select DISTINCT
  item_id
, item_name
from stage.user_order_log;
    """)
    conn.commit()

    cur.close()
    conn.close()

    return 200        

def update_mart_f_sales(ds, ti):
 
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
 
    f_sales = """
 
        DELETE FROM mart.f_sales;
 
        insert into mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
select dc.date_id, item_id, customer_id, city_id, quantity, 
case
    when status = 'shipped' then payment_amount
    else payment_amount * (-1)
end as payment_amount,
status
from staging.user_order_log uol
left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
where uol.date_time::Date = '{{ds}}';
 
    """ 
    cur.execute(f_sales)
    conn.commit()


def update_mart_f_customer_retention():
 
    conn = psycopg2.connect(f"dbname='de' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    f_customer_retention= """
 
    DELETE FROM mart.f_customer_retention;
insert into mart.f_customer_retention (new_customers_count, returning_customers_count,
    refunded_customer_count, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
--  tables for calculations
WITH CTE AS
  (SELECT fs.item_id,
          fs.customer_id,
          fs.payment_amount,
          fs.status,
          dc.week_of_year AS period_id
   FROM mart.f_sales fs
   LEFT JOIN mart.d_calendar dc ON fs.date_id = dc.date_id),

CTE1 AS
  (SELECT period_id,
          customer_id,
          item_id,
          count(customer_id) AS num_orders,
          SUM (payment_amount) AS sum_payment_amount
   FROM CTE
   GROUP BY period_id,
            customer_id,
            item_id
   ORDER BY period_id,
            customer_id,
            item_id),
-- calculation by orders 

CTE2 AS (           
SELECT period_id,
       item_id,
       COUNT (CASE WHEN num_orders = 1 THEN num_orders END) AS new_customers_count,
             SUM (CASE WHEN num_orders = 1 THEN sum_payment_amount END) AS new_customers_revenue,
                 COUNT (CASE WHEN num_orders > 1 THEN num_orders END) AS returning_customers_count,
                       SUM (CASE WHEN num_orders > 1 THEN sum_payment_amount END) AS returning_customers_revenue
FROM CTE1
GROUP BY period_id,
         item_id
ORDER BY period_id,
         item_id),
-- calculate: number of customers who issued a refund and return revenue
CTE3 AS
(SELECT period_id,
          item_id,
          COUNT (DISTINCT CASE
                              WHEN status = 'refunded' THEN customer_id END) AS refunded_customer_count,
                COUNT (CASE WHEN status = 'refunded' THEN customer_id END) AS customers_refunded
   FROM CTE
   GROUP BY period_id,
            item_id
   ORDER BY period_id,
            item_id),            

CTE4 AS
(SELECT DISTINCT period_id,
                   item_id
   FROM CTE
   ORDER BY period_id,
            item_id)
-- result            
SELECT CTE2.new_customers_count,
       CTE2.returning_customers_count,
       CTE3.refunded_customer_count,
       CTE4.period_id,
       CTE4.item_id,
       CTE2.new_customers_revenue,
       CTE2.returning_customers_revenue,
       CTE3.customers_refunded
FROM CTE4
LEFT JOIN CTE2 ON (CTE4.period_id = CTE2.period_id AND CTE4.item_id = CTE2.item_id)
LEFT JOIN CTE3 ON (CTE4.period_id = CTE3.period_id AND CTE4.item_id = CTE3.item_id)
order by CTE4.period_id, CTE4.item_id;
    """
    cur.execute(f_customer_retention)
    conn.commit()
 
    cur.close()
    conn.close()




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

update_mart_d_tables = PythonOperator(task_id='update_mart_d_tables',
                                        python_callable=update_mart_d_tables,
                                        op_kwargs={ "headers" : headers},
                                        dag=dag
)

update_mart_f_sales = PythonOperator(
    task_id='update_mart_f_sales',
    python_callable=update_mart_f_sales,
    op_kwargs={ "headers": headers},
    dag=dag
)

update_f_customer_retention = PythonOperator(
    task_id='update_f_customer_retention',
    python_callable=update_f_customer_retention,
    op_kwargs={ "headers": headers},
    dag=dag
)


generate_report >> get_report >> get_increment >> upload_data >> update_mart_d_tables >> update_mart_f_sales >> update_f_customer_retention