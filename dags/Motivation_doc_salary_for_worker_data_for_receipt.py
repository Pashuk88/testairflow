from clickhouse_driver import connection
from airflow import DAG
from airflow.configuration import get
from airflow.utils.dates import days_ago
import pandas as pd
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python import PythonOperator, task
from functions import execute_comm, default_args, crud #, ch_query, get_two_dates
from datetime import datetime

def drop_tab():
	sql = f''''''
	#ch_query(connection).execute(sql)

def create_tab():
	sql = f''''''
	#ch_query(connection).execute(sql)

def insert_data():
    query = """
        SELECT 2
    """

    result = execute_comm(query, 'dwh_test')
    
    new_query = """
        INSERT INTO DWH.test.super_test3
            (id, region_sudna, sudno, tip_sudna, predpriyatie, [data], vid_ryibolovstva, nomer_razresheniya, region_kvotoderzhatelya, kvotoderzhatel_, rajon_promyisla, ob_ekt_promyisla, vyilov__tonn_, ostatok_na_bortu__tonn_, naimenovanie_i_ob_em_produktsii__tonn_, insert_date)
            VALUES(6392, 'danetvnature', '', '', '', '', '', '', '', '', '', '', 0, '', '', '');
    """
    
    
    crud(new_query, 'dwh_test')
    # for j in range(1):
    #     ch_query(connection).execute("""INSERT INTO dns_retail.Motivation_doc_salary_for_worker_data_for_receipt VALUES""", result.to_dict('records'))
    #ch_query(connection).execute('''INSERT INTO {name_table_to_insert} VALUES ''', result.to_dict('records'))


default_args = default_args()

with DAG('LimanovNew',
    start_date=datetime(2022, 2, 6),
    default_args=default_args,
    description='Тест',
    schedule_interval='@monthly',
    tags=['ssd']
) as dag:
    t1 = PythonOperator(
        task_id='drop_data',
        python_callable=drop_tab
        )

    t2 = PythonOperator(
        task_id='create_tab',
        python_callable=create_tab        
        )
	
    t3 = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data
        )	
   
t1 >> t2 >> t3