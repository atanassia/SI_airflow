from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import json


def _get_data(execution_date=None, ti=None):
    hook = PostgresHook(postgres_conn_id='postgres2')
    pg_conn = hook.get_conn()
    cursor = pg_conn.cursor()
    sql_stmt = """
        SELECT show_name, count(show_name) FROM tvshows.data 
        WHERE EXTRACT(MONTH FROM airdate) = %s and EXTRACT(YEAR FROM airdate) = %s group by show_name;"""
    cursor.execute(sql_stmt, (execution_date.month, execution_date.year))
    pg_conn.commit()
    result = [(str(i[0]), i[1]) for i in cursor.fetchall()]
    print(result)
    ti.xcom_push(key='result', value=result)


def _store_data(execution_date=None, ti=None):
    data = ti.xcom_pull(key='result')
    print(type(data))
    hook = PostgresHook(postgres_conn_id='postgres2')
    pg_conn = hook.get_conn()
    cursor = pg_conn.cursor()
    
    for item in data:
        if item:
            sql_stmt = """
                Insert into tvshows.count_shows(
                    show, countd, show_date) values(%s, %s, %s);
                """
            cursor.execute(sql_stmt,
                (item[0], item[1], execution_date)
            )
            pg_conn.commit()
    return None




with DAG(
    'tvseries_count_dag',
    start_date = datetime(2022, 1, 1),
    schedule_interval='@monthly',
    tags=["tv"],
    catchup = True
)as dag:

    get_data = PythonOperator(
        task_id = 'get_data',
        python_callable = _get_data
    )

    store_data = PythonOperator(
        task_id = 'store_data',
        python_callable = _store_data
    )

    get_data >> store_data