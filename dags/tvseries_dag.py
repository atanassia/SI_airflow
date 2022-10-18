from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime
import json



def _store_data(ti):
    data = ti.xcom_pull(task_ids="extract_data")
    hook = PostgresHook(postgres_conn_id='postgres2')
    pg_conn = hook.get_conn()
    cursor = pg_conn.cursor()

    for item in data:
        sql_stmt = """
            Insert into tvshows.data(
                show_name, show_url, show_series_name, season, airdate) 
                values(%s, %s, %s, %s, %s);"""
        cursor.execute(sql_stmt, 
            (item['show']['name'], item['show']['officialSite'],
            item['name'], item['season'], item['airdate'])
        )
        pg_conn.commit()
    return None


with DAG(
    'tvseries_dag',
    start_date = datetime(2022, 1, 1),
    schedule_interval='@daily',
    tags=["tv"],
    catchup = True
)as dag:

    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'tv_api',
        endpoint = 'schedule?country=RU&date={{ execution_date.strftime("%Y-%m-%d") }}'
    )

    extract_data = SimpleHttpOperator(
        task_id = 'extract_data',
        http_conn_id = 'tv_api',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True,
        trigger_rule='all_success',
        endpoint = 'schedule?country=RU&date={{ execution_date.strftime("%Y-%m-%d") }}'
    )

    store_data = PythonOperator(
        task_id = 'store_data',
        trigger_rule='all_success',
        python_callable = _store_data
    )

    is_api_available >> extract_data >> store_data
    

