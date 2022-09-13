from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import json
from datetime import datetime

def _store_user(ti):
    data = ti.xcom_pull(task_ids=['extract_bpi'])[0]
    l = []
    for i in data["bpi"]:
        l.append(i)
    update_time = datetime.strptime(data['time']['updatedISO'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    data = data['bpi']
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    for i in l:
        sql_stmt = f"Insert into bpi(code, rate, updatedD) values('{data[i]['code']}', {data[i]['rate_float']}, '{update_time}');"
        print(sql_stmt)
        cursor.execute(sql_stmt)
        pg_conn.commit()
    return None


with DAG(
    'currentprice_dag',
    start_date = datetime(2022, 1, 1),
    schedule_interval = '* * * * *',
    tags = ["mine"],
    catchup = False
)as dag:

    create_table = PostgresOperator(
        postgres_conn_id = 'postgres',
        task_id = 'create_table',
        sql = '''
            CREATE TABLE IF NOT EXISTS bpi
            (
                id serial4 NOT NULL,
                code TEXT NOT NULL,
                rate FLOAT NOT NULL,
                updatedD timestamp NOT NULL default CURRENT_TIMESTAMP
            );'''
    )

    is_api_available = HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'bpi_api',
        endpoint = '/v1/bpi/currentprice.json'
    )

    extract_user = SimpleHttpOperator(
        task_id = 'extract_bpi',
        http_conn_id = 'bpi_api',
        endpoint = '/v1/bpi/currentprice.json',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response = True
    )

    store_user = PythonOperator(
        task_id = 'store_user',
        python_callable = _store_user
    )

    create_table >> is_api_available >> extract_user >> store_user