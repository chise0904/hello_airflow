from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id='db_interaction_tutor',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    # 任務 1：建立資料表 (使用 SQL Operator)
    create_table = SQLExecuteQueryOperator(
        task_id='create_table_task',
        conn_id='my_postgres_conn', # 使用我們剛才在 UI 設定的 ID
        sql="""
            CREATE TABLE IF NOT EXISTS employee_growth (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                join_date DATE
            );
        """
    )

    # 任務 2：插入資料
    insert_data = SQLExecuteQueryOperator(
        task_id='insert_data_task',
        conn_id='my_postgres_conn',
        sql="""
            INSERT INTO employee_growth (name, join_date)
            VALUES ('Airflow Instructor', CURRENT_DATE);
        """
    )

    # 任務 3：用 Python 檢查剛才插入的資料
    @task
    def check_data():
        # 講師筆記：這裡示範如何在 Python 中使用 Hook 呼叫連線
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        records = hook.get_records("SELECT * FROM employee_growth;")
        for row in records:
            print(f"Found employee: {row}")

    # 設定順序
    create_table >> insert_data >> check_data()
