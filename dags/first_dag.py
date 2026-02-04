from airflow.decorators import dag, task
from datetime import datetime

# 1. 定義基礎設定 (Default Arguments)
@dag(
    dag_id='my_first_tutor_dag',        # DAG 在 UI 顯示的名字
    schedule='* * * * *',               # 執行頻率 (Cron 語法：每分鐘)
    start_date=datetime(2023, 1, 1),    # 啟動日期 (必須是過去的時間)
    catchup=False,                      # 不要補跑過去沒跑的任務 (重要！)
    tags=['learning'],
)
def my_first_dag():

    # 2. 定義第一個任務 (Task)
    @task()
    def print_hello():
        print("Hello, I am learning Airflow!")
        return "Step 1 Done"

    # 3. 定義第二個任務
    @task()
    def print_world(prev_message):
        print(f"Airflow says: {prev_message} and World!")

    # 4. 設定任務之間的順序 (Dependency)
    # 這裡我們直接傳遞變數，Airflow 會自動知道 print_hello 跑完才跑 print_world
    status = print_hello()
    print_world(status)

# 5. 實例化 DAG
my_first_dag()
