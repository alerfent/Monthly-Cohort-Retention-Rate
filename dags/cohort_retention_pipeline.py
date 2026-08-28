"""
cohort_retention_pipeline.py

專案：Monthly Cohort Retention Rate
用途：將資料清洗、匯入 MySQL、執行 cohort 分析 SQL，串成一條自動化 Pipeline。

Task 依賴關係：
    clean_data >> load_to_mysql >> run_cohort_analysis

資料庫連線資訊透過 Airflow Connection（Connection ID: mysql_cohort）管理，
不寫死在程式碼裡，避免帳密外洩。
"""

from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.mysql.hooks.mysql import MySqlHook


PROJECT_DIR = "/opt/airflow/project"
RAW_DATA_PATH = f"{PROJECT_DIR}/online_retail_II.csv"
CLEANED_DATA_PATH = f"{PROJECT_DIR}/cleaned_online_retail_ii.csv"
SQL_FILE_PATH = f"{PROJECT_DIR}/02_cohort_analysis.sql"

MYSQL_CONN_ID = "mysql_cohort"


with DAG(
    dag_id="cohort_retention_pipeline",
    description="Monthly Cohort Retention Rate 資料清洗、載入、分析 Pipeline",
    schedule=None,  # 先不設定自動排程，之後確認整條流程跑得通再加上排程週期
    start_date=datetime(2026, 1, 1),
    catchup=False,  # 不用補跑過去的排程，只在意手動觸發或未來的排程
    tags=["cohort", "retention", "portfolio-project"],
) as dag:

    @task
    def clean_data():
        """
        Task 1：讀取原始資料，排除無 Customer ID 的交易，輸出清洗後 CSV。
        邏輯與 01_data_cleaning.py 相同，這裡改寫成 Airflow task 的形式。
        """
        df = pd.read_csv(RAW_DATA_PATH, encoding="ISO-8859-1")
        before_count = df.shape[0]

        df_clean = df[df["Customer ID"].notnull()].copy()
        after_count = df_clean.shape[0]

        df_clean.to_csv(CLEANED_DATA_PATH, index=False)

        print(f"清洗前筆數：{before_count}")
        print(f"清洗後筆數：{after_count}")
        print(f"已輸出至：{CLEANED_DATA_PATH}")

    @task
    def load_to_mysql():
        """
        Task 2：讀取清洗後的 CSV，寫入 MySQL 的 transactions 表。
        使用 MySqlHook 透過 Airflow Connection 取得連線資訊，
        並先清空既有資料再重新寫入，確保每次執行結果一致（避免重複匯入造成筆數翻倍）。
        """
        df = pd.read_csv(CLEANED_DATA_PATH, encoding="ISO-8859-1")

        # 欄位名稱對應到資料庫表結構（Customer ID 帶空格，需改名）
        df = df.rename(columns={"Customer ID": "customer_id"})

        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        engine = hook.get_sqlalchemy_engine()

        # if_exists="replace"：每次執行都重新建表寫入，
        # 確保重跑 DAG 不會造成資料重複累加
        df.to_sql(
            name="transactions",
            con=engine,
            if_exists="replace",
            index=False,
            chunksize=5000,  # 分批寫入，避免一次寫入 80 萬筆造成記憶體或逾時問題
        )

        print(f"已寫入 transactions 表，共 {df.shape[0]} 筆")

    @task
    def run_cohort_analysis():
        """
        Task 3：執行 02_cohort_analysis.sql 裡定義的 View
        （clean_transactions、customer_cohort、cohort_size、cohort_retention_summary）。
        """
        with open(SQL_FILE_PATH, "r", encoding="utf-8") as f:
            sql_content = f.read()
 
        # 先逐「行」過濾掉整行都是註解的內容（以 -- 開頭），
        # 再把剩下的乾淨 SQL 內容組合起來。
        # 這一步必須在切分語句「之前」完成——
        # 如果先用分號切分、再逐段判斷是否為註解，
        # 一旦某句中文註解內容裡剛好包含分號（例如提及 "SET GLOBAL local_infile = 1;"），
        # 會被誤判成語句結尾，導致註解被攔腰截斷、殘餘文字被當成 SQL 執行而出錯。
        cleaned_lines = [
            line for line in sql_content.split("\n")
            if not line.strip().startswith("--")
        ]
        cleaned_content = "\n".join(cleaned_lines)
 
        # 現在整份內容已經不含任何註解，可以安全地用分號切分成一句一句的語句
        statements = [
            stmt.strip()
            for stmt in cleaned_content.split(";")
            if stmt.strip()
        ]
 
        hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
 
        for stmt in statements:
            hook.run(stmt)
 
        print(f"cohort 分析 SQL 執行完成，共執行 {len(statements)} 句語句")
 
    # 定義 Task 依賴關係：依序執行
    clean_data() >> load_to_mysql() >> run_cohort_analysis()
 