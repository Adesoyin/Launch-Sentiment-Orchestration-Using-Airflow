from airflow import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
#import airflow.utils.dates
import pendulum

with DAG(
    dag_id='create_wikipage_table',
    start_date=pendulum.datetime(2025, 12, 29),
    schedule=None,
    catchup=False
) as dag:
    
    create_wikipage_table = SQLExecuteQueryOperator(
        task_id='create_wikipage_table',
        conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS wiki_pages (
            domain VARCHAR(255),
            pagename VARCHAR(255) NOT NULL,
            viewcount INT NOT NULL,
            created_at TIMESTAMP NOT NULL,
            PRIMARY KEY (domain, pagename, viewcount)
        );
        """
    )
    create_wikipage_table