from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models import Variable
from pendulum import datetime
from airflow_wiki_pageview.include.get_gzfile import _download_wiki_page
from airflow_wiki_pageview.include.Extract_pageviews import _extract_gz_to_csv
from airflow_wiki_pageview.include.filter_rows import _filter_companies
from airflow_wiki_pageview.include.load_dataset import _load_to_db

default_args = {
    'owner':'adebola',
    'retries':1,
    'retry_delay':50,
    'email_on_failure':False,
    'email':[Variable.get("alert_email")]
}

with DAG(
    dag_id='launchsentiment_wiki_dag',
    default_args=default_args,
    description='A DAG to process Wikipedia pageviews for sentiment analysis',
    schedule='0 8 * * *',
    start_date=datetime(2025, 12, 30),
    end_date=datetime(2026, 1, 2),
) as dag:

    download_task = PythonOperator(
        task_id='download_wiki_page',
        python_callable=_download_wiki_page,
        op_kwargs={"filename": "pageviews-20251220-100000.gz"},

    )

    extract_task = PythonOperator(
        task_id='extract_gz_to_csv',
        python_callable=_extract_gz_to_csv,
        op_kwargs={"gz_path": "{{ ti.xcom_pull(task_ids='download_wiki_page') }}"},
    )

    filter_task = PythonOperator(
        task_id='filter_companies',
        python_callable=_filter_companies,
        op_kwargs={"csv_path": "{{ ti.xcom_pull(task_ids='extract_gz_to_csv') }}"},
    )

    load_task = PythonOperator(
        task_id='load_to_db',
        python_callable=_load_to_db,
        op_kwargs={"csv_path": "{{ ti.xcom_pull(task_ids='filter_companies') }}"},
    )

    download_task >> extract_task >> filter_task >> load_task