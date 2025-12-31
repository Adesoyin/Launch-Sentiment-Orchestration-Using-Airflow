import csv
import datetime as dt
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _load_to_db(csv_path: str):
    """
    Loads filtered pageviews into Postgres.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO wiki_pages (
                    "domain", "pagename", "viewcount", "pagedatetime", "created_at"
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                row["domain_code"],
                row["page_title"],
                int(row["views_count"]),
                '2025-12-20 10:00:00',
                dt.datetime.now().isoformat()
            ))

    conn.commit()
    cursor.close()
