import csv
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _load_to_db(csv_path: str, page_date: str, page_hour: int):
    """
    Loads filtered pageviews into Postgres.
    """
    pg_hook = PostgresHook(conn_id="postgres")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cursor.execute("""
                INSERT INTO wiki_pages (
                    "domain", "pagename", "viewcount"
                )
                VALUES (%s, %s, %s, timestamp %s)
                ON CONFLICT DO NOTHING
            """, (
                row["domain"],
                row["pagename"],
                int(row["viewcount"])
            ))

    conn.commit()
    cursor.close()
