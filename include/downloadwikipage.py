import urllib.request
import urllib.error
import logging
import os

def _download_wiki_page(filename: str) -> str:
    """
    Downloads a Wikipedia pageview gzip file.
    Returns the local file path.
    """
    base_dir = "/opt/airflow/datalake/wikipageviews"
    os.makedirs(base_dir, exist_ok=True)

    local_path = os.path.join(base_dir, filename)
    url = f"https://dumps.wikimedia.org/other/pageviews/2025/2025-12/{filename}"

    try:
        downloaded_path, _ = urllib.request.urlretrieve(url, local_path)
        logging.info(f"Downloaded file to {downloaded_path}")
        return downloaded_path

    except urllib.error.HTTPError as e:
        logging.error(f"HTTP error {e.code}: {e.reason}")
        raise
    except urllib.error.URLError as e:
        logging.error(f"Connection error: {e.reason}")
        raise
