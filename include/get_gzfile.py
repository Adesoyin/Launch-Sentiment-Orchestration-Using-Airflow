import requests
import logging
import os

def _download_wiki_page(filename: str) -> str:
    """
    Downloads a Wikipedia pageview gzip file using streaming to prevent ContentTooShortError.
    """
    base_dir = "/opt/airflow/datalake/wikipageviews"
    os.makedirs(base_dir, exist_ok=True)

    local_path = os.path.join(base_dir, filename)
    url = f"https://dumps.wikimedia.org/other/pageviews/2025/2025-12/{filename}"
    #f"dumps.wikimedia.org{filename}"

    try:
        # Use stream=True to handle large files efficiently
        with requests.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()  # Raise exception for 4xx/5xx errors
            
            with open(local_path, 'wb') as f:
                # Download in 1MB chunks
                for chunk in r.iter_content(chunk_size=1024*1024):
                    if chunk: 
                        f.write(chunk)
                        
        logging.info(f"Successfully downloaded: {local_path}")
        return local_path

    except requests.exceptions.RequestException as e:
        logging.error(f"Download failed: {e}")
        raise
