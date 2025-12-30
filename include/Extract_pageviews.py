import gzip
import csv

def _extract_gz_to_csv(gz_path: str) -> str:
    """
    Extracts the gzip file to CSV.
    Returns CSV file path as output
    """
    csv_path = gz_path.replace(".gz", ".csv")

    with gzip.open(gz_path, "rt", encoding="utf-8") as gz_file, \
         open(csv_path, "w", newline="", encoding="utf-8") as csv_file:

        csvfile = csv.writer(csv_file)
        csvfile.writerow(["domain_code", "page_title", "views_count", "response_size"])

        for row in gz_file:
            csvfile.writerow(row.strip().split(" "))

    return csv_path
