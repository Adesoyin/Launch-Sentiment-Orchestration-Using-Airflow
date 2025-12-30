import csv

companies = ["Apple_Inc.","Facebook","Microsoft","Google", "Amazon"]

def _filter_companies(csv_path: str) -> str:
    """
    Filters CSV data for the selected companies only.
    Returns filtered CSV path.
    """
    filtered_path = csv_path.replace(".csv", "_filtered.csv")

    with open(csv_path, "r", encoding="utf-8") as infile, \
         open(filtered_path, "w", newline="", encoding="utf-8") as outfile:

        outputreading = csv.DictReader(infile)
        writer = csv.DictWriter(
            outfile,
            fieldnames=["domain", "pagename", "viewcount", "response_size"]
        )
        writer.writeheader()

        for row in outputreading:
            if row["page_title"] in companies:
                writer.writerow(row)

    return filtered_path

