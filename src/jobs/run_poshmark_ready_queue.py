import csv
from dotenv import load_dotenv
load_dotenv()

from src.backend.db.database import acquire_conn, release_conn
from src.services.crosslisting.crosslist_service import CrosslistService

CSV_FILE = "poshmark_ready_queue.csv"

unit_ids = []

with open(CSV_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        unit_ids.append(row["id"])

print(f"Loaded {len(unit_ids)} units to crosslist")

conn = acquire_conn()

try:
    service = CrosslistService(conn)
    result = service.bulk_crosslist(unit_ids)
    print(result)
finally:
    release_conn(conn)
