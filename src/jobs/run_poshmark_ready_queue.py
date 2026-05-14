import csv
from dotenv import load_dotenv
load_dotenv()

from src.backend.db.database import SessionLocal
from src.services.crosslisting.crosslist_service import CrosslistService

CSV_FILE = "poshmark_ready_queue.csv"

unit_ids = []

with open(CSV_FILE, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        unit_ids.append(row["id"])

print(f"Loaded {len(unit_ids)} units to crosslist")

db = SessionLocal()

try:
    service = CrosslistService(db)
    result = service.bulk_crosslist(unit_ids)
    print(result)
finally:
    db.close()
