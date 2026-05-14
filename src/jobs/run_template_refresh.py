from dotenv import load_dotenv
load_dotenv()

from src.backend.db.database import SessionLocal
from src.services.sync_service import SyncService

print("Starting template refresh via eBay sync...")

db = SessionLocal()

try:
    service = SyncService(db)
    result = service.sync_ebay_listings()
    print("Sync result:", result)

finally:
    db.close()
    print("Done.")
