from dotenv import load_dotenv
load_dotenv()

from src.backend.db.database import acquire_conn, release_conn
from src.services.sync_service import SyncService

print("Starting template refresh via eBay sync...")

conn = acquire_conn()

try:
    service = SyncService(conn)
    result = service.sync_ebay_listings()
    print("Sync result:", result)

finally:
    release_conn(conn)
    print("Done.")
