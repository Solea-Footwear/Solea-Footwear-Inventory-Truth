import os
from dotenv import load_dotenv

load_dotenv()

print("DATABASE_URL exists:", bool(os.getenv("DATABASE_URL")))
print("EBAY_APP_ID exists:", bool(os.getenv("EBAY_APP_ID")))
print("EBAY_DEV_ID exists:", bool(os.getenv("EBAY_DEV_ID")))
print("EBAY_CERT_ID exists:", bool(os.getenv("EBAY_CERT_ID")))
print("EBAY_REFRESH_TOKEN exists:", bool(os.getenv("EBAY_REFRESH_TOKEN")))

from database import SessionLocal
from sync_service import SyncService

db = SessionLocal()

try:
    service = SyncService(db)
    result = service.sync_ebay_listings()
    print(result)
finally:
    db.close()
