# import logging

# # Configure logging
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler('delisting_test.log'),  # Log to file
#         logging.StreamHandler()  # Also print to console
#     ]
# )

# logger = logging.getLogger(__name__)

# # Your existing code
# from delisting.gmail_service import GmailService
# from delisting.email_parser_service import EmailParserService
# from delisting.delist_service import DelistService
# from database import get_db
# import json

# db = next(get_db())

# logger.info("Starting delisting test...")



# # Get sale emails
# gmail = GmailService()
# emails = gmail.get_sale_emails(since_minutes=60)
# email_number = emails[0]

# logger.info(f"Found {len(emails)} emails")
# logger.info("here is email 1")
# logger.info(json.dumps(email_number,indent=2,default=str))


# # # # Parse email
# parser = EmailParserService()
# parsed = parser.parse_sale_email(email_number)

# logger.info(f"Parsed email: {parsed}")
# logger.info("here is parsed email")
# logger.info(parsed)

# # Process sale
# delist = DelistService(db)
# result = delist.process_sale(parsed)

# logger.info(f"Delist result: {result}")


# gmail.mark_as_read(email_number.get('message_id'))


                
                
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delisting_test.log'),  # Log to file
        logging.StreamHandler()  # Also print to console
    ]
)

logger = logging.getLogger(__name__)

# Your existing code
from delisting.gmail_service import GmailService
from delisting.email_parser_service import EmailParserService
from delisting.delist_service import DelistService
from database import get_db
import json

db = next(get_db())

logger.info("Starting delisting test...")

# Get sale emails
gmail = GmailService()
emails = gmail.get_sale_emails(since_minutes=60)

if not emails:
    logger.error("No emails found")
    exit()

email_number = emails[0]  # Test first email

logger.info(f"Found {len(emails)} emails")
logger.info("Testing email:")
logger.info(json.dumps(email_number, indent=2, default=str))

# Parse email - now returns LIST of items
parser = EmailParserService()
sale_items = parser.parse_sale_email(email_number)

if not sale_items:
    logger.error("Failed to parse email")
    exit()

logger.info(f"Parsed {len(sale_items)} item(s) from email")

# Log each item
for i, item in enumerate(sale_items, 1):
    logger.info(f"\nItem {i}:")
    logger.info(json.dumps(item, indent=2, default=str))

# Process each item
delist = DelistService(db)
results = []

for i, item in enumerate(sale_items, 1):
    logger.info(f"\n{'='*60}")
    logger.info(f"Processing item {i}/{len(sale_items)}")
    logger.info(f"{'='*60}")
    
    result = delist.process_sale(item)
    results.append(result)
    
    logger.info(f"Result for item {i}:")
    logger.info(json.dumps(result, indent=2, default=str))

# Summary
logger.info(f"\n{'='*60}")
logger.info("SUMMARY")
logger.info(f"{'='*60}")
logger.info(f"Total items: {len(sale_items)}")
logger.info(f"Successful: {sum(1 for r in results if r.get('success'))}")
logger.info(f"Failed: {sum(1 for r in results if not r.get('success'))}")

# Mark email as read only if at least one item succeeded
if any(r.get('success') for r in results):
    gmail.mark_as_read(email_number.get('message_id'))
    logger.info("✓ Email marked as read")
else:
    logger.error("✗ Email NOT marked as read (all items failed)")

db.close()
logger.info("\nDelisting test complete!")