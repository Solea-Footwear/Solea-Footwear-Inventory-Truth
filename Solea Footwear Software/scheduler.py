"""
Scheduler Service for Automated eBay Sync
Runs sync tasks in background at scheduled intervals
"""
import logging
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import os

from delisting.gmail_service import GmailService
from delisting.email_parser_service import EmailParserService
from delisting.delist_service import DelistService
from database import SessionLocal

# Import return tracking services
from returns.ebay_return_parser import EbayReturnParser
from returns.return_service import ReturnService
from returns.email_processing_service import EmailProcessingService


logger = logging.getLogger(__name__)

from crosslisting.crosslist_service import CrosslistService
from database import SessionLocal


from dotenv import load_dotenv

load_dotenv()

def auto_crosslist_check():
    """
    Check for units that need cross-listing
    This runs after eBay sync to detect new listings
    """
    logger.info("Checking for units needing cross-listing...")
    
    db = SessionLocal()
    
    try:
        from database import Unit, Listing, ListingUnit, Channel
        from sqlalchemy.orm import joinedload
    
        # Get all listed units
        # listed_units = db.query(Unit).filter(
        #     Unit.status == 'listed'
        # ).all()

        listed_units = db.query(Unit).filter(
            Unit.status == 'listed'
        ).options(
            joinedload(Unit.listing_units).joinedload(ListingUnit.listing).joinedload(Listing.channel)
        ).all()
        
        if not listed_units:
            logger.debug("No listed units found")
            return
        
        logger.info(f"Found {len(listed_units)} listed units, checking cross-listing status...")
        
        crosslist_service = CrosslistService(db)
        
        # Track results
        total_checked = 0
        total_created = 0
        
        for unit in listed_units:
            try:
                result = crosslist_service.check_and_crosslist(unit.id)
                total_checked += 1
                
                if result.get('created_listings'):
                    total_created += len(result['created_listings'])
                    logger.info(f"Unit {unit.unit_code}: Created {len(result['created_listings'])} listings")
                
            except Exception as e:
                logger.error(f"Error cross-listing unit {unit.unit_code}: {e}")
        
        logger.info(f"Auto cross-listing complete: Checked {total_checked} units, created {total_created} listings")
        
    except Exception as e:
        logger.error(f"Error in auto_crosslist_check: {e}")
    
    finally:
        db.close()


def check_return_emails():
    """Check for return emails and process them.

    Runs once per day via scheduler.

    R1-06: each email is processed in its own database transaction so the
    Return + ReturnEvent rows and the email_processing_log row succeed or
    fail together. On any exception we roll back that one email and keep
    going with the next one.
    """
    logger.info("Checking for return emails...")
    db = SessionLocal()
    try:
        gmail = GmailService()
        if not gmail.is_connected():
            logger.error("Gmail not connected, skipping return email check")
            return

        parser = EbayReturnParser()
        return_service = ReturnService(db)
        email_processing = EmailProcessingService(db)

        label_name = os.getenv('EBAY_RETURNS_GMAIL_LABEL', 'EBAY_RETURNS_TRACKING')
        # R3-22: gmail service now paginates internally; large catch-ups work.
        emails = gmail.get_emails_from_label(label_name, max_results=1000)
        if not emails:
            logger.info("No return emails found in label")
            return

        logger.info(f"Found {len(emails)} email(s) in return label")
        processed = errors = skipped = 0

        for email_msg in emails:
            message_id = email_msg.get('message_id')
            if email_processing.is_email_processed(message_id):
                logger.debug(f"Email {message_id} already processed, skipping")
                skipped += 1
                continue

            try:
                parsed = parser.parse(email_msg)
                if not parsed:
                    email_processing.mark_email_processed(
                        message_id,
                        'failed',
                        f"parser returned None (subject={email_msg.get('subject', 'N/A')})",
                        email_msg.get('subject'),
                        email_msg.get('from'),
                    )
                    db.commit()
                    errors += 1
                    continue

                result = return_service.process_return_email(parsed)
                if result.get('success'):
                    email_processing.mark_email_processed(
                        message_id,
                        'success',
                        f"Return {result.get('return_id')} {result.get('action')}",
                        email_msg.get('subject'),
                        email_msg.get('from'),
                    )
                    db.commit()
                    processed += 1
                else:
                    db.rollback()
                    # Log failure outside the rolled-back transaction.
                    email_processing.mark_email_processed(
                        message_id,
                        'failed',
                        result.get('error'),
                        email_msg.get('subject'),
                        email_msg.get('from'),
                    )
                    db.commit()
                    errors += 1

            except Exception as e:
                db.rollback()
                logger.error(f"Error processing email {message_id}: {e}", exc_info=True)
                try:
                    email_processing.mark_email_processed(
                        message_id,
                        'failed',
                        str(e),
                        email_msg.get('subject'),
                        email_msg.get('from'),
                    )
                    db.commit()
                except Exception:
                    db.rollback()
                errors += 1

        logger.info(
            f"Return email check complete: {processed} processed, "
            f"{skipped} skipped, {errors} errors"
        )
    except Exception as e:
        logger.error(f"Error in check_return_emails: {e}", exc_info=True)
    finally:
        db.close()


# def check_sale_emails():
#     """
#     Check for new sale emails and process them
#     This function runs every 2-3 minutes via scheduler
#     """
#     logger.info("Checking for sale emails...")
    
#     db = SessionLocal()
    
#     try:
#         # Initialize services
#         gmail = GmailService()
        
#         if not gmail.is_connected():
#             logger.error("Gmail not connected, skipping email check")
#             return
        
#         parser = EmailParserService()
#         delist_service = DelistService(db)

#         # coz we are running every X minutes so it will only get last emails since last X minutes + added 3 minute buffer to be safe.

#         since_last_minutes = int(os.getenv("EMAIL_CHECK_INTERVAL_MINUTES")) + 3

#         # Get new sale emails (last 5 minutes)
#         emails = gmail.get_sale_emails(since_minutes=since_last_minutes)
        
#         if not emails:
#             logger.debug("No new sale emails found")
#             return
        
#         logger.info(f"Found {len(emails)} sale email(s)")
        
#         # Process each email
#         processed = 0
#         errors = 0
        
#         for email in emails:
#             try:
#                 # Parse email
#                 parsed = parser.parse_sale_email(email)
                
#                 if not parsed:
#                     logger.warning(f"Failed to parse email {email.get('message_id')}")
#                     continue
                
#                 logger.info(f"Processing sale: Platform={parsed.get('platform')}, SKU={parsed.get('sku')}")
                
#                 # Process sale and delist
#                 result = delist_service.process_sale(parsed)
                
#                 if result.get('success'):
#                     processed += 1
                    
#                     # Mark email as read
#                     gmail.mark_as_read(email.get('message_id'))
                    
#                     logger.info(f"Successfully processed sale for unit {result.get('unit_code')}")
#                     logger.info(f"Delisted from {len(result.get('delisted', []))} platform(s)")
#                 else:
#                     errors += 1
#                     logger.error(f"Failed to process sale: {result.get('errors')}")
                
#             except Exception as e:
#                 errors += 1
#                 logger.error(f"Error processing email: {e}")
        
#         logger.info(f"Email check complete: {processed} processed, {errors} errors")
        
#     except Exception as e:
#         logger.error(f"Error in check_sale_emails: {e}")
    
#     finally:
#         db.close()



def check_sale_emails():
    """
    Check for new sale emails and process them
    This function runs every 2-3 minutes via scheduler
    Handles both single sales and bundle sales
    """
    logger.info("Checking for sale emails...")
    
    db = SessionLocal()
    
    try:
        # Initialize services
        gmail = GmailService()
        
        if not gmail.is_connected():
            logger.error("Gmail not connected, skipping email check")
            return
        
        parser = EmailParserService()
        delist_service = DelistService(db)

        # Get interval with 3 minute buffer
        since_last_minutes = int(os.getenv("EMAIL_CHECK_INTERVAL_MINUTES", "3")) + 5

        # Get new sale emails
        emails = gmail.get_sale_emails(since_minutes=since_last_minutes)
        
        if not emails:
            logger.debug("No new sale emails found")
            return
        
        logger.info(f"Found {len(emails)} sale email(s)")
        
        # Process each email
        total_emails_processed = 0
        total_items_processed = 0
        total_errors = 0
        
        for email in emails:
            try:
                # Parse email - returns LIST of items (handles bundles)
                sale_items = parser.parse_sale_email(email)
                logger.info(f"Parsed {len(sale_items)} items (message_id={email.get('message_id')})")
                
                if not sale_items:
                    logger.warning(f"Failed to parse email {email.get('message_id')}")
                    total_errors += 1
                    continue
                
                # Log what we found
                first_item = sale_items[0] if isinstance(sale_items[0], dict) else {}

                platform = first_item.get('platform', 'unknown')
                
                if len(sale_items) > 1:
                    logger.info(f"📦 BUNDLE SALE: {len(sale_items)} items from {platform}")
                else:
                    sku = first_item.get('sku')
                
                    # Only check skus if it's actually a list
                    if not sku and isinstance(first_item.get('skus'), list):
                        sku = first_item.get('skus')[0]
                
                    logger.info(f"📦 Single sale: Platform={platform}, SKU={sku}")
                
                # Process EACH item in the sale
                items_processed = 0
                items_failed = 0
                
                for i, item in enumerate(sale_items, 1):
                    try:
                        if not isinstance(item, dict):
                            logger.error(f"  ✗ Invalid sale item format: expected dict, got {type(item).__name__}: {item}")
                            items_failed += 1
                            continue
                
                        sku = item.get('sku')

                        if not sku and isinstance(item.get('skus'), list):
                            sku = item['skus'][0]
                        
                        if not sku:
                            logger.error(f"  ✗ Skipping item {i}: No SKU found")
                            items_failed += 1
                            continue
                        
                        logger.info(f"  Processing item {i}/{len(sale_items)}: SKU={sku}")
                        
                        # NOW process
                        result = delist_service.process_sale(item)
                        
                        if result.get('success'):
                            items_processed += 1
                            unit_code = result.get('unit_code', 'unknown')
                            delisted_count = len(result.get('delisted', []))
                            logger.info(f"  ✓ Item {i} processed: Unit={unit_code}, Delisted from {delisted_count} platform(s)")
                        else:
                            items_failed += 1
                            logger.error(f"  ✗ Item {i} failed: {result.get('errors')}")
                        
                    except Exception as e:
                        items_failed += 1
                        logger.error(f"  ✗ Error processing item {i}: {e}")
                
                # Mark email as read if at least one item was processed successfully
                if items_processed > 0:
                    gmail.mark_as_read(email.get('message_id'))
                    total_emails_processed += 1
                    total_items_processed += items_processed
                    logger.info(f"✓ Email processed: {items_processed}/{len(sale_items)} items successful")
                else:
                    total_errors += 1
                    logger.error(f"✗ Email failed: No items processed successfully")
                
            except Exception as e:
                total_errors += 1
                logger.error(f"Error processing email {email.get('message_id')}: {e}")
        
        logger.info(f"Email check complete: {total_emails_processed} emails processed, {total_items_processed} items processed, {total_errors} errors")
        
    except Exception as e:
        logger.error(f"Error in check_sale_emails: {e}")
    
    finally:
        db.close()



class SyncScheduler:
    """
    Background scheduler for automated eBay sync
    """
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.is_running = False
        self.last_sync_time = None
        self.last_sync_status = None
        self.sync_interval_minutes = int(os.getenv('SYNC_INTERVAL_MINUTES', 60))
        
        # Add event listeners
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
    
    # def start(self, sync_function):
    #     """
    #     Start the scheduler with the sync function
        
    #     Args:
    #         sync_function: The function to call for sync (should accept no args)
    #     """
    #     if self.is_running:
    #         logger.warning("Scheduler already running")
    #         return False
    #     try:
            
    #         # Run once immediately - ADD THIS ()

    #         # it means our scheduler doesnt need to wait X Minutes before starting first schdule so it will start first schdule instantly.

    #         logger.info("Running initial sync immediately...")
    #         sync_function()

    #         # Add job to run at specified interval
    #         self.scheduler.add_job(
    #             func=sync_function,
    #             trigger=IntervalTrigger(minutes=self.sync_interval_minutes),
    #             id='ebay_sync_job',
    #             name='eBay Sync Job',
    #             replace_existing=True,
    #             max_instances=1  # Prevent overlapping syncs
    #         )
            
    #         self.scheduler.start()
    #         self.is_running = True
            
    #         logger.info(f"Scheduler started - sync will run every {self.sync_interval_minutes} minutes")
    #         return True
            
    #     except Exception as e:
    #         logger.error(f"Failed to start scheduler: {e}")
    #         return False


    def start(self, sync_function):
        """Start eBay sync scheduler"""
        try:
            from datetime import datetime, timedelta
            
            # Check if job already exists
            if self.scheduler.get_job('ebay_sync_job'):
                logger.warning("eBay sync job already running")
                return False
            
            # Add job with 5 second delay
            start_time = datetime.now() + timedelta(seconds=5)
            
            self.scheduler.add_job(
                func=sync_function,
                trigger='interval',
                minutes=int(os.getenv('SYNC_INTERVAL_MINUTES', '60')),
                id='ebay_sync_job',
                name='eBay Sync Job',
                replace_existing=True,
                next_run_time=start_time,
                max_instances=1
            )
            
            logger.info("eBay sync job added to scheduler")
            return True
            
        except Exception as e:
            logger.error(f"Error starting scheduler: {e}")
            return False
    
    # def stop(self):
    #     """Stop the scheduler"""
    #     if not self.is_running:
    #         logger.warning("Scheduler not running")
    #         return False
        
    #     try:
    #         self.scheduler.shutdown()
    #         self.is_running = False
    #         logger.info("Scheduler stopped")
    #         return True
    #     except Exception as e:
    #         logger.error(f"Failed to stop scheduler: {e}")
    #         return False


    def stop(self):
        """Stop eBay sync scheduler (keeps email & crosslist running)"""
        try:
            job = self.scheduler.get_job('ebay_sync_job')
            if job:
                self.scheduler.remove_job('ebay_sync_job')
                self.is_running = False
                logger.info("eBay sync job removed")
                return True
            return False
        except Exception as e:
            logger.error(f"Error stopping scheduler: {e}")
            return False
    
    def trigger_now(self):
        """Trigger sync job immediately (in addition to scheduled runs)"""
        if not self.is_running:
            logger.warning("Scheduler not running")
            return False
        
        try:
            job = self.scheduler.get_job('ebay_sync_job')
            if job:
                job.modify(next_run_time=datetime.now())
                logger.info("Sync job triggered manually")
                return True
            else:
                logger.error("Sync job not found")
                return False
        except Exception as e:
            logger.error(f"Failed to trigger sync: {e}")
            return False
    
    # def get_status(self):
    #     """
    #     Get scheduler status
        
    #     Returns:
    #         dict: Status information
    #     """
    #     next_run = None
        
    #     if self.is_running:
    #         job = self.scheduler.get_job('ebay_sync_job')
    #         if job and job.next_run_time:
    #             next_run = job.next_run_time.isoformat()
        
    #     return {
    #         'is_running': self.is_running,
    #         'sync_interval_minutes': self.sync_interval_minutes,
    #         'last_sync_time': self.last_sync_time.isoformat() if self.last_sync_time else None,
    #         'last_sync_status': self.last_sync_status,
    #         'next_run_time': next_run
    #     }


    def get_status(self):
        """Get scheduler status"""
        try:
            from database import SyncLog, SessionLocal
            from sqlalchemy import desc
            
            # Check scheduler
            jobs = self.scheduler.get_jobs()
            ebay_job = self.scheduler.get_job('ebay_sync_job')

            is_running = ebay_job is not None 
            
            # Get last sync from database
            db = SessionLocal()
            last_sync = db.query(SyncLog).order_by(
                desc(SyncLog.completed_at)
            ).first()
            db.close()
            
            status = {
                'is_running': is_running and len(jobs) > 0,
                'job_count': len(jobs),
                'sync_interval_minutes': int(os.getenv('SYNC_INTERVAL_MINUTES', '60')),
                'next_run_time': ebay_job.next_run_time.isoformat() if ebay_job and ebay_job.next_run_time else None,
                'last_sync_time': last_sync.completed_at.isoformat() if last_sync and last_sync.completed_at else None,
                'last_sync_status': last_sync.status if last_sync else None
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting status: {e}")
            return {'is_running': False, 'error': str(e)}

    def _job_executed(self, event):
        """Event listener for successful job execution"""
        self.last_sync_time = datetime.now()
        self.last_sync_status = 'success'
        logger.info(f"Scheduled sync completed successfully at {self.last_sync_time}")
    
    def _job_error(self, event):
        """Event listener for job errors"""
        self.last_sync_time = datetime.now()
        self.last_sync_status = 'failed'
        logger.error(f"Scheduled sync failed at {self.last_sync_time}: {event.exception}")


    # monitoring emails service to check for sold items and delisting on platforms
    def start_email_monitoring(self):
        """Start email monitoring for auto-delisting"""
        
        # Get interval from environment (default 3 minutes)
        import os
        from datetime import datetime, timedelta
        
        email_check_interval = int(os.getenv('EMAIL_CHECK_INTERVAL_MINUTES', '3'))
        start_time = datetime.now() + timedelta(seconds=5)


        print("Here are service environment variables")
        print("Email check Interval Minutes ",email_check_interval)
        
        # Add job
        self.scheduler.add_job(
            func=check_sale_emails,
            trigger=IntervalTrigger(minutes=email_check_interval),
            id='email_check_job',
            name='Check Sale Emails Job',
            replace_existing=True,
            max_instances=1 , # prevent overalapping if already
            next_run_time=start_time
        )
        
        logger.info(f"Email monitoring started - checking every {email_check_interval} minutes")


    def start_crosslist_monitoring(self):
        """Start cross-listing monitoring"""
        
        import os
        from datetime import datetime, timedelta

        crosslist_interval = int(os.getenv('CROSSLIST_CHECK_INTERVAL_MINUTES', '60'))

        print("CrossListing Service Environment Variables...")
        print("CrossList Check Interval Minutes",crosslist_interval)

        # First run after 5 seconds, then every X minutes
        start_time = datetime.now() + timedelta(seconds=5)

        # # Add job
        # self.scheduler.add_job(
        #     func=auto_crosslist_check,
        #     trigger=IntervalTrigger(minutes=crosslist_interval),
        #     id='crosslist_check_job',
        #     name='Cross-listing Check Job',
        #     replace_existing=True,
        #     max_instances=1
        # )

        # Add job with immediate first run
        self.scheduler.add_job(
            func=auto_crosslist_check,
            trigger='interval',  # Changed from IntervalTrigger to 'interval'
            minutes=crosslist_interval,
            id='crosslist_check_job',
            name='Cross-listing Check Job',
            replace_existing=True,
            max_instances=1,
            next_run_time=start_time  # Runs after 5 seconds
        )
    
        
        logger.info(f"Cross-listing monitoring started - checking every {crosslist_interval} minutes")


    def start_return_monitoring(self):
        """R3-21: schedule the return job at a fixed wall-clock hour each day.

        Configurable via env var RETURN_CHECK_HOUR (0-23, default 3).
        The legacy RETURN_CHECK_INTERVAL_HOURS env var is now ignored - kept
        in .env.example with a deprecation comment.
        """
        import os
        from datetime import datetime, timedelta
        from apscheduler.triggers.cron import CronTrigger

        hour = int(os.getenv('RETURN_CHECK_HOUR', '3'))

        # First run shortly after startup so smoke tests don't have to wait
        # until the configured hour comes around.
        start_time = datetime.now() + timedelta(seconds=5)

        self.scheduler.add_job(
            func=check_return_emails,
            trigger=CronTrigger(hour=hour, minute=0),
            id='return_check_job',
            name='Return Email Check Job',
            replace_existing=True,
            max_instances=1,
            next_run_time=start_time,
        )
        logger.info(
            f"Return monitoring started - daily at {hour:02d}:00 (cron schedule)"
        )
    
    def stop_return_monitoring(self):
        """Stop return email monitoring"""
        try:
            job = self.scheduler.get_job('return_check_job')
            if job:
                self.scheduler.remove_job('return_check_job')
                logger.info("Return monitoring stopped")
                return True
            else:
                logger.warning("Return monitoring job not found")
                return False
        except Exception as e:
            logger.error(f"Error stopping return monitoring: {e}")
            return False
    
    def get_return_monitoring_status(self):
        """Get return monitoring status"""
        try:
            job = self.scheduler.get_job('return_check_job')
            if job:
                return {
                    'running': True,
                    'next_run': job.next_run_time.isoformat() if job.next_run_time else None,
                    'cron_hour': int(os.getenv('RETURN_CHECK_HOUR', '3'))
                }
            else:
                return {
                    'running': False,
                    'next_run': None,
                    'cron_hour': int(os.getenv('RETURN_CHECK_HOUR', '3'))
                }
        except Exception as e:
            logger.error(f"Error getting return monitoring status: {e}")
            return {
                'running': False,
                'error': str(e)
            }


# Global scheduler instance
sync_scheduler = SyncScheduler()
