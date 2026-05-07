"""
Email Processing Service
Tracks processed emails to prevent duplicate processing
"""
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from database import EmailProcessingLog

logger = logging.getLogger(__name__)


class EmailProcessingService:
    """Service for tracking processed emails"""
    
    def __init__(self, db: Session):
        self.db = db
    
    def is_email_processed(self, email_message_id: str) -> bool:
        """
        Check if email has already been processed
        
        Args:
            email_message_id: Gmail message ID
        
        Returns:
            bool: True if already processed
        """
        try:
            log = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.email_message_id == email_message_id
            ).first()
            
            return log is not None
            
        except Exception as e:
            logger.error(f"Error checking email processed status: {e}")
            return False
    
    def mark_email_processed(
        self,
        email_message_id: str,
        status: str,
        notes: Optional[str] = None,
        email_subject: Optional[str] = None,
        email_sender: Optional[str] = None,
        received_date: Optional[datetime] = None
    ) -> EmailProcessingLog:
        """
        Mark email as processed
        
        Args:
            email_message_id: Gmail message ID
            status: Processing status (success, failed, skipped)
            notes: Additional notes
            email_subject: Email subject
            email_sender: Email sender
            received_date: Email received date
        
        Returns:
            EmailProcessingLog: Created log entry
        """
        try:
            # Check if already exists
            existing = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.email_message_id == email_message_id
            ).first()
            
            if existing:
                # Update existing
                logger.info(f"[EMAIL_LOG] Updating existing email log: {email_message_id} → status: {status}")
                existing.processing_status = status
                existing.processing_notes = notes
                existing.processed_at = datetime.utcnow()
                log = existing
            else:
                # Create new
                logger.info(f"[EMAIL_LOG] Creating new email log: {email_message_id} → status: {status}")
                if notes:
                    logger.debug(f"[EMAIL_LOG] Notes: {notes}")
                log = EmailProcessingLog(
                    email_message_id=email_message_id,
                    email_subject=email_subject,
                    email_sender=email_sender,
                    received_date=received_date,
                    processing_status=status,
                    processing_notes=notes
                )
                self.db.add(log)
            
            self.db.commit()
            
            return log
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"[EMAIL_LOG] ❌ Error marking email as processed: {e}", exc_info=True)
            raise
    
    def get_unprocessed_emails(self, email_list: list) -> list:
        """
        Filter list of emails to only unprocessed ones
        
        Args:
            email_list: List of email dicts with 'message_id' key
        
        Returns:
            list: Filtered list of unprocessed emails
        """
        try:
            unprocessed = []
            
            for email in email_list:
                message_id = email.get('message_id')
                if message_id and not self.is_email_processed(message_id):
                    unprocessed.append(email)
            
            return unprocessed
            
        except Exception as e:
            logger.error(f"Error filtering unprocessed emails: {e}")
            return email_list  # Return all if error
    
    def get_processing_stats(self) -> dict:
        """
        Get email processing statistics
        
        Returns:
            dict: Processing stats
        """
        try:
            from sqlalchemy import func
            
            total = self.db.query(EmailProcessingLog).count()
            
            success = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.processing_status == 'success'
            ).count()
            
            failed = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.processing_status == 'failed'
            ).count()
            
            skipped = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.processing_status == 'skipped'
            ).count()
            
            return {
                'total': total,
                'success': success,
                'failed': failed,
                'skipped': skipped,
                'success_rate': round((success / total * 100) if total > 0 else 0, 2)
            }
            
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'skipped': 0,
                'success_rate': 0
            }
