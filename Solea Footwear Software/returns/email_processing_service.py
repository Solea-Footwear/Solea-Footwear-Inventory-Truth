"""
Email Processing Service
Tracks processed emails to prevent duplicate processing.

R1-06: this service no longer commits on its own. The caller is expected
to commit once per email so that the return record, return_event row,
and email_processing_log row all land together (or all roll back).
"""
import logging
from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from database import EmailProcessingLog

logger = logging.getLogger(__name__)


class EmailProcessingService:
    """Service for tracking processed emails."""

    def __init__(self, db: Session):
        self.db = db

    def is_email_processed(self, email_message_id: str) -> bool:
        try:
            return self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.email_message_id == email_message_id,
                EmailProcessingLog.processing_status == 'success',
            ).first() is not None
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
        received_date: Optional[datetime] = None,
    ) -> EmailProcessingLog:
        """Insert/update the email-processing log row.

        R1-06: NO commit happens here. The caller commits.
        """
        try:
            existing = self.db.query(EmailProcessingLog).filter(
                EmailProcessingLog.email_message_id == email_message_id
            ).first()
            if existing:
                existing.processing_status = status
                existing.processing_notes = notes
                existing.processed_at = datetime.utcnow()
                log = existing
            else:
                log = EmailProcessingLog(
                    email_message_id=email_message_id,
                    email_subject=email_subject,
                    email_sender=email_sender,
                    received_date=received_date,
                    processing_status=status,
                    processing_notes=notes,
                )
                self.db.add(log)
            # Flush only - caller commits.
            self.db.flush()
            return log
        except Exception as e:
            logger.error(f"[EMAIL_LOG] Error marking email as processed: {e}",
                         exc_info=True)
            raise

    def get_unprocessed_emails(self, email_list: list) -> list:
        try:
            return [
                e for e in email_list
                if e.get('message_id') and not self.is_email_processed(e['message_id'])
            ]
        except Exception as e:
            logger.error(f"Error filtering unprocessed emails: {e}")
            return email_list

    def get_processing_stats(self) -> dict:
        try:
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
                'success_rate': round((success / total * 100) if total > 0 else 0, 2),
            }
        except Exception as e:
            logger.error(f"Error getting processing stats: {e}")
            return {'total': 0, 'success': 0, 'failed': 0, 'skipped': 0, 'success_rate': 0}
