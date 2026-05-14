"""
Email Processing Service
Tracks processed emails to prevent duplicate processing
"""
import logging
from datetime import datetime
from typing import Optional

import psycopg2.extras

logger = logging.getLogger(__name__)


class EmailProcessingService:
    """Service for tracking processed emails"""

    def __init__(self, conn):
        self.conn = conn

    def is_email_processed(self, email_message_id: str) -> bool:
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM email_processing_logs WHERE email_message_id = %s LIMIT 1",
                    [email_message_id],
                )
                return cur.fetchone() is not None
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
    ):
        """Mark email as processed. Returns the log row as a dict."""
        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id FROM email_processing_logs WHERE email_message_id = %s LIMIT 1",
                    [email_message_id],
                )
                existing = cur.fetchone()

                if existing:
                    logger.info(f"[EMAIL_LOG] Updating existing email log: {email_message_id} → status: {status}")
                    cur.execute(
                        """
                        UPDATE email_processing_logs
                        SET processing_status=%s, processing_notes=%s, processed_at=%s
                        WHERE id=%s
                        RETURNING *
                        """,
                        [status, notes, datetime.utcnow(), existing['id']],
                    )
                else:
                    logger.info(f"[EMAIL_LOG] Creating new email log: {email_message_id} → status: {status}")
                    if notes:
                        logger.debug(f"[EMAIL_LOG] Notes: {notes}")
                    cur.execute(
                        """
                        INSERT INTO email_processing_logs
                            (id, email_message_id, email_subject, email_sender,
                             received_date, processing_status, processing_notes, processed_at)
                        VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, now())
                        RETURNING *
                        """,
                        [email_message_id, email_subject, email_sender, received_date, status, notes],
                    )
                log = dict(cur.fetchone())

            self.conn.commit()
            return log

        except Exception as e:
            self.conn.rollback()
            logger.error(f"[EMAIL_LOG] Error marking email as processed: {e}", exc_info=True)
            raise

    def get_unprocessed_emails(self, email_list: list) -> list:
        try:
            return [e for e in email_list if e.get('message_id') and not self.is_email_processed(e['message_id'])]
        except Exception as e:
            logger.error(f"Error filtering unprocessed emails: {e}")
            return email_list

    def get_processing_stats(self) -> dict:
        try:
            with self.conn.cursor() as cur:
                def count(sql):
                    cur.execute(sql)
                    return cur.fetchone()[0]

                total = count("SELECT COUNT(*) FROM email_processing_logs")
                success = count("SELECT COUNT(*) FROM email_processing_logs WHERE processing_status = 'success'")
                failed = count("SELECT COUNT(*) FROM email_processing_logs WHERE processing_status = 'failed'")
                skipped = count("SELECT COUNT(*) FROM email_processing_logs WHERE processing_status = 'skipped'")

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
