"""
Gmail Service for Email Monitoring
Monitors Gmail inbox for sale notification emails
"""
import logging
import os
import base64
import pickle
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Gmail API scopes
SCOPES = ['https://www.googleapis.com/auth/gmail.modify']


class GmailService:
    """Service for monitoring Gmail for sale notifications"""
    
    def __init__(self):
        self.service = None
        self.credentials = None
        self._initialize_gmail_api()

    def _initialize_gmail_api(self):
        """Initialize Gmail API connection"""
        try:
            # Load credentials from token file
            token_path = os.getenv('GMAIL_TOKEN_PATH', 'gmail_token.pickle')
            
            if os.path.exists(token_path):
                with open(token_path, 'rb') as token:
                    self.credentials = pickle.load(token)
            
            # Refresh if expired
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
                # Save refreshed credentials
                with open(token_path, 'wb') as token:
                    pickle.dump(self.credentials, token)
            
            # Create service
            if self.credentials and self.credentials.valid:
                self.service = build('gmail', 'v1', credentials=self.credentials)
                logger.info("Gmail API initialized successfully")
            else:
                logger.warning("Gmail credentials not valid, need to authenticate")
                
        except Exception as e:
            logger.error(f"Error initializing Gmail API: {e}")
    
    def authenticate(self, credentials_path: str) -> bool:
        """
        Authenticate with Gmail using OAuth
        
        Args:
            credentials_path (str): Path to credentials.json from Google Cloud Console
        
        Returns:
            bool: Success status
        """
        try:
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES
            )
            
            self.credentials = flow.run_local_server(port=0)
            
            # Save credentials
            token_path = os.getenv('GMAIL_TOKEN_PATH', 'gmail_token.pickle')
            with open(token_path, 'wb') as token:
                pickle.dump(self.credentials, token)
            
            # Create service
            self.service = build('gmail', 'v1', credentials=self.credentials)
            
            logger.info("Gmail authentication successful")
            return True
            
        except Exception as e:
            logger.error(f"Gmail authentication failed: {e}")
            return False
    
    def is_connected(self) -> bool:
        """Check if Gmail API is connected"""
        return self.service is not None
    
    def get_unread_messages(self, query: str = 'is:unread', max_results: int = 50) -> List[Dict]:
        """
        Get unread messages from Gmail
        
        Args:
            query (str): Gmail search query
            max_results (int): Max messages to retrieve
        
        Returns:
            list: List of message objects
        """

        if not self.service:
            logger.error("Gmail service not initialized")
            return []
        
        try:
            # Get message IDs
            results = self.service.users().messages().list(
                userId='me',
                q=query,
                maxResults=max_results
            ).execute()
            
            messages = results.get('messages', [])
            
            if not messages:
                logger.debug("No unread messages found")
                return []
            
            # Get full message details
            full_messages = []
            for msg in messages:
                message = self.service.users().messages().get(
                    userId='me',
                    id=msg['id'],
                    format='full'
                ).execute()
                
                full_messages.append(message)
            
            logger.info(f"Retrieved {len(full_messages)} unread messages")
            return full_messages
            
        except HttpError as error:
            logger.error(f"Gmail API error: {error}")
            return []
    
    # def get_sale_emails(self, since_minutes: int = 60) -> List[Dict]:
    #     """
    #     Get sale notification emails from all platforms
        
    #     Args:
    #         since_minutes (int): Look for emails from last N minutes
        
    #     Returns:
    #         list: Parsed email data
    #     """
    #     if not self.service:
    #         logger.error("Gmail service not initialized")
    #         return []
        
    #     try:
    #         # Build query for sale emails
    #         # Search for common sale notification keywords from all platforms
    #         # BUILD BETTER QUERY
    #         query_parts = [
    #             'is:unread',
    #             '(from:ebay.com OR from:poshmark.com OR from:mercari.com)',
    #             '('
    #             'subject:"just sold to" OR '                    # Poshmark
    #             'subject:"You made the sale for" OR '           # eBay
    #             'subject:"You\'ve made a sale" OR '             # Mercari
    #             'subject:"Congratulations! You made a sale"'    # Mercari alternate
    #             ')'
    #         ]

    #         # query_parts = [
    #         #     'is:unread (from:ebay.com subject:"Adidas Adizero") after:2026/02/19'
    #         # ]

    #         # query_parts = [
    #         #     "is:starred"
    #         # ]
            
    #         # Add time filter
    #         after_date = (datetime.utcnow() - timedelta(minutes=since_minutes)).strftime('%Y/%m/%d')
    #         query_parts.append(f'after:{after_date}')
            
    #         query = ' '.join(query_parts)
            
    #         logger.debug(f"Gmail query: {query}")

    #         print(f"Gmail query: {query}")
            
    #         # Get messages
    #         messages = self.get_unread_messages(query=query, max_results=50)
            
    #         # Parse messages
    #         parsed_emails = []
    #         for msg in messages:
    #             parsed = self._parse_message(msg)
    #             if parsed:
    #                 parsed_emails.append(parsed)
            
    #         logger.info(f"Found {len(parsed_emails)} sale emails")
    #         return parsed_emails
            
    #     except Exception as e:
    #         logger.error(f"Error getting sale emails: {e}")
    #         return []
    
    
    def get_sale_emails(self, since_minutes: int = 60) -> List[Dict]:
        if not self.service:
            logger.error("Gmail service not initialized")
            return []
        
        try:
            # Only filter by sender and unread - NO subject filter in query
            query_parts = [
                'is:unread',
                '(from:ebay@ebay.com OR from:orders@poshmark.com OR from:no-reply@alerts.us.mercari.com)',
            ]
            
            after_date = (datetime.utcnow() - timedelta(minutes=since_minutes)).strftime('%Y/%m/%d')
            query_parts.append(f'after:{after_date}')
            
            query = ' '.join(query_parts)
            
            logger.debug(f"Gmail query: {query}")
            print(f"Gmail query: {query}")
            
            # Get messages
            messages = self.get_unread_messages(query=query, max_results=50)
            
            # Subject keywords to match - filter in Python, not Gmail
            SALE_SUBJECT_KEYWORDS = [
                'just sold to',                     # Poshmark
                'you made the sale for',             # eBay
                "you've made a sale",                # Mercari
                'congratulations! you made a sale',  # Mercari alternate
            ]
            
            # Parse and filter by subject in Python
            parsed_emails = []
            for msg in messages:
                parsed = self._parse_message(msg)
                if not parsed:
                    continue
                
                subject_lower = parsed['subject'].lower()
                
                # Check if subject matches any sale keyword
                if any(keyword in subject_lower for keyword in SALE_SUBJECT_KEYWORDS):
                    parsed_emails.append(parsed)
                    logger.debug(f"Matched sale email: {parsed['subject']}")
                else:
                    logger.debug(f"Skipped non-sale email: {parsed['subject']}")
            
            logger.info(f"Found {len(parsed_emails)} sale emails")
            return parsed_emails
            
        except Exception as e:
            logger.error(f"Error getting sale emails: {e}")
            return []
    
    def _parse_message(self, message: Dict) -> Optional[Dict]:
        """
        Parse Gmail message
        
        Args:
            message (dict): Gmail message object
        
        Returns:
            dict: Parsed email data
        """
        try:
            headers = message['payload']['headers']
            
            # Extract headers
            subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), '')
            from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
            date = next((h['value'] for h in headers if h['name'].lower() == 'date'), '')
            
            # Get email body
            body = self._get_message_body(message)
            
            # Detect platform
            platform = self._detect_platform(from_email, subject, body)
            
            return {
                'message_id': message['id'],
                'thread_id': message['threadId'],
                'subject': subject,
                'from': from_email,
                'date': date,
                'body': body,
                'platform': platform,
                'raw': message
            }
            
        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None
    
    # def _get_message_body(self, message: Dict) -> str:
    #     logger.info("here is message from _get_message_body")
    #     logger.info(message)

    #     """Extract message body from Gmail message"""
    #     try:
    #         if 'parts' in message['payload']:
    #             # Multipart message
    #             for part in message['payload']['parts']:
    #                 if part['mimeType'] == 'text/plain':
    #                     data = part['body'].get('data', '')
    #                     if data:
    #                         return base64.urlsafe_b64decode(data).decode('utf-8')
    #                 elif part['mimeType'] == 'text/html':
    #                     data = part['body'].get('data', '')
    #                     if data:
    #                         return base64.urlsafe_b64decode(data).decode('utf-8')
    #         else:
    #             # Single part message
    #             data = message['payload']['body'].get('data', '')
    #             if data:
    #                 return base64.urlsafe_b64decode(data).decode('utf-8')
            
    #         return ''
            
    #     except Exception as e:
    #         logger.error(f"Error extracting body: {e}")
    #         return ''
        
    def _get_message_body(self, message: Dict) -> str:
        """Extract message body from Gmail message"""
        try:
            payload = message['payload']
            
            # Helper function to decode
            def decode_data(data):
                if data:
                    return base64.urlsafe_b64decode(data).decode('utf-8', errors='ignore')
                return ''
            
            # Check if body has data directly
            if 'body' in payload and payload['body'].get('data'):
                return decode_data(payload['body']['data'])
            
            # Check parts (multipart message)
            if 'parts' in payload:
                body_text = ''
                
                # Recursively search all parts
                def extract_from_parts(parts):
                    text = ''
                    for part in parts:
                        mime_type = part.get('mimeType', '')
                        
                        # Check nested parts first (for forwarded emails)
                        if 'parts' in part:
                            text += extract_from_parts(part['parts'])
                        
                        # Extract text/plain or text/html
                        if mime_type in ['text/plain', 'text/html']:
                            data = part.get('body', {}).get('data', '')
                            if data:
                                text += decode_data(data) + '\n'
                    
                    return text
                
                body_text = extract_from_parts(payload['parts'])
                return body_text
            
            return ''
            
        except Exception as e:
            logger.error(f"Error extracting body: {e}")
            return ''
    
    def _detect_platform(self, from_email: str, subject: str, body: str) -> str:
        from_lower = from_email.lower()
        subject_lower = subject.lower()
        
        # Check sender first (most reliable)
        if 'poshmark' in from_lower:
            return 'poshmark'
        elif 'ebay' in from_lower:
            return 'ebay'
        elif 'mercari' in from_lower:
            return 'mercari'
        
        # Check subject patterns
        if 'just sold to' in subject_lower and 'poshmark' in subject_lower:
            return 'poshmark'
        elif 'you made the sale for' in subject_lower:
            return 'ebay'
        elif "you've made a sale" in subject_lower or 'congratulations! you made a sale' in subject_lower:
            return 'mercari'
        
        return 'unknown'
    
    def mark_as_read(self, message_id: str) -> bool:
        """
        Mark message as read
        
        Args:
            message_id (str): Gmail message ID
        
        Returns:
            bool: Success status
        """
        if not self.service:
            return False
        
        try:
            self.service.users().messages().modify(
                userId='me',
                id=message_id,
                body={'removeLabelIds': ['UNREAD']}
            ).execute()
            
            logger.debug(f"Marked message {message_id} as read")
            return True
            
        except Exception as e:
            logger.error(f"Error marking message as read: {e}")
            return False
    
    def get_test_connection(self) -> Dict:
        """
        Test Gmail connection
        
        Returns:
            dict: Connection status
        """
        if not self.service:
            return {
                'connected': False,
                'error': 'Service not initialized'
            }
        
        try:
            # Try to get profile
            profile = self.service.users().getProfile(userId='me').execute()
            
            return {
                'connected': True,
                'email': profile.get('emailAddress'),
                'messages_total': profile.get('messagesTotal', 0),
                'threads_total': profile.get('threadsTotal', 0)
            }
            
        except Exception as e:
            return {
                'connected': False,
                'error': str(e)
            }
        

    
    # ! Labels Management
    def apply_label(self, message_id: str, label_name: str) -> bool:
        """
        Apply a label to an email
        
        Args:
            message_id: Gmail message ID
            label_name: Label name (e.g., "eBay Sales Not In System")
        
        Returns:
            bool: Success status
        """
        try:
            # Get or create label
            label_id = self._get_or_create_label(label_name)
            
            if not label_id:
                logger.error(f"Could not get/create label: {label_name}")
                return False
            
            # Apply label to message
            self.service.users().messages().modify(
                userId='me',
                id=message_id,
                body={'addLabelIds': [label_id]}
            ).execute()
            
            logger.info(f"Applied label '{label_name}' to message {message_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error applying label: {e}")
            return False


    def _get_or_create_label(self, label_name: str) -> str:
        """
        Get label ID by name, create if doesn't exist
        
        Args:
            label_name: Label name
        
        Returns:
            str: Label ID or None
        """
        try:
            # List all labels
            results = self.service.users().labels().list(userId='me').execute()
            labels = results.get('labels', [])
            
            # Check if label exists
            for label in labels:
                if label['name'] == label_name:
                    logger.debug(f"Found existing label: {label_name}")
                    return label['id']
            
            # Label doesn't exist - create it
            logger.info(f"Creating new label: {label_name}")
            
            label_object = {
                'name': label_name,
                'labelListVisibility': 'labelShow',
                'messageListVisibility': 'show'
            }
            
            created_label = self.service.users().labels().create(
                userId='me',
                body=label_object
            ).execute()
            
            logger.info(f"Created label '{label_name}' with ID: {created_label['id']}")
            return created_label['id']
            
        except Exception as e:
            logger.error(f"Error getting/creating label: {e}")
            return None


    def move_to_label(self, message_id: str, label_name: str, remove_inbox: bool = False) -> bool:
        """
        Move email to a label (optionally remove from inbox)
        
        Args:
            message_id: Gmail message ID
            label_name: Label name
            remove_inbox: If True, also remove INBOX label (archives the email)
        
        Returns:
            bool: Success status
        """
        try:
            label_id = self._get_or_create_label(label_name)
            
            if not label_id:
                return False
            
            # Prepare modification
            add_labels = [label_id]
            remove_labels = ['INBOX'] if remove_inbox else []
            
            # Apply changes
            self.service.users().messages().modify(
                userId='me',
                id=message_id,
                body={
                    'addLabelIds': add_labels,
                    'removeLabelIds': remove_labels
                }
            ).execute()
            
            action = "Moved and archived" if remove_inbox else "Labeled"
            logger.info(f"{action} message {message_id} to '{label_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error moving to label: {e}")
            return False