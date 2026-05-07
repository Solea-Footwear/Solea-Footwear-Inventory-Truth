from gmail_service import GmailService
import os 

gmail = GmailService()
# Get path relative to this file
script_dir = os.path.dirname(os.path.abspath(__file__))
credentials_path = os.path.join(script_dir, 'credentials.json')

gmail.authenticate(credentials_path)  # Opens browser for OAuth