"""
eBay Delisting Module
Handles delisting items from eBay using API
"""
import logging
import os
from typing import Dict
from ebaysdk.trading import Connection as Trading
from dotenv import load_dotenv
logger = logging.getLogger(__name__)


load_dotenv()

def delist_ebay_item(item_id: str) -> Dict:
    """
    Delist item from eBay by setting quantity to 0
    
    Args:
        item_id (str): eBay item ID
    
    Returns:
        dict: Result with success status
    """
    try:
        # Initialize eBay API
        api = Trading(
            appid=os.getenv('EBAY_APP_ID'),
            devid=os.getenv('EBAY_DEV_ID'),
            certid=os.getenv('EBAY_CERT_ID'),
            token=os.getenv('EBAY_AUTH_TOKEN'),
            config_file=None,
            domain='api.ebay.com' if os.getenv('EBAY_ENVIRONMENT') == 'production' else 'api.sandbox.ebay.com'
        )
        
        # Use EndItem (not ReviseItem)
        response = api.execute('EndItem', {
            'ItemID': item_id,
            'EndingReason': 'NotAvailable'
        })
        
        return {'success': True, 'item_id': item_id}
        
    except Exception as e:
        logger.error(f"Error delisting eBay item {item_id}: {e}")
        
        # Try method 2: End item early
        try:
            response = api.execute('EndItem', {
                'ItemID': item_id,
                'EndingReason': 'NotAvailable'
            })
            
            logger.info(f"eBay item {item_id} ended successfully")
            
            return {
                'success': True,
                'item_id': item_id,
                'method': 'end_item'
            }
            
        except Exception as e2:
            logger.error(f"Error ending eBay item {item_id}: {e2}")
            return {
                'success': False,
                'item_id': item_id,
                'error': str(e2)
            }


# delist_ebay_item("306644391978")