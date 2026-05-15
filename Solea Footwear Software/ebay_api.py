"""
eBay API Integration
Handles fetching active listings and matching them to inventory units
"""
import os
from datetime import datetime
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError
import logging
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)



import re
from html import unescape

def strip_html(html_text: str) -> str:
    """Remove HTML tags and convert to plain text"""
    if not html_text:
        return ''
    
    # Unescape HTML entities (&nbsp; → space, etc.)
    text = unescape(html_text)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    return text



class eBayAPI:
    """
    eBay API wrapper for inventory management
    """
    
    def __init__(self):
        """Initialize eBay API connection"""
        self.app_id = os.getenv('EBAY_APP_ID')
        self.cert_id = os.getenv('EBAY_CERT_ID')
        self.dev_id = os.getenv('EBAY_DEV_ID')
        self.auth_token = os.getenv('EBAY_AUTH_TOKEN')  # Legacy Auth'n'Auth token
        self.oauth_token = os.getenv('EBAY_OAUTH_TOKEN')  # OAuth 2.0 token
        self.environment = os.getenv('EBAY_ENVIRONMENT', 'production')
        
        if not all([self.app_id, self.cert_id, self.dev_id, self.auth_token]):
            logger.warning("eBay API credentials not fully configured")
        
        self.api = None
        if self.app_id and self.auth_token:
            try:
                self.api = Trading(
                    appid=self.app_id,
                    certid=self.cert_id,
                    devid=self.dev_id,
                    token=self.auth_token,
                    config_file=None,
                    domain='api.ebay.com' if self.environment == 'production' else 'api.sandbox.ebay.com'
                )
                logger.info(f"eBay API initialized in {self.environment} mode")
            except Exception as e:
                logger.error(f"Failed to initialize eBay API: {e}")
    
    def is_configured(self):
        """Check if eBay API is properly configured"""
        return self.api is not None
    
    def get_active_listings(self, page=1, entries_per_page=100):
        """
        Fetch active eBay listings
        
        Args:
            page (int): Page number (1-indexed)
            entries_per_page (int): Items per page (max 200)
        
        Returns:
            dict: {
                'listings': [...],
                'total': total_count,
                'has_more': bool
            }
        """
        if not self.is_configured():
            logger.error("eBay API not configured")
            return {'listings': [], 'total': 0, 'has_more': False}
        
        try:
            response = self.api.execute('GetMyeBaySelling', {
                'ActiveList': {
                    'Include': True,
                    'Pagination': {
                        'EntriesPerPage': entries_per_page,
                        'PageNumber': page
                    }
                },
                'DetailLevel': 'ReturnAll'
            })
            
            result = response.dict()
            active_list = result.get('ActiveList', {})
            
            # Extract listings
            item_array = active_list.get('ItemArray', {})
            items = item_array.get('Item', [])
            
            # Ensure items is a list
            if not isinstance(items, list):
                items = [items] if items else []
            
            # Parse listings
            listings = []
            for item in items:
                listing_data = self._parse_ebay_item(item)
                listings.append(listing_data)
            
            # Pagination info
            pagination = active_list.get('PaginationResult', {})
            total_entries = int(pagination.get('TotalNumberOfEntries', 0))
            total_pages = int(pagination.get('TotalNumberOfPages', 1))
            
            return {
                'listings': listings,
                'total': total_entries,
                'has_more': page < total_pages,
                'current_page': page,
                'total_pages': total_pages
            }
            
        except ConnectionError as e:
            logger.error(f"eBay API connection error: {e}")
            return {'listings': [], 'total': 0, 'has_more': False}
        except Exception as e:
            logger.error(f"Error fetching eBay listings: {e}")
            return {'listings': [], 'total': 0, 'has_more': False}
    
    # def get_all_active_listings(self):
    #     """
    #     Fetch ALL active eBay listings (all pages)
        
    #     Returns:
    #         list: All active listings
    #     """
    #     all_listings = []
    #     page = 1
        
    #     while True:
    #         result = self.get_active_listings(page=page, entries_per_page=200)
    #         all_listings.extend(result['listings'])

    #         # if page == 5:
    #         #     break
    #         if not result['has_more']:
    #             break
            
    #         page += 1
    #         logger.info(f"Fetched page {page} of eBay listings...")
        
    #     logger.info(f"Total eBay listings fetched: {len(all_listings)}")
    #     return all_listings
    # 
    # 
    # 
    # def get_all_active_listings(self):
    #     """
    #     Fetch ALL active eBay listings with FULL details
        
    #     Returns:
    #         list: All active listings with complete data
    #     """
    #     all_listings = []
    #     page = 1
        
    #     while True:
    #         result = self.get_active_listings(page=page, entries_per_page=200)
            
    #         # Get full details for each listing
    #         for listing in result['listings']:
    #             item_id = listing['item_id']
                
    #             # Call GetItem to get FULL details
    #             full_details = self.get_item_details(item_id)
                
    #             if full_details:
    #                 # Use full details instead of limited data
    #                 all_listings.append(full_details)
    #             else:
    #                 # Fallback to limited data if GetItem fails
    #                 all_listings.append(listing)

    #             # return all_listings
            
    #         if not result['has_more']:
    #             break
            
    #         page += 1
    #         logger.info(f"Fetched page {page} of eBay listings...")
        
    #     logger.info(f"Total eBay listings fetched: {len(all_listings)}")
    #     return all_listings    
    
    
    def get_all_active_listings(self):
        """
        Fetch ALL active eBay listings with BASIC data only
        
        Returns:
            list: All active listings (basic data from GetMyeBaySelling)
        """
        all_listings = []
        page = 1
        
        while True:
            result = self.get_active_listings(page=page, entries_per_page=200)
            all_listings.extend(result['listings'])
            
            if not result['has_more']:
                break
            
            page += 1
            logger.info(f"Fetched page {page} of eBay listings...")
        
        logger.info(f"Total eBay listings fetched: {len(all_listings)}")
        return all_listings
    
    from typing import List,Dict
    def get_listings_by_skus(self, skus: List[str]) -> List[Dict]:
        """
        Get eBay listings for specific SKUs (optimized version)
        Uses GetMyeBaySelling for better performance
        """
        if not skus:
            logger.warning("No SKUs provided")
            return []
        
        logger.info(f"Fetching active listings to match against {len(skus)} SKUs...")
        
        try:
            matching_listings = []
            page = 1
            sku_set = set(skus)  # Faster lookup
            
            while True:

                # if page == 15:
                #     return matching_listings
                
                logger.info(f"Fetching page {page}")
                
                response = self.api.execute('GetMyeBaySelling', {
                    'ActiveList': {
                        'Include': True,
                        'Pagination': {
                            'EntriesPerPage': 200,
                            'PageNumber': page
                        }
                    },
                    'DetailLevel': 'ReturnAll'
                })
                
                result = response.dict()
                active_list = result.get('ActiveList', {})
                
                # Get items
                item_array = active_list.get('ItemArray', {})
                items = item_array.get('Item', [])
                
                if isinstance(items, dict):
                    items = [items]
                
                # Filter by SKU
                for item in items:
                    item_sku = item.get('SKU', '')
                    if item_sku in sku_set:
                        # print("sku item found ✅✅✅ ",item_sku)
                        matching_listings.append(self._parse_ebay_item(item))
                
                # Check pagination
                pagination = active_list.get('PaginationResult', {})
                total_pages = int(pagination.get('TotalNumberOfPages', 1))
                
                logger.info(f"Page {page}/{total_pages}: Found {len(matching_listings)} total matches so far")
                
                if page >= total_pages:
                    break
                
                page += 1
                time.sleep(1)  # Rate limiting
            
            logger.info(f"Found {len(matching_listings)} listings matching {len(skus)} SKUs")
            return matching_listings
            
        except Exception as e:
            logger.error(f"Error fetching listings: {e}")
            logger.exception("Full traceback:")
            return []

    def get_item_details(self, item_id):
        """
        Get detailed information about a specific listing
        
        Args:
            item_id (str): eBay Item ID
        
        Returns:
            dict: Detailed listing information
        """
        if not self.is_configured():
            logger.error("eBay API not configured")
            return None
        
        try:
            response = self.api.execute('GetItem', {
                'ItemID': item_id,
                'DetailLevel': 'ReturnAll'
            })
            
            result = response.dict()
            item = result.get('Item', {})
            
            return self._parse_ebay_item(item)
            
        except ConnectionError as e:
            logger.error(f"eBay API connection error for item {item_id}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching item {item_id}: {e}")
            return None
    
    def _parse_ebay_item_old(self, item):
        """
        Parse eBay item data into our format
        
        Args:
            item (dict): Raw eBay item data
        
        Returns:
            dict: Parsed listing data
        """

        print("here are item details")
        print(item)
        # Extract SKU (Custom Label)
        sku = item.get('SKU', '')
        
        # Extract photos
        picture_details = item.get('PictureDetails', {})
        photo_url = picture_details.get('PictureURL', [])
        if isinstance(photo_url, str):
            photo_url = [photo_url]
        
        # Extract item specifics
        item_specifics_list = item.get('ItemSpecifics', {}).get('NameValueList', [])
        if not isinstance(item_specifics_list, list):
            item_specifics_list = [item_specifics_list] if item_specifics_list else []
        
        item_specifics = {}
        for specific in item_specifics_list:
            name = specific.get('Name', '')
            value = specific.get('Value', '')
            if name and value:
                item_specifics[name] = value
        
        # Extract selling status
        selling_status = item.get('SellingStatus', {})
        current_price = float(selling_status.get('CurrentPrice', {}).get('value', 0))
        
        # Build listing URL
        item_id = item.get('ItemID', '')
        listing_url = f"https://www.ebay.com/itm/{item_id}" if item_id else ''
        
        return {
            'item_id': item_id,
            'sku': sku,
            'title': item.get('Title', ''),
            'description': item.get('Description', ''),
            'current_price': current_price,
            'currency': selling_status.get('CurrentPrice', {}).get('_currencyID', 'USD'),
            'quantity': int(item.get('Quantity', 1)),
            'quantity_sold': int(item.get('SellingStatus', {}).get('QuantitySold', 0)),
            'listing_url': listing_url,
            'photos': photo_url,
            'item_specifics': item_specifics,
            'category_id': item.get('PrimaryCategory', {}).get('CategoryID', ''),
            'condition_id': item.get('ConditionID', ''),
            'condition_display_name': item.get('ConditionDisplayName', ''),
            'listing_type': item.get('ListingType', ''),
            'start_time': item.get('ListingDetails', {}).get('StartTime', ''),
            'end_time': item.get('ListingDetails', {}).get('EndTime', ''),
        }
    
    
    def _parse_ebay_item(self, item):
        """
        Parse eBay item data into our format
        
        Args:
            item (dict): Raw eBay item data
        
        Returns:
            dict: Parsed listing data
        """
        # Extract SKU (Custom Label)
        sku = item.get('SKU', '')
        
        # Extract photos
        picture_details = item.get('PictureDetails', {})
        photo_url = picture_details.get('PictureURL', [])
        if isinstance(photo_url, str):
            photo_url = [photo_url]
        
        # Extract item specifics
        item_specifics_list = item.get('ItemSpecifics', {}).get('NameValueList', [])
        if isinstance(item_specifics_list, dict):
            item_specifics_list = [item_specifics_list]
        elif not isinstance(item_specifics_list, list):
            item_specifics_list = []
        
        item_specifics = {}
        for specific in item_specifics_list:
            if not isinstance(specific, dict):
                continue
            name = specific.get('Name', '')
            value = specific.get('Value', '')
            if name:
                if isinstance(value, list):
                    item_specifics[name] = ', '.join(str(v) for v in value if v)
                elif value:
                    item_specifics[name] = str(value)
        
        # ✨ NEW: Extract Brand from ProductListingDetails (eBay catalog)
        product_details = item.get('ProductListingDetails', {})
        brand_mpn = product_details.get('BrandMPN', {})
        brand = brand_mpn.get('Brand', '')
        
        # Add brand to item_specifics if found and not already there
        if brand and 'Brand' not in item_specifics:
            item_specifics['Brand'] = brand
        
        # Extract selling status
        selling_status = item.get('SellingStatus', {})
        current_price = float(selling_status.get('CurrentPrice', {}).get('value', 0))
        
        # ✨ NEW: Extract full PrimaryCategory data
        primary_category = item.get('PrimaryCategory', {})
        category_id = primary_category.get('CategoryID', '')
        category_name = primary_category.get('CategoryName', '')
        
        # Build listing URL
        item_id = item.get('ItemID', '')
        listing_url = f"https://www.ebay.com/itm/{item_id}" if item_id else ''
        
        return {
            'item_id': item_id,
            'sku': sku,
            'title': item.get('Title', ''),
            'description': strip_html(item.get('Description', '')),
            'current_price': current_price,
            'currency': selling_status.get('CurrentPrice', {}).get('_currencyID', 'USD'),
            'quantity': int(item.get('Quantity', 1)),
            'quantity_sold': int(item.get('SellingStatus', {}).get('QuantitySold', 0)),
            'listing_url': listing_url,
            'photos': photo_url,
            'item_specifics': item_specifics,  # ✅ Now includes Brand from catalog
            'category_id': category_id,
            'category_name': category_name,  # ✨ NEW
            'condition_id': item.get('ConditionID', ''),
            'condition_display_name': item.get('ConditionDisplayName', ''),
            'listing_type': item.get('ListingType', ''),
            'start_time': item.get('ListingDetails', {}).get('StartTime', ''),
            'end_time': item.get('ListingDetails', {}).get('EndTime', ''),
        }
        
    def revise_item_quantity(self, item_id, new_quantity):
        """
        Update the quantity of an existing listing
        
        Args:
            item_id (str): eBay Item ID
            new_quantity (int): New quantity value
        
        Returns:
            bool: Success status
        """
        if not self.is_configured():
            logger.error("eBay API not configured")
            return False
        
        try:
            response = self.api.execute('ReviseItem', {
                'Item': {
                    'ItemID': item_id,
                    'Quantity': new_quantity
                }
            })
            
            logger.info(f"Updated quantity for item {item_id} to {new_quantity}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating quantity for item {item_id}: {e}")
            return False
    
    def end_item(self, item_id, reason='NotAvailable'):
        """
        End an active listing
        
        Args:
            item_id (str): eBay Item ID
            reason (str): Reason for ending ('NotAvailable', 'Incorrect', 'LostOrBroken', 'OtherListingError', 'SellToHighBidder')
        
        Returns:
            bool: Success status
        """
        if not self.is_configured():
            logger.error("eBay API not configured")
            return False
        
        try:
            response = self.api.execute('EndItem', {
                'ItemID': item_id,
                'EndingReason': reason
            })
            
            logger.info(f"Ended listing {item_id} with reason: {reason}")
            return True
            
        except Exception as e:
            logger.error(f"Error ending item {item_id}: {e}")
            return False
        
    
    # new methods added in phaes 2
    def get_sold_listings(self, days_back=30, page=1, entries_per_page=100):
        """
        Fetch sold eBay listings
        
        Args:
            days_back (int): Number of days to look back
            page (int): Page number (1-indexed)
            entries_per_page (int): Items per page (max 200)
        
        Returns:
            dict: {
                'listings': [...],
                'total': total_count,
                'has_more': bool
            }
        """
        if not self.is_configured():
            logger.error("eBay API not configured")
            return {'listings': [], 'total': 0, 'has_more': False}
        
        try:
            # Calculate date range
            from datetime import datetime, timedelta
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=days_back)
            
            response = self.api.execute('GetMyeBaySelling', {
                'SoldList': {
                    'Include': True,
                    'Pagination': {
                        'EntriesPerPage': entries_per_page,
                        'PageNumber': page
                    },
                    'OrderStatusFilter': 'All'
                },
                'DetailLevel': 'ReturnAll'
            })
            
            result = response.dict()
            sold_list = result.get('SoldList', {})
            
            # Extract listings
            order_array = sold_list.get('OrderTransactionArray', {})
            orders = order_array.get('OrderTransaction', [])
            
            # Ensure orders is a list
            if not isinstance(orders, list):
                orders = [orders] if orders else []
            
            # Parse sold listings
            listings = []
            for order in orders:
                listing_data = self._parse_sold_item(order)
                if listing_data:
                    listings.append(listing_data)
            
            
            # Pagination info
            pagination = sold_list.get('PaginationResult', {})
            total_entries = int(pagination.get('TotalNumberOfEntries', 0))
            total_pages = int(pagination.get('TotalNumberOfPages', 1))
            
            return {
                'listings': listings,
                'total': total_entries,
                'has_more': page < total_pages,
                'current_page': page,
                'total_pages': total_pages
            }
            
        except ConnectionError as e:
            logger.error(f"eBay API connection error: {e}")
            return {'listings': [], 'total': 0, 'has_more': False}
        except Exception as e:
            logger.error(f"Error fetching sold listings: {e}")
            return {'listings': [], 'total': 0, 'has_more': False}

    def get_all_sold_listings(self, days_back=30):
        """
        Fetch ALL sold eBay listings (all pages)
        
        Args:
            days_back (int): Number of days to look back
        
        Returns:
            list: All sold listings
        """
        all_listings = []
        page = 1
        
        while True:
            result = self.get_sold_listings(days_back=days_back, page=page, entries_per_page=200)
            all_listings.extend(result['listings'])
            
            if not result['has_more']:
                break
            
            page += 1
            logger.info(f"Fetched page {page} of sold listings...")
        
        logger.info(f"Total sold listings fetched: {len(all_listings)}")
        return all_listings

    def _parse_sold_item(self, order_transaction):
        """
        Parse sold item from eBay order transaction
        
        Args:
            order_transaction (dict): Raw eBay order transaction data
        
        Returns:
            dict: Parsed sold listing data
        """
        try:
            transaction = order_transaction.get('Transaction', {})
            item = transaction.get('Item', {})
            
            # Extract SKU
            sku = item.get('SKU', '')
            
            # Extract sale details
            transaction_price = transaction.get('TransactionPrice', {})
            sale_price = float(transaction_price.get('value', 0))
            
            # Extract sale date
            created_date = transaction.get('CreatedDate', '')
            
            # Extract buyer info (optional)
            buyer = transaction.get('Buyer', {})
            buyer_id = buyer.get('UserID', '')
            
            # Item details
            item_id = item.get('ItemID', '')
            title = item.get('Title', '')
            
            # Quantity sold
            quantity = int(transaction.get('QuantityPurchased', 1))
            
            return {
                'item_id': item_id,
                'sku': sku,
                'title': title,
                'sold_price': sale_price,
                'sold_at': created_date,
                'quantity_sold': quantity,
                'buyer_id': buyer_id,
                'order_id': order_transaction.get('OrderID', ''),
                'transaction_id': transaction.get('TransactionID', '')
            }
        except Exception as e:
            logger.error(f"Error parsing sold item: {e}")
            return None
        

    # IT IS USED WHEN IN SYNC SERVICE WE ONLY CHECK FOR LISTING STATUS OF CURRENRLY RUNNING LISTS ON OUR DASHBOAR/PYTHON SYSTEM

    # IT WILL CHECK EACH LISTING ONE BY ONE AND RETURN DATA REGARDING IF ITS IS SOLD OR NOT.
    def check_listing_status(self, item_id):
        """
        Check current status of a listing
        
        Args:
            item_id (str): eBay Item ID
        
        Returns:
            dict: Status info including if sold
        """
        try:
            item = self.get_item_details(item_id)
            if not item:
                return None
            
            selling_status = item.get('selling_status', {})
            listing_status = item.get('listing_status', 'Unknown')
            quantity_sold = item.get('quantity_sold', 0)
            
            is_sold = (listing_status == 'Completed' and quantity_sold > 0)
            
            return {
                'item_id': item_id,
                'is_sold': is_sold,
                'quantity_sold': quantity_sold,
                'listing_status': listing_status,
                'current_price': item.get('current_price', 0)
            }
        except Exception as e:
            logger.error(f"Error checking listing status: {e}")
            return None

    def get_return_details(self, return_id):
        """
        Get detailed information about a return using eBay Post-Order API v2
        
        NOTE: This uses the Post-Order API v2 with IAF (legacy Auth'n'Auth) token
        Format: Authorization: IAF v^1.1#i^1#I^3#...
        
        Args:
            return_id (str): eBay Return ID
        
        Returns:
            dict: Return details including order number, SKU, item details
        """
        if not self.auth_token:
            logger.error("eBay Auth Token not configured. Post-Order API requires EBAY_AUTH_TOKEN")
            return None
        
        try:
            # Use Post-Order API v2 (REST API)
            # Endpoint: GET /post-order/v2/return/{returnId}
            # Documentation: https://developer.ebay.com/api-docs/post-order/return/resources/return/methods/getReturn
            
            import requests
            
            # Build API URL
            base_url = 'https://api.ebay.com' if self.environment == 'production' else 'https://api.sandbox.ebay.com'
            url = f"{base_url}/post-order/v2/return/{return_id}"
            
            # Headers for IAF (legacy Auth'n'Auth token)
            # Format: Authorization: IAF v^1.1#i^1#I^3#r^1#f^0#p^3#t^...
            headers = {
                'Authorization': f'IAF {self.oauth_token}',  # ✅ Uses IAF format with legacy token
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            # Make REST API call
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return_data = response.json()
                return self._parse_postorder_return_details(return_data)
            elif response.status_code == 404:
                logger.warning(f"Return {return_id} not found via Post-Order API")
                return None
            elif response.status_code == 401:
                logger.error("IAF authentication failed - token may be expired or invalid")
                logger.info("Generate new token at: https://developer.ebay.com/my/auth/?env=production&index=0")
                return None
            else:
                logger.error(f"Post-Order API error: {response.status_code} - {response.text}")
                return None
            
        except ImportError:
            logger.error("requests library not installed. Run: pip install requests")
            return None
        except Exception as e:
            logger.error(f"Error fetching return details for {return_id}: {e}")
            return None
    
    def _get_return_via_disputes_deprecated(self, return_id):
        """
        DEPRECATED: Fallback method using GetUserDisputes (Trading API)
        
        This method uses the deprecated GetUserDisputes call and should only
        be used as a temporary fallback. Will be removed when OAuth 2.0 is implemented.
        
        Args:
            return_id (str): eBay Return ID
        
        Returns:
            dict: Return details or None
        """
        try:
            logger.warning("Using DEPRECATED GetUserDisputes API - migrate to Post-Order API v2")
            
            response = self.api.execute('GetUserDisputes', {
                'DisputeFilterType': 'AllInvolvedDisputes',
                'DisputeSortType': 'None',
                'ModTimeFrom': (datetime.utcnow() - timedelta(days=90)).isoformat(),
                'ModTimeTo': datetime.utcnow().isoformat(),
                'Pagination': {
                    'EntriesPerPage': 200,
                    'PageNumber': 1
                }
            })
            
            result = response.dict()
            disputes = result.get('DisputeArray', {}).get('Dispute', [])
            
            if not isinstance(disputes, list):
                disputes = [disputes] if disputes else []
            
            # Find the specific return
            for dispute in disputes:
                dispute_id = dispute.get('DisputeID', '')
                if dispute_id == return_id:
                    return self._parse_return_details(dispute)
            
            logger.warning(f"Return {return_id} not found in disputes")
            return None
            
        except Exception as e:
            logger.error(f"Error in deprecated GetUserDisputes: {e}")
            return None
    
    
    def _parse_postorder_return_details(self, return_data):
        """
        Parse eBay Post-Order API v2 return data
        
        Args:
            return_data (dict): Raw Post-Order API return data
        
        Returns:
            dict: Parsed return details
        """
        try:
            # Post-Order API v2 response structure
            # Documentation: https://developer.ebay.com/api-docs/post-order/return/resources/return/methods/getReturn
            
            summary = return_data.get('summary', {})
            detail = return_data.get('detail', {})
            
            # Extract basic return info
            return_id = summary.get('returnId', '')
            order_id = summary.get('orderId', '')
            buyer_username = summary.get('buyerLoginName', '')
            
            # Extract item details
            item_detail = detail.get('itemDetail', {})
            item_id = item_detail.get('itemId', '')
            transaction_id = item_detail.get('transactionId', '')
            title = item_detail.get('itemTitle', '')
            item_price = item_detail.get('itemPrice', {}).get('value', 0)
            transaction_date = item_detail.get('transactionDate', {}).get('value', '')
            
            # Extract creation info
            creation_info = summary.get('creationInfo', {})
            reason = creation_info.get('reason', '')
            reason_type = creation_info.get('reasonType', '')
            comments = creation_info.get('comments', '')
            creation_date = creation_info.get('creationDate', {}).get('value', '')
            
            # Extract state and status
            state = summary.get('state', '')
            status = summary.get('status', '')
            return_type = summary.get('currentType', '')
            
            # Extract refund info
            seller_refund = summary.get('sellerTotalRefund', {})
            estimated_refund = seller_refund.get('estimatedRefundAmount', {}).get('value', 0)
            actual_refund = seller_refund.get('actualRefundAmount', {}).get('value', 0)
            
            # Extract shipping info
            return_shipment = detail.get('returnShipmentInfo', {})
            shipment_tracking = return_shipment.get('shipmentTracking', {})
            tracking_number = shipment_tracking.get('trackingNumber', '')
            carrier_name = shipment_tracking.get('carrierName', '')
            delivery_status = shipment_tracking.get('deliveryStatus', '')
            actual_ship_date = shipment_tracking.get('actualShipDate', {}).get('value', '')
            actual_delivery_date = shipment_tracking.get('actualDeliveryDate', {}).get('value', '')
            
            # Extract label cost
            label_cost_info = return_shipment.get('shippingLabelCost', {})
            label_cost = label_cost_info.get('totalAmount', {}).get('value', 0)
            
            # Extract close info
            close_info = detail.get('closeInfo', {})
            return_close_reason = close_info.get('returnCloseReason', '')
            buyer_close_reason = close_info.get('buyerCloseReason', '')
            close_date = close_info.get('returnCloseDate', {}).get('value', '')
            
            return {
                'return_id': return_id,
                'order_id': order_id,
                'item_id': item_id,
                'transaction_id': transaction_id,
                'sku': '',  # SKU not directly in Post-Order API response
                'title': title,
                'buyer_username': buyer_username,
                'dispute_reason': reason,
                'reason_type': reason_type,
                'buyer_comment': comments,
                'dispute_state': state,
                'dispute_status': status,
                'return_type': return_type,
                'item_price': item_price,
                'estimated_refund': estimated_refund,
                'actual_refund': actual_refund,
                'tracking_number': tracking_number,
                'carrier_name': carrier_name,
                'delivery_status': delivery_status,
                'actual_ship_date': actual_ship_date,
                'actual_delivery_date': actual_delivery_date,
                'label_cost': label_cost,
                'return_close_reason': return_close_reason,
                'buyer_close_reason': buyer_close_reason,
                'created_time': creation_date,
                'transaction_date': transaction_date,
                'closed_time': close_date,
                'modified_time': ''  # Not in this response
            }
        except Exception as e:
            logger.error(f"Error parsing Post-Order API return details: {e}")
            logger.exception("Full traceback:")
            return None
    
    def _get_return_via_orders(self, return_id):
        """
        Alternative method to get return info by searching recent orders
        
        Args:
            return_id (str): eBay Return ID
        
        Returns:
            dict: Return details or None
        """
        try:
            # Get recent sold items (last 90 days)
            sold_items = self.get_all_sold_listings(days_back=90)
            
            # Search for return ID in order details
            # Note: This is a fallback and may not always work
            logger.info(f"Searching {len(sold_items)} sold items for return {return_id}")
            
            # Return None for now - this would require additional API calls
            # to check each order for return status
            return None
            
        except Exception as e:
            logger.error(f"Error in alternative return lookup: {e}")
            return None
    
    def _parse_return_details(self, dispute):
        """
        Parse eBay dispute/return data
        
        Args:
            dispute (dict): Raw eBay dispute data
        
        Returns:
            dict: Parsed return details
        """
        try:
            # Extract item details
            item = dispute.get('Item', {})
            transaction = dispute.get('Transaction', {})
            
            return {
                'return_id': dispute.get('DisputeID', ''),
                'order_id': dispute.get('OrderLineItemID', ''),
                'item_id': item.get('ItemID', ''),
                'sku': item.get('SKU', ''),
                'title': item.get('Title', ''),
                'transaction_id': transaction.get('TransactionID', ''),
                'buyer_username': dispute.get('OtherPartyName', ''),
                'dispute_reason': dispute.get('DisputeReason', ''),
                'dispute_state': dispute.get('DisputeState', ''),
                'created_time': dispute.get('DisputeCreatedTime', ''),
                'modified_time': dispute.get('DisputeModifiedTime', ''),
            }
        except Exception as e:
            logger.error(f"Error parsing return details: {e}")
            return None

# Singleton instance
ebay_api = eBayAPI()
