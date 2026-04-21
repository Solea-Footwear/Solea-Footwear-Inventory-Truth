"""
Delist Service - Main Delisting Coordinator
Handles delisting logic when items sell on any platform
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional
from delisting.gmail_service import GmailService

logger = logging.getLogger(__name__)


# class Colors:
#     RED = '\033[91m'
#     YELLOW = '\033[93m'
#     BOLD = '\033[1m'
#     END = '\033[0m'


class Colors:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    CYAN = '\033[96m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'

class DelistService:
    """Main service for coordinating cross-platform delisting"""
    
    def __init__(self, db):
        self.db = db
    
    def process_sale(self, parsed_email: Dict) -> Dict:
        """
        Process a sale and delist from other platforms
        
        Args:
            parsed_email (dict): Parsed email data with SKU, platform, price
        
        Returns:
            dict: Processing results
        """
        logger.info(f"Processing sale from {parsed_email.get('platform')}")
        
        results = {
            'success': False,
            'unit_found': False,
            'unit_updated': False,
            'listings_found': [],
            'delisted': [],
            'errors': []
        }
        
        try:
            # Find unit by SKU or title
            unit = self._find_unit(parsed_email)
            
            if not unit:


                #  ! just for logging
                email_subject = parsed_email.get('title', 'Unknown subject')[:100]
                sku = parsed_email.get('sku', 'N/A')
                order_id = parsed_email.get('order_id', 'N/A')
                platform = parsed_email.get('platform', 'unknown')
                
                # Print to terminal in RED
                print(f"\n{Colors.RED}{Colors.BOLD}{'='*80}{Colors.END}")
                print(f"{Colors.RED}{Colors.BOLD}❌ UNIT NOT FOUND ❌{Colors.END}")
                print(f"{Colors.YELLOW}Subject:{Colors.END} {email_subject}")
                print(f"{Colors.YELLOW}SKU:{Colors.END} {sku} | {Colors.YELLOW}Order:{Colors.END} {order_id} | {Colors.YELLOW}Platform:{Colors.END} {platform}")
                print(f"{Colors.RED}{Colors.BOLD}{'='*80}{Colors.END}\n")
                
                # Also log to file
                logger.warning(f"Unit not found: SKU={sku}, Order={order_id}, Subject={email_subject}")

                # ! end of logging


                # ! Move Email TO "EBAY SALES NOT IN SYSTEM" LABEL
                message_id = parsed_email.get('message_id')
                if message_id:
                    try:
                        gmail = GmailService()
                        success = gmail.move_to_label(
                            message_id,
                            'eBay Sales Not In System',
                            remove_inbox=True  # Archive it
                        )
                        if success:
                            logger.info("📧 Email moved to 'eBay Sales Not In System' label and archived")
                        else:
                            logger.warning("⚠️  Failed to move email to label")
                    except Exception as e:
                        logger.error(f"Error moving email to label: {e}")
                else:
                    logger.warning("No message_id in parsed_email, cannot move to label")


                results['errors'].append('Unit not found by SKU or title')
                logger.warning(f"Unit not found: SKU={parsed_email.get('sku')}, Title={parsed_email.get('title')}")
                return results
            
            results['unit_found'] = True
            results['unit_code'] = unit.unit_code
            
            # Update unit status
            self._update_unit_sold(unit, parsed_email)
            results['unit_updated'] = True
            
            # Find all listings for this unit
            listings = self._find_unit_listings(unit.id)
            results['listings_found'] = [str(l.id) for l in listings]
            
            logger.info(f"Found {len(listings)} listings for unit {unit.unit_code}")
            
            # Process each listing
            sold_platform = parsed_email.get('platform')
            
            for listing in listings:
                try:
                    listing_platform = self._get_listing_platform(listing)
                    
                    if listing_platform == sold_platform:
                        # This is the platform where it sold - just update status
                        self._update_listing_sold(listing, parsed_email)
                        logger.info(f"Updated {listing_platform} listing as sold")
                    else:
                        # This is another platform - delist it
                        delist_result = self._delist_from_platform(listing, listing_platform)
                        
                        if delist_result['success']:
                            self._update_listing_ended(listing)
                            results['delisted'].append({
                                'platform': listing_platform,
                                'listing_id': str(listing.id),
                                'status': 'delisted'
                            })
                            logger.info(f"Delisted from {listing_platform}")
                        else:
                            # Delist failed - check if it's because listing already ended
                            error_msg = str(delist_result.get('error', '')).lower()
                            
                            # eBay error codes for already-ended listings
                            already_ended_indicators = [
                                'already been closed',
                                'already closed',
                                'auction has been closed',
                                'listing has ended',
                                'no longer available'
                            ]
                            
                            is_already_ended = any(indicator in error_msg for indicator in already_ended_indicators)
                            
                            if is_already_ended:
                                # Listing already ended on platform, sync database to match
                                logger.warning(f"{listing_platform} listing already ended on platform, syncing database status")
                                self._update_listing_ended(listing)
                                results['delisted'].append({
                                    'platform': listing_platform,
                                    'listing_id': str(listing.id),
                                    'status': 'sync_ended'
                                })
                            else:
                                results['errors'].append({
                                    'platform': listing_platform,
                                    'error': delist_result.get('error')
                                })
                                logger.error(f"Failed to delist from {listing_platform}: {delist_result.get('error')}")
                
                except Exception as e:
                    logger.error(f"Error processing listing {listing.id}: {e}")
                    results['errors'].append({
                        'listing_id': str(listing.id),
                        'error': str(e)
                    })
            
            self.db.commit()
            results['success'] = True
            
            logger.info(f"Sale processed: Unit {unit.unit_code}, Delisted from {len(results['delisted'])} platforms")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error processing sale: {e}")
            results['errors'].append(str(e))
        
        return results
    
    # def _find_unit(self, parsed_email: Dict) -> Optional[object]:
    #     """
    #     Find unit by SKU or title matching
        
    #     Args:
    #         parsed_email (dict): Parsed email data
        
    #     Returns:
    #         Unit: Unit object or None
    #     """
    #     from database import Unit, Product
        
    #     sku = parsed_email.get('sku')
    #     title = parsed_email.get('title', '')
        
    #     # Try exact SKU match first
    #     if sku:
    #         unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
    #         if unit:
    #             logger.debug(f"Found unit by exact SKU: {sku}")
    #             return unit
        
    #     # Try partial SKU match (in case email has extra chars)
    #     if sku and len(sku) > 3:
    #         unit = self.db.query(Unit).filter(Unit.unit_code.like(f'%{sku}%')).first()
    #         if unit:
    #             logger.debug(f"Found unit by partial SKU: {sku}")
    #             return unit
        
    #     # Try title matching (find product then unit)
    #     if title:
    #         # Extract keywords from title
    #         keywords = title.lower().split()[:5]  # First 5 words
            
    #         for keyword in keywords:
    #             if len(keyword) > 3:  # Skip short words
    #                 products = self.db.query(Product).filter(
    #                     (Product.brand.ilike(f'%{keyword}%')) |
    #                     (Product.model.ilike(f'%{keyword}%'))
    #                 ).all()
                    
    #                 for product in products:
    #                     # Find unit for this product that's currently listed
    #                     unit = self.db.query(Unit).filter(
    #                         Unit.product_id == product.id,
    #                         Unit.status == 'listed'
    #                     ).first()
                        
    #                     if unit:
    #                         logger.debug(f"Found unit by title matching: {title}")
    #                         return unit
        
    #     return None


    # def _find_unit(self, parsed_email: Dict) -> Optional[object]:
    #     """
    #     Find unit by listing ID or order ID
        
    #     Args:
    #         parsed_email (dict): Parsed email data
        
    #     Returns:
    #         Unit: Unit object or None
    #     """
    #     from database import Unit, Listing, ListingUnit
        
    #     platform = parsed_email.get('platform')
    #     listing_id = parsed_email.get('listing_id')
    #     order_id = parsed_email.get('order_id')
        
    #     # Method 1: Find by listing_id (Poshmark, Mercari)
    #     if listing_id:
    #         listing = self.db.query(Listing).filter(
    #             Listing.channel_listing_id == listing_id
    #         ).first()
            
    #         if listing:
    #             # Get unit from listing
    #             listing_unit = self.db.query(ListingUnit).filter(
    #                 ListingUnit.listing_id == listing.id
    #             ).first()
                
    #             if listing_unit:
    #                 unit = self.db.query(Unit).filter(
    #                     Unit.id == listing_unit.unit_id
    #                 ).first()
                    
    #                 if unit:
    #                     logger.debug(f"Found unit by listing_id: {unit.unit_code}")
    #                     return unit
        
    #     # Method 2: Find by order_id (eBay) - fetch item_id from API
    #     if platform == 'ebay' and order_id:
    #         print("Platform is ebay and here is order id",order_id)
    #         item_id = self._get_ebay_item_from_order(order_id)
    #         print("here is item id ",item_id)
            
    #         if item_id:
    #             listing = self.db.query(Listing).filter(
    #                 Listing.channel_listing_id == item_id
    #             ).first()
                
    #             if listing:
    #                 listing_unit = self.db.query(ListingUnit).filter(
    #                     ListingUnit.listing_id == listing.id
    #                 ).first()
                    
    #                 if listing_unit:
    #                     unit = self.db.query(Unit).filter(
    #                         Unit.id == listing_unit.unit_id
    #                     ).first()
                        
    #                     if unit:
    #                         logger.debug(f"Found unit by eBay order_id: {unit.unit_code}")
    #                         return unit
        
    #     # Method 3: Fallback to SKU (if available)
    #     sku = parsed_email.get('sku')
    #     if sku:
    #         unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
    #         if unit:
    #             logger.debug(f"Found unit by SKU: {sku}")
    #             return unit
        
    #     logger.warning(f"Unit not found for platform={platform}, listing_id={listing_id}, order_id={order_id}")
    #     return None


    def _find_unit(self, parsed_email: Dict) -> Optional[object]:
        """
        Find unit by listing ID or order ID
        Handles "Sell Similar" by falling back to SKU and updating listing ID
        """
        from database import Unit, Listing, ListingUnit, Channel
        
        platform = parsed_email.get('platform')
        listing_id = parsed_email.get('listing_id')
        order_id = parsed_email.get('order_id')
        
        # Method 1: Find by listing_id (Poshmark, Mercari)
        if listing_id:
            listing = self.db.query(Listing).filter(
                Listing.channel_listing_id == listing_id
            ).first()
            
            if listing:
                listing_unit = self.db.query(ListingUnit).filter(
                    ListingUnit.listing_id == listing.id
                ).first()
                
                if listing_unit:
                    unit = self.db.query(Unit).filter(
                        Unit.id == listing_unit.unit_id
                    ).first()
                    
                    if unit:
                        logger.debug(f"Found unit by listing_id: {unit.unit_code}")
                        return unit
        
        # Method 2: Find by order_id (eBay) with SKU fallback for "Sell Similar"
        if platform == 'ebay' and order_id:
            logger.debug(f"Platform is ebay, order_id={order_id}")
            
            # Get BOTH item_id and SKU from eBay API
            ebay_data = self._get_ebay_item_from_order(order_id)
            
            if ebay_data:
                item_id = ebay_data.get('item_id')
                sku = ebay_data.get('sku')
                
                logger.debug(f"eBay order data: item_id={item_id}, sku={sku}")
                
                # Try to find listing by item_id first
                listing = self.db.query(Listing).filter(
                    Listing.channel_listing_id == item_id
                ).first()
                
                if listing:
                    # Found by item_id (normal case)
                    listing_unit = self.db.query(ListingUnit).filter(
                        ListingUnit.listing_id == listing.id
                    ).first()
                    
                    if listing_unit:
                        unit = self.db.query(Unit).filter(
                            Unit.id == listing_unit.unit_id
                        ).first()
                        
                        if unit:
                            logger.debug(f"Found unit by eBay item_id: {unit.unit_code}")
                            return unit
                
                # NOT FOUND by item_id - Try fallback to SKU (handles "Sell Similar")
                if sku:
                    logger.info(f"Item ID {item_id} not found, trying SKU fallback: {sku}")
                    
                    unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
                    
                    if unit:
                        logger.info(f"✓ Found unit by SKU: {unit.unit_code}")
                        
                        # Find the old eBay listing for this unit
                        old_listing = self.db.query(Listing).join(ListingUnit).join(Channel).filter(
                            ListingUnit.unit_id == unit.id,
                            Channel.name == 'eBay',
                            # Listing.status == 'active'
                        ).first()
                        
                        if old_listing:
                            old_item_id = old_listing.channel_listing_id
                            logger.warning(f"🔄 Detected 'Sell Similar': Updating listing from {old_item_id} → {item_id}")
                            
                            # Update listing with new item ID
                            old_listing.channel_listing_id = item_id
                            old_listing.updated_at = datetime.utcnow()
                            
                            # Store in parsed_email so we process the updated listing
                            parsed_email['_updated_listing'] = True
                        
                        return unit
                    else:
                        logger.warning(f"SKU {sku} not found in database - listing may be very old or never synced")
        
        # Method 3: Fallback to SKU (if available in parsed_email)
        sku = parsed_email.get('sku')
        if sku:
            unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
            if unit:
                logger.debug(f"Found unit by SKU: {sku}")
                return unit
        
        logger.warning(f"Unit not found for platform={platform}, listing_id={listing_id}, order_id={order_id}, sku={sku}")
        return None

    # def _get_ebay_item_from_order(self, order_id: str) -> Optional[str]:
    #     """
    #     Get eBay item ID from order ID using API
        
    #     Args:
    #         order_id (str): eBay order ID
        
    #     Returns:
    #         str: Item ID or None
    #     """
    #     try:
    #         from ebay_api import ebay_api
            
    #         # Call eBay GetOrders API
    #         response = ebay_api.api.execute('GetOrders', {
    #             'OrderIDArray': {'OrderID': order_id}
    #         })
            
    #         # Extract item ID from response
    #         if response.reply.OrderArray:
    #             order = response.reply.OrderArray.Order[0]
    #             if order.TransactionArray:
    #                 transaction = order.TransactionArray.Transaction[0]
    #                 item_id = transaction.Item.ItemID
    #                 logger.debug(f"Found eBay item_id {item_id} for order {order_id}")
    #                 return item_id
            
    #         return None
            
    #     except Exception as e:
    #         logger.error(f"Error fetching eBay item from order {order_id}: {e}")
    #         return None
    

    def _get_ebay_item_from_order(self, order_id: str) -> Optional[Dict]:
        """
        Get eBay item ID and SKU from order ID using API
        
        Args:
            order_id (str): eBay order ID
        
        Returns:
            dict: {'item_id': str, 'sku': str} or None
        """
        try:
            from ebay_api import ebay_api
            
            # Call eBay GetOrders API
            response = ebay_api.api.execute('GetOrders', {
                'OrderIDArray': {'OrderID': order_id}
            })
            
            # Extract item ID and SKU from response
            if response.reply.OrderArray:
                order = response.reply.OrderArray.Order[0]
                if order.TransactionArray:
                    transaction = order.TransactionArray.Transaction[0]
                    item_id = transaction.Item.ItemID
                    sku = transaction.Item.SKU if hasattr(transaction.Item, 'SKU') else None
                    
                    logger.debug(f"Found eBay order {order_id}: item_id={item_id}, sku={sku}")
                    return {
                        'item_id': item_id,
                        'sku': sku
                    }
            
            return None
            
        except Exception as e:
            logger.error(f"Error fetching eBay item from order {order_id}: {e}")
            return None
    
    def _update_unit_sold(self, unit: object, parsed_email: Dict):
        """Update unit as sold"""
        unit.status = 'sold'
        unit.sold_at = datetime.utcnow()
        unit.sold_price = parsed_email.get('price')
        unit.sold_platform = parsed_email.get('platform')
        
        logger.debug(f"Unit {unit.unit_code} marked as sold")
    
    def _find_unit_listings(self, unit_id) -> List:
        """Find all active listings for a unit"""
        from database import Listing, ListingUnit
        
        listings = self.db.query(Listing).join(ListingUnit).filter(
            ListingUnit.unit_id == unit_id,
            Listing.status == 'active'
        ).all()
        
        return listings
    
    def _get_listing_platform(self, listing: object) -> str:
        """Get platform name for a listing"""
        from database import Channel
        
        if listing.channel_id:
            channel = self.db.query(Channel).filter(Channel.id == listing.channel_id).first()
            if channel:
                return channel.name.lower()
        
        return 'unknown'
    
    def _update_listing_sold(self, listing: object, parsed_email: Dict):
        """Update listing as sold (platform where it sold)"""
        listing.status = 'sold'
        listing.sold_at = datetime.utcnow()
        listing.sold_price = parsed_email.get('price')
        listing.ended_at = datetime.utcnow()
        
        logger.debug(f"Listing {listing.id} marked as sold")
    
    def _update_listing_ended(self, listing: object):
        """Update listing as ended (delisted from other platform)"""
        listing.status = 'ended'
        listing.ended_at = datetime.utcnow()
        
        logger.debug(f"Listing {listing.id} marked as ended")
    

    def _delist_from_platform(self, listing: object, platform: str) -> Dict:
        """
        Delist from specific platform
        
        Args:
            listing: Listing object
            platform (str): Platform name
        
        Returns:
            dict: Delist result
        """

        # SKIP POSHMARK (account not active)
        # if platform == 'poshmark':
        #     logger.warning(f"Skipping Poshmark delisting (account inactive)")
        #     return {
        #         'success': True,  # Return success so listing gets marked as 'ended' in database
        #         'skipped': True,
        #         'reason': 'Poshmark account inactive'
        #     }
    
        if platform == 'ebay':
            print(f"delisting from {platform}")
            print(listing.channel_listing_id)
            from delisting.ebay_delist import delist_ebay_item
            return delist_ebay_item(listing.channel_listing_id)
        
        elif platform in ['poshmark', 'mercari']:
        # elif platform in ['mercari',"pos"]:
            print(f"delisting from {platform}")
            print(listing.channel_listing_id)
            from delisting.selenium_delist import delist_item
            return delist_item(platform, listing.channel_listing_id)
        
        else:
            return {
                'success': False,
                'error': f'Unknown platform: {platform}'
            }
        

# import sys
# import os

# # Add parent directory to path so we can import database
# sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# # Now import from database
# from database import (
#     get_db, init_db, Product, Unit, Location, Category, 
#     ConditionGrade, Listing, Channel, Alert, SyncLog,
#     ListingTemplate
# )

# if __name__ == "__main__":
#     db = next(get_db())
#     service = DelistService(db)  # Need to pass db to DelistService
#     result = service._get_ebay_item_from_order("07-14263-04801")
#     print(f"Item ID: {result}")
#     db.close()