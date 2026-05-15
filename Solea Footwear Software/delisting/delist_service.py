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
    

    def _find_unit(self, parsed_email: Dict) -> Optional[object]:
        """
        Find unit by SKU first, then listing ID.
        SKU is the primary durable key for delisting.
        """
        from database import Unit, Listing, ListingUnit

        platform = parsed_email.get('platform')
        listing_id = parsed_email.get('listing_id')
        order_id = parsed_email.get('order_id')

        # Normalize SKU
        raw_sku = parsed_email.get('sku')
        sku = str(raw_sku).strip().upper() if raw_sku else None

        # ✅ Method 1: SKU FIRST (MOST IMPORTANT)
        if sku:
            unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
            if unit:
                logger.info(f"✓ Found unit by SKU FIRST: {sku}")
                return unit

        # Method 2: listing_id fallback
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

        logger.warning(
            f"Unit not found for platform={platform}, listing_id={listing_id}, order_id={order_id}, sku={sku}"
        )
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
    
  
    def _update_unit_sold(self, unit: object, parsed_email: Dict):
        """Update unit as sold.

        R1-01: store the marketplace order number on the unit so a future return
        email carrying the same order number can be matched back to this unit.
        We accept either 'order_id' (eBay parser) or 'order_number' (other
        platforms) keys to stay forward-compatible.
        """
        unit.status = 'sold'
        unit.sold_at = datetime.utcnow()
        unit.sold_price = parsed_email.get('price')
        unit.sold_platform = parsed_email.get('platform')

        order_number = parsed_email.get('order_id') or parsed_email.get('order_number')
        if order_number:
            unit.external_order_number = str(order_number)
            logger.debug(f"Unit {unit.unit_code} marked sold (order={order_number})")
        else:
            logger.debug(f"Unit {unit.unit_code} marked sold (no order number in email)")
    
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
