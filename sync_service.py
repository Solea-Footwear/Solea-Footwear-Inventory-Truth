"""
Sync Service
Matches eBay listings with inventory units and creates alerts for mismatches
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from database import (
    Unit, Listing, ListingUnit, Product, Channel, 
    SyncLog, Alert, ListingTemplate
)
from ebay_api import ebay_api
from datetime import datetime
from sqlalchemy import and_

logger = logging.getLogger(__name__)


class SyncService:
    """
    Service for syncing eBay listings with inventory
    """
    
    def __init__(self, db: Session):
        self.db = db
    
    def sync_ebay_listings(self):
        """
        Main sync function - fetches eBay listings and matches to units
        
        Returns:
            dict: Sync results
        """
        logger.info("Starting eBay sync...")
        
        # Create sync log
        ebay_channel = self.db.query(Channel).filter(Channel.name == 'ebay').first()
        if not ebay_channel:
            logger.error("eBay channel not found in database")
            return {'success': False, 'error': 'eBay channel not configured'}
        
        sync_log = SyncLog(
            channel_id=ebay_channel.id,
            sync_type='active_listings',
            status='running',
            started_at=datetime.utcnow()
        )
        self.db.add(sync_log)
        self.db.commit()


        sync_log_id = sync_log.id  # ✨ Store ID before closing session
        
        try:
            # Fetch all active eBay listings
            # ebay_listings = ebay_api.get_all_active_listings()

            # ! new code
            # ✨ NEW: Get all SKUs from database
            broken_units = (
                self.db.query(Unit)
                .outerjoin(ListingTemplate, ListingTemplate.product_id == Unit.product_id)
                .filter(Unit.status == "listed")
                .filter(
                    (ListingTemplate.id == None) |
                    (ListingTemplate.photos == None) |
                    (ListingTemplate.category_mappings == None)
                )
                .limit(750)
                .all()
            )
            
            our_skus = [unit.unit_code for unit in broken_units if unit.unit_code]
            
            logger.info(f"REPAIR MODE: refreshing {len(our_skus)} broken/missing-template SKUs")
            
            logger.info(f"Found {len(our_skus)} SKUs in database")
            
            if not our_skus:
                logger.warning("No SKUs in database to sync")
                return {'synced': 0, 'errors': []}
            
            
            # ! 1/21/2026  
            # ✨ NEW: Close DB session before long eBay fetch
            logger.info("Closing DB session before eBay fetch (prevents timeout)...")
            self.db.commit()
            self.db.close()
            # ! 1/21/2026  
            
            # ✨ NEW: Fetch ONLY listings matching our SKUs
            ebay_listings = ebay_api.get_listings_by_skus(our_skus)
            logger.info(f"Found {len(ebay_listings)} matching eBay listings")


            # ! 1/21/2026  
            # ✨ NEW: Reconnect to database after fetch
            logger.info("Reopening database session...")
            from database import SessionLocal
            self.db = SessionLocal()
            # ! 1/21/2026  


            # ! 1/21/2026  
            # Re-fetch objects with new session
            ebay_channel = self.db.query(Channel).filter(Channel.name == 'ebay').first()
            sync_log = self.db.query(SyncLog).filter(SyncLog.id == sync_log_id).first()
            # ! 1/21/2026  



            # ! end of new code

            
            if not ebay_listings:
                logger.warning("No eBay listings fetched")
                sync_log.status = 'completed'
                sync_log.completed_at = datetime.utcnow()
                sync_log.records_processed = 0
                self.db.commit()
                return {'success': True, 'message': 'No listings to process'}
            
            # Process each listing
            results = {
                'processed': 0,
                'matched': 0,
                'created': 0,
                'updated': 0,
                'unmatched_skus': [],
                'missing_skus': [],
                'errors': []
            }
            
            for ebay_item in ebay_listings:
                try:
                    result = self._process_ebay_listing(ebay_item, ebay_channel.id)
                    results['processed'] += 1
                    
                    if result['matched']:
                        results['matched'] += 1
                    if result['created']:
                        results['created'] += 1
                    if result['updated']:
                        results['updated'] += 1
                    if result['unmatched_sku']:
                        results['unmatched_skus'].append(result['unmatched_sku'])
                    if result['missing_sku']:
                        results['missing_skus'].append(ebay_item['item_id'])
                        
                except Exception as e:
                    logger.error(f"Error processing listing {ebay_item.get('item_id')}: {e}")
                    results['errors'].append({
                        'item_id': ebay_item.get('item_id'),
                        'error': str(e)
                    })
            
            # Update sync log
            sync_log.status = 'completed'
            sync_log.completed_at = datetime.utcnow()
            sync_log.records_processed = results['processed']
            sync_log.records_updated = results['updated']
            sync_log.records_created = results['created']
            sync_log.errors = results['errors'] if results['errors'] else None
            
            self.db.commit()
            
            # Create alerts for issues
            self._create_sync_alerts(results)
            
            logger.info(f"Sync completed: {results['matched']} matched, {results['created']} created, {results['updated']} updated")
            
            return {
                'success': True,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            sync_log.status = 'failed'
            sync_log.completed_at = datetime.utcnow()
            sync_log.errors = [{'error': str(e)}]
            self.db.commit()
            
            return {
                'success': False,
                'error': str(e)
            }
    
    def _process_ebay_listing(self, ebay_item, channel_id):
        """
        Process a single eBay listing
        
        Args:
            ebay_item (dict): eBay listing data
            channel_id (uuid): Channel ID
        
        Returns:
            dict: Processing result
        """
        result = {
            'matched': False,
            'created': False,
            'updated': False,
            'unmatched_sku': None,
            'missing_sku': False
        }
        
        item_id = ebay_item['item_id']
        sku = ebay_item.get('sku', '').strip()
        
        # Check if SKU exists
        if not sku:
            # logger.warning(f"Listing {item_id} has no SKU")
            result['missing_sku'] = True
            return result
        
        # Find unit by SKU (unit_code)
        unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
        
        if not unit:
            # logger.warning(f"No unit found for SKU: {sku} (eBay Item: {item_id})")
            result['unmatched_sku'] = sku
            return result
        
        # ✨ SKU MATCHED! Now fetch FULL details from eBay
        logger.info(f"SKU matched: {sku}, fetching full details for item {item_id}")
        full_details = ebay_api.get_item_details(item_id)

        
        if full_details:
            # Use full details instead of basic data
            ebay_item = full_details
            
            # ✨ NEW: Use AI to parse item specifics for cross-listing
            from ai_parser_service import AIParserService
            
            try:
                ai_parser = AIParserService()
                
                # Prepare data for AI parsing
                listing_for_ai = {
                    'title': ebay_item['title'],
                    'description': ebay_item['description'],
                    'ebay_condition': ebay_item.get('condition_display_name', ''),
                    'category_name': ebay_item.get('category_name', ''),  # ✅ Now available!
                    'brand': ebay_item.get('item_specifics', {}).get('Brand', '')  # ✅ Now available!
                }
                
                # Get AI-parsed data
                try:
                    ai_parsed = ai_parser.parse_listing_for_crosslisting(listing_for_ai)
                except Exception as e:
                    logger.warning(f"AI timeout/failure for {item_id}, using fallback: {e}")
                    ai_parsed = {
                        "item_specifics": ebay_item.get("item_specifics", {}),
                        "poshmark": {},
                        "mercari": {}
                    }
                                
                # Update ebay_item with AI-extracted item_specifics
                ebay_item['item_specifics'] = ai_parsed.get('item_specifics', {})
                
                # Store platform-specific data for later use
                ebay_item['poshmark_data'] = ai_parsed.get('poshmark', {})
                ebay_item['mercari_data'] = ai_parsed.get('mercari', {})
                
                logger.info(f"AI extracted item specifics: {list(ebay_item['item_specifics'].keys())}")
                
            except Exception as e:
                logger.error(f"AI parsing failed for {item_id}: {e}")
                
        else:
            # Fallback to basic data if GetItem fails
            logger.warning(f"Could not fetch full details for {item_id}, using basic data")
        
        # Unit found - match successful
        result['matched'] = True
        
        # Check if listing already exists
        existing_listing = self.db.query(Listing).filter(
            Listing.channel_listing_id == item_id,
            Listing.channel_id == channel_id
        ).first()
        
        if existing_listing:
            # Update existing listing
            existing_listing.title = ebay_item['title']
            existing_listing.description = ebay_item['description']
            existing_listing.current_price = ebay_item['current_price']
            existing_listing.listing_url = ebay_item['listing_url']
            existing_listing.photos = ebay_item['photos']
            existing_listing.item_specifics = ebay_item['item_specifics']
            existing_listing.status = 'active'
            existing_listing.updated_at = datetime.utcnow()
            
            result['updated'] = True

            ebay_category_data = {
                'category_id': ebay_item.get('category_id', ''),
                'category_name': ebay_item.get('category_name', '')
            }

            self._create_listing_template(
                ebay_item,
                unit.product_id,
                ebay_category_data
            )
            
            # Check if unit is linked
            existing_link = self.db.query(ListingUnit).filter(
                ListingUnit.listing_id == existing_listing.id,
                ListingUnit.unit_id == unit.id
            ).first()
            
            if not existing_link:
                # Create link
                link = ListingUnit(
                    listing_id=existing_listing.id,
                    unit_id=unit.id
                )
                self.db.add(link)
            
        else:
            # Create new listing
            new_listing = Listing(
                product_id=unit.product_id,
                channel_id=channel_id,
                channel_listing_id=item_id,
                title=ebay_item['title'],
                description=ebay_item['description'],
                current_price=ebay_item['current_price'],
                listing_url=ebay_item['listing_url'],
                status='active',
                mode='single_quantity',  # Default
                photos=ebay_item['photos'],
                item_specifics=ebay_item['item_specifics']
            )
            self.db.add(new_listing)
            self.db.flush()  # Get the ID
            
            # Link to unit
            link = ListingUnit(
                listing_id=new_listing.id,
                unit_id=unit.id
            )
            self.db.add(link)
            
            result['created'] = True


            # Create/update listing template with eBay category data
            ebay_category_data = {
                'category_id': ebay_item.get('category_id', ''),
                 'category_name': ebay_item.get('category_name', '')
            }
            self._create_listing_template(ebay_item, unit.product_id, ebay_category_data)
                        
            # Create/update listing template
            # self._create_listing_template(ebay_item,unit.product_id)
        
        # Update unit status to 'listed' if not already
        if unit.status == 'ready_to_list':
            unit.status = 'listed'
        
        self.db.commit()
        
        return result
    
    # def _create_listing_template(self, product_id, channel_id, ebay_item):
    #     """
    #     Create or update listing template from eBay data
        
    #     Args:
    #         product_id (uuid): Product ID
    #         channel_id (uuid): Channel ID
    #         ebay_item (dict): eBay listing data
    #     """
    #     # Check if template already exists
    #     template = self.db.query(ListingTemplate).filter(
    #         ListingTemplate.product_id == product_id,
    #         ListingTemplate.source_channel_id == channel_id
    #     ).first()
        
    #     if template:
    #         # Update existing template
    #         template.title = ebay_item['title']
    #         template.description = ebay_item['description']
    #         template.photos = ebay_item['photos']
    #         template.item_specifics = ebay_item['item_specifics']
    #         template.base_price = ebay_item['current_price']
    #         template.updated_at = datetime.utcnow()
    #     else:
    #         # Create new template
    #         template = ListingTemplate(
    #             product_id=product_id,
    #             source_channel_id=channel_id,
    #             title=ebay_item['title'],
    #             description=ebay_item['description'],
    #             photos=ebay_item['photos'],
    #             item_specifics=ebay_item['item_specifics'],
    #             base_price=ebay_item['current_price']
    #         )
    #         self.db.add(template)
        
    #     self.db.commit()
   
    

    # def _create_listing_template(self, listing, product_id):
    #     """
    #     Create or update enhanced listing template
        
    #     Args:
    #         listing (dict): Parsed eBay listing data
    #         product_id (uuid): Product ID
    #     """
    #     from template_service import TemplateService
        
    #     # Use TemplateService for enhanced template creation
    #     template_service = TemplateService(self.db)
        
    #     # Get channel ID
    #     ebay_channel = self.db.query(Channel).filter(Channel.name == 'ebay').first()
        
    #     # Create enhanced template
    #     template = template_service.create_enhanced_template(
    #         product_id=product_id,
    #         listing_data=listing, # listing data / ebay listing item data that our sync gets
    #         channel_id=ebay_channel.id if ebay_channel else None
    #     )
        
    #     logger.info(f"Enhanced template created for product {product_id}, validated: {template.is_validated}")
        
    #     return template



    def _create_listing_template(self, listing, product_id,ebay_category_data=None):
        """Create or update enhanced listing template"""
        from template_service import TemplateService
        
        # Get channel ID
        ebay_channel = self.db.query(Channel).filter(Channel.name == 'ebay').first()
        
        # Transform eBay data to template format
        formatted_data = {
            'title': listing.get('title', ''),
            'description': listing.get('description', ''),
            'photos': listing.get('photos', []),
            'item_specifics': listing.get('item_specifics', {}),
            'current_price': listing.get('current_price', 0),
            'poshmark_data': listing.get('poshmark_data', {}),  # NEW: AI-parsed Poshmark data
        'mercari_data': listing.get('mercari_data', {})     # NEW: AI-parsed Mercari data
        }
        
        # Create template
        template_service = TemplateService(self.db)
        template = template_service.create_enhanced_template(
            product_id=product_id,
            listing_data=formatted_data,
            channel_id=ebay_channel.id if ebay_channel else None,
                    ebay_category_data=ebay_category_data  # NEW: Pass eBay category info
        )
        
        return template


    """
    ADD THIS NEW METHOD to sync_service.py for template refresh:
    """

    def refresh_templates(self):
        """
        Refresh all templates from current eBay listings
        Updates templates with latest eBay data
        
        Returns:
            dict: Refresh results
        """
        from template_service import TemplateService
        
        logger.info("Refreshing listing templates...")
        
        template_service = TemplateService(self.db)
        
        # Get all active listings
        listings = self.db.query(Listing).filter(
            Listing.status == 'active'
        ).all()
        
        results = {
            'processed': 0,
            'updated': 0,
            'errors': []
        }
        
        for listing in listings:
            try:
                # Get fresh eBay data
                fresh_data = ebay_api.get_item_details(listing.channel_listing_id)
                
                if fresh_data:
                    # Update template with fresh data
                    template = template_service.create_enhanced_template(
                        product_id=listing.product_id,
                        listing_data=fresh_data,
                        channel_id=listing.channel_id
                    )
                    
                    results['updated'] += 1
                
                results['processed'] += 1
                
            except Exception as e:
                logger.error(f"Error refreshing template for listing {listing.id}: {e}")
                results['errors'].append({
                    'listing_id': str(listing.id),
                    'error': str(e)
                })
        
        logger.info(f"Template refresh complete: {results['updated']} updated")
        
        return results

    
    def _create_sync_alerts(self, results):
        """Create alerts with separate database session"""
        from database import SessionLocal  # Import at top of file
        
        # Create NEW session (separate from main sync)
        alert_db = SessionLocal()
        
        try:
            # Alert for missing SKUs
            if results['missing_skus']:
                alert = Alert(
                    alert_type='missing_sku',
                    severity='warning',
                    title=f"{len(results['missing_skus'])} eBay listings missing SKU",
                    message=f"eBay Item IDs: {', '.join(results['missing_skus'][:10])}{'...' if len(results['missing_skus']) > 10 else ''}",
                    is_resolved=False
                )
                alert_db.add(alert)
            
            # Alert for unmatched SKUs
            if results['unmatched_skus']:
                alert = Alert(
                    alert_type='unmatched_sku',
                    severity='error',
                    title=f"{len(results['unmatched_skus'])} SKUs not found in inventory",
                    message=f"SKUs: {', '.join(results['unmatched_skus'][:10])}{'...' if len(results['unmatched_skus']) > 10 else ''}",
                    is_resolved=False
                )
                alert_db.add(alert)
            
            # Alert for errors
            if results['errors']:
                alert = Alert(
                    alert_type='sync_error',
                    severity='critical',
                    title=f"{len(results['errors'])} errors during sync",
                    message=f"Check sync logs for details",
                    is_resolved=False
                )
                alert_db.add(alert)
            
            alert_db.commit()  # Commit on separate session
            
        except Exception as e:
            logger.error(f"Failed to save alerts: {e}")
            alert_db.rollback()
        finally:
            alert_db.close()  # Always close
    
    def check_units_without_listings(self):
        """
        Find units that are marked as 'listed' but have no eBay listing
        
        Returns:
            list: Units without listings
        """
        units = self.db.query(Unit).filter(
            Unit.status == 'listed'
        ).all()
        
        units_without_listings = []
        
        for unit in units:
            # Check if unit has any active listing
            has_listing = self.db.query(ListingUnit).join(Listing).filter(
                ListingUnit.unit_id == unit.id,
                Listing.status == 'active'
            ).first()
            
            if not has_listing:
                units_without_listings.append(unit)
        
        if units_without_listings:
            alert = Alert(
                alert_type='unit_without_listing',
                severity='warning',
                title=f"{len(units_without_listings)} units marked as 'listed' but no active listing found",
                message=f"Units: {', '.join([u.unit_code for u in units_without_listings[:10]])}",
                is_resolved=False
            )
            self.db.add(alert)
            self.db.commit()
        
        return units_without_listings
    

    """
    SYNC SERVICE ENHANCEMENTS FOR SOLD ITEM DETECTION

    Add these methods to the SyncService class in sync_service.py:
    """

    def sync_sold_items(self):
        """
        Sync sold items from eBay and update inventory
        
        Returns:
            dict: Sync results
        """
        logger.info("Starting sold items sync...")
        
        ebay_channel = self.db.query(Channel).filter(Channel.name == 'ebay').first()
        if not ebay_channel:
            logger.error("eBay channel not found")
            return {'success': False, 'error': 'eBay channel not configured'}
        
        # Create sync log
        sync_log = SyncLog(
            channel_id=ebay_channel.id,
            sync_type='sold_items',
            status='running',
            started_at=datetime.utcnow()
        )
        self.db.add(sync_log)
        self.db.commit()
        
        try:
            # Fetch sold listings from last 30 days
            sold_listings = ebay_api.get_all_sold_listings(days_back=30)
            
            results = {
                'processed': 0,
                'updated': 0,
                'not_found': [],
                'errors': []
            }
            
            for sold_item in sold_listings:
                try:
                    result = self._process_sold_item(sold_item, ebay_channel.id)
                    results['processed'] += 1
                    
                    if result['updated']:
                        results['updated'] += 1
                    if result['not_found']:
                        results['not_found'].append(sold_item['sku'])
                        
                except Exception as e:
                    logger.error(f"Error processing sold item {sold_item.get('item_id')}: {e}")
                    results['errors'].append({
                        'item_id': sold_item.get('item_id'),
                        'error': str(e)
                    })
            
            # Update sync log
            sync_log.status = 'completed'
            sync_log.completed_at = datetime.utcnow()
            sync_log.records_processed = results['processed']
            sync_log.records_updated = results['updated']
            sync_log.errors = results['errors'] if results['errors'] else None
            
            self.db.commit()
            
            logger.info(f"Sold items sync completed: {results['updated']} updated")
            
            return {
                'success': True,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"Sold items sync failed: {e}")
            sync_log.status = 'failed'
            sync_log.completed_at = datetime.utcnow()
            sync_log.errors = [{'error': str(e)}]
            self.db.commit()
            
            return {
                'success': False,
                'error': str(e)
            }

    def _process_sold_item(self, sold_item, channel_id):
        """
        Process a single sold item
        
        Args:
            sold_item (dict): Sold item data from eBay
            channel_id (uuid): Channel ID
        
        Returns:
            dict: Processing result
        """
        result = {
            'updated': False,
            'not_found': False
        }
        
        sku = sold_item.get('sku', '').strip()
        
        if not sku:
            # logger.warning(f"Sold item {sold_item['item_id']} has no SKU")
            result['not_found'] = True
            return result
        
        # Find unit by SKU
        unit = self.db.query(Unit).filter(Unit.unit_code == sku).first()
        
        if not unit:
            # logger.warning(f"No unit found for sold SKU: {sku}")
            result['not_found'] = True
            return result
        
        # Check if already marked as sold
        if unit.status == 'sold' and unit.sold_at:
            logger.debug(f"Unit {sku} already marked as sold")
            return result
        
        # Parse sold date
        try:
            sold_at = datetime.fromisoformat(sold_item['sold_at'].replace('Z', '+00:00'))
        except:
            sold_at = datetime.utcnow()
        
        # Update unit
        unit.status = 'sold'
        unit.sold_at = sold_at
        unit.sold_price = sold_item['sold_price']
        unit.sold_platform = 'ebay'
        
        # Update listing
        listing = self.db.query(Listing).filter(
            Listing.channel_listing_id == sold_item['item_id'],
            Listing.channel_id == channel_id
        ).first()
        
        if listing:
            listing.status = 'sold'
            listing.sold_at = sold_at
            listing.sold_price = sold_item['sold_price']
            listing.ended_at = sold_at
        
        result['updated'] = True
        
        self.db.commit()
        
        logger.info(f"Marked unit {sku} as sold: ${sold_item['sold_price']} on {sold_at}")
        
        return result

   
    def check_active_listings_for_sold(self):
        """
        Check currently active listings to see if any sold
        (Quick check without full sold items sync)
        
        Returns:
            dict: Results
        """
        logger.info("Checking active listings for sold items...")
        
        # Get all active listings
        active_listings = self.db.query(Listing).filter(
            Listing.status == 'active'
        ).all()
        
        results = {
            'checked': 0,
            'found_sold': 0,
            'updated': []
        }
        
        for listing in active_listings:
            results['checked'] += 1
            
            # Check status on eBay
            status = ebay_api.check_listing_status(listing.channel_listing_id)
            
            if status and status['is_sold']:
                # Get unit
                listing_unit = self.db.query(ListingUnit).filter(
                    ListingUnit.listing_id == listing.id
                ).first()
                
                if listing_unit and listing_unit.unit:
                    unit = listing_unit.unit
                    
                    # Update unit
                    unit.status = 'sold'
                    unit.sold_at = datetime.utcnow()
                    unit.sold_price = status.get('current_price', listing.current_price)
                    unit.sold_platform = 'ebay'
                    
                    # Update listing
                    listing.status = 'sold'
                    listing.sold_at = datetime.utcnow()
                    listing.sold_price = status.get('current_price', listing.current_price)
                    
                    results['found_sold'] += 1
                    results['updated'].append(unit.unit_code)
                    
                    logger.info(f"Detected sold item: {unit.unit_code}")
        
        self.db.commit()
        
        logger.info(f"Checked {results['checked']} listings, found {results['found_sold']} sold")
        
        return results






