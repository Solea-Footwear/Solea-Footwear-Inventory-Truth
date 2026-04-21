"""
Cross-listing Service
Main coordinator for automated listing creation on multiple platforms
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class CrosslistService:
    """Service for managing cross-listing to multiple platforms"""
    
    def __init__(self, db):
        self.db = db
    

    def check_and_crosslist(self, unit_id) -> Dict:
        """
        Check if unit needs cross-listing and create listings
        
        Args:
            unit_id: Unit UUID
        
        Returns:
            dict: Cross-listing results
        """
        from database import Unit, Listing, ListingUnit, Channel, ListingTemplate
        
        logger.info(f"Checking cross-listing for unit {unit_id}")
        
        results = {
            'unit_id': str(unit_id),
            'needs_crosslisting': False,
            'platforms_to_list': [],
            'created_listings': [],
            'errors': []
        }
        
        try:
            # Get unit
            unit = self.db.query(Unit).filter(Unit.id == unit_id).first()
            
            if not unit:
                results['errors'].append('Unit not found')
                return results
            
            # Check if unit is listed (status = 'listed')
            if unit.status != 'listed':
                logger.debug(f"Unit {unit.unit_code} not listed yet, skipping cross-listing")
                return results
            
            # Get all active listings for this unit
            # active_listings = self.db.query(Listing).join(ListingUnit).filter(
            #     ListingUnit.unit_id == unit.id,
            #     Listing.status == 'active'
            # ).all()
            
            # Get channels for each listing
            # listed_platforms = []
            # for listing in active_listings:
            #     if listing.channel_id:
            #         channel = self.db.query(Channel).filter(
            #             Channel.id == listing.channel_id
            #         ).first()
            #         if channel:
            #             listed_platforms.append(channel.name.lower())
            
            listed_platforms = []
            for listing_unit in unit.listing_units:
                listing = listing_unit.listing
                if listing and listing.status == 'active' and listing.channel:
                    listed_platforms.append(listing.channel.name.lower())
            
            logger.debug(f"Unit {unit.unit_code} currently listed on: {listed_platforms}")
            
            # Determine which platforms need listings
            # target_platforms = ['ebay', 'poshmark']
            target_platforms = ['ebay','poshmark']
            platforms_to_list = [p for p in target_platforms if p not in listed_platforms]
            
            if not platforms_to_list:
                logger.debug(f"Unit {unit.unit_code} already listed on all platforms")
                return results
            
            results['needs_crosslisting'] = True
            results['platforms_to_list'] = platforms_to_list
            
            # Get listing template
            template = self.db.query(ListingTemplate).filter(
                ListingTemplate.product_id == unit.product_id
            ).first()
            
            if not template:
                results['errors'].append('No listing template found')
                return results
            
            if not template.is_validated:
                results['errors'].append('Template not validated')
                return results
            
            
            # Create listings on each platform
            for platform in platforms_to_list:
                if platform == 'ebay':
                    # eBay is already listed (that's how we got here)
                    continue
                
                try:
                    listing_result = self._create_listing_on_platform(
                        unit=unit,
                        template=template,
                        platform=platform
                    )
                    
                    if listing_result['success']:
                        results['created_listings'].append({
                            'platform': platform,
                            'listing_id': listing_result['listing_id'],
                            'channel_listing_id': listing_result['channel_listing_id']
                        })
                        logger.info(f"Successfully created {platform} listing for unit {unit.unit_code}")
                    else:
                        results['errors'].append({
                            'platform': platform,
                            'error': listing_result.get('error')
                        })
                        logger.error(f"Failed to create {platform} listing: {listing_result.get('error')}")
                
                except Exception as e:
                    logger.error(f"Error creating {platform} listing: {e}")
                    results['errors'].append({
                        'platform': platform,
                        'error': str(e)
                    })
            
            self.db.commit()
            
            logger.info(f"Cross-listing complete for unit {unit.unit_code}: {len(results['created_listings'])} listings created")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error in cross-listing: {e}")
            results['errors'].append(str(e))
        
        return results
    
    def _create_listing_on_platform(self, unit, template, platform: str) -> Dict:
        """
        Create listing on specific platform
        
        Args:
            unit: Unit object
            template: ListingTemplate object
            platform (str): 'poshmark' or 'mercari'
        
        Returns:
            dict: Creation result with listing_id and channel_listing_id
        """
        from database import Listing, ListingUnit, Channel
        from crosslisting.image_handler import ImageHandler
        
        # Get platform-specific template data
        platform_data = self._format_for_platform(template, platform)
        
        # Download images
        image_handler = ImageHandler()
        local_images = image_handler.download_images(template.photos)
        
        if not local_images:
            return {
                'success': False,
                'error': 'Failed to download images'
            }
        
        # Create listing using Selenium
        if platform == 'poshmark':
            from crosslisting.poshmark_lister import PoshmarkLister
            lister = PoshmarkLister()
            result = lister.create_listing(platform_data, local_images)
        
        elif platform == 'mercari':
            from crosslisting.mercari_lister import MercariLister
            lister = MercariLister()
            result = lister.create_listing(platform_data, local_images)
        
        else:
            return {
                'success': False,
                'error': f'Unknown platform: {platform}'
            }
        
        # Clean up downloaded images
        image_handler.cleanup(local_images)
        
        if not result['success']:
            return result
        
        # Save listing to database
        channel = self.db.query(Channel).filter(
            Channel.name == platform
        ).first()
        
        if not channel:
            return {
                'success': False,
                'error': f'Channel {platform} not found in database'
            }
        
        # Create listing record
        listing = Listing(
            channel_id=channel.id,
            product_id=unit.product_id,
            channel_listing_id=result['channel_listing_id'],
            title=platform_data['title'],
            description=platform_data['description'],
            current_price=platform_data['price'],
            status='active',
            created_at=datetime.utcnow()
        )
        
        self.db.add(listing)
        self.db.flush()  # Get listing ID
        
        # Link listing to unit
        listing_unit = ListingUnit(
            listing_id=listing.id,
            unit_id=unit.id
        )
        
        self.db.add(listing_unit)
        
        logger.debug(f"Created {platform} listing in database: {listing.id}")
        
        return {
            'success': True,
            'listing_id': str(listing.id),
            'channel_listing_id': result['channel_listing_id']
        }
    
    # def _format_for_platform(self, template, platform: str) -> Dict:
    #     """
    #     Format template data for specific platform
        
    #     Args:
    #         template: ListingTemplate object
    #         platform (str): Platform name
        
    #     Returns:
    #         dict: Platform-formatted listing data
    #     """
    #     # Get platform-specific pricing
    #     # price = template.pricing.get(platform, template.base_price)

    #     # Platform-specific pricing
    #     if platform == 'poshmark':
    #         # Same price as eBay, NO free shipping
    #         price = template.base_price
    #         shipping = 'buyer_pays'  # Buyer pays shipping
    #     elif platform == 'mercari':
    #         # Same price as eBay, FREE shipping
    #         price = template.base_price
    #         shipping = 'free'  # Seller pays shipping
    #     else:
    #         price = template.base_price
    #         shipping = None
        
    #     # Get platform-specific category
    #     category = template.category_mappings.get(platform, '')
        
    #     # Format title (platform-specific limits)
    #     title = template.title
    #     if platform == 'poshmark':
    #         title = title[:80]  # Poshmark 80 char limit
    #     elif platform == 'mercari':
    #         title = title[:40]  # Mercari 40 char limit
        
    #     # Format description
    #     description = template.description
    #     if platform == 'poshmark':
    #         description = description[:500]  # Poshmark 500 char limit
    #     elif platform == 'mercari':
    #         description = description[:1000]  # Mercari 1000 char limit
        
    #     return {
    #         'title': title,
    #         'description': description,
    #         'price': price,
    #         'shipping': shipping,  # ✅ Platform-specific shipping
    #         'photos': template.photos,
    #         'category': category,
    #         'item_specifics': template.item_specifics,
    #         'sku': template.product.sku_prefix if hasattr(template, 'product') else None
    #     }
    
    
    def _format_for_platform(self, template, platform: str) -> Dict:
        # Platform-specific pricing
        if platform == 'poshmark':
            price = template.base_price
            shipping = 'buyer_pays'
        elif platform == 'mercari':
            price = template.base_price
            shipping = 'free'
        else:
            price = template.base_price
            shipping = None
        
        # Format title (platform-specific limits)
        title = template.title
        if platform == 'poshmark':
            title = title[:80]
        elif platform == 'mercari':
            title = title[:80]
        
        # Format description
        description = template.description
        if platform == 'poshmark':
            description = description[:500]
        elif platform == 'mercari':
            description = description[:1000]
        
        # ✨ NEW: Get AI-parsed platform data from category_mappings
        platform_data_key = f'{platform}_data'
        platform_specifics = template.category_mappings.get(platform_data_key, {})
        
        # Build formatted data
        formatted_data = {
            'title': title,
            'description': description,
            'price': price,
            'shipping': shipping,
            'photos': template.photos,
            'sku': template.product.sku_prefix if hasattr(template, 'product') else None
        }
        
        # ✨ Add AI-parsed platform-specific fields
        if platform_specifics:
            # Direct mapping: Just pass the AI data
            if 'category' in platform_specifics:
                formatted_data['category'] = platform_specifics['category']  # Dict with level_1, level_2, level_3
            
            if 'condition' in platform_specifics:
                formatted_data['condition'] = platform_specifics['condition']
            
            if 'size' in platform_specifics:
                formatted_data['size'] = platform_specifics['size']
            
            if 'brand' in platform_specifics:
                formatted_data['brand'] = platform_specifics['brand']
            
            if 'color' in platform_specifics:
                formatted_data['color'] = platform_specifics['color']
            
            logger.info(f"✓ Using AI data for {platform}: category={formatted_data.get('category')}, brand={formatted_data.get('brand')}")
        else:
            # Fallback
            logger.warning(f"⚠ No AI data for {platform}, using fallback")
            formatted_data['item_specifics'] = template.item_specifics
            formatted_data['category'] = template.category_mappings.get(platform, '')
        
        return formatted_data
    
    
    def bulk_crosslist(self, unit_ids: List) -> Dict:
        """
        Cross-list multiple units at once
        
        Args:
            unit_ids (list): List of unit UUIDs
        
        Returns:
            dict: Bulk results
        """
        results = {
            'total': len(unit_ids),
            'processed': 0,
            'created': 0,
            'errors': []
        }
        
        for unit_id in unit_ids:
            try:
                result = self.check_and_crosslist(unit_id)
                results['processed'] += 1
                results['created'] += len(result.get('created_listings', []))
                
                if result.get('errors'):
                    results['errors'].extend(result['errors'])

                # ADD DELAY between listings
                import time
                time.sleep(60)  # Wait 60 seconds between each unit
                    
            except Exception as e:
                logger.error(f"Error cross-listing unit {unit_id}: {e}")
                results['errors'].append({
                    'unit_id': str(unit_id),
                    'error': str(e)
                })
        
        logger.info(f"Bulk cross-listing complete: {results['created']} listings created")
        
        return result