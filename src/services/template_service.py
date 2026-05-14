"""
Template Service for Enhanced Listing Templates
Handles template creation, validation, and platform-specific formatting
"""
import logging
import re
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

class TemplateService:
    """Service for managing enhanced listing templates"""
    
    def __init__(self, db):
        self.db = db
    
    def create_enhanced_template(self, product_id, listing_data, channel_id=None, ebay_category_data=None):
        """
        Create or update enhanced listing template
        
        Args:
            product_id (uuid): Product ID
            listing_data (dict): eBay listing data
            channel_id (uuid): Channel ID (optional)
        
        Returns:
            ListingTemplate: Created/updated template
        """
        from src.backend.db.database import ListingTemplate
        
        # Check if template exists
        template = self.db.query(ListingTemplate).filter(
            ListingTemplate.product_id == product_id
        ).first()
        
        if not template:
            template = ListingTemplate(product_id=product_id)
            self.db.add(template)
        
        # Basic fields
        # template.channel_id = channel_id
        template.source_channel_id = channel_id
        template.title = listing_data.get('title', '')
        template.description = listing_data.get('description', '')
        template.photos = listing_data.get('photos', [])
        template.item_specifics = listing_data.get('item_specifics', {})
        template.base_price = listing_data.get('current_price', 0)
        
        # Enhanced fields
        template.photo_metadata = self._create_photo_metadata(listing_data.get('photos', []))
        template.pricing = self._calculate_platform_pricing(listing_data.get('current_price', 0))
        # template.category_mappings = self._map_categories(listing_data.get('item_specifics', {}))
        template.category_mappings = self._map_categories(
            listing_data.get('item_specifics', {}),
            ebay_category_data,
            listing_data.get('poshmark_data', {}),  # AI-parsed Poshmark categories
            listing_data.get('mercari_data', {})    # AI-parsed Mercari categories
        )
        template.seo_keywords = self._extract_keywords(
            listing_data.get('title', ''),
            listing_data.get('description', '')
        )
        template.template_version = 2
        template.last_synced_at = datetime.utcnow()
        
        # Validate template
        validation_result = self.validate_template(template)
        template.is_validated = validation_result['valid']
        template.validation_errors = validation_result.get('errors')
        
        self.db.commit()
        
        logger.info(f"Enhanced template created for product {product_id}, validated: {template.is_validated}")
        
        return template
    
    def _create_photo_metadata(self, photos: List[str]) -> Dict:
        """
        Create photo metadata
        
        Args:
            photos (list): List of photo URLs
        
        Returns:
            dict: Photo metadata
        """
        if not photos:
            return {
                'count': 0,
                'primary': 0,
                'validated': False
            }
        
        return {
            'count': len(photos),
            'primary': 0,  # First photo is primary
            'validated': len(photos) >= 1,
            'urls': photos[:12]  # Max 12 photos for most platforms
        }
    
    def _calculate_platform_pricing(self, base_price: float) -> Dict:
        """
        Calculate platform-specific pricing
        
        Args:
            base_price (float): Base eBay price
        
        Returns:
            dict: Platform pricing
        """
        if not base_price or base_price <= 0:
            return {}
        
        # Platform fee adjustments
        return {
            'ebay': round(base_price, 2),
            'poshmark': round(base_price * 0.97, 2),  # 3% lower (20% Poshmark fee vs 13% eBay)
            'mercari': round(base_price * 0.93, 2),   # 7% lower (10% fee vs 13% eBay + lower expectations)
            'shopify': round(base_price * 1.00, 2),   # Same price
            'suggested_range': {
                'min': round(base_price * 0.85, 2),
                'max': round(base_price * 1.15, 2)
            }
        }
    
    # def _map_categories(self, item_specifics: Dict) -> Dict:
    #     """
    #     Map eBay categories to other platforms
        
    #     Args:
    #         item_specifics (dict): eBay item specifics
        
    #     Returns:
    #         dict: Category mappings
    #     """
    #     # Extract brand and category info
    #     brand = item_specifics.get('Brand', '').lower()
    #     category = item_specifics.get('Type', '').lower()
        
    #     # Basic category mapping (expand this based on your inventory)
    #     mappings = {
    #         'ebay': item_specifics.get('PrimaryCategoryID', ''),
    #         'ebay_path': item_specifics.get('PrimaryCategoryName', '')
    #     }
        
    #     # Poshmark mapping (example)
    #     if 'nike' in brand or 'adidas' in brand or 'jordan' in brand:
    #         mappings['poshmark'] = 'Men > Shoes > Athletic Shoes'
    #         mappings['mercari'] = "Men's Shoes > Sneakers"
    #     elif 'boot' in category:
    #         mappings['poshmark'] = 'Men > Shoes > Boots'
    #         mappings['mercari'] = "Men's Shoes > Boots"
    #     else:
    #         mappings['poshmark'] = 'Men > Shoes'
    #         mappings['mercari'] = "Men's Shoes"
        
    #     return mappings
    
    
    def _map_categories(self, item_specifics: Dict, ebay_category_data: Dict = None, 
                   poshmark_data: Dict = None, mercari_data: Dict = None) -> Dict:
        """Map eBay categories to other platforms"""
        
        mappings = {}
        
        # eBay category
        if ebay_category_data:
            mappings['ebay'] = ebay_category_data.get('category_id', '')
            mappings['ebay_path'] = ebay_category_data.get('category_name', '')
        
        # Poshmark - simplified path
        if poshmark_data and 'category' in poshmark_data:
            posh_cat = poshmark_data['category']
            category_parts = [
                posh_cat.get('level_1', ''),
                posh_cat.get('level_2', ''),
                posh_cat.get('level_3', '')
            ]
            mappings['poshmark'] = ' > '.join([p for p in category_parts if p and p != 'None'])
            
            # ✨ NEW: Store full Poshmark data
            mappings['poshmark_data'] = poshmark_data
        
        # Mercari - simplified path
        if mercari_data and 'category' in mercari_data:
            merc_cat = mercari_data['category']
            category_parts = [
                merc_cat.get('level_1', ''),
                merc_cat.get('level_2', ''),
                merc_cat.get('level_3', '')
            ]
            mappings['mercari'] = ' > '.join([p for p in category_parts if p and p != 'None'])
            
            # ✨ NEW: Store full Mercari data
            mappings['mercari_data'] = mercari_data
        
        return mappings

    def _extract_keywords(self, title: str, description: str) -> List[str]:
        """
        Extract SEO keywords from title and description
        
        Args:
            title (str): Listing title
            description (str): Listing description
        
        Returns:
            list: Keywords
        """
        # Combine text
        text = f"{title} {description}".lower()
        
        # Remove special characters
        text = re.sub(r'[^a-z0-9\s]', ' ', text)
        
        # Split into words
        words = text.split()
        
        # Common stop words to exclude
        stop_words = {
            'the', 'and', 'for', 'with', 'this', 'that', 'from', 'have', 
            'been', 'will', 'your', 'their', 'what', 'when', 'where', 
            'which', 'there', 'these', 'those', 'about', 'into', 'through'
        }
        
        # Extract keywords (length > 3, not stop words, unique)
        keywords = []
        seen = set()
        
        for word in words:
            if len(word) > 3 and word not in stop_words and word not in seen:
                keywords.append(word)
                seen.add(word)
                
                if len(keywords) >= 20:  # Limit to 20 keywords
                    break
        
        return keywords
    
    def validate_template(self, template) -> Dict:
        """
        Validate template has all required fields
        
        Args:
            template (ListingTemplate): Template to validate
        
        Returns:
            dict: Validation result
        """
        errors = []
        
        # Check title
        if not template.title or len(template.title) < 10:
            errors.append("Title too short or missing (minimum 10 characters)")
        
        # Check description
        if not template.description or len(template.description) < 50:
            errors.append("Description too short or missing (minimum 50 characters)")
        
        # Check photos
        if not template.photos or len(template.photos) < 1:
            errors.append("At least 1 photo required")
        
        # Check price
        if not template.base_price or template.base_price <= 0:
            errors.append("Invalid price")
        
        # Check item specifics
        if not template.item_specifics:
            errors.append("Item specifics missing")
        else:
            # Check for important specifics
            required_specifics = ['Brand', 'Size']
            missing_specifics = [s for s in required_specifics if s not in template.item_specifics]
            if missing_specifics:
                errors.append(f"Missing item specifics: {', '.join(missing_specifics)}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors if errors else None
        }
    
    def get_template_for_platform(self, template_id, platform: str) -> Dict:
        """
        Get template formatted for specific platform
        
        Args:
            template_id (uuid): Template ID
            platform (str): Platform name (poshmark, mercari, shopify)
        
        Returns:
            dict: Platform-formatted template
        """
        from src.backend.db.database import ListingTemplate
        
        template = self.db.query(ListingTemplate).filter(
            ListingTemplate.id == template_id
        ).first()
        
        if not template:
            return None
        
        # Get platform-specific price
        # in template.pricig json is stored
        # like this {"ebay": 150, "poshmark": 145} and we can get specific platofrm price if not key exists then it returns template base price 
        price = template.pricing.get(platform, template.base_price)
        
        # Get platform-specific category
        category = template.category_mappings.get(platform, '')
        
        # Format for platform
        formatted = {
            'title': template.title,
            'description': template.description,
            'price': price,
            'photos': template.photos[:12],  # Most platforms limit photos
            'category': category,
            'keywords': template.seo_keywords[:10] if template.seo_keywords else [],
            'item_specifics': template.item_specifics
        }
        
        # Platform-specific adjustments
        if platform == 'poshmark':
            # Poshmark has character limits
            formatted['title'] = template.title[:80]
            formatted['description'] = template.description[:500]
        
        elif platform == 'mercari':
            # Mercari formatting
            formatted['title'] = template.title[:40]
            formatted['description'] = template.description[:1000]
        
        return formatted
    
    def bulk_validate_templates(self) -> Dict:
        """
        Validate all templates in database
        
        Returns:
            dict: Validation summary
        """
        from src.backend.db.database import ListingTemplate
        
        templates = self.db.query(ListingTemplate).all()
        
        results = {
            'total': len(templates),
            'valid': 0,
            'invalid': 0,
            'updated': []
        }
        
        for template in templates:
            validation_result = self.validate_template(template)
            template.is_validated = validation_result['valid']
            template.validation_errors = validation_result.get('errors')
            
            if template.is_validated:
                results['valid'] += 1
            else:
                results['invalid'] += 1
            
            results['updated'].append({
                'product_id': str(template.product_id),
                'is_validated': template.is_validated,
                'errors': validation_result.get('errors')
            })
        
        self.db.commit()
        
        logger.info(f"Bulk validation complete: {results['valid']} valid, {results['invalid']} invalid")
        
        return results