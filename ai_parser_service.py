"""
AI-Based Item Specifics Parser
Uses Claude API to extract structured data from eBay listings for cross-listing to Poshmark and Mercari
"""
import os
import json
import logging
import re
from typing import Dict
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


class AIParserService:
    """Parse eBay listing data using AI for platform-specific formatting"""
    
    def __init__(self):
        self.api_key = os.getenv('ANTHROPIC_API_KEY')
        
        # Platform-specific data structures
        self.platform_specs = {
            'poshmark': {
                'categories': {
                    'level_1': ['Women', 'Men', 'Kids', 'Home', 'Pets', 'Electronics'],
                    'level_2_men': ['Accessories', 'Bags', 'Jackets & Coats', 'Jeans', 'Pants', 'Shirts', 'Shoes', 'Shorts', 'Suits & Blazers', 'Sweaters', 'Swim', 'Underwear & Socks', 'Grooming', 'Global & Traditional Wear', 'Other'],
                    'level_2_women': ['Dresses', 'Tops & blouses', 'Sweaters', 'Jeans', 'Pants', 'Skirts', 'Coats & jackets', 'Suits & blazers', 'Athletic apparel', 'Swimwear', "Women's handbags", "Women's accessories", 'Jewelry', 'Maternity', 'Shoes', 'Underwear', 'Shorts', 'Sleepwear & robes', 'Kimono / Yukata', 'School Uniform', 'Other'],
                    'level_2_kids': ['Girls accessories', 'Girls bottoms', 'Girls coats & jackets', 'Girls dresses', 'Girls one-pieces', 'Girls shoes', 'Girls swimwear', 'Girls tops & t-shirts', 'Girls other', 'Boys accessories', 'Boys bottoms', 'Boys coats & jackets', 'Boys one-pieces', 'Boys swimwear', 'Boys shoes', 'Boys tops & t-shirts', 'Boys other'],
                    'level_3_men_shoes': ['Athletic Shoes', 'Boat Shoes', 'Boots', 'Chukka Boots', 'Cowboy & Western Boots', 'Loafers & Slip-Ons', 'Oxfords & Derbys', 'Rain & Snow Boots', 'Sandals & Flip-Flops', 'Sneakers', 'None'],
                    'level_3_women_shoes': ['Ankle Boots & Booties', 'Athletic Shoes', 'Combat & Moto Boots', 'Espadrilles', 'Flats & Loafers', 'Heeled Boots', 'Heels', 'Lace Up Boots', 'Moccasins', 'Mules & Clogs', 'Over the Knee Boots', 'Platforms', 'Sandals', 'Slippers', 'Sneakers', 'Wedges', 'Winter & Rain Boots', 'None'],
                    'level_3_kids_shoes': ['Baby & Walker', 'Boots', 'Dress Shoes', 'Moccasins', 'Rain & Snow Boots', 'Sandals & Flip Flops', 'Slippers', 'Sneakers', 'Water Shoes', 'None']
                },
                'conditions': ['New With Tags (NWT)', 'New Without Tags (NWOT)', 'Like New', 'Good', 'Fair'],
                'colors': ['Red', 'Pink', 'Orange', 'Yellow', 'Green', 'Blue', 'Purple', 'Gold', 'Silver', 'Black', 'Gray', 'White', 'Cream', 'Brown', 'Tan']
            },
            'mercari': {
                'categories': {
                    'level_1': ['Women', 'Men', 'Kids', 'Home', 'Vintage & collectibles', 'Beauty', 'Electronics', 'Sports & outdoors', 'Handmade', 'Other'],
                    'level_2_men': ['Tops', 'Sweats & hoodies', 'Sweaters', 'Jeans', 'Pants', 'Shorts', 'Coats & jackets', 'Blazers & sport coats', 'Suits', 'Athletic apparel', 'Swimwear', "Men's accessories", 'Shoes', 'Jewelry', 'Other'],
                    'level_2_women': ['Dresses', 'Tops & blouses', 'Sweaters', 'Jeans', 'Pants', 'Skirts', 'Coats & jackets', 'Suits & blazers', 'Athletic apparel', 'Swimwear', "Women's handbags", "Women's accessories", 'Jewelry', 'Maternity', 'Shoes', 'Other'],
                    'level_2_kids': ['Girls shoes', 'Boys shoes'],
                    'level_3_men_shoes': ['Athletic', 'Boots', 'Fashion sneakers', 'Outdoor', 'Oxfords', 'Sandals', 'Slippers', 'Work & safety', 'Loafers', 'Slip-Ons', 'Clogs', 'Mules', 'All Shoes'],
                    'level_3_women_shoes': ['Athletic', 'Boots', 'Fashion sneakers', 'Flats', 'Outdoor', 'Oxfords', 'Heels', 'Sandals', 'Slippers', 'Work & safety', 'Loafers', 'Slip-Ons', 'Clogs', 'Mules', 'Other', 'All Shoes'],
                    'level_3_boys_shoes': ['Boys 0-24 mos', 'Boys 2T-5T', 'Boys (4+)', 'All Boys shoes'],
                    'level_3_girls_shoes': ['Girls 0-24 mos', 'Girls 2T-5T', 'Girls (4+)', 'All Girls shoes']
                },
                'conditions': {
                    'ConditionNew': 'New',
                    'ConditionLikeNew': 'Like New',
                    'ConditionGood': 'Good',
                    'ConditionFair': 'Fair',
                    'ConditionPoor': 'Poor'
                }
            }
        }
    
    def parse_listing_for_crosslisting(self, listing_data: Dict) -> Dict:
        """
        Extract item specifics from eBay listing using AI for both Poshmark and Mercari
        
        Args:
            listing_data: {
                'title': str,
                'description': str,
                'ebay_condition': str,
                'category_name': str,
                'brand': str (optional - extracted from ProductListingDetails)
            }
        
        Returns:
            dict: {
                'poshmark': {...},
                'mercari': {...},
                'item_specifics': {...}
            }
        """
        
        # Create comprehensive prompt with platform specifications
        prompt = self._create_parsing_prompt(listing_data)
        
        try:
            import anthropic
            
            client = anthropic.Anthropic(api_key=self.api_key)
            
            message = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=2000,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract JSON from response
            response_text = message.content[0].text
            
            # Remove markdown code blocks if present
            if '```json' in response_text:
                response_text = response_text.split('```json')[1].split('```')[0]
            elif '```' in response_text:
                response_text = response_text.split('```')[1].split('```')[0]
            
            # Parse JSON
            parsed_data = json.loads(response_text.strip())
            
            logger.info(f"AI parsed listing data successfully")
            logger.debug(f"Parsed data: {json.dumps(parsed_data, indent=2)}")

            parsed_data = self._apply_shoe_size_category_overrides(parsed_data, listing_data)
            parsed_data = self._apply_condition_overrides(parsed_data, listing_data)
            logger.debug(f"Post-override parsed data: {json.dumps(parsed_data, indent=2)}")
            
            return parsed_data
            	
        except Exception as e:
            logger.error(f"Error parsing with AI: {e}")
            # Return fallback structure
            return self._create_fallback_data(listing_data)

    def _map_ebay_condition_to_poshmark(self, ebay_condition: str) -> str:
        condition = (ebay_condition or '').strip().lower()

        if 'new with box' in condition:
            return 'New With Tags (NWT)'

        if 'new without box' in condition:
            return 'New Without Tags (NWOT)'

        if 'new with defects' in condition:
            return 'Like New'

        if condition == 'new' or condition.startswith('new '):
            return 'New Without Tags (NWOT)'

        if 'excellent' in condition:
            return 'Like New'

        if 'good' in condition:
            return 'Good'

        if 'fair' in condition:
            return 'Fair'

        return 'Good'

    def _apply_condition_overrides(self, parsed_data: Dict, listing_data: Dict) -> Dict:
        ebay_condition = listing_data.get('ebay_condition', '')
        poshmark_condition = self._map_ebay_condition_to_poshmark(ebay_condition)

        parsed_data.setdefault('poshmark', {})
        parsed_data.setdefault('category_data', {})

        parsed_data['poshmark']['condition'] = poshmark_condition
        parsed_data['category_data']['condition'] = poshmark_condition

        return parsed_data
    
    def _apply_shoe_size_category_overrides(self, parsed_data: Dict, listing_data: Dict) -> Dict:
        """
        Apply deterministic overrides for kids/youth/toddler shoe sizing.
        This runs AFTER AI parsing so we keep AI for brand/color/style, but
        do not fully trust it for size/category when Y/C/toddler signals exist.
        """
        title = (listing_data.get('title') or '').strip()
        ebay_category = (listing_data.get('category_name') or '').strip()
        description = (listing_data.get('description') or '').strip()

        raw_size = self._extract_raw_size_signal(title, description)
        normalized_size = self._normalize_size_for_marketplaces(raw_size)

        size_type = self._detect_size_type(title, ebay_category, raw_size)

        title_lower = title.lower()
        category_lower = ebay_category.lower()
        looks_like_shoes = (
            'shoe' in title_lower or 'shoes' in title_lower or 'sneaker' in title_lower or 'sneakers' in title_lower
            or 'boot' in title_lower or 'boots' in title_lower
            or 'sandal' in title_lower or 'sandals' in title_lower
            or 'slipper' in title_lower or 'slippers' in title_lower
            or 'shoe' in category_lower or 'shoes' in category_lower
            or 'sneaker' in category_lower or 'boot' in category_lower
        )

        if not looks_like_shoes:
            return parsed_data

        parsed_data.setdefault('poshmark', {})
        parsed_data.setdefault('mercari', {})
        parsed_data.setdefault('item_specifics', {})
        parsed_data['poshmark'].setdefault('category', {})
        parsed_data['mercari'].setdefault('category', {})

        if raw_size:
            parsed_data['item_specifics']['OriginalSize'] = raw_size

        if normalized_size:
            parsed_data['poshmark']['size'] = normalized_size
            parsed_data['item_specifics']['Size'] = normalized_size

        # Force kids/youth/toddler into Kids categories
        if size_type in ['youth', 'child', 'toddler', 'infant', 'baby', 'kids']:
            kids_gender = self._detect_kids_gender(title, ebay_category)

            # Poshmark
            parsed_data['poshmark']['category']['level_1'] = 'Kids'
            parsed_data['poshmark']['category']['level_2'] = 'Boys shoes' if kids_gender == 'boys' else 'Girls shoes'

            current_pm_level3 = parsed_data['poshmark']['category'].get('level_3', '')
            parsed_data['poshmark']['category']['level_3'] = self._normalize_poshmark_kids_shoe_type(
                current_pm_level3,
                title
            )

            # Mercari
            parsed_data['mercari']['category']['level_1'] = 'Kids'
            parsed_data['mercari']['category']['level_2'] = 'Boys shoes' if kids_gender == 'boys' else 'Girls shoes'
            parsed_data['mercari']['category']['level_3'] = self._mercari_kids_age_bucket(
                normalized_size,
                kids_gender
            )

            if normalized_size:
                parsed_data['mercari']['size'] = normalized_size

            parsed_data['item_specifics']['Department'] = 'Kids'
            return parsed_data

        # Adult Mercari size formatting only if category is already adult shoes
        mercari_level1 = parsed_data['mercari']['category'].get('level_1', '')
        mercari_level2 = parsed_data['mercari']['category'].get('level_2', '')

        if normalized_size and mercari_level1 in ['Men', 'Women'] and mercari_level2 == 'Shoes':
            mercari_size = self._format_mercari_adult_size(normalized_size, mercari_level1)
            if mercari_size:
                parsed_data['mercari']['size'] = mercari_size

        return parsed_data

    def _extract_raw_size_signal(self, title: str, description: str = '') -> str:
        """
        Pull a likely shoe size token from title/description.
        Examples: 13C, 4.5Y, 10M, 11.5
        Intentionally does NOT treat trailing W as a suffix, because 8W can mean wide.
        """
        text = f"{title} {description}".upper()

        patterns = [
            r'\b(?:SIZE|SZ)\s*(\d{1,2}(?:\.\d)?\s?(?:Y|C|TD|T|M)?)\b',
            r'\b(\d{1,2}(?:\.\d)?\s?(?:Y|C|TD|T|M))\b',
            r'\b(\d{1,2}(?:\.\d)?)\b',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).replace(' ', '').upper()

        return ''

    def _normalize_size_for_marketplaces(self, raw_size: str) -> str:
        """
        Keep kids sizes with suffixes:
        - 6.5Y stays 6.5Y
        - 13C stays 13C
        - 10M becomes 10
        - 8W stays 8W because W may mean wide
        """
        if not raw_size:
            return ''

        cleaned = raw_size.strip().upper().replace(' ', '')

        # Normalize toddler markers to C
        cleaned = re.sub(r'(TD|T)$', 'C', cleaned)

        # Preserve youth/child suffixes
        if re.match(r'^\d{1,2}(?:\.\d)?[YC]$', cleaned):
            return cleaned

        # Remove men's marker only
        cleaned = re.sub(r'M$', '', cleaned).strip()

        return cleaned

    def _detect_size_type(self, title: str, ebay_category: str, raw_size: str) -> str:
        """
        Determine whether this is kids/toddler/youth vs adult.
        """
        title_lower = (title or '').lower()
        category_lower = (ebay_category or '').lower()
        raw_upper = (raw_size or '').upper()

        kids_words = [
            'youth', 'boys', "boys'", 'girls', "girls'",
            'kids', 'kid', 'toddler', 'little kid', 'big kid',
            'child', 'children', 'baby', 'infant', 'walker'
        ]

        if any(word in title_lower for word in kids_words):
            return 'kids'

        if 'boys' in category_lower or 'girls' in category_lower or 'kids' in category_lower:
            return 'kids'

        if raw_upper.endswith('Y'):
            return 'youth'
        if raw_upper.endswith('C'):
            return 'child'
        if raw_upper.endswith('TD') or raw_upper.endswith('T'):
            return 'toddler'

        return 'adult'

    def _detect_kids_gender(self, title: str, ebay_category: str) -> str:
        """
        Return 'boys' or 'girls' for kids footwear.
        Default to boys if unclear.
        """
        text = f"{title} {ebay_category}".lower()

        if any(word in text for word in ['girls', 'girl', "girls'"]):
            return 'girls'

        if any(word in text for word in ['boys', 'boy', "boys'"]):
            return 'boys'

        return 'boys'

    def _normalize_poshmark_kids_shoe_type(self, current_level3: str, title: str) -> str:
        """
        Keep Poshmark kids level_3 inside the allowed list.
        """
        title_lower = (title or '').lower()
        allowed = {
            'Baby & Walker', 'Boots', 'Dress Shoes', 'Moccasins',
            'Rain & Snow Boots', 'Sandals & Flip Flops', 'Slippers',
            'Sneakers', 'Water Shoes', 'None'
        }

        if current_level3 in allowed:
            return current_level3

        if 'boot' in title_lower:
            return 'Boots'
        if 'sandal' in title_lower or 'flip flop' in title_lower or 'flip-flop' in title_lower or 'slides' in title_lower:
            return 'Sandals & Flip Flops'
        if 'slipper' in title_lower:
            return 'Slippers'
        if 'rain' in title_lower or 'snow' in title_lower:
            return 'Rain & Snow Boots'
        if 'water shoe' in title_lower or 'water shoes' in title_lower:
            return 'Water Shoes'
        if 'walker' in title_lower or 'baby' in title_lower:
            return 'Baby & Walker'

        return 'Sneakers'

    def _mercari_kids_age_bucket(self, normalized_size: str, kids_gender: str) -> str:
        """
        Map kids shoe size to Mercari's exact level_3 buckets.
        Y sizes should usually be Boys/Girls (4+).
        C/T toddler-child sizes should usually be Boys/Girls 2T-5T.
        """
        prefix = 'Girls' if kids_gender == 'girls' else 'Boys'

        size_text = (normalized_size or '').upper().strip()

        if size_text.endswith('Y'):
            return f'{prefix} (4+)'

        if size_text.endswith('C'):
            return f'{prefix} 2T-5T'

        number_only = re.sub(r'[^0-9.]', '', size_text)

        try:
            size_val = float(number_only)
        except (TypeError, ValueError):
            return f'{prefix} (4+)'

        if size_val <= 1.5:
            return f'{prefix} 0-24 mos'
        elif size_val <= 13.5:
            return f'{prefix} 2T-5T'
        else:
            return f'{prefix} (4+)'

    def _format_mercari_adult_size(self, normalized_size: str, department: str) -> str:
        """
        Convert adult shoe size to Mercari's display format when possible.
        """
        men_map = {
            '5': '5 (38)', '5.5': '5.5 (38.5)', '6': '6 (39)', '6.5': '6.5 (39.5)',
            '7': '7 (40)', '7.5': '7.5 (40.5)', '8': '8 (41)', '8.5': '8.5 (41.5)',
            '9': '9 (42)', '9.5': '9.5 (42.5)', '10': '10 (43)', '10.5': '10.5 (43.5)',
            '11': '11 (44)', '11.5': '11.5 (44.5)', '12': '12 (45)', '12.5': '12.5 (45.5)',
            '13': '13 (46)', '13.5': '13.5 (46.5)', '14': '14 (47)', '14.5': '14.5 (47.5)',
            '15': '15 (48)', '15.5': '15.5 (48.5)', '16': '16 (49)', '17': '17 (49.5)',
            '18': '18 (50)', '19': '19 (50.5)', '20': '20 (51)'
        }

        women_map = {
            '4': '4 (35)', '4.5': '4.5 (35)', '5': '5 (35.5)', '5.5': '5.5 (36)',
            '6': '6 (36.5)', '6.5': '6.5 (37)', '7': '7 (37.5)', '7.5': '7.5 (38)',
            '8': '8 (38.5)', '8.5': '8.5 (39)', '9': '9 (39.5)', '9.5': '9.5 (40)',
            '10': '10 (40.5)', '10.5': '10.5 (41)', '11': '11 (41.5)', '11.5': '11.5 (42)',
            '12': '12 (42.5)', '12.5': '12.5 (43)', '13': '13 (43.5)', '13.5': '13.5 (44)'
        }

        if department == 'Men':
            return men_map.get(normalized_size, normalized_size)
        if department == 'Women':
            return women_map.get(normalized_size, normalized_size)

        return normalized_size
#     def _create_parsing_prompt(self, listing_data: Dict) -> str:
#         """Create detailed prompt for AI parsing with platform specifications"""
        
#         title = listing_data.get('title', '')
#         description = listing_data.get('description', '')[:500]  # Limit description
#         ebay_condition = listing_data.get('ebay_condition', '')
#         category_name = listing_data.get('category_name', '')
#         brand = listing_data.get('brand', '')  # From ProductListingDetails
        
#         prompt = f"""You are a cross-listing expert. Extract item specifics from this eBay listing and format them for Poshmark and Mercari.

# eBay Listing Data:
# - Title: {title}
# - Description: {description}
# - eBay Condition: {ebay_condition}
# - eBay Category: {category_name}
# - Brand (from eBay catalog): {brand}

# POSHMARK PLATFORM SPECS:
# Categories (3-level structure):
# - Level 1: {', '.join(self.platform_specs['poshmark']['categories']['level_1'])}
# - Men > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_men_shoes'])}
# - Women > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_women_shoes'])}
# - Kids > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_kids_shoes'])}

# Poshmark Conditions (EXACT TEXT): {', '.join(self.platform_specs['poshmark']['conditions'])}
# Poshmark Colors: Choose up to TWO colors when clearly present in the title. Return as a list.: {', '.join(self.platform_specs['poshmark']['colors'])}
# Poshmark Sizes: Standard US sizes (4, 4.5, 5, 5.5... up to 16) OR custom text

# MERCARI PLATFORM SPECS:
# Categories (3-level structure):
# - Level 1: {', '.join(self.platform_specs['mercari']['categories']['level_1'])}
# - Men > Shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_men_shoes'])}
# - Women > Shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_women_shoes'])}
# - Boys shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_boys_shoes'])}
# - Girls shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_girls_shoes'])}

# Mercari Conditions (use data-testid): ConditionNew, ConditionLikeNew, ConditionGood, ConditionFair, ConditionPoor

# Mercari Sizes (CATEGORY-SPECIFIC - VERY IMPORTANT):
  
#   IF Men > Shoes → Use format "US (EU)":
#     Available: 5 (38), 5.5 (38.5), 6 (39), 6.5 (39.5), 7 (40), 7.5 (40.5), 8 (41), 8.5 (41.5), 9 (42), 9.5 (42.5), 10 (43), 10.5 (43.5), 11 (44), 11.5 (44.5), 12 (45), 12.5 (45.5), 13 (46), 13.5 (46.5), 14 (47), 14.5 (47.5), 15 (48), 15.5 (48.5), 16 (49), 17 (49.5), 18 (50), 19 (50.5), 20 (51), 5.5 (38.5) and below, 13 (50)+
  
#   IF Women > Shoes → Use format "US (EU)":
#     Available: 4 (35), 4.5 (35), 5 (35.5), 5.5 (36), 6 (36.5), 6.5 (37), 7 (37.5), 7.5 (38), 8 (38.5), 8.5 (39), 9 (39.5), 9.5 (40), 10 (40.5), 10.5 (41), 11 (41.5), 11.5 (42), 12 (42.5), 12.5 (43), 13 (43.5), 13.5 (44), 14 & Up (44.5), 3.5 and below
  
#   IF Kids > Boys shoes OR Kids > Girls shoes → Preserve youth/child suffixes when present:
#       Examples:
#       4Y
#       4.5Y
#       6Y
#       10C
#       10.5C
#       13C

#       If no suffix is present, use numeric size only.
#       Never convert 4Y into 4.
#       Never convert 13C into 13.

# CRITICAL: Check the category level_1 and level_2 to determine which size format to use!

# 2. SIZE EXTRACTION:
#    - Remove suffixes: "6.5Y" → "6.5Y", "10M" → "10", "8W" → "8"
#    - For Poshmark adult sizes: use just the number.
#    - For Poshmark kids sizes: preserve suffixes like 4Y, 6.5Y, 10C, and 13C when present.
#    - For Mercari: Format depends on category:
   
#    **MEN'S SHOES** - Format "US (EU)":
#    5 (38), 5.5 (38.5), 6 (39), 6.5 (39.5), 7 (40), 7.5 (40.5), 8 (41), 8.5 (41.5), 
#    9 (42), 9.5 (42.5), 10 (43), 10.5 (43.5), 11 (44), 11.5 (44.5), 12 (45), 
#    12.5 (45.5), 13 (46), 13.5 (46.5), 14 (47), 14.5 (47.5), 15 (48), 15.5 (48.5), 
#    16 (49), 17 (49.5), 18 (50), 19 (50.5), 20 (51)
   
#    **WOMEN'S SHOES** - Format "US (EU)":
#    4 (35), 4.5 (35), 5 (35.5), 5.5 (36), 6 (36.5), 6.5 (37), 7 (37.5), 7.5 (38), 
#    8 (38.5), 8.5 (39), 9 (39.5), 9.5 (40), 10 (40.5), 10.5 (41), 11 (41.5), 
#    11.5 (42), 12 (42.5), 12.5 (43), 13 (43.5), 13.5 (44), 14 & Up (44.5)
   
   
#    **KIDS SHOES (Boys/Girls)** - Format: Just number (NO EU):
#    0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6, 6.5, 7, 7.5, 8, 8.5, 
#    9, 9.5, 10, 10.5, 11, 11.5, 12, 12.5, 13, 13.5
   
#    **CRITICAL RULES:**
#    - If category is Men/Women + Shoes → Use "US (EU)" format
#    - If category is Kids + (Boys Shoes OR Girls Shoes) → Preserve Y/C suffixes when present, such as 4Y, 6.5Y, 13C
#    - Match the EXACT format from the tables above

# CRITICAL INSTRUCTIONS:

# 1. CATEGORY MAPPING:
#    - Determine gender from title (Men/Women/Boys/Girls/Kids)
#    - For shoes: Pick the most specific subcategory (e.g., "Sneakers" not "All Shoes")
#    - For Poshmark Kids: Use "Men" for boys shoes (Poshmark structure)
#    - For Mercari Kids: Use "Boys shoes" or "Girls shoes" then age subcategory

# 2. SIZE EXTRACTION:
#    - Normalize kids/adult markers carefully: "6.5Y" → "6.5Y" only after recognizing it is youth/kids, "13C" → "13C" only after recognizing it is child/kids, "10M" may mean men's, and "8W" may mean wide so do NOT assume W always means women's
#    - For Poshmark adult sizes: use just the number.
#    - For Poshmark kids sizes: preserve suffixes like 4Y, 6.5Y, 10C, and 13C when present.
#    - For Mercari Adult Men/Women Shoes: use "US (EU)" format
#    - For Mercari Kids Shoes: preserve kids size codes like 4Y, 6.5Y, 13C
#    - Common conversions: 10=43, 10.5=43.5, 11=44, 11.5=44.5, 12=45, etc.

# 3. CONDITION MAPPING:
#    - "New" / "New with tags" → Poshmark: "New With Tags (NWT)", Mercari: "ConditionNew"
#    - "Like New" / "Excellent" → Poshmark: "Like New", Mercari: "ConditionLikeNew"
#    - "Good" / "Pre-owned - Good" / "Very Good" → Poshmark: "Good", Mercari: "ConditionGood"
#    - "Fair" / "Acceptable" → Poshmark: "Fair", Mercari: "ConditionFair"
#    - "Poor" / "For Parts" → Mercari: "ConditionPoor" (Poshmark doesn't have Poor)

# 4. COLOR:
#    - For Poshmark: Pick up to TWO colors from the allowed list when clearly present. If only one is clear, return one.
#    - For Mercari: Not needed (skip)

# 5. BRAND:
#    - Use the brand from eBay catalog data if provided
#    - Otherwise extract from title

# Return ONLY this JSON structure (no other text):

# {{
#   "poshmark": {{
#     "category": {{
#       "level_1": "Men",
#       "level_2": "Shoes",
#       "level_3": "Sneakers"
#     }},
#     "condition": "Good",
#     "size": "10.5",
#     "color": ["Black", "White"]
#     "brand": "Nike"
#   }},
#   "mercari": {{
#     "category": {{
#       "level_1": "Men",
#       "level_2": "Shoes",
#       "level_3": "Athletic"
#     }},
#     "condition": "ConditionGood",
#     "size": "10.5 (43.5)",
#     "brand": "Nike"
#   }},
#   "item_specifics": {{
#     "Brand": "Nike",
#     "Size": "10.5",
#     "Color": ["Black", "White"],
#     "Condition": "Good",
#     "Department": "Men",
#     "Style": "Sneakers"
#   }}
# }}

# IMPORTANT:
# - Use EXACT category names from the lists provided
# - Use EXACT condition values (Poshmark: full text, Mercari: data-testid)
# - Adult size should be clean number for Poshmark and "US (EU)" for Mercari. Kids sizes should preserve suffixes like 4Y, 6.5Y, 13C, and 10C when available.
# - If unsure about a field, use "Other" for categories or best guess
# - Brand should match exactly what's in eBay catalog data
# """
        
#         return prompt
    
    
    def _create_parsing_prompt(self, listing_data: Dict) -> str:
        """Create detailed prompt for AI parsing with platform specifications"""
        
        title = listing_data.get('title', '')
        description = listing_data.get('description', '')[:500]  # Limit description
        ebay_condition = listing_data.get('ebay_condition', '')
        category_name = listing_data.get('category_name', '')
        brand = listing_data.get('brand', '')  # From ProductListingDetails
        
        prompt = f"""You are a cross-listing expert. Extract item specifics from this eBay listing and format them for Poshmark and Mercari.

eBay Listing Data:
- Title: {title}
- Description: {description}
- eBay Condition: {ebay_condition}
- eBay Category: {category_name}
- Brand (from eBay catalog): {brand}

POSHMARK PLATFORM SPECS:
Categories (3-level structure):
- Level 1: {', '.join(self.platform_specs['poshmark']['categories']['level_1'])}
- Men > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_men_shoes'])}
- Women > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_women_shoes'])}
- Kids > Shoes subcategories: {', '.join(self.platform_specs['poshmark']['categories']['level_3_kids_shoes'])}

Poshmark Conditions (EXACT TEXT): {', '.join(self.platform_specs['poshmark']['conditions'])}
Poshmark Colors: Choose up to TWO colors when clearly present in the title. Return as a list: {', '.join(self.platform_specs['poshmark']['colors'])}
Poshmark Sizes: Standard US sizes (4, 4.5, 5, 5.5... up to 16) OR custom text

MERCARI PLATFORM SPECS:
Categories (3-level structure):
- Level 1: {', '.join(self.platform_specs['mercari']['categories']['level_1'])}
- Men > Shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_men_shoes'])}
- Women > Shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_women_shoes'])}
- Boys shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_boys_shoes'])}
- Girls shoes subcategories: {', '.join(self.platform_specs['mercari']['categories']['level_3_girls_shoes'])}

Mercari Conditions (use data-testid): ConditionNew, ConditionLikeNew, ConditionGood, ConditionFair, ConditionPoor

Mercari Sizes (CATEGORY-SPECIFIC - VERY IMPORTANT):
  
  IF Men > Shoes → Use format "US (EU)":
    Available: 5 (38), 5.5 (38.5), 6 (39), 6.5 (39.5), 7 (40), 7.5 (40.5), 8 (41), 8.5 (41.5), 9 (42), 9.5 (42.5), 10 (43), 10.5 (43.5), 11 (44), 11.5 (44.5), 12 (45), 12.5 (45.5), 13 (46), 13.5 (46.5), 14 (47), 14.5 (47.5), 15 (48), 15.5 (48.5), 16 (49), 17 (49.5), 18 (50), 19 (50.5), 20 (51), 5.5 (38.5) and below, 13 (50)+
  
  IF Women > Shoes → Use format "US (EU)":
    Available: 4 (35), 4.5 (35), 5 (35.5), 5.5 (36), 6 (36.5), 6.5 (37), 7 (37.5), 7.5 (38), 8 (38.5), 8.5 (39), 9 (39.5), 9.5 (40), 10 (40.5), 10.5 (41), 11 (41.5), 11.5 (42), 12 (42.5), 12.5 (43), 13 (43.5), 13.5 (44), 14 & Up (44.5), 3.5 and below
  IF Kids > Boys shoes OR Kids > Girls shoes → Preserve youth/child suffixes when present:
  Examples:
  4Y
  4.5Y
  6Y
  10C
  10.5C
  13C

If no suffix is present, use numeric size only.
Never convert 4Y into 4.
Never convert 13C into 13.

CRITICAL: Check the category level_1 and level_2 to determine which size format to use!

2. SIZE EXTRACTION:
   - Normalize kids/adult markers carefully: "6.5Y" → "6.5Y" only after recognizing it is youth/kids, "13C" → "13C" only after recognizing it is child/kids, "10M" may mean men's, and "8W" may mean wide so do NOT assume W always means women's
   - For Poshmark adult sizes: use just the number.
   - For Poshmark kids sizes: preserve suffixes like 4Y, 6.5Y, 10C, and 13C when present.
   - For Mercari: Format depends on category:
   
   **MEN'S SHOES** - Format "US (EU)":
   5 (38), 5.5 (38.5), 6 (39), 6.5 (39.5), 7 (40), 7.5 (40.5), 8 (41), 8.5 (41.5), 
   9 (42), 9.5 (42.5), 10 (43), 10.5 (43.5), 11 (44), 11.5 (44.5), 12 (45), 
   12.5 (45.5), 13 (46), 13.5 (46.5), 14 (47), 14.5 (47.5), 15 (48), 15.5 (48.5), 
   16 (49), 17 (49.5), 18 (50), 19 (50.5), 20 (51)
   
   **WOMEN'S SHOES** - Format "US (EU)":
   4 (35), 4.5 (35), 5 (35.5), 5.5 (36), 6 (36.5), 6.5 (37), 7 (37.5), 7.5 (38), 
   8 (38.5), 8.5 (39), 9 (39.5), 9.5 (40), 10 (40.5), 10.5 (41), 11 (41.5), 
   11.5 (42), 12 (42.5), 12.5 (43), 13 (43.5), 13.5 (44), 14 & Up (44.5)
   
   
   **KIDS SHOES (Boys/Girls)** - Format:
    Preserve suffixes when known.

    Examples:
    4Y
    5Y
    6.5Y
    10C
    12C
    13C

    If suffix unknown, use numeric only.
   
   **CRITICAL RULES:**
   - If category is Men/Women + Shoes → Use "US (EU)" format
   - If category is Kids + (Boys Shoes OR Girls Shoes) → Preserve Y/C suffixes when present, such as 4Y, 6.5Y, 10C, 13C
   - Match the EXACT format from the tables above

CRITICAL INSTRUCTIONS:

1. CATEGORY MAPPING:
   
   **STEP 1 - DETECT KIDS FIRST (CHECK THIS BEFORE ANYTHING ELSE):**
   If title contains: "Youth", "Boys", "Girls", "Kids", "Toddler", "Baby", "Child", "Children"
   OR size has Y/C suffix: "6.5Y", "4C", "10C"
   OR eBay category contains: "Boys" or "Girls"
   → MUST use Poshmark Level 1: "Kids"
   
   Examples:
   - "Nike Air Jordan 1 Mid Boys Size 6.5Y" → Kids > Boys shoes > Sneakers
   - "Adidas Girls Size 4C" → Kids > Girls shoes > Sneakers
   - "Youth Vans Size 5Y" → Kids > Boys shoes > Sneakers
   
   **STEP 2 - ONLY IF NOT KIDS, then determine Men/Women:**
   - Title contains "Men", "Mens", "Male" → Men
   - Title contains "Women", "Womens", "Ladies", "Female" → Women
   
   **STEP 3 - Pick subcategories:**
   - For Kids: level_2 must be "Boys shoes" or "Girls shoes"
   - Then pick most specific level_3 (Sneakers, Boots, etc. - NOT "None")
   - For Men/Women: Same pattern

   **CRITICAL - USE ONLY PROVIDED CATEGORIES:**
   - Level 1 MUST be from the Level 1 list provided
   - Level 2 MUST be from the corresponding Level 2 list for that platform
   - Level 3 MUST be from the corresponding Level 3 list for that platform
   - If you cannot find an exact match, use "Other" or "None" (whichever exists in the list)
   - NEVER invent category names not in the provided lists

2. SIZE EXTRACTION:
   - Preserve kids suffixes: "6.5Y" → "6.5Y", "13C" → "13C"
   - Remove adult men's marker only when clear: "10M" → "10"
   - Do not remove W automatically because "8W" may mean wide
   - For Poshmark adult sizes: use just the number.
   - For Poshmark kids sizes: preserve suffixes like 4Y, 6.5Y, 10C, and 13C when present.
   - For Mercari Adult Men/Women Shoes: use "US (EU)" format
   - For Mercari Kids Shoes: preserve kids size codes like 4Y, 6.5Y, 13C
   - Common conversions: 10=43, 10.5=43.5, 11=44, 11.5=44.5, 12=45, etc.

3. CONDITION MAPPING:
   - "New" / "New with tags" → Poshmark: "New With Tags (NWT)", Mercari: "ConditionNew"
   - "Like New" → Poshmark: "Like New", Mercari: "ConditionLikeNew"
   - "Excellent" → Poshmark: "Like New", Mercari: "ConditionGood"
   - "Good" / "Pre-owned - Good" / "Very Good" → Poshmark: "Good", Mercari: "ConditionGood"
   - "Fair" / "Acceptable" → Poshmark: "Fair", Mercari: "ConditionFair"
   - "Poor" / "For Parts" → Mercari: "ConditionPoor" (Poshmark doesn't have Poor)

4. COLOR:
   - For Poshmark: Pick up to TWO colors from the allowed list when clearly present. If only one is clear, return one.
   - For Mercari: Not needed (skip)

5. BRAND:
   - Use the brand from eBay catalog data if provided
   - Otherwise extract from title

Return ONLY this JSON structure (no other text):

{{
  "poshmark": {{
    "category": {{
      "level_1": "Men",
      "level_2": "Shoes",
      "level_3": "Sneakers"
    }},
    "condition": "Good",
    "size": "10.5",
    "color": ["Black", "White"],
    "brand": "Nike"
  }},
  "mercari": {{
    "category": {{
      "level_1": "Men",
      "level_2": "Shoes",
      "level_3": "Athletic"
    }},
    "condition": "ConditionGood",
    "size": "10.5 (43.5)",
    "brand": "Nike"
  }},
  "item_specifics": {{
    "Brand": "Nike",
    "Size": "10.5",
    "color": ["Black", "White"],
    "Condition": "Good",
    "Department": "Men",
    "Style": "Sneakers"
  }}
}}

IMPORTANT:
- Use EXACT category names from the lists provided
- Use EXACT condition values (Poshmark: full text, Mercari: data-testid) 
- Adult size should be clean number for Poshmark and "US (EU)" for Mercari. Kids sizes should preserve suffixes like 4Y, 6.5Y, 13C, and 10C when available.
- If unsure about a field, use "Other" for categories or best guess
- Brand should match exactly what's in eBay catalog data
"""
        
        return prompt
    
    
    
    def _create_fallback_data(self, listing_data: Dict) -> Dict:
        """Create fallback data structure if AI parsing fails"""
        
        title = listing_data.get('title', '').lower()
        brand = listing_data.get('brand', 'Unknown')
        
        # Simple fallback logic
        department = 'Men'
        if any(word in title for word in ['women', 'womens', 'ladies']):
            department = 'Women'
        elif any(word in title for word in ['boys', 'youth', 'girls', 'kids', 'children', 'toddler', 'baby', 'infant']):
            department = 'Kids'
        
        return {
            'poshmark': {
                'category': {
                    'level_1': department,
                    'level_2': 'Shoes',
                    'level_3': 'Other'
                },
                'condition': 'Good',
                'size': '',
                'color': 'Other',
                'brand': brand
            },
            'mercari': {
                'category': {
                    'level_1': department,
                    'level_2': 'Shoes',
                    'level_3': 'Other'
                },
                'condition': 'ConditionGood',
                'size': '',
                'brand': brand
            },
            'item_specifics': {
                'Brand': brand,
                'Size': '',
                'Color': '',
                'Condition': 'Good',
                'Department': department,
                'Style': 'Shoes'
            }
        }


# Test function
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    
    # Test data
    test_listing = {
        'title': 'Nike Air Jordan 1 Mid Boys Size 6.5Y Red Black White Athletic Shoes Sneakers',
        'description': 'Pre-owned - Good: This item has been gently used but is in good condition...',
        'ebay_condition': 'Pre-owned - Good',
        'category_name': "Clothing, Shoes & Accessories:Kids:Boys:Boys' Shoes",
        'brand': 'Nike'
    }

    # Test data
    # test_listing = {
    #     'title': 'Adidas Ultraboost 22 Womens Size 8 Grey White Running Shoes Sneakers',
    #     'description': 'Pre-owned - Very Good: Worn only a few times, minimal signs of use. Soles show light wear. No tears, stains, or odors. Original laces included.',
    #     'ebay_condition': 'Pre-owned - Very Good',
    #     'category_name': "Clothing, Shoes & Accessories:Women:Women's Shoes",
    #     'brand': 'Adidas'
    # }
    
    parser = AIParserService()
    result = parser.parse_listing_for_crosslisting(test_listing)
    
    print("\n" + "="*60)
    print("AI PARSING RESULT:")
    print("="*60)
    print(json.dumps(result, indent=2))
