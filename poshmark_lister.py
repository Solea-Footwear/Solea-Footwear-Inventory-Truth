"""
Poshmark Lister - Selenium
Automates listing creation on Poshmark
"""
import logging
import os
import time
import tempfile
from PIL import Image
from typing import Dict, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


# Style tag mapping - keywords in title → Poshmark style tags
STYLE_TAG_MAP = {
    # Strong style / era signals first
    'Vintage': ['vintage', '70s', '80s', '90s', 'y2k'],
    'Retro': ['retro', 'classic', 'old school', 'throwback', 'og'],

    # Shoe-specific style signals
    'Platform': ['platform', 'platforms', 'stacked sole', 'thick sole'],
    'Western': ['western', 'cowboy', 'cowgirl', 'roper'],
    'Formal': ['dress shoe', 'dress shoes', 'oxford', 'oxfords', 'derby', 'derbies', 'loafer', 'loafers', 'wingtip', 'cap toe'],

    # Brand-driven tags
    'Luxury': [
        'gucci', 'prada', 'louis vuitton', 'lv', 'burberry', 'balenciaga',
        'saint laurent', 'ysl', 'valentino', 'versace', 'ferragamo',
        'dolce', 'gabbana', 'givenchy', 'moncler', 'golden goose', 'alexander mcqueen'
    ],

    # Common lifestyle tags
    'Streetwear': ['vans', 'converse', 'skate', 'skater', 'streetwear', 'street'],
    'Activewear': ['running', 'runner', 'athletic', 'training', 'basketball', 'tennis', 'cross training', 'gym'],
    'Outdoor': ['trail', 'hiking', 'hiker', 'outdoor', 'waterproof', 'gore-tex', 'gtx', 'bootie', 'snow', 'winter'],

    # Default broad tag
    'Casual': ['shoe', 'shoes', 'sneaker', 'sneakers', 'boot', 'boots', 'sandal', 'sandals', 'slip on', 'everyday', 'casual'],
}

def extract_style_tags_from_title(title: str, max_tags: int = 3) -> list:
    title_lower = title.lower()
    matched_tags = []

    for tag, keywords in STYLE_TAG_MAP.items():
        if len(matched_tags) >= max_tags:
            break

        for keyword in keywords:
            if keyword in title_lower:
                if tag not in matched_tags:
                    matched_tags.append(tag)
                break

    if 'Casual' not in matched_tags:
        matched_tags.append('Casual')

    fallback_tags = ['Outdoor']

    for tag in fallback_tags:
        if len(matched_tags) >= max_tags:
           break
        if tag not in matched_tags:
            matched_tags.append(tag)

    return matched_tags[:max_tags]

def parse_poshmark_colors(color_data, title: str = "") -> list:
    """Normalize raw color data into up to 2 Poshmark-supported colors."""
    if not color_data:
        return []

    known_colors = [
        "Black", "White", "Gray", "Grey", "Blue", "Red", "Brown", "Tan",
        "Cream", "Pink", "Purple", "Green", "Yellow", "Orange", "Gold", "Silver"
    ]

    if isinstance(color_data, list):
        raw_text = " ".join(str(color) for color in color_data)
    else:
        raw_text = str(color_data)

    colors_lower = raw_text.lower()

    if colors_lower in ["other", "unknown", "multi", "multicolor", "multi-color"]:
        colors_lower = title.lower()

    found_colors = []

    for known_color in known_colors:
        if known_color.lower() in colors_lower:
            normalized_color = "Gray" if known_color == "Grey" else known_color
            if normalized_color not in found_colors:
                found_colors.append(normalized_color)

    return (found_colors if found_colors else ["Black"])[:2]

def extract_size_from_title(title: str) -> str:
    """Extract common shoe sizes from title as fallback."""
    import re

    if not title:
        return None

    patterns = [
        r'\bsize\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bsz\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bus\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bmens?\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bwomens?\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bmen\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\bwomen\s*([0-9]{1,2}(?:\.5)?[YCMB]?)\b',
        r'\b([0-9]{1,2}(?:\.5)?[YCMB])\b'
    ]

    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    return None

def get_listing_size(listing_data: Dict) -> str:
    """Get size from all likely listing_data locations."""
    item_specifics = listing_data.get("item_specifics") or {}
    category_data = listing_data.get("category_data") or {}
    ai_data = listing_data.get("ai_data") or {}

    possible_size = (
        listing_data.get("size")
        or listing_data.get("Size")
        or item_specifics.get("US Shoe Size")
        or item_specifics.get("US Shoe Size (Men's)")
        or item_specifics.get("US Shoe Size (Women's)")
        or item_specifics.get("US Size")
        or item_specifics.get("Shoe Size")
        or item_specifics.get("us shoe size")
        or item_specifics.get("shoe size")
        or item_specifics.get("size")
        or category_data.get("size")
        or ai_data.get("size")
        or extract_size_from_title(listing_data.get("title", ""))
    )

    if not possible_size:
        return None

    return (
        str(possible_size)
        .strip()
        .upper()
        .replace("US", "")
        .replace(" ", "")
    )

def map_poshmark_condition(raw_condition: str) -> str:
    if not raw_condition:
        return "Good"

    condition = str(raw_condition).strip().lower()

    if condition in [
        "new",
        "new with box",
        "new without box"
    ]:
        return "New With Tags"

    if condition in [
        "new with defects"
    ]:
        return "Like New"

    return "Good"

class PoshmarkLister:
    """Selenium-based listing creation for Poshmark"""
    
    def __init__(self, profile_dir: str = None):

        # Get profile path
        if profile_dir:
            self.profile_dir = os.path.abspath(profile_dir)
        else:
            # self.profile_dir = os.path.join(os.path.dirname(base_dir), 'delisting', 'profiles')
            # Default: profiles folder next to this file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.profile_dir = os.path.join(base_dir, "profiles")
        
        self.driver = None
    
    def create_listing(self, listing_data: Dict, image_paths: List[str]) -> Dict:
        """
        Create listing on Poshmark
        
        Args:
            listing_data (dict): Listing information (title, description, price, etc.)
            image_paths (list): Local paths to images
        
        Returns:
            dict: Result with success status and listing ID
        """
        try:
            logger.debug("here is listing data")
            logger.debug(listing_data)
            validation_error = self._validate_listing_data(listing_data, image_paths)
            if validation_error:
                return {'success': False, 'error': validation_error}

            if not self._init_driver():
                return {'success': False, 'error': 'Failed to initialize driver'}

          
            # Navigate to create listing page
            logger.info("Navigating to Poshmark create listing page...")
            self.driver.get('https://poshmark.com/create-listing')

            
            # Upload images
            logger.info(f"Uploading {len(image_paths)} images...")
            if not self._upload_images(image_paths):
                return {'success': False, 'error': 'Failed to upload images'}
            
            # Fill in listing details
            logger.info("Filling listing details...")
            if not self._fill_listing_form(listing_data):
                return {'success': False, 'error': 'Failed to fill form'}
            
            # Submit listing
            logger.info("Submitting listing...")
            listing_id = self._submit_listing()
            
            if not listing_id:
                return {'success': False, 'error': 'Failed to get listing ID'}
            
            logger.info(f"Poshmark listing created successfully: {listing_id}")
            
            return {
                'success': True,
                'channel_listing_id': listing_id,
                'platform': 'poshmark'
            }
            
        except Exception as e:
            logger.error(f"Error creating Poshmark listing: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        
        finally:
            self._close_driver()

    def _validate_listing_data(self, listing_data: Dict, image_paths: List[str]) -> str:
        """Validate required listing data before opening Selenium."""
        required_fields = ["title", "price", "sku"]

        size_value = get_listing_size(listing_data)

        if not size_value:
            logger.error(f"[VALIDATION] Missing size. listing_data keys: {list(listing_data.keys())}")
            logger.error(f"[VALIDATION] item_specifics: {listing_data.get('item_specifics')}")
            logger.error(f"[VALIDATION] title: {listing_data.get('title')}")
            return "Missing required field: size"

        listing_data["size"] = size_value
        logger.info(f"[SIZE] Final resolved size: {listing_data['size']}")

        for field in required_fields:
            if not listing_data.get(field):
                return f"Missing required field: {field}"

        if not image_paths:
            return "Missing image paths"

        category = listing_data.get("category")
        if category and not isinstance(category, dict):
            return "Category must be a dictionary with level_1, level_2, and level_3"

        return None
    
    def _init_driver(self) -> bool:
        """Initialize Chrome driver with Poshmark profile"""
        try:
            chrome_options = Options()
            
            # Use Poshmark profile (pre-logged in)
            profile_path = os.path.join(self.profile_dir, 'poshmark')
            profile_path = os.path.abspath(profile_path)
            chrome_options.add_argument(f"user-data-dir={profile_path}")

            # Anti-detection options
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            # chrome_options.add_argument('--disable-dev-shm-usage')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)

            # Remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Poshmark Chrome driver initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing driver: {e}")
            return False

    def _make_poshmark_cover_image(self, input_path: str) -> str:
        """
        Create a temporary 3:4 portrait cover image for Poshmark.
        Keeps the full original image visible by adding white padding.
        """
        img = Image.open(input_path).convert("RGB")

        target_w = 1200
        target_h = 1600

        img.thumbnail((target_w, target_h))

        canvas = Image.new("RGB", (target_w, target_h), "white")
        x = (target_w - img.width) // 2
        y = (target_h - img.height) // 2
        canvas.paste(img, (x, y))

        temp_dir = tempfile.gettempdir()
        base_name = os.path.basename(input_path)
        output_path = os.path.join(temp_dir, f"poshmark_cover_{base_name}.jpg")

        canvas.save(output_path, "JPEG", quality=95)
        return output_path
    
    def _upload_images(self, image_paths: List[str]) -> bool:
        """Upload images to Poshmark"""
        try:
            # Find file input element
            file_input = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='file']"))
            )
            
            # Upload all images at once (Poshmark accepts multiple)
            upload_paths = image_paths[:16]

            if upload_paths:
                cover_path = self._make_poshmark_cover_image(upload_paths[0])
                upload_paths = [cover_path] + upload_paths[1:]

            all_paths = '\n'.join(upload_paths)
            file_input.send_keys(all_paths)
            

            apply_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, '//button[@data-et-name="apply" and contains(text(),"Apply")]'))
             )
            apply_button.click()

            
            logger.info(f"Uploaded {len(image_paths)} images")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading images: {e}")
            return False
    
    def _fill_listing_form(self, listing_data: Dict) -> bool:
        """Fill in listing form fields"""
        try:
            # Title
            title_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-vv-name="title"]'))
            )
            title_input.clear()
            title_input.send_keys(listing_data['title'][:80])
            
            
            # Description
            desc_input = self.driver.find_element(By.CSS_SELECTOR, '[data-vv-name="description"]')
            desc_input.clear()
            # desc_input.send_keys(listing_data['description'][:500])
            hardcoded_description = """
Please review all photos carefully for condition and overall appearance.

Any wear, marks, creasing, or signs of prior use will be shown in the photos. We provide close up photos for hard to see flaws if present.

Please feel free to message us with any questions before purchasing. Thanks!
            """
            desc_input.send_keys(hardcoded_description)
                     
            
            # ✨ NEW: Category using AI data
            if listing_data.get('category'):
                try:

                    category_dropdown = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@class='dropdown__selector dropdown__selector--select-tag dropdown__selector--select-tag--large ellipses']"))
                    )
                    category_dropdown.click()
                    
                    # Get category from AI data
                    category = listing_data.get('category', {})

                    level_1 = category.get('level_1', 'Men')
                    level_2 = category.get('level_2', 'Shoes')
                    level_3 = category.get('level_3')
                    
                    # Normalize category values
                    level_1_lower = str(level_1).lower()
                    level_2_lower = str(level_2).lower()
                    
                    # Force correct Poshmark structure
                    if level_1_lower in ["boys", "girls", "baby", "kids"]:
                        level_1 = "Kids"
                    
                    if str(level_1).lower() == "kids":
                        level_2 = "Shoes"
                    elif level_2_lower in ["mens shoes", "women shoes", "boys shoes", "girls shoes"]:
                        level_2 = "Shoes"
                    
                    title_lower = listing_data.get('title', '').lower()

                    if level_3 in ["Other", "Unknown", None, ""]:
                        if any(word in title_lower for word in ["boot", "boots"]):
                            level_3 = "Boots"
                        elif any(word in title_lower for word in ["sandal", "sandals", "flip flop", "slide"]):
                            level_3 = "Sandals"
                        elif any(word in title_lower for word in ["loafer", "oxford", "dress"]):
                            level_3 = "Dress Shoes"
                        elif any(word in title_lower for word in ["heel", "heels", "pump", "pumps", "wedge"]):
                            level_3 = "Heels"
                        elif any(word in title_lower for word in ["cleat", "cleats"]):
                            if level_1.lower() == "kids":
                                level_3 = "Sneakers"
                            else:
                                level_3 = "Athletic Shoes"
                        else:
                            if level_1.lower() == "kids":
                                level_3 = "Sneakers"
                            else:
                                level_3 = "Athletic Shoes"

                    logger.info(f"[CATEGORY] Input category: {category}")
                    logger.info(f"[CATEGORY] Final path: {level_1} > {level_2} > {level_3}")
                    
                    # Click level 1 (Men/Women/Kids)
                    level_1_elem = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, f'//p[contains(text(),"{level_1}")]'))
                    )
                    self.driver.execute_script("arguments[0].click();", level_1_elem)
                    time.sleep(0.5)
                                        
                    # Click level 2 (Shoes/Accessories/etc)
                    # level_2_elem = self.driver.find_element(By.XPATH, f'//div[contains(text(),"{level_2}")]')
                    level_2_elem = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, f'//div[contains(text(),"{level_2}")]'))
                    )
                    self.driver.execute_script("arguments[0].click();", level_2_elem)
                    time.sleep(0.5)
                    
                    # Click level 3 (Sneakers/Boots/etc) - it opens automatically
                    # level_3_elem = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{level_3}')]")
                    try:
                        clicked_level_3 = False

                        if level_3:
                            for attempt in range(2):
                                try:
                                    logger.debug(f"[CATEGORY] Attempting level_3 click: {level_3}")
                                    level_3_elem = WebDriverWait(self.driver, 5).until(
                                        EC.element_to_be_clickable(
                                            (By.XPATH, f"//*[contains(text(), '{level_3}')]")
                                        )
                                    )
                                    
                                    self.driver.execute_script(
                                        "arguments[0].scrollIntoView({block: 'center'});",
                                        level_3_elem
                                    )
                                    time.sleep(0.3)
                                    
                                    self.driver.execute_script("arguments[0].click();", level_3_elem)
                                    clicked_level_3 = True
                                    break
                                except Exception:
                                    time.sleep(1)

                        if not clicked_level_3 and level_3:
                            raise Exception("Level 3 category not clicked")

                    except Exception:
                        fallback_by_level_1 = {
                            "men": "Athletic Shoes",
                            "women": "Athletic Shoes",
                            "kids": "Sneakers"
                        }

                        fallback_level_3 = fallback_by_level_1.get(level_1.lower(), "Sneakers")

                        logger.warning(
                            f"Could not click level_3 '{level_3}', defaulting to {fallback_level_3}"
                        )

                        fallback_elem = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable(
                                (By.XPATH, f"//*[normalize-space()='{fallback_level_3}']")
                            )
                        )
                        fallback_elem.click()
                        level_3 = fallback_level_3
                    
                except Exception as e:
                    logger.error(f"[CATEGORY] Could not set category: {e}")
                    return False


            # ✨ NEW: Size using AI data
            size_data = get_listing_size(listing_data)
            logger.info(f"[SIZE] Using size in UI: {size_data}")

            logger.debug(f"[SIZE] Raw size data: {size_data}")
            logger.debug(f"[SIZE] Category data: {listing_data.get('category')}")

            if size_data:
                try:
                    size_input = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-test="size"]'))
                    )
                    size_input.click()
                    time.sleep(0.5)

                   
                    size_value = str(size_data).strip().upper().replace(" ", "")
                    category_data = listing_data.get('category', {})
                    level_1_for_size = str(category_data.get('level_1', '')).lower()

                    # Convert kids sizes to Poshmark format (remove Y/C)
                    if level_1_for_size == 'kids':
                        size_candidates = [
                            size_value,
                            size_value.replace('Y', '').replace('C', '')
                        ]
                    else:
                        size_candidates = [size_value]

                    logger.debug(f"[SIZE] Candidates: {size_candidates}")

                    try:
                        size_button = None

                        for candidate in size_candidates:
                            try:
                                size_button = WebDriverWait(self.driver, 4).until(
                                    EC.element_to_be_clickable(
                                        (
                                            By.XPATH,
                                            f"//button[contains(@class,'multi-size-selector__button') and normalize-space()='{candidate}']"
                                        )
                                    )
                                )
                                size_value = candidate
                                break
                            except Exception:
                                continue

                        if not size_button:
                            raise Exception(f"No standard size matched candidates: {size_candidates}")

                        size_button.click()
                        logger.info(f"✓ Set size: {size_value}")

                        logger.debug(f"[SIZE] XPath used: //button[contains(@class,'multi-size-selector__button') and normalize-space()='{size_value}']")

                    except Exception:
                        logger.warning(f"Size '{size_value}' not found in standard sizes, using custom")

                        custom_tab_btn = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, '//span[contains(text(),"Custom")]'))
                        )
                        custom_tab_btn.click()

                        custom_size_input = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="customSize"]'))
                        )
                        custom_size_input.clear()
                        custom_size_input.send_keys(size_value)
                        time.sleep(1)

                        save_btn = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Save')]"))
                        )
                        save_btn.click()

                        done_btn = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="apply"]'))
                        )
                        done_btn.click()

                        logger.info(f"✓ Set custom size: {size_value}")

                except Exception as e:
                    logger.warning(f"Could not set size: {e}")


            # ============================================
            # CONDITION SELECTION
            
            try:
                # Get condition from category_data (platform-specific structured data)
                raw_condition = (
                    listing_data.get('condition')
                    or listing_data.get('category_data', {}).get('condition')
                    or listing_data.get('item_specifics', {}).get('Condition')
                )

                poshmark_condition = map_poshmark_condition(raw_condition)
                
                logger.debug(f"[CONDITION] Poshmark condition: {poshmark_condition}")
                
                logger.info(f"Setting condition: {poshmark_condition}")
                
                # Click condition dropdown
                condition_dropdown = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//div[contains(text(),"Select Condition")]'))
                )
                condition_dropdown.click()
                time.sleep(1)
                
                # Click condition option
                condition_xpath = f"//div[@class='fw--med' and contains(text(), '{poshmark_condition}')]"
                condition_option = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, condition_xpath))
                )
                condition_option.click()
                time.sleep(1)
                
                logger.info(f"✓ Set condition: {poshmark_condition}")
                
            except Exception as e:
                logger.warning(f"Could not set condition: {e}")


            # ============================================
            # STYLE TAGS (up to 3 tags)
            # ============================================
            title = listing_data.get('title', '')
            style_tags = extract_style_tags_from_title(title, max_tags=2)
            
            logger.debug(f"[STYLE] Style tags: {style_tags}")
            
            for tag in style_tags:
                try:
                    style_input = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="style-tag-input"]'))
                    )
            
                    style_input.click()
                    time.sleep(0.3)
            
                    style_input.send_keys(tag)
                    time.sleep(1.5)
            
                    style_input.send_keys(Keys.ARROW_DOWN)
                    time.sleep(0.3)
                    style_input.send_keys(Keys.ENTER)
                    time.sleep(0.5)
                    
                    # Click outside to commit the selected tag
                    self.driver.find_element(By.TAG_NAME, "body").click()
                    time.sleep(0.5)
            
                    logger.info(f"✓ Added style tag: {tag}")
            
                except Exception as e:
                    logger.warning(f"Style tag '{tag}' failed, skipping: {e}")
                    continue
                        
            # Brand
            brand_value = listing_data.get('brand') or listing_data.get('item_specifics', {}).get('Brand')

            if brand_value:
                try:
                    brand = str(brand_value).strip()

                    brand_input = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable(
                            (By.CSS_SELECTOR, '[placeholder="Enter the Brand/Designer"]')
                        )
                    )
                    brand_input.clear()
                    brand_input.send_keys(brand)

                    # WAIT for dropdown to appear (critical fix)
                    try:
                        brand_option = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located(
                                (By.XPATH, f"//li[contains(., '{brand}')]")
                            )
                        )

                        self.driver.execute_script(
                            "arguments[0].scrollIntoView({block: 'center'});",
                            brand_option
                        )
                        time.sleep(0.3)
                        self.driver.execute_script("arguments[0].click();", brand_option)

                    except Exception:
                        # fallback: just tab out
                        brand_input.send_keys(Keys.TAB)
                        logger.warning(f"[BRAND] Dropdown not found, used typed value: {brand}")

                    logger.info(f"✓ Set brand: {brand}")

                except Exception as e:
                    logger.warning(f"Could not set brand: {e}")

            # Color selection supports up to 2 colors
            color_data = listing_data.get('color') or listing_data.get('item_specifics', {}).get('Color')

            logger.debug(f"[COLOR] Raw color data: {color_data}")

            if color_data:
                try:
                    color_dropdown = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="color"]'))
                    )
                    color_dropdown.click()
                    time.sleep(1)

                    colors = parse_poshmark_colors(color_data, listing_data.get("title", ""))

                    logger.debug(f"[COLOR] Parsed colors: {colors}")

                    for color in colors:
                        try:
                            color = str(color).strip()
                            if not color:
                                continue

                            color_option = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable(
                                    (
                                        By.XPATH,
                                        f"//*[self::button or self::label or self::span or self::div][contains(@aria-label, '{color}') or normalize-space()='{color}']"
                                    )
                                )
                            )

                            self.driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center'});",
                                color_option
                            )
                            time.sleep(0.3)
                            self.driver.execute_script("arguments[0].click();", color_option)

                            time.sleep(0.5)
                            logger.info(f"✓ Added color: {color}")

                        except Exception as e:
                            logger.error(f"[COLOR] Required color '{color}' failed: {e}")
                            return False

                    logger.info(f"✓ Set colors: {colors}")

                except Exception as e:
                    logger.warning(f"Could not set colors: {e}")        
            
            # Price
            # clicing on price input box to open popup
            price_box = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="listingPrice"]')))
            price_box.click()

            price_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".listing-price-input")))
            price_input.clear()
            price_input.send_keys(str(int(listing_data['price'])))
            

            # clicking on done button
            done_btn = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-test="modal-footer"] [class="btn btn--primary"]')))
            done_btn.click()
            

            
            # SKU (in Additional Details section)
            if listing_data.get('sku'):

                # show private details input box
                show_details = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[class="listing-editor-toggle-link"]')))
                show_details.click()

                try:
                    sku_input = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="sku"]'))
                    )
                    sku_input.clear()
                    sku_input.send_keys(listing_data['sku'])
                    time.sleep(1)
                except:
                    logger.warning("Could not set SKU")
            
            logger.info("Filled listing form")
            return True
            
        except Exception as e:
            logger.error(f"Error filling form: {e}")
            return False
    
    def _submit_listing(self) -> str:
        """Submit listing and get listing ID"""
        try:
            
            # first extracting the listing id coz its alreayd present
            listing_id_item = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="discard"]'))
            )
            listing_id  = listing_id_item.get_attribute("data-et-prop-listing_id")

            logger.debug(f"[SUBMIT] Listing ID before submit: {listing_id}")
            

            # clicking on next button 
            logger.info("[SUBMIT] Clicking next button")
            next_btn = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="next"]'))
            )
            next_btn.click()

            # Find and click List button
            logger.info("[SUBMIT] Clicking list button")
            list_button = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="list"]'))
            )
            list_button.click()
            
            # Wait for redirect to listing page
            # time.sleep(20)
            # Wait for URL to change (max 20 seconds)
            current_url = self.driver.current_url
            WebDriverWait(self.driver, 20).until(
                EC.url_changes(current_url)
            )

            logger.info(f"[SUBMIT] URL changed to: {self.driver.current_url}")

            return listing_id
            
        except Exception as e:
            logger.error(f"Error submitting listing: {e}")
            return None
    
    def _close_driver(self):
        """Close driver"""
        if self.driver:
            try:
                self.driver.quit()
                self.driver = None
            except:
                pass
