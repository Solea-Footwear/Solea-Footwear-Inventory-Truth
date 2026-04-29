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
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


# Style tag mapping - keywords in title → Poshmark style tags
STYLE_TAG_MAP = {
    'Retro': ['retro', 'vintage', 'classic', 'og', 'original', 'throwback'],
    'Vintage': ['vintage', 'antique', '70s', '80s', '90s', 'y2k'],
    '90s': ['90s', 'nineties'],
    '80s': ['80s', 'eighties'],
    'Y2K': ['y2k', '2000s'],

    'Activewear': ['athletic', 'sport', 'gym', 'training', 'performance', 'running', 'runner', 'basketball', 'tennis', 'cross training', 'cross-trainer'],
    'Athleisure': ['athleisure', 'sporty', 'lifestyle'],
    'Casual': ['casual', 'everyday', 'daily'],

    'Streetwear': ['streetwear', 'street', 'urban', 'hype', 'skate', 'skater'],
    'Minimalist': ['minimalist', 'minimal', 'simple', 'clean'],
    'Luxury': ['luxury', 'premium', 'designer'],

    'Leather': ['leather', 'full grain', 'top grain'],
    'Suede': ['suede', 'nubuck'],
    'Mesh': ['mesh', 'breathable'],
    'Knit': ['knit', 'knitted', 'woven', 'primeknit', 'flyknit'],

    'Chunky': ['chunky', 'thick', 'platform', 'dad shoe'],
    'Lightweight': ['lightweight', 'light', 'ultralight'],
    'Waterproof': ['waterproof', 'water-resistant', 'weatherproof', 'gore-tex', 'gtx'],

    'Outdoor': ['trail', 'hiking', 'hiker', 'outdoor', 'terrain'],
    'Workwear': ['work', 'workwear', 'utility', 'service'],
    'Preppy': ['preppy', 'prep', 'ivy', 'collegiate'],
    'Colorful': ['colorful', 'multicolor', 'rainbow', 'vibrant'],
    'Neutral': ['neutral', 'beige', 'tan', 'cream', 'nude', 'taupe'],
}

def extract_style_tags_from_title(title: str, max_tags: int = 3) -> list:
    """
    Extract up to 3 style tags from title using keyword matching
    
    Args:
        title: Product title string
        max_tags: Maximum tags to return (Poshmark allows 3)
    
    Returns:
        List of matched Poshmark style tag strings
    """
    title_lower = title.lower()
    matched_tags = []
    
    for tag, keywords in STYLE_TAG_MAP.items():
        if len(matched_tags) >= max_tags:
            break
            
        for keyword in keywords:
            if keyword in title_lower:
                matched_tags.append(tag)
                break  # Move to next tag once matched
    
    return matched_tags[:max_tags]

class PoshmarkLister:
    """Selenium-based listing creation for Poshmark"""
    
    def __init__(self, profile_dir: str = None):

        # Get profile path
        if profile_dir:
            self.profile_dir = os.path.abspath(profile_dir)
        else:
            # base_dir = os.path.dirname(os.path.abspath(__file__))
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
            print("here is listing data")
            print(listing_data)

            if not self._init_driver():
                return {'success': False, 'error': 'Failed to initialize driver'}
            
            # Navigate to create listing page
            logger.info("Navigating to Poshmark create listing page...")
            self.driver.get('https://poshmark.com/create-listing')
            
            # time.sleep(3)
            
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
            
            # chrome_options.add_argument('--no-sandbox')
            # chrome_options.add_argument('--disable-dev-shm-usage')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(options=chrome_options)

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
            
            # Wait for upload to complete
            # time.sleep(5)

            apply_button = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.XPATH, '//button[@data-et-name="apply" and contains(text(),"Apply")]'))
            )
            apply_button.click()

            # time.sleep(5)
            
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
            
            # time.sleep(1)
            
            # Description
            desc_input = self.driver.find_element(By.CSS_SELECTOR, '[data-vv-name="description"]')
            desc_input.clear()
            # desc_input.send_keys(listing_data['description'][:500])
            hardcoded_description = """
Please Review All Photos For An Accurate Depiction. Any scuff marks and wear that may be present on the shoes will be visible in the pictures. To get a complete view of the shoes, please see all of the pictures. Some of the items will have wear that does not appear in every image of the shoes. To receive the best insight into the shoes, please refer to all photos because if there is any wear, it will be made clear in the other photos. If you have any questions, feel free to reach out to us.
            """
            desc_input.send_keys(hardcoded_description)
            
            # time.sleep(1)
            
            # Category - Two-level dropdown (Main Category + Subcategory)
            # if listing_data.get('category'):
            #     try:
            #         # Click main category dropdown
            #         category_dropdown = self.driver.find_element(By.XPATH, "//div[@class='dropdown__selector dropdown__selector--select-tag dropdown__selector--select-tag--large ellipses']")
            #         category_dropdown.click()
            #         time.sleep(1)
                    
            #         # Click "Men" (or extract from category string)
            #         main_category = self.driver.find_element(By.XPATH, '//p[contains(text(),"Men")]')
            #         main_category.click()
            #         time.sleep(1)
                    
            #         # Click "Shoes" subcategory
            #         shoes_option = self.driver.find_element(By.XPATH, '//div[contains(text(),"Shoes")]')
            #         shoes_option.click()
            #         time.sleep(1)

            #         # coz subcategory automatically opens thats why we dont click that by selenium
                    
            #         # Click subcategory dropdown (Athletic Shoes, Boots, etc.)
            #         # subcategory_dropdown = self.driver.find_element(By.XPATH, "//div[@class='dropdown__selector dropdown__selector--select-tag dropdown__selector--select-tag--large']")
            #         # subcategory_dropdown.click()
            #         time.sleep(1)
                    
            #         # Click specific subcategory (e.g., "Athletic Shoes")
            #         athletic_shoes = self.driver.find_element(By.XPATH, "//a[contains(text(), 'Athletic Shoes')]")
            #         athletic_shoes.click()
            #         time.sleep(1)
                    
            #     except Exception as e:
            #         logger.warning(f"Could not set category: {e}")
                        
            
            # ✨ NEW: Category using AI data
            if listing_data.get('category'):
                try:
                    # category_dropdown = self.driver.find_element(By.XPATH, "//div[@class='dropdown__selector dropdown__selector--select-tag dropdown__selector--select-tag--large ellipses']")

                    category_dropdown = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@class='dropdown__selector dropdown__selector--select-tag dropdown__selector--select-tag--large ellipses']"))
                    )
                    category_dropdown.click()
                    # time.sleep(1)
                    
                    # Get category from AI data
                    category = listing_data.get('category', {})
                    level_1 = category.get('level_1', 'Men')
                    level_2 = category.get('level_2', 'Shoes')
                    level_3 = category.get('level_3', 'Sneakers')
                    
                    # Click level 1 (Men/Women/Kids)
                    # level_1_elem = self.driver.find_element(By.XPATH, f'//p[contains(text(),"{level_1}")]')
                    level_1_elem = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f'//p[contains(text(),"{level_1}")]')))
                    level_1_elem.click()
                    # time.sleep(1)
                    
                    # Click level 2 (Shoes/Accessories/etc)
                    # level_2_elem = self.driver.find_element(By.XPATH, f'//div[contains(text(),"{level_2}")]')
                    level_2_elem = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f'//div[contains(text(),"{level_2}")]')))
                    level_2_elem.click()
                    # time.sleep(1)
                    
                    # Click level 3 (Sneakers/Boots/etc) - it opens automatically
                    # time.sleep(1)
                    # level_3_elem = self.driver.find_element(By.XPATH, f"//a[contains(text(), '{level_3}')]")
                    level_3_elem = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, f"//a[contains(text(), '{level_3}')]")))
                    level_3_elem.click()
                    # time.sleep(1)
                    
                    logger.info(f"✓ Set category: {level_1} > {level_2} > {level_3}")
                    
                except Exception as e:
                    logger.warning(f"Could not set category: {e}")


            # ✨ NEW: Size using AI data
            if listing_data.get('size'):
                try:
                    
                    # size_input = self.driver.find_element(By.CSS_SELECTOR, '[data-test="size"]')
                    size_input = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-test="size"]')))
                    size_input.click()

                    # Try to find the size button (AI formatted it as just "10.5", "6.5", etc.)
                    size_button = WebDriverWait(self.driver, 7).until(
                        EC.element_to_be_clickable((By.XPATH, f"//button[contains(@class,'multi-size-selector__button') and text()='{listing_data['size']}']"))
                    )

                    # //button[contains(@class,'multi-size-selector__button') and text()='4c']

                    print("here is size xpath")
                    print(f"//button[contains(@class,'multi-size-selector__button') and text()='{listing_data['size']}']")
                    size_button.click()
                    # time.sleep(1)
                    
                    logger.info(f"✓ Set size: {listing_data['size']}")
                    
                except:
                    # If not in standard sizes, use custom
                    try:
                        # custom_tab_btn = WebDriverWait(self.driver, 10).until(
                        #     EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-test="horizontal-nav-1"]'))
                        # )
                        custom_tab_btn = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, '//span[contains(text(),"Custom")]'))
                        )
                        custom_tab_btn.click()

                        size_input = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="customSize"]'))
                        )
                        size_input.send_keys(listing_data['size'])
                        time.sleep(0.5)

                        
                        # save_btn = self.driver.find_element(By.XPATH,"//button[contains(text(),'Save')]")
                        save_btn = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, "//button[contains(text(),'Save')]")))
                        save_btn.click()
                        # time.sleep(1)

                        # done_btn = self.driver.find_element(By.CSS_SELECTOR,'[data-et-name="apply"]')
                        done_btn = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-et-name="apply"]')))
                        done_btn.click()
                        # time.sleep(1)
                        
                        logger.info(f"✓ Set custom size: {listing_data['size']}")
                        
                    except Exception as e:
                        logger.warning(f"Could not set size: {e}")
            


            # ============================================
            # CONDITION SELECTION 
            # ============================================
            # CONDITION SELECTION
            
            try:
                # Get condition from category_data (platform-specific structured data)
                poshmark_condition = listing_data.get('category_data', {}).get('condition', 'Good')
                print("here is poshmark condition",poshmark_condition)
                
                logger.info(f"Setting condition: {poshmark_condition}")
                
                # Click condition dropdown
                condition_dropdown = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//div[contains(text(),"Select Condition")]'))
                )
                condition_dropdown.click()
                time.sleep(0.5)
                
                # Click condition option
                condition_xpath = f"//div[@class='fw--med' and contains(text(), '{poshmark_condition}')]"
                condition_option = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, condition_xpath))
                )
                condition_option.click()
                time.sleep(0.5)
                
                logger.info(f"✓ Set condition: {poshmark_condition}")
                
            except Exception as e:
                logger.warning(f"Could not set condition: {e}")


            # ============================================
            # STYLE TAGS (up to 3 tags)
            # ============================================
            try:
                # Extract style tags from title
                title = listing_data.get('title', '')
                style_tags = extract_style_tags_from_title(title, max_tags=3)

                print("here are style tags")
                print(style_tags)
                
                if style_tags:
                    logger.info(f"Setting style tags: {style_tags}")
                    
                    # Click the style tag input field
                    style_input = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="style-tag-input"]'))
                    )
                    
                    # Type each tag and click the matching dropdown option
                    for tag in style_tags:
                        # Clear and type the tag name
                        style_input.clear()
                        style_input.send_keys(tag)
                        time.sleep(1)
                        
                        # Wait for dropdown to appear and click the matching tag
                        tag_option_xpath = f"//div[@data-et-name='{tag}']"
                        try:
                            tag_option = WebDriverWait(self.driver, 5).until(
                                EC.element_to_be_clickable((By.XPATH, tag_option_xpath))
                            )
                            tag_option.click()
                            time.sleep(0.5)
                            logger.info(f"✓ Added style tag: {tag}")
                        except:
                            # If exact match not found in dropdown, skip this tag
                            logger.warning(f"Style tag '{tag}' not found in dropdown, skipping")
                            continue
                    
                    logger.info(f"✓ Set {len(style_tags)} style tags")
                else:
                    logger.debug("No style tags matched from title")
                    
            except Exception as e:
                logger.warning(f"Could not set style tags: {e}")
            
            
            # Size (from item_specifics if available)
            # if listing_data.get('item_specifics', {}).get('Size'):
            #     try:
            #         # clicking on dropdown
            #         size_input = self.driver.find_element(By.CSS_SELECTOR, '[data-test="size"]')
            #         size_input.click()

            #         # first try to select already showing option 
            #         size_button = WebDriverWait(self.driver, 7).until(
            #             EC.element_to_be_clickable((By.XPATH, f"//button[contains(@class,'multi-size-selector__button') and text()='{listing_data['item_specifics']['Size']}']"))
            #         )

            #         if size_input is not None:
            #             size_button.click()
            #         else:
            #             # now clicking on custom tab in new opened popup
            #             custom_tab_btn = WebDriverWait(self.driver, 10).until(
            #                 EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-test="horizontal-nav-1"]'))
            #             )
            #             custom_tab_btn.click()

            #             # now typing size and clicking on save and done button
            #             size_input = WebDriverWait(self.driver, 10).until(
            #                 EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-vv-name="customSize"]'))
            #             )
            #             size_input.send_keys(listing_data['item_specifics']['Size'])
            #             time.sleep(2)

            #             # clicking on save button
            #             save_btn = self.driver.find_element(By.XPATH,"//button[contains(text(),'Save')]")
            #             save_btn.click()
            #             time.sleep(1)

            #             # now clicking on `Done` Btn
            #             done_btn = self.driver.find_element(By.CSS_SELECTOR,'[data-et-name="apply"]')
            #             done_btn.click()
            #             time.sleep(1)

            #     except Exception as e:
            #         logger.warning("Could not set size",e)



            
            # # Brand
            # if listing_data.get('item_specifics', {}).get('Brand'):
            #     try:
            #         brand_input = self.driver.find_element(By.CSS_SELECTOR, '[placeholder="Enter the Brand/Designer"]')
            #         brand_input.clear()
            #         brand_input.send_keys(listing_data['item_specifics']['Brand'])
            #         time.sleep(1)
            #     except:
            #         logger.warning("Could not set brand")


            # ✨ NEW: Brand using AI data
            if listing_data.get('brand'):
                try:
                    # brand_input = self.driver.find_element(By.CSS_SELECTOR, '[placeholder="Enter the Brand/Designer"]')
                    brand_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[placeholder="Enter the Brand/Designer"]')))
                    brand_input.clear()
                    brand_input.send_keys(listing_data['brand'])
                    time.sleep(1)
                    
                    logger.info(f"✓ Set brand: {listing_data['brand']}")
                except Exception as e:
                    logger.warning(f"Could not set brand: {e}")


            # ✨ NEW: Color selection (Poshmark only)
            if listing_data.get('color'):
                try:
                    # Click color dropdown
                    # color_dropdown = self.driver.find_element(By.CSS_SELECTOR, '[data-et-name="color"]')
                    color_dropdown = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-et-name="color"]')))
                    color_dropdown.click()
                    # time.sleep(1)
                    
                    # Click the matching color
                    # color_option = self.driver.find_element(By.XPATH, f"//span[text()='{listing_data['color']}']")
                    # color_option = self.driver.find_element(By.XPATH, f"//span[contains(text(),'{listing_data['color']}')]")
                    color_option = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, f"//span[contains(text(),'{listing_data['color']}')]")))
                    color_option.click()
                    # time.sleep(1)
                    
                    logger.info(f"✓ Set color: {listing_data['color']}")
                    
                except Exception as e:
                    logger.warning(f"Could not set color: {e}")

            
            # Price
            # clicing on price input box to open popup
            # price_box = self.driver.find_element(By.CSS_SELECTOR,'[data-vv-name="listingPrice"]')
            price_box = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-vv-name="listingPrice"]')))
            price_box.click()

            # price_input = self.driver.find_element(By.CSS_SELECTOR, ".listing-price-input")
            price_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, ".listing-price-input")))
            price_input.clear()
            price_input.send_keys(str(int(listing_data['price'])))
            # time.sleep(2)
            

            # clicking on done button
            # done_btn = self.driver.find_element(By.CSS_SELECTOR,'[data-test="modal-footer"] [class="btn btn--primary"]')
            done_btn = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-test="modal-footer"] [class="btn btn--primary"]')))
            done_btn.click()
            # time.sleep(2)
            

            
            # SKU (in Additional Details section)
            if listing_data.get('sku'):

                # show private details input box
                # show_details = self.driver.find_element(By.CSS_SELECTOR,'[class="listing-editor-toggle-link"]')
                show_details = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[class="listing-editor-toggle-link"]')))
                show_details.click()

                try:
                    sku_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-vv-name="sku"]'))
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

            print("here is listing id",listing_id)
            

            # clicking on next button 
            print("clicking on next button")
            next_btn = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="next"]'))
            )
            next_btn.click()

            # Find and click List button
            print("clicking on list button")
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

            print(f"URL changed to: {self.driver.current_url}")

            return listing_id
            
            # Get listing ID from URL
            # URL format: https://poshmark.com/listing/{LISTING_ID}
            # current_url = self.driver.current_url
            
            # if '/listing/' in current_url:
            #     listing_id = current_url.split('/listing/')[-1].split('?')[0]
            #     logger.info(f"Got listing ID from URL: {listing_id}")
            #     return listing_id
            
            logger.error("Could not extract listing ID from URL")
            return None
            
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



"""
TESTING CODE - Add to end of poshmark_lister.py
"""

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    
    # Test data
    test_listing_data = {
        'title': 'Nike Air Max 90 Mens Size 10 Black White Running Shoes Sneakers',
        'description': 'Great condition Nike Air Max 90 in size 10. Black and white colorway. Minimal wear, clean inside and out. Perfect for casual wear or running.',
        'price': 85.00,
        'category': 'Men > Shoes > Athletic Shoes',
        'item_specifics': {
            'Brand': 'Nike',
            'Size': '10',
            'Color': 'Black/White',
            'Condition': 'Good'
        },
        'sku': 'NIKE-AM90-001'
    }
    
    # Test images (download some sample shoe images or use your own)
    test_images = [
        r'E:\Ebay Crosslisting\sample_images\1.png',
        r'E:\Ebay Crosslisting\sample_images\2.png',
        r'E:\Ebay Crosslisting\sample_images\3.png',
        r'E:\Ebay Crosslisting\sample_images\4.png',
        r'E:\Ebay Crosslisting\sample_images\5.png',
    ]
    
    # Create lister
    lister = PoshmarkLister()
    
    # Run test
    print("Starting Poshmark listing test...")
    result = lister.create_listing(test_listing_data, test_images)
    
    print("\n=== RESULT ===")
    print(f"Success: {result['success']}")
    if result['success']:
        print(f"Listing ID: {result['channel_listing_id']}")
    else:
        print(f"Error: {result['error']}")
