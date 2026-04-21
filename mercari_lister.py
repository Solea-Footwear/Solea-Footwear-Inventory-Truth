"""
Mercari Lister - Selenium
Automates listing creation on Mercari
"""
import logging
import os
import time
from typing import Dict, List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


logger = logging.getLogger(__name__)


class MercariLister:
    """Selenium-based listing creation for Mercari"""
    
    def __init__(self, profile_dir: str = None):
        # Get profile path
        if profile_dir:
            self.profile_dir = os.path.abspath(profile_dir)
        else:
            # base_dir = os.path.dirname(os.path.abspath(__file__))
            # self.profile_dir = os.path.join(os.path.dirname(base_dir), 'delisting', 'profiles')

            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.profile_dir = os.path.join(base_dir, "profiles")
        
        self.driver = None
    
    
    def create_listing(self, listing_data: Dict, image_paths: List[str]) -> Dict:
        """
        Create listing on Mercari
        
        Args:
            listing_data (dict): Listing information
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
            logger.info("Navigating to Mercari create listing page...")
            self.driver.get('https://www.mercari.com/sell/')
            

            time.sleep(3)

            # opening page one time more to avoid any issues
            try:
                WebDriverWait(self.driver, 7).until(
                    EC.presence_of_element_located((By.XPATH, '//p[contains(text(),"Oops! Something wrong happened. Please try again.")]'))
                )
                self.driver.get('https://www.mercari.com/sell/')
            except:
                print("No error occured...actually on mercari")
                pass
            
            # # Upload images
            # logger.info(f"Uploading {len(image_paths)} images...")
            # if not self._upload_images(image_paths):
            #     return {'success': False, 'error': 'Failed to upload images'}
            
            
            # Fill in listing details
            logger.info("Filling listing details...")
            if not self._fill_listing_form(listing_data):
                return {'success': False, 'error': 'Failed to fill form'}
            

            # Upload images
            logger.info(f"Uploading {len(image_paths)} images...")
            if not self._upload_images(image_paths):
                return {'success': False, 'error': 'Failed to upload images'}
            
            # Submit listing
            logger.info("Submitting listing...")
            listing_id = self._submit_listing()
            
            if not listing_id:
                return {'success': False, 'error': 'Failed to get listing ID'}
            
            logger.info(f"Mercari listing created successfully: {listing_id}")
            
            return {
                'success': True,
                'channel_listing_id': listing_id,
                'platform': 'mercari'
            }
            
        except Exception as e:
            logger.error(f"Error creating Mercari listing: {e}")
            return {
                'success': False,
                'error': str(e)
            }
        
        finally:
            self._close_driver()
    
    def _init_driver(self) -> bool:
        """Initialize Chrome driver with Mercari profile"""
        try:
            chrome_options = Options()
            
            # Use Mercari profile (pre-logged in)
            profile_path = os.path.join(self.profile_dir, 'mercari')
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
            
            
            logger.info("Mercari Chrome driver initialized")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing driver: {e}")
            return False
    
    def _upload_images(self, image_paths: List[str]) -> bool:
        """Upload images to Mercari"""
        try:
            # Find file input element
            file_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[accept="image/*"]'))
            )
            
            # Upload all images at once (Mercari accepts multiple)
            all_paths = '\n'.join(image_paths[:12])  # Mercari max 12 images
            file_input.send_keys(all_paths)

            try:
                WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//p[contains(text(),"Oops! Something wrong happened. Please try again.")]'))
                )
                self.driver.get('https://www.mercari.com/sell/')
            except:
                print("No error occured...actually on mercari")
                pass
            
            # Wait for upload to complete
            time.sleep(5)
            
            logger.info(f"Uploaded {len(image_paths)} images")
            return True
            
        except Exception as e:
            logger.error(f"Error uploading images: {e}")
            return False
    
    def _fill_listing_form(self, listing_data: Dict) -> bool:
        """Fill in listing form fields"""
        try:
            
            
            # Category (if available)
            # if listing_data.get('category'):
            #     try:
            #         category_btn = self.driver.find_element(By.XPATH, "//button[contains(text(),'Select category')]")
            #         category_btn.click()
            #         time.sleep(2)

            #         # now wait for the category list to appear
            #         category_list = WebDriverWait(self.driver, 10).until(
            #             EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="DialogBody"]'))
            #         )

            #         # now clicking on the men category
            #         men_category = self.driver.find_element(By.XPATH, "//div[contains(text(), 'Men')]")
            #         men_category.click()
            #         time.sleep(1)

            #         # now clicking on the shoes category
            #         shoes_category = self.driver.find_element(By.XPATH, "//div[contains(text(), 'Shoes')]")
            #         shoes_category.click()
            #         time.sleep(1)

            #         # now clicking on the athletic shoes category
            #         athletic_shoes_category = self.driver.find_element(By.XPATH, "//div[contains(text(), 'Athletic')]")
            #         athletic_shoes_category.click()
            #         time.sleep(1)

            #     except:
            #         logger.warning("Could not set category")
            
            
            # ✨ NEW: Category using AI data
            if listing_data.get('category'):
                try:
                    category_btn = self.driver.find_element(By.XPATH, "//button[contains(text(),'Select category')]")
                    category_btn.click()
                    time.sleep(2)

                    category_list = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="DialogBody"]'))
                    )

                    # ✨ Get category from AI data
                    category = listing_data.get('category', {})
                    level_1 = category.get('level_1', 'Men')
                    level_2 = category.get('level_2', 'Shoes')
                    level_3 = category.get('level_3', 'Athletic')

                    # Click level 1 (Men/Women/Kids)
                    level_1_elem = self.driver.find_element(By.XPATH, f"//div[contains(text(), '{level_1}')]")
                    level_1_elem.click()
                    time.sleep(1)

                    # Click level 2 (Shoes/Tops/etc)
                    level_2_elem = self.driver.find_element(By.XPATH, f"//div[contains(text(), '{level_2}')]")
                    level_2_elem.click()
                    time.sleep(1)

                    # Click level 3 (Athletic/Boots/etc)
                    level_3_elem = self.driver.find_element(By.XPATH, f"//div[contains(text(), '{level_3}')]")
                    level_3_elem.click()
                    time.sleep(1)

                    logger.info(f"✓ Set category: {level_1} > {level_2} > {level_3}")

                except Exception as e:
                    logger.warning(f"Could not set category: {e}")
            


            # Title
            title_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="Title"]'))
            )
            title_input.clear()
            title_input.send_keys(listing_data['title'])  # Mercari 40 char limit
            
            time.sleep(1)
            
            # Description
            desc_input = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="Description"]')
            desc_input.clear()
            hardcoded_description = """
            Please Review All Photos For An Accurate Depiction. Any scuff marks and wear that may be present on the shoes will be visible in the pictures. To get a complete view of the shoes, please see all of the pictures. Some of the items will have wear that does not appear in every image of the shoes. To receive the best insight into the shoes, please refer to all photos because if there is any wear, it will be made clear in the other photos. If you have any questions, feel free to reach out to us.
            """
            # desc_input.send_keys(listing_data['description'][:950])
            desc_input.send_keys(hardcoded_description)
            
            time.sleep(1)
            
            
            # Condition selection
            # try:
            #     # Map eBay condition to Mercari
            #     # eBay: New, Used, etc. → Mercari: New, Like new, Good, Fair, Poor
            #     condition_mapping = {
            #         'new': 'ConditionNew',
            #         'like new': 'ConditionLikeNew',
            #         'good': 'ConditionGood',
            #         'fair': 'ConditionFair',
            #         'poor': 'ConditionPoor'
            #     }
                
            #     # Get condition from listing data (default to "Good")
            #     ebay_condition = listing_data.get('item_specifics', {}).get('Condition', 'Good').lower()
                
            #     # Find matching Mercari condition (default to Good)
            #     mercari_condition = 'ConditionGood'
            #     for key, value in condition_mapping.items():
            #         if key in ebay_condition:
            #             mercari_condition = value
            #             break
                
            #     # Click the condition radio button
            #     condition_label = self.driver.find_element(By.XPATH, f"//label[@data-testid='{mercari_condition}']")
            #     condition_label.click()
            #     time.sleep(1)
                
            #     logger.info(f"Selected condition: {mercari_condition}")
                
            # except Exception as e:
            #     logger.warning(f"Could not set condition: {e}")


            # ✨ NEW: Condition using AI data
            try:
                # AI already formatted it correctly (ConditionGood, ConditionNew, etc.)
                mercari_condition = listing_data.get('condition', 'ConditionGood')
                
                condition_label = self.driver.find_element(By.XPATH, f"//label[@data-testid='{mercari_condition}']")
                condition_label.click()
                time.sleep(1)
                
                logger.info(f"✓ Set condition: {mercari_condition}")
                
            except Exception as e:
                logger.warning(f"Could not set condition: {e}")
            
            # Brand
            # if listing_data.get('item_specifics', {}).get('Brand'):
            #     try:
            #         brand_input = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="Brand"]')
            #         brand_input.clear()
            #         brand_input.send_keys(listing_data['item_specifics']['Brand'])
            #         time.sleep(2)

            #         # now clicking on first brand but with webdriver wait option [data-testid="Brand-option"]
            #         brand_option = WebDriverWait(self.driver, 10).until(
            #             EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="Brand-option"]'))
            #         )   
            #         brand_option.click()
            #         time.sleep(1)
            #     except:
            #         logger.warning("Could not set brand")


            # ✨ NEW: Brand using AI data
            if listing_data.get('brand'):
                try:
                    brand_input = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="Brand"]')
                    brand_input.clear()
                    brand_input.send_keys(listing_data['brand'])
                    time.sleep(2)

                    brand_option = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="Brand-option"]'))
                    )   
                    brand_option.click()
                    time.sleep(1)
                    
                    logger.info(f"✓ Set brand: {listing_data['brand']}")
                except Exception as e:
                    logger.warning(f"Could not set brand: {e}")


            # Size - Dropdown with list
            # try:
            #     size = listing_data.get('item_specifics', {}).get('Size', '')
                
            #     if size:
            #         # Click size dropdown
            #         size_dropdown = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='Size']")
            #         size_dropdown.click()
            #         time.sleep(1)
                    
            #         # Find and click size option
            #         # Format: "13 (46)" - contains US size at start
            #         size_option = self.driver.find_element(
            #             By.XPATH, 
            #             f"//li[@data-testid='Size-option' and starts-with(text(), '{size} (')]"
            #         )
            #         size_option.click()
            #         time.sleep(1)
                    
            #         logger.info(f"Selected size: {size}")
                    
            # except Exception as e:
            #     logger.warning(f"Could not set size: {e}")


            # ✨ NEW: Size using AI data (already formatted as "10.5 (43.5)")
            try:
                size = listing_data.get('size', '')
                
                if size:
                    size_dropdown = self.driver.find_element(By.CSS_SELECTOR, "[data-testid='Size']")
                    size_dropdown.click()
                    time.sleep(1)
                    
                    # AI already formatted it: "10.5 (43.5)"
                    size_option = self.driver.find_element(
                        By.XPATH, 
                        f"//li[@data-testid='Size-option' and text()='{size}']"
                    )
                    size_option.click()
                    time.sleep(1)
                    
                    logger.info(f"✓ Set size: {size}")
                    
            except Exception as e:
                logger.warning(f"Could not set size: {e}")

            # Price
            print('setting base price for mercari listing ',str(listing_data['price']))
            # price_input = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="Price"]')
            # price_input.clear()
            # price_input.send_keys(str(int(listing_data['price'])))


            # Price - Use execCommand method (simulates native typing)
            try:
                price_input = self.driver.find_element(By.CSS_SELECTOR, '[data-testid="Price"]')
                price_value = str(listing_data['price'])
                
                # Focus the input
                price_input.click()
                time.sleep(0.5)
                
                # Use JavaScript execCommand to simulate native input
                self.driver.execute_script(
                    """
                    const input = arguments[0];
                    const price = arguments[1];
                    
                    // Focus
                    input.focus();
                    
                    // Select all existing text
                    input.select();
                    
                    // Delete existing content
                    document.execCommand('delete');
                    
                    // Insert new text (triggers proper events)
                    document.execCommand('insertText', false, price);
                    
                    // Blur to trigger validation
                    input.blur();
                    """,
                    price_input,
                    price_value
                )
                
                time.sleep(1)
                
                # Verify
                final_value = price_input.get_attribute('value')
                logger.info(f"✓ Price set: ${final_value}")

                # turn off auto adjusting the price
                auto_price_adjuster = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="SmartPricingButton"]'))
                )
                auto_price_adjuster.click()
                time.sleep(1)
                
                popup_confirm_btn = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '[data-testid="SmartPricingTurnOffButton"]'))
                )
                popup_confirm_btn.click()
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error setting price: {e}")
            
            
            time.sleep(1)
            
            # Shipping
            try:
                # Select shipping method (default to "Ship on your own")
                shipping_option = self.driver.find_element(By.CSS_SELECTOR,'[data-testid="ShipOnYourOwn"]')
                shipping_option.click()
                time.sleep(1)
            except:
                logger.warning("Could not set shipping")
            
            logger.info("Filled listing form")
            return True
            
        except Exception as e:
            logger.error(f"Error filling form: {e}")
            return False
    
    def _submit_listing(self) -> str:
        """Submit listing and get listing ID"""
        try:
            # Scroll to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # Find and click List button
            list_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'List')]"))
            )
            list_button.click()

            # lets wait for url to change
            WebDriverWait(self.driver, 10).until(EC.url_changes(self.driver.current_url))   
            
            # Wait for redirect to listing page
            time.sleep(5)
            
            # Get listing ID from URL
            # URL format: https://www.mercari.com/sell/confirmation/m22378876369/
            current_url = self.driver.current_url
            
            # adjust the current url to get the listing id
            if '/sell/confirmation/' in current_url:
                listing_id = current_url.split('/sell/confirmation/')[-1].split('/')[0]
                logger.info(f"Got listing ID from URL: {listing_id}")
                return listing_id
            
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
TESTING CODE - Add to end of mercari_lister.py
"""

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO) 

    # Test data
    # test_listing_data = {
    #     'title': 'Nike Air Max 90 Mens Size 10 Black White Running Shoes Sneakers',
    #     'description': 'Great condition Nike Air Max 90 in size 10. Black and white colorway. Minimal wear, clean inside and out. Perfect for casual wear or running.',
    #     'price': 85.00,
    #     'category': 'Men > Shoes > Athletic Shoes',
    #     'item_specifics': {
    #         'Brand': 'Nike',
    #         'Size': '10',
    #         'Color': 'Black/White',
    #         'Condition': 'Good'
    #     },
    #     'sku': 'NIKE-AM90-001'
    # }


    test_listing_data = {'title': 'Nike Air Jordan 1 Mid Toddler Size 5C Orange White Basketball Shoes Sneakers', 'description': 'Please Review All Photos For An Accurate Depiction:Any scuff marks and wear that may be present on the shoes will be visible in the pictures. To get a complete view of the shoes, please see all of the pictures. Some of the items will have wear that does not appear in every image of the shoes. To receive the best insight into the shoes, please refer to all photos because if there is any wear, it will be made clear in the other photos. If you have any questions, feel free to reach out to us and we would be happy to help. AdditionalThe last photo of the listing is our (SKU) number. This number is for inventory purposes and is not included with purchase. Shoe trees are not includedNo box unless specified Shipping Policy: Handling Time: Shipping is done on the same day or the next business day.Shipments are sent Monday - Friday. All orders placed over the weekend will be shipped on Monday.', 'price': 39.99, 'shipping': 'free', 'photos': ['https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/DdUAAOSwyQRoWTAG/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/JSAAAOSwyjNoWTAC/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/UNUAAOSwRlxoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/oV4AAOSwrnhoWTAC/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/DKkAAOSwHp5oWTAB/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/A9wAAOSwMTJoWTAA/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/jMQAAOSwGEFoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/fQoAAOSwx9RoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/PocAAOSwL4BoWTAA/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/NTAwWDUwMA==/z/bkAAAOSwR5doWTAU/$_57.PNG?set_id=8800005007', 'https://i.ebayimg.com/00/s/NTIzWDUyMw==/z/HRoAAOSwwG5oWTA9/$_57.PNG?set_id=880000500F'], 'sku': '00048441', 'category': {'level_1': 'Kids', 'level_2': 'Boys shoes', 'level_3': 'Boys 2T-5T'}, 'condition': 'ConditionGood', 'size': '5', 'brand': 'Nike'}
    
    # Test images (download some sample shoe images or use your own)    
    # test_images = [
    #     r'E:\Ebay Crosslisting\sample_images\1.png',
    #     r'E:\Ebay Crosslisting\sample_images\2.png',
    #     r'E:\Ebay Crosslisting\sample_images\3.png',
    #     r'E:\Ebay Crosslisting\sample_images\4.png',
    #     r'E:\Ebay Crosslisting\sample_images\5.png',
    # ]   

    test_images = ['https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/DdUAAOSwyQRoWTAG/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/JSAAAOSwyjNoWTAC/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/UNUAAOSwRlxoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/oV4AAOSwrnhoWTAC/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/DKkAAOSwHp5oWTAB/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/A9wAAOSwMTJoWTAA/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/jMQAAOSwGEFoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/fQoAAOSwx9RoWTAD/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/MTYwMFgxNjAw/z/PocAAOSwL4BoWTAA/$_57.JPG?set_id=880000500F', 'https://i.ebayimg.com/00/s/NTAwWDUwMA==/z/bkAAAOSwR5doWTAU/$_57.PNG?set_id=8800005007', 'https://i.ebayimg.com/00/s/NTIzWDUyMw==/z/HRoAAOSwwG5oWTA9/$_57.PNG?set_id=880000500F']

    # Create lister
    lister = MercariLister()
    
    # Run test
    print("Starting Mercari listing test...")   
    result = lister.create_listing(test_listing_data, test_images)
    
    print("\n=== RESULT ===")
    print(f"Success: {result['success']}")
    if result['success']:
        print(f"Listing ID: {result['channel_listing_id']}")
    else:
        print(f"Error: {result['error']}")  


