"""
Selenium Delisting Module
Handles delisting from Poshmark and Mercari using Selenium
"""
import logging
import os
import time
from typing import Dict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

logger = logging.getLogger(__name__)
from dotenv import load_dotenv


load_dotenv()


class SeleniumDelister:
    """Selenium-based delisting for Poshmark and Mercari"""
    
    def __init__(self, profile_dir: str = None):
        # Use provided path or get from environment
        if profile_dir:
            self.profile_dir = os.path.abspath(profile_dir)
        else:
            # Default: profiles folder next to this file
            base_dir = os.path.dirname(os.path.abspath(__file__))
            self.profile_dir = os.path.join(base_dir, "profiles")
        self.driver = None
    
    def _init_driver(self, platform: str):
        """Initialize Chrome driver with profile"""
        try:
            chrome_options = Options()
            
            # Use user profile (pre-logged in)
            # Use user profile (must be absolute path)
            if self.profile_dir:
                profile_path = os.path.join(self.profile_dir, platform)
                profile_path = os.path.abspath(profile_path)  # Ensure absolute
                chrome_options.add_argument(f"user-data-dir={profile_path}")
            
            # Headless mode (optional)
            # chrome_options.add_argument('--headless')
            
            # Anti-detection options
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(options=chrome_options)

            # Remove webdriver property
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        

            logger.info(f"Chrome driver initialized for {platform}")
            return True
            
        except Exception as e:
            logger.error(f"Error initializing driver: {e}")
            return False
    
    def _close_driver(self):
        """Close driver"""
        if self.driver:
            self.driver.quit()
            self.driver = None


    def delist_poshmark(self, listing_id: str, retry_count: int = 0) -> Dict:
        """
        Delist item from Poshmark
        Handles pending offers by modifying listing and retrying
        
        Args:
            listing_id (str): Poshmark listing ID
            retry_count (int): Internal counter to prevent infinite recursion
        
        Returns:
            dict: Result
        """
        MAX_RETRIES = 2  # Prevent infinite loop
        
        try:
            if not self._init_driver('poshmark'):
                return {'success': False, 'error': 'Failed to initialize driver'}
            
            # Navigate to listing edit page
            url = f"https://poshmark.com/edit-listing/{listing_id}"
            self.driver.get(url)
            time.sleep(3)
            
            # STEP 1: Try to mark as "Not For Sale"
            try:
                # Click availability dropdown
                availability_dropdown = WebDriverWait(self.driver, 20).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        '[data-et-name="listingEditorAvailabilitySection"] .dropdown__selector'
                    ))
                )
                availability_dropdown.click()
                time.sleep(2)
                
                # Click "Not For Sale"
                not_for_sale_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="not_for_sale"]'))
                )
                not_for_sale_btn.click()
                time.sleep(1)
                
                # Click Update/Confirm
                confirm_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="update"]'))
                )
                confirm_btn.click()
                time.sleep(2)
                
                # Click "List this item" button
                list_it_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="list"]'))
                )
                list_it_btn.click()
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"Error in Not For Sale flow: {e}")
                self._close_driver()
                return {'success': False, 'error': f'Failed to mark Not For Sale: {str(e)}'}
            
            
            # STEP 2: Check if error popup appeared (pending offers)
            try:
                error_popup = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located((
                        By.XPATH, 
                        "//div[contains(text(),'Sorry! This listing cannot be marked as Not For Sale as you have active offers')]"
                    ))
                )
                
                # ERROR DETECTED - Pending offers blocking us!
                logger.warning(f"⚠️  Pending offers detected on listing {listing_id}")
                
                # Check if we've already retried too many times
                if retry_count >= MAX_RETRIES:
                    logger.error(f"❌ Max retries ({MAX_RETRIES}) reached for listing {listing_id}")
                    self._close_driver()
                    return {
                        'success': False,
                        'error': 'Pending offers - max retries exceeded',
                        'requires_manual': True
                    }
                
                # WORKAROUND: Modify listing to clear pending offers
                logger.info(f"🔄 Attempting workaround (retry {retry_count + 1}/{MAX_RETRIES})...")
                
                # Click OK on error popup
                ok_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[class="btn btn--primary"]'))
                )
                ok_btn.click()
                time.sleep(2)
                
                # Click Back button to return to edit page
                back_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[class="td--ul tc--lg fw--reg f--left"]'))
                )
                back_btn.click()
                time.sleep(3)
                
                # TRICK: Change size to invalidate pending offers
                success = self._modify_listing_to_clear_offers()
                
                if not success:
                    logger.error("Failed to modify listing for workaround")
                    self._close_driver()
                    return {'success': False, 'error': 'Workaround failed'}
                
                # Close driver before recursive call
                self._close_driver()
                
                # RECURSIVE CALL: Try again with retry counter incremented
                logger.info(f"🔄 Retrying delist after modification...")
                return self.delist_poshmark(listing_id, retry_count + 1)
                
            except TimeoutException:
                # No error popup = Success!
                logger.info(f"✅ Poshmark listing {listing_id} marked as Not For Sale successfully")
                self._close_driver()
                
                return {
                    'success': True,
                    'platform': 'poshmark',
                    'listing_id': listing_id,
                    'method': 'not_for_sale',
                    'retries': retry_count
                }
            
        except Exception as e:
            logger.error(f"Error delisting Poshmark item: {e}")
            self._close_driver()
            return {
                'success': False,
                'platform': 'poshmark',
                'listing_id': listing_id,
                'error': str(e)
            }


    def _modify_listing_to_clear_offers(self) -> bool:
        """
        Modify listing to clear pending offers by changing size to a different size
        No need to change back - listing will be Not For Sale anyway
        
        Returns:
            bool: Success status
        """
        try:
            logger.info("Modifying listing to clear pending offers (changing size)...")


            # STEP 1: Set availability to "For Sale" first
            try:
                logger.info("Setting availability to 'For Sale'...")
                availability_dropdown = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        '[data-et-name="listingEditorAvailabilitySection"] .dropdown__selector'
                    ))
                )
                self.driver.execute_script(
                    "arguments[0].scrollIntoView({block: 'center'});", 
                    availability_dropdown
                )
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", availability_dropdown)
                time.sleep(1)
                
                # Click "For Sale"
                for_sale_btn = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="available"]'))
                )
                self.driver.execute_script("arguments[0].click();", for_sale_btn)
                time.sleep(1)
                logger.info("✓ Set to 'For Sale'")
            except Exception as e:
                logger.warning(f"Could not set to For Sale (may already be): {e}")
            
            # Find size dropdown
            size_dropdown = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, 
                    '[selectortestlocator="size"] .dropdown__selector'
                ))
            )

            # Scroll element into view (with offset to avoid header)
            self.driver.execute_script(
                "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", 
                size_dropdown
            )
            time.sleep(1)

            # Wait until element is clickable
            size_dropdown = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, 
                    '[selectortestlocator="size"] .dropdown__selector'
                ))
            )
            size_dropdown.click()
            time.sleep(1)
            
            # Get current size
            current_size = size_dropdown.text.strip()
            logger.info(f"Current size: {current_size}")
            
            # Find all size options
            size_options = self.driver.find_elements(
                By.CSS_SELECTOR,
                '[class="d--fl fw--w"] li'
            )
            
            # Pick a different size (first available that's not current)
            for option in size_options:
                if option.text.strip() != current_size:
                    logger.info(f"Changing to new size: {option.text}")
                    option.click()
                    time.sleep(1)
                    break
            else:
                logger.error("No different size option found")
                return False
            
            # Click Update to save
            update_btn = self.driver.find_element(
                By.CSS_SELECTOR, 
                '[data-et-name="update"]'
            )
            update_btn.click()
            time.sleep(3)

            # Click "List this item" to publish changes
            list_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="list"]'))
            )
            self.driver.execute_script("arguments[0].click();", list_btn)
            time.sleep(3)
            
            logger.info("✅ Successfully modified listing (size changed)")
            return True
            
        except Exception as e:
            logger.error(f"Error modifying listing: {e}")
            return False
    
    # def delist_poshmark(self, listing_id: str) -> Dict:
    #     """
    #     Delist item from Poshmark
        
    #     Args:
    #         listing_id (str): Poshmark listing ID
        
    #     Returns:
    #         dict: Result
    #     """
    #     try:
    #         if not self._init_driver('poshmark'):
    #             return {'success': False, 'error': 'Failed to initialize driver'}
            
    #         # Navigate to listing
    #         url = f"https://poshmark.com/edit-listing/{listing_id}"
    #         self.driver.get(url)
            
    #         time.sleep(2)
            
    #         # Click "Edit" button
    #         availability_dropdown = WebDriverWait(self.driver, 20).until(
    #             EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="listingEditorAvailabilitySection"] .dropdown__selector'))
    #         )
    #         availability_dropdown.click()
            
    #         time.sleep(2)
            
    #         # Click "Not For Sale" button
    #         not_for_sale_btn = WebDriverWait(self.driver, 10).until(
    #             EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="not_for_sale"]'))
    #         )
    #         not_for_sale_btn.click()
            
    #         time.sleep(1)
            
    #         # Confirm
    #         confirm_btn = WebDriverWait(self.driver, 10).until(
    #             EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="update"]'))
    #         )
    #         confirm_btn.click()
    #         time.sleep(2)

    #         # then clicking on List this item
    #         list_it_btn = WebDriverWait(self.driver, 10).until(
    #             EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-et-name="list"]'))
    #         )
    #         list_it_btn.click()

    #         # then wait for error if happens for pending offers
    #         error_pending_offers = WebDriverWait(self.driver, 10).until(
    #             EC.element_to_be_clickable((By.XPATH, "//div[contains(text(),'Sorry! This listing cannot be marked as Not For Sale as you have active offers on it')]"))
    #         )

    #         if error_pending_offers is not None:
                
    #             ok_btn = self.driver.find_element(By.CSS_SELECTOR,'[class="btn btn--primary"]')
    #             ok_btn.click()

    #             time.sleep(2)

    #             back_btn = self.driver.find_element(By.CSS_SELECTOR,'[class="td--ul tc--lg fw--reg f--left"]')
    #             back_btn.click()



            
            

    #         time.sleep(2)
            
    #         logger.info(f"Poshmark listing {listing_id} delisted successfully")
            
    #         self._close_driver()
            
    #         return {
    #             'success': True,
    #             'platform': 'poshmark',
    #             'listing_id': listing_id
    #         }
            
    #     except Exception as e:
    #         logger.error(f"Error delisting Poshmark item: {e}")
    #         self._close_driver()
    #         return {
    #             'success': False,
    #             'platform': 'poshmark',
    #             'listing_id': listing_id,
    #             'error': str(e)
    #         }
    
    def delist_mercari(self, listing_id: str) -> Dict:
        """
        Delist item from Mercari
        
        Args:
            listing_id (str): Mercari listing ID
        
        Returns:
            dict: Result
        """
        try:
            if not self._init_driver('mercari'):
                return {'success': False, 'error': 'Failed to initialize driver'}
            
            # Navigate to listing
            url = f"https://www.mercari.com/sell/edit/{listing_id}/"
            self.driver.get(url)
            
            time.sleep(2)
        
            # Click "Delete" option
            delete_btn = WebDriverWait(self.driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="ActivateDeactivateButton"]'))
            )
            delete_btn.click()
            
            time.sleep(5)
            
            logger.info(f"Mercari listing {listing_id} delisted successfully")
            
            self._close_driver()
            
            return {
                'success': True,
                'platform': 'mercari',
                'listing_id': listing_id
            }
            
        except Exception as e:
            logger.error(f"Error delisting Mercari item: {e}")
            self._close_driver()
            return {
                'success': False,
                'platform': 'mercari',
                'listing_id': listing_id,
                'error': str(e)
            }


# Convenience functions
def delist_item(platform: str, listing_id: str) -> Dict:
    """
    Delist item from platform
    
    Args:
        platform (str): 'poshmark' or 'mercari'
        listing_id (str): Listing ID
    
    Returns:
        dict: Result
    """
    delister = SeleniumDelister()
    
    if platform == 'poshmark':
        return delister.delist_poshmark(listing_id)
    elif platform == 'mercari':
        return delister.delist_mercari(listing_id)
    else:
        return {
            'success': False,
            'error': f'Unknown platform: {platform}'
        }
    

# if __name__ == "__main__":
#     # delist_item("mercari","m16702750949")
#     delist_item("poshmark","6992c77a7321b00af4a6d28c")
