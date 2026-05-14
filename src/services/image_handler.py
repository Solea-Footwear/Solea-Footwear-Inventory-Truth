"""
Image Handler
Downloads and processes images for cross-listing
"""
import logging
import os
import requests
import tempfile
from typing import List
from PIL import Image

logger = logging.getLogger(__name__)

class ImageHandler:
    """Handles image downloading and processing for listings"""
    
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
        logger.debug(f"Created temp directory: {self.temp_dir}")
    
    def download_images(self, image_urls: List[str], max_images: int = 12) -> List[str]:
        """
        Download images from URLs
        
        Args:
            image_urls (list): List of image URLs
            max_images (int): Maximum images to download
        
        Returns:
            list: List of local file paths
        """
        if not image_urls:
            logger.warning("No image URLs provided")
            return []
        
        local_paths = []
        
        for idx, url in enumerate(image_urls[:max_images]):
            try:
                logger.debug(f"Downloading image {idx + 1}: {url}")
                
                # Download image
                response = requests.get(url, timeout=10)
                response.raise_for_status()
                
                # Save to temp file
                ext = self._get_extension(url)
                filename = f"image_{idx + 1}{ext}"
                filepath = os.path.join(self.temp_dir, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                # Validate image
                if self._validate_image(filepath):
                    local_paths.append(filepath)
                    logger.debug(f"Downloaded: {filepath}")
                else:
                    logger.warning(f"Invalid image: {filepath}")
                    os.remove(filepath)
                
            except Exception as e:
                logger.error(f"Error downloading image {url}: {e}")
        
        logger.info(f"Downloaded {len(local_paths)} images")
        return local_paths
    
    def _get_extension(self, url: str) -> str:
        """Get file extension from URL"""
        # Try to get from URL
        if '.' in url:
            ext = '.' + url.split('.')[-1].split('?')[0].lower()
            if ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
                return ext
        
        # Default to .jpg
        return '.jpg'
    
    def _validate_image(self, filepath: str) -> bool:
        """
        Validate image file
        
        Args:
            filepath (str): Path to image file
        
        Returns:
            bool: True if valid image
        """
        try:
            with Image.open(filepath) as img:
                img.verify()  # Verify it's a valid image
                return True
        except Exception as e:
            logger.error(f"Image validation failed: {e}")
            return False
    
    def resize_image(self, filepath: str, max_size: tuple = (2048, 2048)) -> str:
        """
        Resize image if too large
        
        Args:
            filepath (str): Path to image
            max_size (tuple): Maximum (width, height)
        
        Returns:
            str: Path to resized image (same as input)
        """
        try:
            with Image.open(filepath) as img:
                # Check if resize needed
                if img.width > max_size[0] or img.height > max_size[1]:
                    logger.debug(f"Resizing image from {img.size} to fit {max_size}")
                    
                    # Maintain aspect ratio
                    img.thumbnail(max_size, Image.Resampling.LANCZOS)
                    
                    # Save back to same file
                    img.save(filepath, quality=95)
                    
                    logger.debug(f"Resized to {img.size}")
            
            return filepath
            
        except Exception as e:
            logger.error(f"Error resizing image: {e}")
            return filepath
    
    def cleanup(self, filepaths: List[str] = None):
        """
        Clean up downloaded images
        
        Args:
            filepaths (list): Specific files to delete, or None for all
        """
        try:
            if filepaths:
                # Delete specific files
                for filepath in filepaths:
                    if os.path.exists(filepath):
                        os.remove(filepath)
                        logger.debug(f"Deleted: {filepath}")
            else:
                # Delete entire temp directory
                import shutil
                if os.path.exists(self.temp_dir):
                    shutil.rmtree(self.temp_dir)
                    logger.debug(f"Deleted temp directory: {self.temp_dir}")
        
        except Exception as e:
            logger.error(f"Error cleaning up images: {e}")