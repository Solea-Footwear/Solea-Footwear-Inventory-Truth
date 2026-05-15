"""
Returns Tracking Module
Handles eBay return email processing, classification, and tracking
"""
from returns.ebay_return_parser import EbayReturnParser
from returns.return_classifier import ReturnClassifier
from returns.return_service import ReturnService
from returns.email_processing_service import EmailProcessingService
