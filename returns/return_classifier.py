"""
Return Classifier
Classifies returns into internal buckets and assigns recommended fixes
"""
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger(__name__)


class ReturnClassifier:
    """Classifier for return reasons and recommended fixes"""
    
    def __init__(self):
        # Classification rules: keyword patterns for each bucket
        self.classification_rules = {
            'Size Issue/Fit': {
                'keywords': [
                    'too small', 'too big', 'too tight', 'too loose',
                    'doesn\'t fit', 'wrong size', 'size', 'fit',
                    'too narrow', 'too wide', 'runs small', 'runs large'
                ],
                'ebay_reasons': [
                    'doesn\'t fit',
                    'ordered wrong size'
                ]
            },
            'Sizing Mismatch': {
                'keywords': [
                    'size mismatch', 'wrong size listed', 'size incorrect',
                    'not the size', 'size was wrong', 'labeled wrong size'
                ],
                'ebay_reasons': []
            },
            'Condition Mismatch': {
                'keywords': [
                    'not as described', 'condition', 'damaged', 'worn',
                    'defect', 'stain', 'scratch', 'tear', 'hole',
                    'used', 'dirty', 'smell', 'odor', 'discolored'
                ],
                'ebay_reasons': [
                    'doesn\'t match description',
                    'item damaged',
                    'item defective'
                ]
            },
            'Wrong Item': {
                'keywords': [
                    'wrong item', 'different item', 'not what i ordered',
                    'wrong product', 'incorrect item', 'wrong shoe',
                    'different color', 'wrong model'
                ],
                'ebay_reasons': [
                    'received wrong item'
                ]
            },
            'Shipping Damage': {
                'keywords': [
                    'shipping damage', 'damaged in transit', 'box damaged',
                    'arrived damaged', 'crushed', 'broken in shipping'
                ],
                'ebay_reasons': [
                    'arrived damaged'
                ]
            },
            'Low Intent Buyer': {
                'keywords': [
                    'changed my mind', 'don\'t want', 'no longer need',
                    'found cheaper', 'bought elsewhere', 'don\'t like',
                    'just because', 'impulse buy'
                ],
                'ebay_reasons': [
                    'changed mind',
                    'found better price',
                    'no longer needed'
                ]
            }
        }
        
        # Recommended fixes for each bucket
        self.recommended_fixes = {
            'Size Issue/Fit': 'Add fit note, width guidance, or brand-specific sizing note to listing.',
            'Sizing Mismatch': 'Audit size mapping and make size conversion clearer in listings.',
            'Condition Mismatch': 'Improve defect photo coverage and listing condition review.',
            'Wrong Item': 'Audit pick/pack verification and SKU scan checks.',
            'Shipping Damage': 'Improve packaging standard.',
            'Low Intent Buyer': 'Review offer policy and buyer-behavior patterns.',
            'Needs Review': 'Manual review required before assigning fix.'
        }
    
    def classify_return(
        self,
        return_reason_ebay: Optional[str],
        buyer_comment: Optional[str]
    ) -> Tuple[str, str, float]:
        """
        Classify return into internal bucket
        
        Args:
            return_reason_ebay: eBay's return reason
            buyer_comment: Buyer's comment
        
        Returns:
            tuple: (internal_bucket, classifier_source, confidence)
        """
        try:
            # Combine text for analysis
            text = ''
            if return_reason_ebay:
                text += return_reason_ebay.lower() + ' '
            if buyer_comment:
                text += buyer_comment.lower()
            
            if not text.strip():
                return 'Needs Review', 'no_data', 0.0
            
            # Try to match against classification rules
            best_match = None
            best_score = 0
            best_source = 'keyword_match'
            
            for bucket, rules in self.classification_rules.items():
                score = 0
                
                # Check eBay reason match (high confidence)
                if return_reason_ebay:
                    for ebay_reason in rules['ebay_reasons']:
                        if ebay_reason.lower() in return_reason_ebay.lower():
                            score += 10  # High weight for eBay reason match
                            best_source = 'ebay_reason'
                
                # Check keyword matches
                keyword_matches = 0
                for keyword in rules['keywords']:
                    if keyword in text:
                        keyword_matches += 1
                
                score += keyword_matches
                
                # Update best match
                if score > best_score:
                    best_score = score
                    best_match = bucket
            
            # Determine confidence
            if best_score >= 10:
                confidence = 0.95  # High confidence (eBay reason match)
            elif best_score >= 3:
                confidence = 0.85  # Good confidence (multiple keywords)
            elif best_score >= 1:
                confidence = 0.65  # Medium confidence (single keyword)
            else:
                confidence = 0.0   # No match
            
            # Return classification
            if best_match and confidence >= 0.65:
                return best_match, best_source, confidence
            else:
                return 'Needs Review', 'no_match', 0.0
            
        except Exception as e:
            logger.error(f"Error classifying return: {e}")
            return 'Needs Review', 'error', 0.0
    
    def get_recommended_fix(self, internal_bucket: str) -> str:
        """
        Get recommended fix for a bucket
        
        Args:
            internal_bucket: Internal classification bucket
        
        Returns:
            str: Recommended fix
        """
        return self.recommended_fixes.get(
            internal_bucket,
            self.recommended_fixes['Needs Review']
        )
    
    def classify_and_recommend(
        self,
        return_reason_ebay: Optional[str],
        buyer_comment: Optional[str]
    ) -> Dict:
        """
        Classify return and get recommendation in one call
        
        Args:
            return_reason_ebay: eBay's return reason
            buyer_comment: Buyer's comment
        
        Returns:
            dict: Classification result with recommendation
        """
        bucket, source, confidence = self.classify_return(
            return_reason_ebay,
            buyer_comment
        )
        
        recommended_fix = self.get_recommended_fix(bucket)
        
        return {
            'internal_bucket': bucket,
            'classifier_source': source,
            'classifier_confidence': confidence,
            'recommended_fix': recommended_fix
        }
