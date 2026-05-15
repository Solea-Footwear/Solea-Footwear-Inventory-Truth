"""
Audit Service for Inventory Management
Performs comprehensive audits and generates reports
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, List
from sqlalchemy import and_, or_, func

logger = logging.getLogger(__name__)

class AuditService:
    """Service for auditing inventory and identifying issues"""
    
    def __init__(self, db):
        self.db = db
    
    def run_full_audit(self) -> Dict:
        """
        Run comprehensive audit of entire system
        
        Returns:
            dict: Audit results with all issues
        """
        logger.info("Starting full inventory audit...")
        
        results = {
            'timestamp': datetime.utcnow().isoformat(),
            'sku_issues': self.audit_sku_issues(),
            'inventory_mismatches': self.audit_inventory_mismatches(),
            'template_issues': self.audit_template_issues(),
            'pricing_issues': self.audit_pricing_issues(),
            'photo_issues': self.audit_photo_issues(),
            'summary': {}
        }
        
        # Calculate summary
        total_issues = sum([
            results['sku_issues']['total'],
            results['inventory_mismatches']['total'],
            results['template_issues']['total'],
            results['pricing_issues']['total'],
            results['photo_issues']['total']
        ])
        
        results['summary'] = {
            'total_issues': total_issues,
            'critical': self._count_by_severity(results, 'critical'),
            'warning': self._count_by_severity(results, 'warning'),
            'info': self._count_by_severity(results, 'info')
        }
        
        logger.info(f"Audit complete: {total_issues} total issues found")
        
        return results
    

    def audit_sku_issues(self) -> Dict:
        """
        Audit SKU-related issues
        
        Returns:
            dict: SKU issues
        """
        from database import Listing, Unit, ListingUnit
        
        issues = {
            'missing_skus': [],
            'unmatched_skus': [],
            'duplicate_skus': [],
            'total': 0
        }
        
        # Find listings without SKU
        listings_no_sku = self.db.query(Listing).filter(
            or_(
                Listing.channel_listing_id.is_(None),
                Listing.channel_listing_id == ''
            ),
            Listing.status == 'active'
        ).all()
        
        for listing in listings_no_sku:
            issues['missing_skus'].append({
                'listing_id': str(listing.id),
                'title': listing.title,
                'severity': 'critical',
                'message': 'Listing has no SKU/Channel Listing ID'
            })
        
        # Find units with SKU but no listing
        units_no_listing = self.db.query(Unit).filter(
            Unit.status == 'listed'
        ).all()
        
        for unit in units_no_listing:
            # Check if unit has any active listings
            has_listing = self.db.query(ListingUnit).join(Listing).filter(
                ListingUnit.unit_id == unit.id,
                Listing.status == 'active'
            ).first()
            
            if not has_listing:
                issues['unmatched_skus'].append({
                    'unit_code': unit.unit_code,
                    'unit_id': str(unit.id),
                    'status': unit.status,
                    'severity': 'warning',
                    'message': f'Unit marked as "listed" but has no active listing'
                })
        
        # Find duplicate SKUs
        duplicate_query = self.db.query(
            Unit.unit_code,
            func.count(Unit.id).label('count')
        ).group_by(Unit.unit_code).having(func.count(Unit.id) > 1).all()
        
        for sku, count in duplicate_query:
            issues['duplicate_skus'].append({
                'sku': sku,
                'count': count,
                'severity': 'critical',
                'message': f'SKU used {count} times'
            })
        
        issues['total'] = (
            len(issues['missing_skus']) + 
            len(issues['unmatched_skus']) + 
            len(issues['duplicate_skus'])
        )
        
        return issues
    

    def audit_inventory_mismatches(self) -> Dict:
        """
        Audit inventory mismatches
        
        Returns:
            dict: Inventory issues
        """
        from database import Unit, Listing, ListingUnit, Product
        
        issues = {
            'units_without_listings': [],
            'listings_without_units': [],
            'status_mismatches': [],
            'location_missing': [],
            'total': 0
        }
        
        # Units ready to list but not listed anywhere
        ready_units = self.db.query(Unit).filter(
            Unit.status == 'ready_to_list'
        ).all()
        
        for unit in ready_units:
            # Check if unit has been ready for > 7 days
            if unit.created_at:
                days_waiting = (datetime.utcnow() - unit.created_at).days
                if days_waiting > 7:
                    issues['units_without_listings'].append({
                        'unit_code': unit.unit_code,
                        'unit_id': str(unit.id),
                        'days_waiting': days_waiting,
                        'severity': 'warning',
                        'message': f'Ready to list for {days_waiting} days'
                    })
        
        # Active listings without linked units
        listings = self.db.query(Listing).filter(
            Listing.status == 'active'
        ).all()
        
        for listing in listings:
            linked_units = self.db.query(ListingUnit).filter(
                ListingUnit.listing_id == listing.id
            ).count()
            
            if linked_units == 0:
                issues['listings_without_units'].append({
                    'listing_id': str(listing.id),
                    'title': listing.title,
                    'channel_listing_id': listing.channel_listing_id,
                    'severity': 'critical',
                    'message': 'Active listing has no linked units'
                })
        
        # Units without location
        units_no_location = self.db.query(Unit).filter(
            Unit.location_id.is_(None),
            Unit.status.in_(['ready_to_list', 'listed', 'reserved'])
        ).all()
        
        for unit in units_no_location:
            issues['location_missing'].append({
                'unit_code': unit.unit_code,
                'unit_id': str(unit.id),
                'severity': 'info',
                'message': 'Unit has no warehouse location assigned'
            })
        
        issues['total'] = (
            len(issues['units_without_listings']) + 
            len(issues['listings_without_units']) + 
            len(issues['location_missing'])
        )
        
        return issues
    
    def audit_template_issues(self) -> Dict:
        """
        Audit template validation issues
        
        Returns:
            dict: Template issues
        """
        from database import ListingTemplate
        
        issues = {
            'invalid_templates': [],
            'missing_photos': [],
            'missing_descriptions': [],
            'total': 0
        }
        
        # Find invalid templates
        invalid_templates = self.db.query(ListingTemplate).filter(
            ListingTemplate.is_validated == False
        ).all()
        
        for template in invalid_templates:
            issues['invalid_templates'].append({
                'template_id': str(template.id),
                'product_id': str(template.product_id),
                'title': template.title,
                'errors': template.validation_errors,
                'severity': 'warning',
                'message': 'Template failed validation'
            })
        
        # Find templates with no photos
        no_photos = self.db.query(ListingTemplate).filter(
            or_(
                ListingTemplate.photos.is_(None),
                ListingTemplate.photos == []
            )
        ).all()
        
        for template in no_photos:
            issues['missing_photos'].append({
                'template_id': str(template.id),
                'product_id': str(template.product_id),
                'title': template.title,
                'severity': 'warning',
                'message': 'Template has no photos'
            })
        
        issues['total'] = (
            len(issues['invalid_templates']) + 
            len(issues['missing_photos'])
        )
        
        return issues
    
    def audit_pricing_issues(self) -> Dict:
        """
        Audit pricing issues
        
        Returns:
            dict: Pricing issues
        """
        from database import Unit, Product, Listing, ListingUnit
        
        issues = {
            'zero_prices': [],
            'cost_exceeds_price': [],
            'total': 0
        }
        
        # Find listings with zero or invalid price
        zero_price_listings = self.db.query(Listing).filter(
            or_(
                Listing.current_price.is_(None),
                Listing.current_price <= 0
            ),
            Listing.status == 'active'
        ).all()
        
        for listing in zero_price_listings:
            issues['zero_prices'].append({
                'listing_id': str(listing.id),
                'title': listing.title,
                'price': float(listing.current_price) if listing.current_price else 0,
                'severity': 'critical',
                'message': 'Listing has invalid price'
            })
        
        # Find units where cost exceeds sold price (negative profit)
        sold_units = self.db.query(Unit).filter(
            Unit.status == 'sold',
            Unit.sold_price.isnot(None),
            Unit.cost_basis.isnot(None),
            Unit.sold_price < Unit.cost_basis
        ).all()
        
        for unit in sold_units:
            loss = unit.cost_basis - unit.sold_price
            issues['cost_exceeds_price'].append({
                'unit_code': unit.unit_code,
                'unit_id': str(unit.id),
                'cost_basis': float(unit.cost_basis),
                'sold_price': float(unit.sold_price),
                'loss': float(loss),
                'severity': 'info',
                'message': f'Sold at loss: ${loss:.2f}'
            })
        
        issues['total'] = (
            len(issues['zero_prices']) + 
            len(issues['cost_exceeds_price'])
        )
        
        return issues
    
    def audit_photo_issues(self) -> Dict:
        """
        Audit photo-related issues
        
        Returns:
            dict: Photo issues
        """
        from database import ListingTemplate
        
        issues = {
            'insufficient_photos': [],
            'total': 0
        }
        
        # Find templates with < 3 photos (recommended minimum)
        templates = self.db.query(ListingTemplate).all()
        
        for template in templates:
            photo_count = len(template.photos) if template.photos else 0
            
            if photo_count < 3 and photo_count > 0:
                issues['insufficient_photos'].append({
                    'template_id': str(template.id),
                    'product_id': str(template.product_id),
                    'title': template.title,
                    'photo_count': photo_count,
                    'severity': 'info',
                    'message': f'Only {photo_count} photo(s), recommended 3+'
                })
        
        issues['total'] = len(issues['insufficient_photos'])
        
        return issues
    
    def get_audit_summary(self) -> Dict:
        """
        Get quick audit summary (counts only)
        
        Returns:
            dict: Issue counts
        """
        from database import Unit, Listing, ListingTemplate, Alert
        
        summary = {
            'units': {
                'total': self.db.query(Unit).count(),
                'ready_to_list': self.db.query(Unit).filter(Unit.status == 'ready_to_list').count(),
                'listed': self.db.query(Unit).filter(Unit.status == 'listed').count(),
                'sold': self.db.query(Unit).filter(Unit.status == 'sold').count(),
                'no_location': self.db.query(Unit).filter(Unit.location_id.is_(None)).count()
            },
            'listings': {
                'total': self.db.query(Listing).count(),
                'active': self.db.query(Listing).filter(Listing.status == 'active').count(),
                'sold': self.db.query(Listing).filter(Listing.status == 'sold').count()
            },
            'templates': {
                'total': self.db.query(ListingTemplate).count(),
                'validated': self.db.query(ListingTemplate).filter(
                    ListingTemplate.is_validated == True
                ).count(),
                'invalid': self.db.query(ListingTemplate).filter(
                    ListingTemplate.is_validated == False
                ).count()
            },
            'alerts': {
                'total': self.db.query(Alert).count(),
                'unresolved': self.db.query(Alert).filter(Alert.is_resolved == False).count()
            }
        }
        
        return summary
    
    def _count_by_severity(self, results: Dict, severity: str) -> int:
        """Count issues by severity across all categories"""
        count = 0
        
        for category in results.values():
            if isinstance(category, dict):
                for issue_list in category.values():
                    if isinstance(issue_list, list):
                        count += sum(1 for issue in issue_list if issue.get('severity') == severity)
        
        return count
    
    
    def export_audit_report(self, audit_results: Dict) -> str:
        """
        Export audit results to CSV format
        
        Args:
            audit_results (dict): Audit results
        
        Returns:
            str: CSV content
        """
        import csv
        from io import StringIO
        
        output = StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow(['Category', 'Issue Type', 'Severity', 'Details', 'Message'])
        
        # Write all issues
        for category, data in audit_results.items():
            if category in ['timestamp', 'summary']:
                continue
            
            for issue_type, issues in data.items():
                if issue_type == 'total':
                    continue
                
                if isinstance(issues, list):
                    for issue in issues:
                        writer.writerow([
                            category,
                            issue_type,
                            issue.get('severity', 'info'),
                            str(issue.get('unit_code') or issue.get('listing_id') or issue.get('sku', '')),
                            issue.get('message', '')
                        ])
        
        return output.getvalue()