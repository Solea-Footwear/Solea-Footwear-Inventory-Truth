"""
Bulk Import Service for CSV Data Processing
Handles bulk import of products and units from CSV files
"""
import logging
import csv
from io import StringIO
from typing import Dict, List, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class BulkImportService:
    """Service for handling bulk CSV imports"""
    
    def __init__(self, db):
        self.db = db
    
    def parse_products_csv(self, csv_content: str) -> Dict:
        """
        Parse products CSV content
        
        Args:
            csv_content (str): CSV file content
        
        Returns:
            dict: Parsed products with validation results
        """
        logger.info("Parsing products CSV...")
        
        results = {
            'valid_rows': [],
            'invalid_rows': [],
            'total_rows': 0
        }
        
        try:
            # Parse CSV
            csv_file = StringIO(csv_content)
            reader = csv.DictReader(csv_file)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                results['total_rows'] += 1
                
                # Validate row
                validation = self._validate_product_row(row, row_num)
                
                if validation['valid']:
                    results['valid_rows'].append({
                        'row_num': row_num,
                        'data': row
                    })
                else:
                    results['invalid_rows'].append({
                        'row_num': row_num,
                        'data': row,
                        'errors': validation['errors']
                    })
            
            logger.info(f"CSV parsed: {len(results['valid_rows'])} valid, {len(results['invalid_rows'])} invalid")
            
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            results['error'] = str(e)
        
        return results
    
    def parse_units_csv(self, csv_content: str) -> Dict:
        """
        Parse units CSV content
        
        Args:
            csv_content (str): CSV file content
        
        Returns:
            dict: Parsed units with validation results
        """
        logger.info("Parsing units CSV...")
        
        results = {
            'valid_rows': [],
            'invalid_rows': [],
            'total_rows': 0
        }
        
        try:
            # Parse CSV
            csv_file = StringIO(csv_content)
            reader = csv.DictReader(csv_file)
            
            for row_num, row in enumerate(reader, start=2):
                print("validating row...",row_num)

                results['total_rows'] += 1
                
                # Validate row
                validation = self._validate_unit_row(row, row_num)
                
                if validation['valid']:
                    print("Row is valid.")
                    results['valid_rows'].append({
                        'row_num': row_num,
                        'data': row
                    })
                else:
                    print("Row is Invalid.")
                    print(f"Row data: {row}")  # ADD THIS LINE
                    # print(f"Missing fields: unit_code={row.get('unit_code')}, product_id={row.get('product_id')}")  # ADD THIS
                    # import time
                    # time.sleep(599)
                    results['invalid_rows'].append({
                        'row_num': row_num,
                        'data': row,
                        'errors': validation['errors']
                    })
            
            logger.info(f"CSV parsed: {len(results['valid_rows'])} valid, {len(results['invalid_rows'])} invalid")
            
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            results['error'] = str(e)
        
        return results
    
    def _validate_product_row(self, row: Dict, row_num: int) -> Dict:
        """
        Validate product CSV row
        
        Args:
            row (dict): CSV row data
            row_num (int): Row number
        
        Returns:
            dict: Validation result
        """
        errors = []
        
        # Required fields
        required_fields = ['brand', 'model', 'size']
        for field in required_fields:
            if not row.get(field, '').strip():
                errors.append(f"Missing required field: {field}")
        
        # Validate size (must be numeric or numeric with letter like 10.5 or 10W)
        size = row.get('size', '').strip()
        if size and not self._is_valid_size(size):
            errors.append(f"Invalid size format: {size}")
        
        # Validate price (if provided)
        price = row.get('default_price', '').strip()
        if price:
            try:
                price_float = float(price)
                if price_float <= 0:
                    errors.append("Price must be greater than 0")
            except ValueError:
                errors.append(f"Invalid price: {price}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _validate_unit_row(self, row: Dict, row_num: int) -> Dict:
        """
        Validate unit CSV row
        
        Args:
            row (dict): CSV row data
            row_num (int): Row number
        
        Returns:
            dict: Validation result
        """
        errors = []
        
        # Required fields
        required_fields = ['unit_code', 'product_sku']
        for field in required_fields:
            if not row.get(field, '').strip():
                errors.append(f"Missing required field: {field}")
        
        # Validate unit_code uniqueness
        unit_code = row.get('unit_code', '').strip()
        if unit_code:
            from database import Unit
            existing = self.db.query(Unit).filter(Unit.unit_code == unit_code).first()
            if existing:
                errors.append(f"Unit code already exists: {unit_code}")
        
        # Validate status
        status = row.get('status', '').strip()
        valid_statuses = ['ready_to_list', 'listed', 'sold', 'shipped', 'returned', 'damaged', 'reserved']
        if status and status not in valid_statuses:
            errors.append(f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}")
        
        # Validate cost_basis (if provided)
        cost = row.get('cost_basis', '').strip()
        if cost:
            try:
                cost_float = float(cost)
                if cost_float < 0:
                    errors.append("Cost basis cannot be negative")
            except ValueError:
                errors.append(f"Invalid cost basis: {cost}")
        
        return {
            'valid': len(errors) == 0,
            'errors': errors
        }
    
    def _is_valid_size(self, size: str) -> bool:
        """Check if size format is valid"""
        import re
        # Matches: 10, 10.5, 10W, 10.5W, etc.
        pattern = r'^\d+(\.\d+)?[A-Z]?$'
        return bool(re.match(pattern, size))
    
    def import_products(self, valid_rows: List[Dict]) -> Dict:
        """
        Import validated products into database
        
        Args:
            valid_rows (list): List of validated product rows
        
        Returns:
            dict: Import results
        """
        from database import Product, Category, ConditionGrade
        
        logger.info(f"Starting product import: {len(valid_rows)} products")
        
        results = {
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        for idx, row_data in enumerate(valid_rows):

            try:
                row = row_data['data']

                logger.info(f"[{idx}/{len(valid_rows)}] Processing: {row.get('brand')} {row.get('model')} size {row.get('size')}")
                
                # Get or create category
                category = self._get_or_create_category(row.get('category', 'Uncategorized'))
                logger.debug(f"Category: {category.display_name if category else 'None'}")

                
                # Get or create condition grade
                condition = self._get_or_create_condition(row.get('condition', 'Good Pre-Owned'))
                logger.debug(f"Condition: {condition.display_name if condition else 'None'}")

                
                # Check if product already exists
                existing = self.db.query(Product).filter(
                    Product.brand == row['brand'],
                    Product.model == row['model'],
                    Product.size == row['size'],
                    # Product.colorway == row.get('colorway', '')
                ).first()
                
                if existing:
                    logger.info(f"SKIPPED - Product {idx} already exists")
                    results['skipped'] += 1
                    logger.debug(f"Product already exists: {row['brand']} {row['model']}")
                    continue
                
                # Create product
                product = Product(
                    brand=row['brand'].strip(),
                    model=row['model'].strip(),
                    size=row['size'].strip(),
                    colorway=row.get('colorway', '').strip(),
                    gender=row.get('gender', '').strip(),
                    category_id=category.id if category else None,
                    condition_grade_id=condition.id if condition else None,
                    default_price_ebay=float(row['default_price']) if row.get('default_price') else None,
                    sku_prefix=row.get('sku_prefix', '').strip() or None,
                    notes=row.get('notes', '').strip() or None
                )
                
                self.db.add(product)
                logger.info(f"CREATED - Product {idx}: {row['brand']} {row['model']} size {row['size']}")
                results['created'] += 1

                # ✅ COMMIT EVERY 100 PRODUCTS
                if idx % 100 == 0:
                    try:
                        self.db.commit()
                        logger.info(f"✅ Batch committed: {idx} products processed")
                    except Exception as e:
                        self.db.rollback()
                        logger.error(f"❌ Batch commit failed at {idx}: {e}")
                        results['errors'].append({'batch': idx, 'error': str(e)})
                
            except Exception as e:
                logger.error(f"Error importing product row {row_data['row_num']}: {e}")
                logger.error(f"ERROR - Product {idx} failed: {e}")
                results['errors'].append({
                    'row_num': row_data['row_num'],
                    'error': str(e)
                })

        # ✅ FINAL COMMIT for remaining products
        try:
            self.db.commit()
            logger.info(f"✅ Final commit: {results['created']} created, {results['skipped']} skipped")
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Final commit failed: {e}")
        
        # # Commit all at once
        # try:
        #     self.db.commit()
        #     logger.info(f"Products imported: {results['created']} created, {results['skipped']} skipped")
        # except Exception as e:
        #     self.db.rollback()
        #     logger.error(f"Failed to commit products: {e}")
        #     results['error'] = str(e)
        
        return results
    
    def import_units(self, valid_rows: List[Dict]) -> Dict:
        """
        Import validated units into database
        
        Args:
            valid_rows (list): List of validated unit rows
        
        Returns:
            dict: Import results
        """
        from database import Unit, Product, Location, ConditionGrade
        
        logger.info(f"Starting unit import: {len(valid_rows)} units")
        
        results = {
            'created': 0,
            'skipped': 0,
            'errors': []
        }
        
        for idx, row_data in enumerate(valid_rows):
            try:
                row = row_data['data']

                # ✅ ADD THIS LOG
                logger.info(f"[{idx}/{len(valid_rows)}] Processing unit: {row.get('unit_code')}")
                
                # Find product by SKU prefix or exact match
                product_sku = row['product_sku'].strip()
                product = self.db.query(Product).filter(
                    Product.sku_prefix == product_sku
                ).first()
                
                if not product:
                    # Try to find by brand/model (fallback)
                    results['errors'].append({
                        'row_num': row_data['row_num'],
                        'error': f"Product not found with SKU: {product_sku}"
                    })
                    continue
                
                # Get location
                location = None
                if row.get('location_code'):
                    location = self.db.query(Location).filter(
                        Location.code == row['location_code'].strip()
                    ).first()
                
                # Get condition
                condition = None
                if row.get('condition'):
                    condition = self._get_or_create_condition(row['condition'])
                
                # Create unit
                unit = Unit(
                    unit_code=row['unit_code'].strip(),
                    product_id=product.id,
                    location_id=location.id if location else None,
                    condition_grade_id=condition.id if condition else product.condition_grade_id,
                    status=row.get('status', 'ready_to_list').strip(),
                    cost_basis=float(row['cost_basis']) if row.get('cost_basis') else None,
                    notes=row.get('notes', '').strip() or None
                )
                
                self.db.add(unit)
                results['created'] += 1
                logger.info(f"CREATED - Unit {idx}: {unit.unit_code}")


                # ✅ BATCH COMMIT EVERY 100 UNITS
                if idx % 100 == 0:
                    try:
                        self.db.commit()
                        logger.info(f"✅ Batch committed: {idx} units processed")
                    except Exception as e:
                        self.db.rollback()
                        logger.error(f"❌ Batch commit failed at {idx}: {e}")
                        results['errors'].append({'batch': idx, 'error': str(e)})
                
            except Exception as e:
                logger.error(f"ERROR - Unit {idx} failed: {e}")
                logger.error(f"Error importing unit row {row_data['row_num']}: {e}")
                results['errors'].append({
                    'row_num': row_data['row_num'],
                    'error': str(e)
                })

        # ✅ FINAL COMMIT for remaining units
        try:
            self.db.commit()
            logger.info(f"✅ Final commit: {results['created']} created, {results['skipped']} skipped")
        except Exception as e:
            self.db.rollback()
            logger.error(f"❌ Final commit failed: {e}")
        
        # Commit all at once
        # try:
        #     self.db.commit()
        #     logger.info(f"Units imported: {results['created']} created")
        # except Exception as e:
        #     self.db.rollback()
        #     logger.error(f"Failed to commit units: {e}")
        #     results['error'] = str(e)
        
        return results
    
    def _get_or_create_category(self, category_name: str):
        """Get or create category"""
        from database import Category
        
        category = self.db.query(Category).filter(
            Category.internal_name == category_name
        ).first()
        
        if not category:
            category = Category(internal_name=category_name,
        display_name=category_name)
            self.db.add(category)
            self.db.flush()  # Get ID without committing
        
        return category
    
    def _get_or_create_condition(self, condition_name: str):
        """Get or create condition grade"""
        from database import ConditionGrade
        
        # condition = self.db.query(ConditionGrade).filter(
        #     ConditionGrade.display_name == condition_name
        # ).first()

        condition = self.db.query(ConditionGrade).filter(
            ConditionGrade.display_name.ilike(condition_name)  # Use .ilike() not ==
        ).first()
        
        # if not condition:
        #     # Create with default eBay condition mapping
        #     condition = ConditionGrade(
        #         display_name=condition_name,
        #         ebay_condition_id=3000  # Default to "Used"
        #     )
        #     self.db.add(condition)
        #     self.db.flush()

        if not condition:
            # Generate internal_code from display_name
            internal_code = condition_name.lower().replace(' ', '_').replace('-', '_')
            
            condition = ConditionGrade(
                internal_code=internal_code,  # ✅ Required field!
                display_name=condition_name,
                ebay_condition_id=3000
            )
            self.db.add(condition)
            self.db.flush()
        
        return condition
    
    def generate_products_template(self) -> str:
        """
        Generate sample products CSV template
        
        Returns:
            str: CSV content
        """
        template = """brand,model,size,colorway,gender,category,condition,default_price,sku_prefix,notes
Nike,Air Jordan 1,10,Black/Red,Men,Athletic Shoes,Excellent Pre-Owned,150,AJ1,
Adidas,Yeezy Boost 350,9.5,Zebra,Men,Athletic Shoes,New with Box,220,YZY,
New Balance,990v5,11,Grey,Men,Athletic Shoes,Good Pre-Owned,130,NB990,
"""
        return template
    
    def generate_units_template(self) -> str:
        """
        Generate sample units CSV template
        
        Returns:
            str: CSV content
        """
        template = """unit_code,product_sku,location_code,condition,cost_basis,status,notes
SHOE-001,AJ1,A1-01-06-03,Excellent Pre-Owned,100,ready_to_list,
SHOE-002,AJ1,A1-01-06-04,Excellent Pre-Owned,95,ready_to_list,
SHOE-003,YZY,A1-02-03-05,New with Box,180,ready_to_list,
"""
        return template