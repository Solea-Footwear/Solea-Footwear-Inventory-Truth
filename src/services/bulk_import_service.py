"""
Bulk Import Service for CSV Data Processing
Handles bulk import of products and units from CSV files
"""
import csv
import logging
import re
from datetime import datetime
from io import StringIO
from typing import Dict, List

import psycopg2.extras

logger = logging.getLogger(__name__)


class BulkImportService:
    """Service for handling bulk CSV imports"""

    def __init__(self, conn):
        self.conn = conn

    # ------------------------------------------------------------------ parsing

    def parse_products_csv(self, csv_content: str) -> Dict:
        logger.info("Parsing products CSV...")
        results = {'valid_rows': [], 'invalid_rows': [], 'total_rows': 0}
        try:
            reader = csv.DictReader(StringIO(csv_content))
            for row_num, row in enumerate(reader, start=2):
                results['total_rows'] += 1
                validation = self._validate_product_row(row, row_num)
                if validation['valid']:
                    results['valid_rows'].append({'row_num': row_num, 'data': row})
                else:
                    results['invalid_rows'].append({'row_num': row_num, 'data': row, 'errors': validation['errors']})
            logger.info(f"CSV parsed: {len(results['valid_rows'])} valid, {len(results['invalid_rows'])} invalid")
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            results['error'] = str(e)
        return results

    def parse_units_csv(self, csv_content: str) -> Dict:
        logger.info("Parsing units CSV...")
        results = {'valid_rows': [], 'invalid_rows': [], 'total_rows': 0}
        try:
            reader = csv.DictReader(StringIO(csv_content))
            for row_num, row in enumerate(reader, start=2):
                print("validating row...", row_num)
                results['total_rows'] += 1
                validation = self._validate_unit_row(row, row_num)
                if validation['valid']:
                    print("Row is valid.")
                    results['valid_rows'].append({'row_num': row_num, 'data': row})
                else:
                    print("Row is Invalid.")
                    print(f"Row data: {row}")
                    results['invalid_rows'].append({'row_num': row_num, 'data': row, 'errors': validation['errors']})
            logger.info(f"CSV parsed: {len(results['valid_rows'])} valid, {len(results['invalid_rows'])} invalid")
        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            results['error'] = str(e)
        return results

    # ----------------------------------------------------------------- validation

    def _validate_product_row(self, row: Dict, row_num: int) -> Dict:
        errors = []
        for field in ['brand', 'model', 'size']:
            if not row.get(field, '').strip():
                errors.append(f"Missing required field: {field}")
        size = row.get('size', '').strip()
        if size and not self._is_valid_size(size):
            errors.append(f"Invalid size format: {size}")
        price = row.get('default_price', '').strip()
        if price:
            try:
                if float(price) <= 0:
                    errors.append("Price must be greater than 0")
            except ValueError:
                errors.append(f"Invalid price: {price}")
        return {'valid': len(errors) == 0, 'errors': errors}

    def _validate_unit_row(self, row: Dict, row_num: int) -> Dict:
        errors = []
        for field in ['unit_code', 'product_sku']:
            if not row.get(field, '').strip():
                errors.append(f"Missing required field: {field}")
        unit_code = row.get('unit_code', '').strip()
        if unit_code:
            with self.conn.cursor() as cur:
                cur.execute("SELECT 1 FROM units WHERE unit_code = %s LIMIT 1", [unit_code])
                if cur.fetchone():
                    errors.append(f"Unit code already exists: {unit_code}")
        status = row.get('status', '').strip()
        valid_statuses = ['ready_to_list', 'listed', 'sold', 'shipped', 'returned', 'damaged', 'reserved']
        if status and status not in valid_statuses:
            errors.append(f"Invalid status: {status}. Must be one of: {', '.join(valid_statuses)}")
        cost = row.get('cost_basis', '').strip()
        if cost:
            try:
                if float(cost) < 0:
                    errors.append("Cost basis cannot be negative")
            except ValueError:
                errors.append(f"Invalid cost basis: {cost}")
        return {'valid': len(errors) == 0, 'errors': errors}

    def _is_valid_size(self, size: str) -> bool:
        return bool(re.match(r'^\d+(\.\d+)?[A-Z]?$', size))

    # ------------------------------------------------------------------ import

    def import_products(self, valid_rows: List[Dict]) -> Dict:
        logger.info(f"Starting product import: {len(valid_rows)} products")
        results = {'created': 0, 'skipped': 0, 'errors': []}

        for idx, row_data in enumerate(valid_rows):
            try:
                row = row_data['data']
                logger.info(f"[{idx}/{len(valid_rows)}] Processing: {row.get('brand')} {row.get('model')} size {row.get('size')}")

                category_id = self._get_or_create_category(row.get('category', 'Uncategorized'))
                condition_id = self._get_or_create_condition(row.get('condition', 'Good Pre-Owned'))

                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1 FROM products WHERE brand = %s AND model = %s AND size = %s LIMIT 1
                        """,
                        [row['brand'], row['model'], row['size']],
                    )
                    if cur.fetchone():
                        logger.info(f"SKIPPED - Product {idx} already exists")
                        results['skipped'] += 1
                        continue

                    cur.execute(
                        """
                        INSERT INTO products
                            (id, brand, model, size, colorway, gender, category_id,
                             condition_grade_id, default_price_ebay, sku_prefix, notes)
                        VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            row['brand'].strip(), row['model'].strip(), row['size'].strip(),
                            row.get('colorway', '').strip(),
                            row.get('gender', '').strip(),
                            category_id, condition_id,
                            float(row['default_price']) if row.get('default_price') else None,
                            row.get('sku_prefix', '').strip() or None,
                            row.get('notes', '').strip() or None,
                        ],
                    )
                results['created'] += 1
                logger.info(f"CREATED - Product {idx}: {row['brand']} {row['model']} size {row['size']}")

                if idx % 100 == 0:
                    try:
                        self.conn.commit()
                        logger.info(f"Batch committed: {idx} products processed")
                    except Exception as e:
                        self.conn.rollback()
                        logger.error(f"Batch commit failed at {idx}: {e}")
                        results['errors'].append({'batch': idx, 'error': str(e)})

            except Exception as e:
                logger.error(f"ERROR - Product {idx} failed: {e}")
                results['errors'].append({'row_num': row_data['row_num'], 'error': str(e)})

        try:
            self.conn.commit()
            logger.info(f"Final commit: {results['created']} created, {results['skipped']} skipped")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Final commit failed: {e}")

        return results

    def import_units(self, valid_rows: List[Dict]) -> Dict:
        logger.info(f"Starting unit import: {len(valid_rows)} units")
        results = {'created': 0, 'skipped': 0, 'errors': []}

        for idx, row_data in enumerate(valid_rows):
            try:
                row = row_data['data']
                logger.info(f"[{idx}/{len(valid_rows)}] Processing unit: {row.get('unit_code')}")

                product_sku = row['product_sku'].strip()
                with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT id, condition_grade_id FROM products WHERE sku_prefix = %s LIMIT 1", [product_sku])
                    product = cur.fetchone()

                if not product:
                    results['errors'].append({'row_num': row_data['row_num'], 'error': f"Product not found with SKU: {product_sku}"})
                    continue

                location_id = None
                if row.get('location_code'):
                    with self.conn.cursor() as cur:
                        cur.execute("SELECT id FROM locations WHERE code = %s LIMIT 1", [row['location_code'].strip()])
                        loc = cur.fetchone()
                        if loc:
                            location_id = loc[0]

                condition_id = product['condition_grade_id']
                if row.get('condition'):
                    condition_id = self._get_or_create_condition(row['condition'])

                with self.conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO units
                            (id, unit_code, product_id, location_id, condition_grade_id,
                             status, cost_basis, notes)
                        VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [
                            row['unit_code'].strip(),
                            product['id'],
                            location_id,
                            condition_id,
                            row.get('status', 'ready_to_list').strip(),
                            float(row['cost_basis']) if row.get('cost_basis') else None,
                            row.get('notes', '').strip() or None,
                        ],
                    )
                results['created'] += 1
                logger.info(f"CREATED - Unit {idx}: {row['unit_code']}")

                if idx % 100 == 0:
                    try:
                        self.conn.commit()
                        logger.info(f"Batch committed: {idx} units processed")
                    except Exception as e:
                        self.conn.rollback()
                        logger.error(f"Batch commit failed at {idx}: {e}")
                        results['errors'].append({'batch': idx, 'error': str(e)})

            except Exception as e:
                logger.error(f"ERROR - Unit {idx} failed: {e}")
                results['errors'].append({'row_num': row_data['row_num'], 'error': str(e)})

        try:
            self.conn.commit()
            logger.info(f"Final commit: {results['created']} created, {results['skipped']} skipped")
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Final commit failed: {e}")

        return results

    # ------------------------------------------------------------------ helpers

    def _get_or_create_category(self, category_name: str):
        """Return category id, creating if needed."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM categories WHERE internal_name = %s LIMIT 1", [category_name])
            row = cur.fetchone()
            if row:
                return row[0]
            cur.execute(
                "INSERT INTO categories (id, internal_name, display_name) VALUES (gen_random_uuid(), %s, %s) RETURNING id",
                [category_name, category_name],
            )
            return cur.fetchone()[0]

    def _get_or_create_condition(self, condition_name: str):
        """Return condition_grade id, creating if needed."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT id FROM condition_grades WHERE LOWER(display_name) = LOWER(%s) LIMIT 1", [condition_name])
            row = cur.fetchone()
            if row:
                return row[0]
            internal_code = condition_name.lower().replace(' ', '_').replace('-', '_')
            cur.execute(
                """
                INSERT INTO condition_grades (id, internal_code, display_name, ebay_condition_id)
                VALUES (gen_random_uuid(), %s, %s, 3000)
                RETURNING id
                """,
                [internal_code, condition_name],
            )
            return cur.fetchone()[0]

    def generate_products_template(self) -> str:
        return (
            "brand,model,size,colorway,gender,category,condition,default_price,sku_prefix,notes\n"
            "Nike,Air Jordan 1,10,Black/Red,Men,Athletic Shoes,Excellent Pre-Owned,150,AJ1,\n"
            "Adidas,Yeezy Boost 350,9.5,Zebra,Men,Athletic Shoes,New with Box,220,YZY,\n"
            "New Balance,990v5,11,Grey,Men,Athletic Shoes,Good Pre-Owned,130,NB990,\n"
        )

    def generate_units_template(self) -> str:
        return (
            "unit_code,product_sku,location_code,condition,cost_basis,status,notes\n"
            "SHOE-001,AJ1,A1-01-06-03,Excellent Pre-Owned,100,ready_to_list,\n"
            "SHOE-002,AJ1,A1-01-06-04,Excellent Pre-Owned,95,ready_to_list,\n"
            "SHOE-003,YZY,A1-02-03-05,New with Box,180,ready_to_list,\n"
        )
