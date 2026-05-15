"""
Flask API Server
Main API endpoints for inventory management system
"""
import json
import os
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, send_file, send_from_directory, Response
from flask_cors import CORS
from dotenv import load_dotenv

import psycopg2.extras

from src.jobs.scheduler import sync_scheduler
from src.services.template_service import TemplateService
from src.services.audit_service import AuditService
from src.services.bulk_import_service import BulkImportService
from src.services.intake_service import register_unit
from src.services.listing_service import (
    create_listing,
    assign_unit_to_listing,
    end_listing,
)
from src.services.order_allocation_service import allocate_order
from src.services.admin_service import get_product_detail, get_sku_list
from src.services.migration_service import backfill_product_ids, get_exception_report

from src.services.delisting.gmail_service import GmailService
from src.services.delisting.email_parser_service import EmailParserService
from src.services.delisting.delist_service import DelistService

load_dotenv()

from src.backend.db.database import acquire_conn, release_conn, init_db
from src.integrations.ebay.ebay_api import ebay_api
from src.services.sync_service import SyncService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

STATIC_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend', 'static')

app = Flask(__name__)
CORS(app)
from src.backend.config import SECRET_KEY
app.config['SECRET_KEY'] = SECRET_KEY

from src.backend.routes.oauth import oauth_bp
app.register_blueprint(oauth_bp)


# ============================================
# ROOT / STATIC PAGES
# ============================================

@app.route('/')
def index():
    return send_from_directory(STATIC_DIR, 'sku_list.html')


# ============================================
# HEALTH & STATUS ENDPOINTS
# ============================================

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'ebay_configured': ebay_api.is_configured()
    })


@app.route('/api/dashboard', methods=['GET'])
def get_dashboard():
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            def count(sql, params=None):
                cur.execute(sql, params or [])
                return cur.fetchone()[0]

            total_units = count("SELECT COUNT(*) FROM units")
            ready_to_list = count("SELECT COUNT(*) FROM units WHERE status = 'ready_to_list'")
            listed = count("SELECT COUNT(*) FROM units WHERE status = 'listed'")
            sold = count("SELECT COUNT(*) FROM units WHERE status = 'sold'")
            shipped = count("SELECT COUNT(*) FROM units WHERE status = 'shipped'")
            total_products = count("SELECT COUNT(*) FROM products")
            active_listings = count("SELECT COUNT(*) FROM listings WHERE status = 'active'")
            unresolved_alerts = count("SELECT COUNT(*) FROM alerts WHERE is_resolved = FALSE")

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, sync_type, status, records_processed, started_at, completed_at "
                "FROM sync_logs ORDER BY started_at DESC LIMIT 5"
            )
            recent_syncs = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'summary': {
                'total_products': total_products,
                'total_units': total_units,
                'ready_to_list': ready_to_list,
                'listed': listed,
                'sold': sold,
                'shipped': shipped,
                'active_listings': active_listings,
                'unresolved_alerts': unresolved_alerts,
            },
            'recent_syncs': [{
                'id': str(s['id']),
                'sync_type': s['sync_type'],
                'status': s['status'],
                'records_processed': s['records_processed'],
                'started_at': s['started_at'].isoformat() if s['started_at'] else None,
                'completed_at': s['completed_at'].isoformat() if s['completed_at'] else None,
            } for s in recent_syncs]
        })
    finally:
        release_conn(conn)


# ============================================
# PRODUCT ENDPOINTS
# ============================================

@app.route('/api/products', methods=['GET'])
def get_products():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]

        brand = request.args.get('brand')
        size = request.args.get('size')
        category_id = request.args.get('category_id')

        if brand:
            where.append("p.brand ILIKE %s")
            params.append(f'%{brand}%')
        if size:
            where.append("p.size = %s")
            params.append(size)
        if category_id:
            where.append("p.category_id = %s")
            params.append(category_id)

        sql = f"""
            SELECT p.id, p.brand, p.model, p.colorway, p.size, p.gender,
                   p.default_price_ebay, p.created_at,
                   c.display_name AS category_name,
                   cg.display_name AS condition_grade_name
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            LEFT JOIN condition_grades cg ON cg.id = p.condition_grade_id
            WHERE {' AND '.join(where)}
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            products = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'products': [{
                'id': str(p['id']),
                'brand': p['brand'],
                'model': p['model'],
                'colorway': p['colorway'],
                'size': p['size'],
                'gender': p['gender'],
                'category': p['category_name'],
                'condition_grade': p['condition_grade_name'],
                'default_price_ebay': float(p['default_price_ebay']) if p['default_price_ebay'] else None,
                'created_at': p['created_at'].isoformat() if p['created_at'] else None,
            } for p in products]
        })
    finally:
        release_conn(conn)


@app.route('/api/products', methods=['POST'])
def create_product():
    conn = acquire_conn()
    try:
        data = request.json
        for field in ['brand', 'model', 'size']:
            if not data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO products
                    (id, brand, model, colorway, size, gender,
                     category_id, condition_grade_id, default_price_ebay, sku_prefix, notes)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, brand, model, size
                """,
                [data['brand'], data['model'], data.get('colorway'), data['size'],
                 data.get('gender'), data.get('category_id'), data.get('condition_grade_id'),
                 data.get('default_price_ebay'), data.get('sku_prefix'), data.get('notes')]
            )
            product = dict(cur.fetchone())

        conn.commit()
        logger.info(f"Created product: {data['brand']} {data['model']}")
        return jsonify({
            'message': 'Product created successfully',
            'product': {'id': str(product['id']), 'brand': product['brand'],
                        'model': product['model'], 'size': product['size']}
        }), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating product: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/products/<product_id>', methods=['GET'])
def get_product(product_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, brand, model, colorway, size, gender, default_price_ebay, notes "
                "FROM products WHERE id = %s",
                [product_id]
            )
            product = cur.fetchone()
            if not product:
                return jsonify({'error': 'Product not found'}), 404
            product = dict(product)

            cur.execute(
                """
                SELECT u.id, u.unit_code, u.status, u.cost_basis, u.created_at,
                       l.code AS location_code,
                       cg.display_name AS condition
                FROM units u
                LEFT JOIN locations l ON l.id = u.location_id
                LEFT JOIN condition_grades cg ON cg.id = u.condition_grade_id
                WHERE u.product_id = %s
                """,
                [product_id]
            )
            units = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'product': {
                'id': str(product['id']),
                'brand': product['brand'],
                'model': product['model'],
                'colorway': product['colorway'],
                'size': product['size'],
                'gender': product['gender'],
                'default_price_ebay': float(product['default_price_ebay']) if product['default_price_ebay'] else None,
                'notes': product['notes'],
            },
            'units': [{
                'id': str(u['id']),
                'unit_code': u['unit_code'],
                'status': u['status'],
                'location_code': u['location_code'],
                'condition': u['condition'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'created_at': u['created_at'].isoformat() if u['created_at'] else None,
            } for u in units]
        })
    finally:
        release_conn(conn)


# ============================================
# UNIT ENDPOINTS
# ============================================

@app.route('/api/units', methods=['GET'])
def get_units():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]

        status = request.args.get('status')
        product_id = request.args.get('product_id')
        location_id = request.args.get('location_id')
        unit_code = request.args.get('unit_code')

        if status:
            where.append("u.status = %s")
            params.append(status)
        if product_id:
            where.append("u.product_id = %s")
            params.append(product_id)
        if location_id:
            where.append("u.location_id = %s")
            params.append(location_id)
        if unit_code:
            where.append("u.unit_code ILIKE %s")
            params.append(f'%{unit_code}%')

        sql = f"""
            SELECT u.id, u.unit_code, u.status, u.cost_basis, u.created_at,
                   p.brand, p.model, p.size,
                   l.code AS location_code,
                   cg.display_name AS condition
            FROM units u
            LEFT JOIN products p ON p.id = u.product_id
            LEFT JOIN locations l ON l.id = u.location_id
            LEFT JOIN condition_grades cg ON cg.id = u.condition_grade_id
            WHERE {' AND '.join(where)}
        """
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            units = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'units': [{
                'id': str(u['id']),
                'unit_code': u['unit_code'],
                'status': u['status'],
                'product': {
                    'brand': u['brand'],
                    'model': u['model'],
                    'size': u['size'],
                } if u['brand'] else None,
                'location_code': u['location_code'],
                'condition': u['condition'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'created_at': u['created_at'].isoformat() if u['created_at'] else None,
            } for u in units]
        })
    finally:
        release_conn(conn)


@app.route('/api/units', methods=['POST'])
def create_unit():
    conn = acquire_conn()
    try:
        data = request.json
        if not data.get('unit_code'):
            return jsonify({'error': 'Missing required field: unit_code'}), 400
        if not data.get('product_id'):
            return jsonify({'error': 'Missing required field: product_id'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM units WHERE unit_code = %s LIMIT 1", [data['unit_code']])
            if cur.fetchone():
                return jsonify({'error': 'Unit code already exists'}), 400

            cur.execute(
                """
                INSERT INTO units
                    (id, unit_code, product_id, location_id, condition_grade_id,
                     status, cost_basis, notes)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, unit_code, status
                """,
                [data['unit_code'], data['product_id'], data.get('location_id'),
                 data.get('condition_grade_id'), data.get('status', 'ready_to_list'),
                 data.get('cost_basis'), data.get('notes')]
            )
            unit = dict(cur.fetchone())

        conn.commit()
        logger.info(f"Created unit: {unit['unit_code']}")
        return jsonify({
            'message': 'Unit created successfully',
            'unit': {'id': str(unit['id']), 'unit_code': unit['unit_code'], 'status': unit['status']}
        }), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating unit: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


# ============================================
# EPIC 2 — Intake endpoints
# ============================================

@app.route('/api/intake/unit', methods=['POST'])
def intake_unit():
    conn = acquire_conn()
    try:
        data = request.get_json(silent=True) or {}

        required = ['brand', 'model', 'style_code', 'gender', 'size', 'condition', 'unit_code']
        missing = [f for f in required if not str(data.get(f, '')).strip()]
        if missing:
            return jsonify({'error': 'Missing required fields', 'fields': missing}), 422

        cost_basis = None
        if data.get('cost_basis') is not None:
            try:
                cost_basis = float(data['cost_basis'])
            except (TypeError, ValueError):
                return jsonify({'error': f"cost_basis must be a number, got {data['cost_basis']!r}"}), 422

        try:
            unit, product, unit_created, product_created = register_unit(
                conn,
                brand=data['brand'],
                model=data['model'],
                style_code=data['style_code'],
                gender=data['gender'],
                size=data['size'],
                condition=data['condition'],
                unit_code=data['unit_code'],
                location_code=data.get('location_code'),
                cost_basis=cost_basis,
                notes=data.get('notes'),
            )
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 422

        conn.commit()

        status_code = 201 if unit_created else 200
        return jsonify({
            'unit_created': unit_created,
            'product_created': product_created,
            'unit': {
                'id': str(unit['id']),
                'unit_code': unit['unit_code'],
                'status': unit['status'],
                'cost_basis': float(unit['cost_basis']) if unit.get('cost_basis') is not None else None,
                'notes': unit.get('notes'),
                'created_at': unit['created_at'].isoformat() if unit.get('created_at') else None,
            },
            'product': {
                'id': str(product['id']),
                'product_id': product.get('product_id'),
                'brand': product['brand'],
                'model': product['model'],
                'style_code': product.get('style_code'),
                'gender': product.get('gender'),
                'size': product.get('size'),
                'condition_code': product.get('condition_code'),
                'is_interchangeable': product.get('is_interchangeable'),
            },
        }), status_code

    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in intake_unit: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/intake/units', methods=['POST'])
def intake_units():
    conn = acquire_conn()
    try:
        data = request.get_json(silent=True) or {}
        items = data.get('units')

        if not isinstance(items, list) or len(items) == 0:
            return jsonify({'error': "'units' must be a non-empty list"}), 422
        if len(items) > 500:
            return jsonify({'error': f"Batch too large: {len(items)} items. Maximum is 500."}), 422

        REQUIRED = ['brand', 'model', 'style_code', 'gender', 'size', 'condition', 'unit_code']

        validation_errors = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                validation_errors.append({'index': idx, 'error': 'item must be an object'})
                continue
            missing = [f for f in REQUIRED if not str(item.get(f, '')).strip()]
            if missing:
                validation_errors.append({
                    'index': idx,
                    'unit_code': item.get('unit_code', ''),
                    'error': f"Missing required fields: {missing}",
                })

        if validation_errors:
            return jsonify({
                'error': 'Validation failed for one or more items',
                'validation_errors': validation_errors,
            }), 422

        result_items = []
        units_created = units_skipped = products_created = products_reused = 0

        for idx, item in enumerate(items):
            cost_basis = None
            if item.get('cost_basis') is not None:
                try:
                    cost_basis = float(item['cost_basis'])
                except (TypeError, ValueError):
                    conn.rollback()
                    return jsonify({
                        'error': f"Item at index {idx}: cost_basis must be a number",
                        'index': idx,
                    }), 422

            try:
                unit, product, unit_created, product_created = register_unit(
                    conn,
                    brand=item['brand'],
                    model=item['model'],
                    style_code=item['style_code'],
                    gender=item['gender'],
                    size=item['size'],
                    condition=item['condition'],
                    unit_code=item['unit_code'],
                    location_code=item.get('location_code'),
                    cost_basis=cost_basis,
                    notes=item.get('notes'),
                )
            except ValueError as exc:
                conn.rollback()
                return jsonify({
                    'error': f"Item at index {idx} (unit_code={item.get('unit_code')!r}): {exc}",
                    'index': idx,
                }), 422

            if unit_created:
                units_created += 1
            else:
                units_skipped += 1

            if product_created:
                products_created += 1
            else:
                products_reused += 1

            result_items.append({
                'index': idx,
                'unit_code': unit['unit_code'],
                'unit_id': str(unit['id']),
                'product_id': str(product['id']),
                'unit_created': unit_created,
                'product_created': product_created,
            })

        conn.commit()
        return jsonify({
            'total': len(items),
            'units_created': units_created,
            'units_skipped': units_skipped,
            'products_created': products_created,
            'products_reused': products_reused,
            'items': result_items,
        }), 201

    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in intake_units batch: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/units/<unit_id>', methods=['PUT'])
def update_unit(unit_id):
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM units WHERE id = %s LIMIT 1", [unit_id])
            if not cur.fetchone():
                return jsonify({'error': 'Unit not found'}), 404

        data = request.json
        updates = {}
        for field in ['location_id', 'status', 'condition_grade_id', 'cost_basis', 'notes']:
            if field in data:
                updates[field] = data[field]

        if updates:
            set_clause = ", ".join(f"{k} = %s" for k in updates)
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE units SET {set_clause} WHERE id = %s",
                    list(updates.values()) + [unit_id]
                )
            conn.commit()

        return jsonify({'message': 'Unit updated successfully'})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating unit: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/units/search/<unit_code>', methods=['GET'])
def search_unit(unit_code):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.unit_code, u.status, u.cost_basis, u.created_at,
                       p.brand, p.model, p.colorway, p.size,
                       l.code AS loc_code, l.description AS loc_desc,
                       cg.display_name AS condition
                FROM units u
                LEFT JOIN products p ON p.id = u.product_id
                LEFT JOIN locations l ON l.id = u.location_id
                LEFT JOIN condition_grades cg ON cg.id = u.condition_grade_id
                WHERE u.unit_code = %s
                """,
                [unit_code]
            )
            unit = cur.fetchone()
            if not unit:
                return jsonify({'error': 'Unit not found'}), 404
            unit = dict(unit)

            cur.execute(
                """
                SELECT li.channel_listing_id, li.title, li.current_price, li.status,
                       li.listing_url, c.display_name AS channel_name
                FROM listings li
                JOIN listing_units lu ON lu.listing_id = li.id
                JOIN channels c ON c.id = li.channel_id
                WHERE lu.unit_id = %s
                LIMIT 1
                """,
                [str(unit['id'])]
            )
            listing_row = cur.fetchone()

        listing_info = None
        if listing_row:
            listing_row = dict(listing_row)
            listing_info = {
                'channel': listing_row['channel_name'],
                'listing_id': listing_row['channel_listing_id'],
                'title': listing_row['title'],
                'price': float(listing_row['current_price']) if listing_row['current_price'] else None,
                'status': listing_row['status'],
                'url': listing_row['listing_url'],
            }

        return jsonify({
            'unit': {
                'id': str(unit['id']),
                'unit_code': unit['unit_code'],
                'status': unit['status'],
                'product': {
                    'brand': unit['brand'],
                    'model': unit['model'],
                    'colorway': unit['colorway'],
                    'size': unit['size'],
                },
                'location': {
                    'code': unit['loc_code'],
                    'description': unit['loc_desc'],
                } if unit['loc_code'] else None,
                'condition': unit['condition'],
                'cost_basis': float(unit['cost_basis']) if unit['cost_basis'] else None,
                'listing': listing_info,
                'created_at': unit['created_at'].isoformat() if unit['created_at'] else None,
            }
        })
    finally:
        release_conn(conn)


# ============================================
# ADMIN ENDPOINTS (EPIC 8)
# ============================================

@app.route('/api/admin/products/<product_id>', methods=['GET'])
def admin_get_product_detail(product_id):
    conn = acquire_conn()
    try:
        try:
            detail = get_product_detail(conn, product_id=product_id)
        except ValueError as exc:
            return jsonify({'error': str(exc)}), 404
        return jsonify({'product': detail})
    except Exception as exc:
        logger.error(f"Error in admin_get_product_detail: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/admin/skus', methods=['GET'])
def admin_get_sku_list():
    conn = acquire_conn()
    try:
        status = request.args.get('status') or None
        skus = get_sku_list(conn, status=status)
        return jsonify({'skus': skus, 'count': len(skus)})
    except Exception as exc:
        logger.error(f"Error in admin_get_sku_list: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


# ============================================
# MIGRATION ENDPOINTS (EPIC 9)
# ============================================

@app.route('/api/admin/migration/backfill', methods=['POST'])
def admin_backfill_product_ids():
    conn = acquire_conn()
    try:
        result = backfill_product_ids(conn)
        conn.commit()
        return jsonify(result)
    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in admin_backfill_product_ids: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/admin/migration/exceptions', methods=['GET'])
def admin_get_exception_report():
    conn = acquire_conn()
    try:
        exceptions = get_exception_report(conn)
        return jsonify({'exceptions': exceptions, 'count': len(exceptions)})
    except Exception as exc:
        logger.error(f"Error in admin_get_exception_report: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


# ============================================
# LOCATION ENDPOINTS
# ============================================

@app.route('/api/locations', methods=['GET'])
def get_locations():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, code, description, is_active FROM locations WHERE is_active = TRUE")
            locations = [dict(r) for r in cur.fetchall()]
        return jsonify({
            'locations': [{'id': str(l['id']), 'code': l['code'],
                           'description': l['description'], 'is_active': l['is_active']}
                          for l in locations]
        })
    finally:
        release_conn(conn)


@app.route('/api/locations', methods=['POST'])
def create_location():
    conn = acquire_conn()
    try:
        data = request.json
        if not data.get('code'):
            return jsonify({'error': 'Missing required field: code'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM locations WHERE code = %s LIMIT 1", [data['code']])
            if cur.fetchone():
                return jsonify({'error': 'Location code already exists'}), 400

            cur.execute(
                "INSERT INTO locations (id, code, description, is_active) "
                "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING id, code",
                [data['code'], data.get('description'), data.get('is_active', True)]
            )
            location = dict(cur.fetchone())

        conn.commit()
        return jsonify({
            'message': 'Location created successfully',
            'location': {'id': str(location['id']), 'code': location['code']}
        }), 201
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating location: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


# ============================================
# CATEGORY & CONDITION ENDPOINTS
# ============================================

@app.route('/api/categories', methods=['GET'])
def get_categories():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, internal_name, display_name, ebay_category_id FROM categories")
            categories = [dict(r) for r in cur.fetchall()]
        return jsonify({
            'categories': [{'id': str(c['id']), 'internal_name': c['internal_name'],
                            'display_name': c['display_name'],
                            'ebay_category_id': c['ebay_category_id']}
                           for c in categories]
        })
    finally:
        release_conn(conn)


@app.route('/api/condition-grades', methods=['GET'])
def get_condition_grades():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, internal_code, display_name, ebay_condition_id, ebay_condition_name "
                "FROM condition_grades ORDER BY sort_order"
            )
            grades = [dict(r) for r in cur.fetchall()]
        return jsonify({
            'condition_grades': [{'id': str(g['id']), 'internal_code': g['internal_code'],
                                  'display_name': g['display_name'],
                                  'ebay_condition_id': g['ebay_condition_id'],
                                  'ebay_condition_name': g['ebay_condition_name']}
                                 for g in grades]
        })
    finally:
        release_conn(conn)


# ============================================
# SYNC ENDPOINTS
# ============================================

@app.route('/api/sync/ebay', methods=['POST'])
def sync_ebay():
    conn = acquire_conn()
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400

        sync_service = SyncService(conn)
        result = sync_service.sync_ebay_listings()
        sync_service.sync_sold_items()

        if result['success']:
            return jsonify({'message': 'Sync completed successfully', 'results': result.get('results')})
        else:
            return jsonify({'error': 'Sync failed', 'details': result.get('error')}), 500
    finally:
        release_conn(conn)


@app.route('/api/sync/logs', methods=['GET'])
def get_sync_logs():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, sync_type, status, records_processed, records_updated, "
                "records_created, errors, started_at, completed_at "
                "FROM sync_logs ORDER BY started_at DESC LIMIT 20"
            )
            logs = [dict(r) for r in cur.fetchall()]
        return jsonify({
            'logs': [{
                'id': str(l['id']),
                'sync_type': l['sync_type'],
                'status': l['status'],
                'records_processed': l['records_processed'],
                'records_updated': l['records_updated'],
                'records_created': l['records_created'],
                'errors': l['errors'],
                'started_at': l['started_at'].isoformat() if l['started_at'] else None,
                'completed_at': l['completed_at'].isoformat() if l['completed_at'] else None,
            } for l in logs]
        })
    finally:
        release_conn(conn)


# ============================================
# ALERT ENDPOINTS
# ============================================

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]
        resolved = request.args.get('resolved')
        if resolved:
            where.append("is_resolved = %s")
            params.append(resolved.lower() == 'true')

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT id, alert_type, severity, title, message, is_resolved, created_at "
                f"FROM alerts WHERE {' AND '.join(where)} ORDER BY created_at DESC LIMIT 50",
                params
            )
            alerts = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'alerts': [{
                'id': str(a['id']),
                'alert_type': a['alert_type'],
                'severity': a['severity'],
                'title': a['title'],
                'message': a['message'],
                'is_resolved': a['is_resolved'],
                'created_at': a['created_at'].isoformat() if a['created_at'] else None,
            } for a in alerts]
        })
    finally:
        release_conn(conn)


@app.route('/api/alerts/<alert_id>/resolve', methods=['POST'])
def resolve_alert(alert_id):
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM alerts WHERE id = %s LIMIT 1", [alert_id])
            if not cur.fetchone():
                return jsonify({'error': 'Alert not found'}), 404
            cur.execute(
                "UPDATE alerts SET is_resolved = TRUE, resolved_at = now() WHERE id = %s",
                [alert_id]
            )
        conn.commit()
        return jsonify({'message': 'Alert resolved'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


# ============================================
# LISTING ENDPOINTS
# ============================================

@app.route('/api/listings', methods=['GET'])
def get_listings():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]
        status = request.args.get('status')
        if status:
            where.append("l.status = %s")
            params.append(status)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT l.id, l.channel_listing_id, l.title, l.current_price,
                       l.status, l.listing_url, l.created_at,
                       c.display_name AS channel_name,
                       p.brand, p.model, p.size
                FROM listings l
                LEFT JOIN channels c ON c.id = l.channel_id
                LEFT JOIN products p ON p.id = l.product_id
                WHERE {' AND '.join(where)}
                """,
                params
            )
            listings = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'listings': [{
                'id': str(l['id']),
                'channel_listing_id': l['channel_listing_id'],
                'title': l['title'],
                'current_price': float(l['current_price']) if l['current_price'] else None,
                'status': l['status'],
                'listing_url': l['listing_url'],
                'channel': l['channel_name'],
                'product': {'brand': l['brand'], 'model': l['model'], 'size': l['size']} if l['brand'] else None,
                'created_at': l['created_at'].isoformat() if l['created_at'] else None,
            } for l in listings]
        })
    finally:
        release_conn(conn)


@app.route('/api/listings', methods=['POST'])
def create_listing_route():
    """Create a new product-based listing on a channel."""
    conn = acquire_conn()
    try:
        data = request.json or {}
        for field in ('product_id', 'channel_name', 'title', 'price'):
            if not data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 422

        try:
            price = float(data['price'])
        except (TypeError, ValueError):
            return jsonify({'error': 'price must be a number'}), 422

        try:
            quantity = int(data.get('quantity', 1))
        except (TypeError, ValueError):
            return jsonify({'error': 'quantity must be an integer'}), 422

        try:
            listing, created = create_listing(
                conn,
                product_id=data['product_id'],
                channel_name=data['channel_name'],
                title=data['title'],
                price=price,
                description=data.get('description'),
                photos=data.get('photos'),
                item_specifics=data.get('item_specifics'),
                channel_listing_id=data.get('channel_listing_id'),
                listing_url=data.get('listing_url'),
                quantity=quantity,
                status=data.get('status', 'draft'),
            )
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return jsonify({'error': str(exc)}), 422

        # Fetch channel name for response
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT name FROM channels WHERE id = %s", [listing['channel_id']])
            ch = cur.fetchone()

        return jsonify({
            'listing_created': created,
            'listing': {
                'id': str(listing['id']),
                'product_id': str(listing['product_id']),
                'channel_name': ch['name'] if ch else None,
                'title': listing['title'],
                'price': float(listing['current_price']) if listing['current_price'] else None,
                'mode': listing['mode'],
                'quantity': listing['quantity'],
                'status': listing['status'],
                'created_at': listing['created_at'].isoformat() if listing['created_at'] else None,
            },
        }), 201
    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in create_listing_route: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/listings/<listing_id>', methods=['GET'])
def get_listing(listing_id):
    """Get a single listing with its channel and attached units."""
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT l.*, c.name AS channel_name, c.display_name AS channel_display_name
                FROM listings l
                LEFT JOIN channels c ON c.id = l.channel_id
                WHERE l.id = %s
                """,
                [listing_id],
            )
            listing = cur.fetchone()

        if not listing:
            return jsonify({'error': 'Listing not found'}), 404

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.unit_code, u.status
                FROM units u
                JOIN listing_units lu ON lu.unit_id = u.id
                WHERE lu.listing_id = %s
                """,
                [listing_id],
            )
            units = cur.fetchall()

        return jsonify({
            'listing': {
                'id': str(listing['id']),
                'product_id': str(listing['product_id']),
                'channel_name': listing['channel_name'],
                'channel_display_name': listing['channel_display_name'],
                'channel_listing_id': listing['channel_listing_id'],
                'title': listing['title'],
                'price': float(listing['current_price']) if listing['current_price'] else None,
                'mode': listing['mode'],
                'status': listing['status'],
                'listing_url': listing['listing_url'],
                'created_at': listing['created_at'].isoformat() if listing['created_at'] else None,
                'ended_at': listing['ended_at'].isoformat() if listing['ended_at'] else None,
                'units': [
                    {'id': str(u['id']), 'unit_code': u['unit_code'], 'status': u['status']}
                    for u in units
                ],
            }
        })
    finally:
        release_conn(conn)


@app.route('/api/listings/<listing_id>/units', methods=['POST'])
def assign_unit_route(listing_id):
    """Assign a unit to a listing."""
    conn = acquire_conn()
    try:
        data = request.json or {}
        unit_id = data.get('unit_id')
        if not unit_id:
            return jsonify({'error': 'Missing required field: unit_id'}), 422

        try:
            lu = assign_unit_to_listing(conn, listing_id=listing_id, unit_id=unit_id)
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return jsonify({'error': str(exc)}), 422

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT unit_code FROM units WHERE id = %s", [unit_id])
            unit_row = cur.fetchone()

        return jsonify({
            'unit_assigned': True,
            'listing_id': str(lu['listing_id']),
            'unit_id': str(lu['unit_id']),
            'unit_code': unit_row['unit_code'] if unit_row else None,
        })
    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in assign_unit_route: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/listings/<listing_id>/end', methods=['POST'])
def end_listing_route(listing_id):
    """End a listing and revert non-sold units to ready_to_list."""
    conn = acquire_conn()
    try:
        try:
            listing = end_listing(conn, listing_id=listing_id)
            conn.commit()
        except ValueError as exc:
            conn.rollback()
            return jsonify({'error': str(exc)}), 422

        return jsonify({
            'ended': True,
            'listing_id': str(listing['id']),
            'ended_at': listing['ended_at'].isoformat() if listing['ended_at'] else None,
        })
    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in end_listing_route: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


# ============================================
# ORDER ENDPOINTS
# ============================================

@app.route('/api/orders', methods=['GET'])
def list_orders():
    """List orders with optional filters: platform, status, needs_reconciliation, from_date, to_date, limit."""
    conn = acquire_conn()
    try:
        platform = request.args.get('platform')
        status = request.args.get('status')
        needs_recon = request.args.get('needs_reconciliation')
        from_date = request.args.get('from_date')
        to_date = request.args.get('to_date')
        limit = min(int(request.args.get('limit', 100)), 500)

        conditions = []
        params = []
        if platform:
            conditions.append("o.platform = %s")
            params.append(platform)
        if status:
            conditions.append("o.status = %s")
            params.append(status)
        if needs_recon is not None:
            conditions.append("o.needs_reconciliation = %s")
            params.append(needs_recon.lower() in ('true', '1', 'yes'))
        if from_date:
            conditions.append("o.created_at >= %s")
            params.append(from_date)
        if to_date:
            conditions.append("o.created_at <= %s")
            params.append(to_date)

        where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
        params.append(limit)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT o.*, COUNT(oa.id) AS allocation_count
                FROM orders o
                LEFT JOIN order_allocations oa ON oa.order_id = o.id
                {where}
                GROUP BY o.id
                ORDER BY o.created_at DESC
                LIMIT %s
                """,
                params,
            )
            orders = [dict(r) for r in cur.fetchall()]

        for o in orders:
            for k in ('created_at', 'updated_at', 'allocated_at', 'shipped_at'):
                if o.get(k) and hasattr(o[k], 'isoformat'):
                    o[k] = o[k].isoformat()
            o['id'] = str(o['id'])
            if o.get('sale_price') is not None:
                o['sale_price'] = float(o['sale_price'])

        return jsonify({'orders': orders, 'total': len(orders)})
    except Exception as exc:
        logger.error(f"Error in list_orders: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/orders/<order_id>', methods=['GET'])
def get_order(order_id):
    """Get a single order with its allocations and unit details."""
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE id = %s", [order_id])
            order = cur.fetchone()
        if not order:
            return jsonify({'error': 'Order not found'}), 404

        order = dict(order)
        for k in ('created_at', 'updated_at', 'allocated_at', 'shipped_at'):
            if order.get(k) and hasattr(order[k], 'isoformat'):
                order[k] = order[k].isoformat()
        order['id'] = str(order['id'])
        if order.get('sale_price') is not None:
            order['sale_price'] = float(order['sale_price'])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT oa.id, oa.order_id, oa.unit_id, oa.listing_id, oa.allocated_at,
                       u.unit_code, u.status AS unit_status, u.sold_price
                FROM order_allocations oa
                JOIN units u ON u.id = oa.unit_id
                WHERE oa.order_id = %s
                """,
                [order_id],
            )
            allocations = []
            for r in cur.fetchall():
                a = dict(r)
                a['id'] = str(a['id'])
                a['order_id'] = str(a['order_id'])
                a['unit_id'] = str(a['unit_id'])
                if a.get('listing_id'):
                    a['listing_id'] = str(a['listing_id'])
                if a.get('allocated_at') and hasattr(a['allocated_at'], 'isoformat'):
                    a['allocated_at'] = a['allocated_at'].isoformat()
                if a.get('sold_price') is not None:
                    a['sold_price'] = float(a['sold_price'])
                allocations.append(a)

        return jsonify({'order': order, 'allocations': allocations})
    except Exception as exc:
        logger.error(f"Error in get_order: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


@app.route('/api/orders/<order_id>/ship', methods=['POST'])
def ship_order(order_id):
    """Mark an order as shipped and update all allocated units to 'shipped'."""
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM orders WHERE id = %s", [order_id])
            order = cur.fetchone()
        if not order:
            return jsonify({'error': 'Order not found'}), 404
        if order['status'] != 'allocated':
            return jsonify({'error': f"Cannot ship order with status '{order['status']}'"}), 422

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "UPDATE orders SET status = 'shipped', shipped_at = %s WHERE id = %s RETURNING *",
                [now, order_id],
            )
            updated_order = dict(cur.fetchone())

        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE units SET status = 'shipped'
                WHERE id IN (
                    SELECT unit_id FROM order_allocations WHERE order_id = %s
                )
                """,
                [order_id],
            )

        conn.commit()
        shipped_at = updated_order['shipped_at']
        return jsonify({
            'shipped': True,
            'order_id': str(updated_order['id']),
            'shipped_at': shipped_at.isoformat() if shipped_at and hasattr(shipped_at, 'isoformat') else None,
        })
    except Exception as exc:
        conn.rollback()
        logger.error(f"Error in ship_order: {exc}")
        return jsonify({'error': str(exc)}), 500
    finally:
        release_conn(conn)


# ============================================
# SCHEDULER ENDPOINTS
# ============================================

def run_scheduled_sync():
    conn = acquire_conn()
    try:
        sync_service = SyncService(conn)
        result = sync_service.sync_ebay_listings()
        sync_service.sync_sold_items()
        logger.info(f"Scheduled sync completed: {result}")
    except Exception as e:
        logger.error(f"Scheduled sync failed: {e}")
    finally:
        release_conn(conn)


@app.route('/api/scheduler/status', methods=['GET'])
def get_scheduler_status():
    try:
        return jsonify(sync_scheduler.get_status())
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/start', methods=['POST'])
def start_scheduler():
    try:
        if ebay_api.is_configured():
            success = sync_scheduler.start(run_scheduled_sync)
            if success:
                return jsonify({'message': 'Scheduler started successfully', 'status': sync_scheduler.get_status()})
            return jsonify({'error': 'Failed to start scheduler'}), 500
        return jsonify({'error': 'eBay API not configured'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/stop', methods=['POST'])
def stop_scheduler():
    try:
        success = sync_scheduler.stop()
        if success:
            return jsonify({'message': 'Scheduler stopped successfully', 'status': sync_scheduler.get_status()})
        return jsonify({'error': 'Failed to stop scheduler'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/scheduler/trigger', methods=['POST'])
def trigger_sync_now():
    try:
        success = sync_scheduler.trigger_now()
        if success:
            return jsonify({'message': 'Sync triggered successfully'})
        return jsonify({'error': 'Failed to trigger sync'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================
# SOLD ITEMS ENDPOINTS
# ============================================

@app.route('/api/sold-items', methods=['GET'])
def get_sold_items():
    conn = acquire_conn()
    try:
        params = ["sold"]
        where = ["u.status = %s"]

        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        platform = request.args.get('platform')

        if start_date:
            where.append("u.sold_at >= %s")
            params.append(start_date)
        if end_date:
            where.append("u.sold_at <= %s")
            params.append(end_date)
        if platform:
            where.append("u.sold_platform = %s")
            params.append(platform)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT u.id, u.unit_code, u.sold_at, u.sold_price, u.sold_platform,
                       u.cost_basis, p.brand, p.model, p.size, p.colorway,
                       l.code AS location_code
                FROM units u
                LEFT JOIN products p ON p.id = u.product_id
                LEFT JOIN locations l ON l.id = u.location_id
                WHERE {' AND '.join(where)}
                ORDER BY u.sold_at DESC
                """,
                params
            )
            units = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'sold_items': [{
                'id': str(u['id']),
                'unit_code': u['unit_code'],
                'product': {'brand': u['brand'], 'model': u['model'],
                            'size': u['size'], 'colorway': u['colorway']} if u['brand'] else None,
                'sold_at': u['sold_at'].isoformat() if u['sold_at'] else None,
                'sold_price': float(u['sold_price']) if u['sold_price'] else None,
                'sold_platform': u['sold_platform'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'profit': float(u['sold_price'] - u['cost_basis']) if (u['sold_price'] and u['cost_basis']) else None,
                'location_code': u['location_code'],
            } for u in units]
        })
    finally:
        release_conn(conn)


@app.route('/api/sales/stats', methods=['GET'])
def get_sales_stats():
    conn = acquire_conn()
    try:
        params = []
        where = ["status = 'sold'", "sold_at IS NOT NULL"]
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        if start_date:
            where.append("sold_at >= %s")
            params.append(start_date)
        if end_date:
            where.append("sold_at <= %s")
            params.append(end_date)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT sold_price, cost_basis, sold_platform FROM units WHERE {' AND '.join(where)}",
                params
            )
            sold_items = [dict(r) for r in cur.fetchall()]

        total_sales = len(sold_items)
        total_revenue = sum(float(u['sold_price']) for u in sold_items if u['sold_price'])
        total_cost = sum(float(u['cost_basis']) for u in sold_items if u['cost_basis'])
        total_profit = total_revenue - total_cost
        avg_sale_price = total_revenue / total_sales if total_sales > 0 else 0
        avg_profit = total_profit / total_sales if total_sales > 0 else 0
        profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0

        platform_breakdown = {}
        for u in sold_items:
            p = u['sold_platform'] or 'unknown'
            if p not in platform_breakdown:
                platform_breakdown[p] = {'count': 0, 'revenue': 0}
            platform_breakdown[p]['count'] += 1
            platform_breakdown[p]['revenue'] += float(u['sold_price']) if u['sold_price'] else 0

        return jsonify({
            'stats': {
                'total_sales': total_sales,
                'total_revenue': float(total_revenue),
                'total_profit': float(total_profit),
                'avg_sale_price': float(avg_sale_price),
                'avg_profit': float(avg_profit),
                'profit_margin_percent': float(profit_margin),
            },
            'platform_breakdown': platform_breakdown,
        })
    finally:
        release_conn(conn)


@app.route('/api/sync/sold-items', methods=['POST'])
def sync_sold_items():
    conn = acquire_conn()
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        sync_service = SyncService(conn)
        result = sync_service.sync_sold_items()
        if result['success']:
            return jsonify({'message': 'Sold items sync completed successfully', 'results': result.get('results')})
        return jsonify({'error': 'Sold items sync failed', 'details': result.get('error')}), 500
    finally:
        release_conn(conn)


@app.route('/api/sync/check-sold', methods=['POST'])
def check_for_sold():
    conn = acquire_conn()
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        sync_service = SyncService(conn)
        results = sync_service.check_active_listings_for_sold()
        return jsonify({'message': 'Check completed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/dashboard/sales', methods=['GET'])
def get_dashboard_sales():
    conn = acquire_conn()
    try:
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.unit_code, u.sold_price, u.sold_at, u.sold_platform,
                       p.brand, p.model
                FROM units u
                LEFT JOIN products p ON p.id = u.product_id
                WHERE u.status = 'sold' AND u.sold_at >= %s
                ORDER BY u.sold_at DESC LIMIT 10
                """,
                [thirty_days_ago]
            )
            recent_sales = [dict(r) for r in cur.fetchall()]

            cur.execute(
                "SELECT COUNT(*) FROM units WHERE status = 'sold' AND sold_at >= %s",
                [today_start]
            )
            today_sales_count = cur.fetchone()[0]

        return jsonify({
            'recent_sales': [{
                'unit_code': u['unit_code'],
                'product': f"{u['brand']} {u['model']}" if u['brand'] else '-',
                'sold_price': float(u['sold_price']) if u['sold_price'] else 0,
                'sold_at': u['sold_at'].isoformat() if u['sold_at'] else None,
                'platform': u['sold_platform'],
            } for u in recent_sales],
            'today_sales_count': today_sales_count,
        })
    finally:
        release_conn(conn)


# ============================================
# TEMPLATE ENDPOINTS
# ============================================

@app.route('/api/templates', methods=['GET'])
def get_templates():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]
        if request.args.get('validated') == 'true':
            where.append("t.is_validated = TRUE")
        product_id = request.args.get('product_id')
        if product_id:
            where.append("t.product_id = %s")
            params.append(product_id)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                SELECT t.id, t.product_id, t.title, t.base_price, t.photos,
                       t.pricing, t.category_mappings, t.is_validated,
                       t.validation_errors, t.template_version, t.last_synced_at,
                       p.brand, p.model, p.size
                FROM listing_templates t
                LEFT JOIN products p ON p.id = t.product_id
                WHERE {' AND '.join(where)}
                """,
                params
            )
            templates = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'templates': [{
                'id': str(t['id']),
                'product_id': str(t['product_id']),
                'product': {'brand': t['brand'], 'model': t['model'], 'size': t['size']} if t['brand'] else None,
                'title': t['title'],
                'base_price': float(t['base_price']) if t['base_price'] else None,
                'photos_count': len(t['photos']) if t['photos'] else 0,
                'pricing': t['pricing'],
                'category_mappings': t['category_mappings'],
                'is_validated': t['is_validated'],
                'validation_errors': t['validation_errors'],
                'template_version': t['template_version'],
                'last_synced_at': t['last_synced_at'].isoformat() if t['last_synced_at'] else None,
            } for t in templates]
        })
    finally:
        release_conn(conn)


@app.route('/api/templates/<template_id>', methods=['GET'])
def get_template(template_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, product_id, title, description, photos, photo_metadata, "
                "item_specifics, base_price, pricing, category_mappings, seo_keywords, "
                "is_validated, validation_errors, template_version, last_synced_at "
                "FROM listing_templates WHERE id = %s",
                [template_id]
            )
            template = cur.fetchone()
            if not template:
                return jsonify({'error': 'Template not found'}), 404
            template = dict(template)

        return jsonify({
            'id': str(template['id']),
            'product_id': str(template['product_id']),
            'title': template['title'],
            'description': template['description'],
            'photos': template['photos'],
            'photo_metadata': template['photo_metadata'],
            'item_specifics': template['item_specifics'],
            'base_price': float(template['base_price']) if template['base_price'] else None,
            'pricing': template['pricing'],
            'category_mappings': template['category_mappings'],
            'seo_keywords': template['seo_keywords'],
            'is_validated': template['is_validated'],
            'validation_errors': template['validation_errors'],
            'template_version': template['template_version'],
            'last_synced_at': template['last_synced_at'].isoformat() if template['last_synced_at'] else None,
        })
    finally:
        release_conn(conn)


@app.route('/api/templates/<template_id>/platform/<platform>', methods=['GET'])
def get_template_for_platform(template_id, platform):
    conn = acquire_conn()
    try:
        template_service = TemplateService(conn)
        formatted = template_service.get_template_for_platform(template_id, platform)
        if not formatted:
            return jsonify({'error': 'Template not found'}), 404
        return jsonify(formatted)
    finally:
        release_conn(conn)


@app.route('/api/templates/<template_id>/validate', methods=['POST'])
def validate_template(template_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, product_id, title, description, photos, item_specifics, "
                "base_price, pricing, category_mappings, is_validated, validation_errors "
                "FROM listing_templates WHERE id = %s",
                [template_id]
            )
            template = cur.fetchone()
            if not template:
                return jsonify({'error': 'Template not found'}), 404
            template = dict(template)

        template_service = TemplateService(conn)
        result = template_service.validate_template(template)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE listing_templates SET is_validated = %s, validation_errors = %s WHERE id = %s",
                [result['valid'], json.dumps(result.get('errors')), template_id]
            )
        conn.commit()
        return jsonify(result)
    finally:
        release_conn(conn)


@app.route('/api/templates/validate-all', methods=['POST'])
def validate_all_templates():
    conn = acquire_conn()
    try:
        template_service = TemplateService(conn)
        results = template_service.bulk_validate_templates()
        return jsonify({'message': 'Bulk validation completed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/templates/refresh', methods=['POST'])
def refresh_templates():
    conn = acquire_conn()
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        sync_service = SyncService(conn)
        results = sync_service.refresh_templates()
        return jsonify({'message': 'Templates refreshed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/templates/stats', methods=['GET'])
def get_template_stats():
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            def count(sql):
                cur.execute(sql)
                return cur.fetchone()[0]
            total = count("SELECT COUNT(*) FROM listing_templates")
            validated = count("SELECT COUNT(*) FROM listing_templates WHERE is_validated = TRUE")
            ready = count("SELECT COUNT(*) FROM listing_templates WHERE is_validated = TRUE AND template_version >= 2")

        return jsonify({
            'stats': {
                'total': total,
                'validated': validated,
                'invalid': total - validated,
                'ready_for_crosslisting': ready,
                'validation_rate': round((validated / total * 100) if total > 0 else 0, 1),
            }
        })
    finally:
        release_conn(conn)


# ============================================
# AUDIT ENDPOINTS
# ============================================

@app.route('/api/audit/full', methods=['POST'])
def run_full_audit():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        results = audit_service.run_full_audit()
        return jsonify({'message': 'Audit completed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/audit/summary', methods=['GET'])
def get_audit_summary():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        return jsonify(audit_service.get_audit_summary())
    finally:
        release_conn(conn)


@app.route('/api/audit/sku-issues', methods=['GET'])
def get_sku_issues():
    conn = acquire_conn()
    try:
        return jsonify(AuditService(conn).audit_sku_issues())
    finally:
        release_conn(conn)


@app.route('/api/audit/inventory-mismatches', methods=['GET'])
def get_inventory_mismatches():
    conn = acquire_conn()
    try:
        return jsonify(AuditService(conn).audit_inventory_mismatches())
    finally:
        release_conn(conn)


@app.route('/api/audit/template-issues', methods=['GET'])
def get_template_issues():
    conn = acquire_conn()
    try:
        return jsonify(AuditService(conn).audit_template_issues())
    finally:
        release_conn(conn)


@app.route('/api/audit/pricing-issues', methods=['GET'])
def get_pricing_issues():
    conn = acquire_conn()
    try:
        return jsonify(AuditService(conn).audit_pricing_issues())
    finally:
        release_conn(conn)


@app.route('/api/audit/export', methods=['POST'])
def export_audit_report():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        results = audit_service.run_full_audit()
        csv_content = audit_service.export_audit_report(results)
        return Response(
            csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment;filename=audit_report.csv'}
        )
    finally:
        release_conn(conn)


@app.route('/api/audit/issues/resolve-bulk', methods=['POST'])
def resolve_bulk_issues():
    conn = acquire_conn()
    try:
        data = request.get_json()
        alert_ids = data.get('alert_ids', [])
        if not alert_ids:
            return jsonify({'error': 'No alert IDs provided'}), 400

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE alerts SET is_resolved = TRUE, resolved_at = now() "
                "WHERE id = ANY(%s::uuid[])",
                [alert_ids]
            )
            updated = cur.rowcount

        conn.commit()
        return jsonify({'message': f'{updated} alerts resolved', 'updated': updated})
    finally:
        release_conn(conn)


@app.route('/api/audit/dashboard', methods=['GET'])
def get_audit_dashboard():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        summary = audit_service.get_audit_summary()
        sku_issues = audit_service.audit_sku_issues()
        inventory_issues = audit_service.audit_inventory_mismatches()
        template_issues = audit_service.audit_template_issues()

        return jsonify({
            'summary': summary,
            'recent_issues': {
                'sku': {
                    'missing_skus': sku_issues['missing_skus'][:10],
                    'unmatched_skus': sku_issues['unmatched_skus'][:10],
                    'duplicate_skus': sku_issues['duplicate_skus'][:10],
                },
                'inventory': {
                    'units_without_listings': inventory_issues['units_without_listings'][:10],
                    'listings_without_units': inventory_issues['listings_without_units'][:10],
                },
                'templates': {'invalid_templates': template_issues['invalid_templates'][:10]},
            },
            'issue_counts': {
                'sku_issues': sku_issues['total'],
                'inventory_mismatches': inventory_issues['total'],
                'template_issues': template_issues['total'],
            },
        })
    finally:
        release_conn(conn)


# ============================================
# BULK IMPORT ENDPOINTS
# ============================================

@app.route('/api/import/products/preview', methods=['POST'])
def preview_products_import():
    conn = acquire_conn()
    try:
        data = request.get_json()
        csv_content = data.get('csv_content')
        if not csv_content:
            return jsonify({'error': 'No CSV content provided'}), 400
        results = BulkImportService(conn).parse_products_csv(csv_content)
        return jsonify({'message': 'CSV parsed successfully', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/import/products/execute', methods=['POST'])
def execute_products_import():
    conn = acquire_conn()
    try:
        data = request.get_json()
        valid_rows = data.get('valid_rows', [])
        if not valid_rows:
            return jsonify({'error': 'No valid rows to import'}), 400
        results = BulkImportService(conn).import_products(valid_rows)
        return jsonify({'message': 'Import completed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/import/units/preview', methods=['POST'])
def preview_units_import():
    conn = acquire_conn()
    try:
        data = request.get_json()
        csv_content = data.get('csv_content')
        if not csv_content:
            return jsonify({'error': 'No CSV content provided'}), 400
        results = BulkImportService(conn).parse_units_csv(csv_content)
        return jsonify({'message': 'CSV parsed successfully', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/import/units/execute', methods=['POST'])
def execute_units_import():
    conn = acquire_conn()
    try:
        data = request.get_json()
        valid_rows = data.get('valid_rows', [])
        if not valid_rows:
            return jsonify({'error': 'No valid rows to import'}), 400
        results = BulkImportService(conn).import_units(valid_rows)
        return jsonify({'message': 'Import completed', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/import/templates/products', methods=['GET'])
def download_products_template():
    conn = acquire_conn()
    try:
        template = BulkImportService(conn).generate_products_template()
        return Response(template, mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment;filename=products_template.csv'})
    finally:
        release_conn(conn)


@app.route('/api/import/templates/units', methods=['GET'])
def download_units_template():
    conn = acquire_conn()
    try:
        template = BulkImportService(conn).generate_units_template()
        return Response(template, mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment;filename=units_template.csv'})
    finally:
        release_conn(conn)


# ============================================
# DELISTING ENDPOINTS
# ============================================

@app.route('/api/delist/check-emails', methods=['POST'])
def manual_check_emails():
    conn = acquire_conn()
    try:
        gmail = GmailService()
        if not gmail.is_connected():
            return jsonify({'error': 'Gmail not connected'}), 400

        parser = EmailParserService()
        delist_service = DelistService(conn)

        since_minutes = request.json.get('since_minutes', 60) if request.json else 60
        logger.info(f"Checking emails for delisting since last {since_minutes} minutes")
        emails = gmail.get_sale_emails(since_minutes=since_minutes)

        results = {
            'emails_found': len(emails),
            'emails_processed': 0,
            'total_items': 0,
            'processed': [],
            'errors': [],
        }

        for email in emails:
            try:
                sale_items = parser.parse_sale_email(email)
                if not sale_items:
                    results['errors'].append({'email': email.get('subject', 'Unknown')[:50],
                                              'error': 'Failed to parse email'})
                    continue

                email_items_processed = 0
                email_items_failed = 0
                results['total_items'] += len(sale_items)

                for i, item in enumerate(sale_items, 1):
                    try:
                        result = delist_service.process_sale(item)
                        if result.get('success'):
                            email_items_processed += 1
                            sku = item.get('sku') or (item.get('skus', [None])[0] if item.get('skus') else None)
                            results['processed'].append({
                                'platform': item.get('platform'),
                                'sku': sku,
                                'unit_code': result.get('unit_code'),
                                'delisted_from': [d.get('platform') for d in result.get('delisted', [])],
                                'delisted_count': len(result.get('delisted', [])),
                            })
                        else:
                            email_items_failed += 1
                            results['errors'].append({'email': email.get('subject', 'Unknown')[:50],
                                                      'item': i, 'sku': item.get('sku'),
                                                      'errors': result.get('errors')})
                    except Exception as e:
                        email_items_failed += 1
                        results['errors'].append({'email': email.get('subject', 'Unknown')[:50],
                                                  'item': i, 'sku': item.get('sku'), 'error': str(e)})

                if email_items_processed > 0:
                    gmail.mark_as_read(email.get('message_id'))
                    results['emails_processed'] += 1
            except Exception as e:
                results['errors'].append({'email': email.get('subject', 'Unknown')[:50], 'error': str(e)})

        message = (f"Checked {results['emails_found']} email(s): "
                   f"{results['emails_processed']} processed, "
                   f"{len(results['processed'])} item(s) delisted")
        if results['errors']:
            message += f", {len(results['errors'])} error(s)"

        return jsonify({'success': True, 'message': message, 'results': results})

    except Exception as e:
        logger.error(f"Error in manual_check_emails: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/delist/history', methods=['GET'])
def get_delist_history():
    conn = acquire_conn()
    try:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT id, created_at, status, message, details FROM sync_logs "
                    "WHERE log_type = 'delist' ORDER BY created_at DESC LIMIT 50"
                )
                logs = [dict(r) for r in cur.fetchall()]
        except Exception:
            logs = []
        return jsonify({
            'history': [{
                'id': str(l['id']),
                'timestamp': l['created_at'].isoformat() if l.get('created_at') else None,
                'status': l.get('status'),
                'message': l.get('message'),
                'details': l.get('details'),
            } for l in logs]
        })
    finally:
        release_conn(conn)


@app.route('/api/delist/stats', methods=['GET'])
def get_delist_stats():
    conn = acquire_conn()
    try:
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT sold_platform, COUNT(id) AS cnt FROM units "
                "WHERE status = 'sold' AND sold_at >= %s GROUP BY sold_platform",
                [thirty_days_ago]
            )
            sold_by_platform_rows = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) FROM listings WHERE status = 'ended' AND ended_at >= %s",
                [thirty_days_ago]
            )
            delisted_count = cur.fetchone()[0]

            cur.execute(
                "SELECT unit_code, sold_platform, sold_price, sold_at FROM units "
                "WHERE status = 'sold' ORDER BY sold_at DESC LIMIT 10"
            )
            recent_sales = [dict(r) for r in cur.fetchall()]

        sold_by_platform = {r['sold_platform']: r['cnt'] for r in sold_by_platform_rows}
        return jsonify({
            'stats': {
                'sold_by_platform': sold_by_platform,
                'total_sold_30_days': sum(sold_by_platform.values()),
                'total_delisted_30_days': delisted_count,
            },
            'recent_sales': [{
                'unit_code': s['unit_code'],
                'sold_platform': s['sold_platform'],
                'sold_price': float(s['sold_price']) if s['sold_price'] else None,
                'sold_at': s['sold_at'].isoformat() if s['sold_at'] else None,
            } for s in recent_sales],
        })
    finally:
        release_conn(conn)


@app.route('/api/delist/gmail-status', methods=['GET'])
def get_gmail_status():
    try:
        return jsonify(GmailService().get_test_connection())
    except Exception as e:
        return jsonify({'connected': False, 'error': str(e)}), 500


@app.route('/api/delist/test-parse', methods=['POST'])
def test_email_parsing():
    try:
        data = request.get_json()
        if not data or 'email_data' not in data:
            return jsonify({'error': 'email_data required'}), 400
        parsed = EmailParserService().parse_sale_email(data['email_data'])
        return jsonify({'parsed': parsed, 'success': parsed is not None})
    except Exception as e:
        return jsonify({'error': str(e), 'success': False}), 500


# ============================================
# CHROME PROFILE ENDPOINTS
# ============================================

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

open_browsers = {}


@app.route('/api/chrome/open-profile/<platform>/<purpose>', methods=['POST'])
def open_chrome_profile(platform, purpose):
    global open_browsers
    if platform not in ['poshmark', 'mercari']:
        return jsonify({'error': 'Invalid platform'}), 400
    if purpose not in ['delisting', 'crosslisting']:
        return jsonify({'error': 'Invalid purpose'}), 400

    browser_key = f"{platform}_{purpose}"
    if browser_key in open_browsers:
        return jsonify({'error': f'{platform.capitalize()} {purpose} profile is already open.'}), 400

    try:
        profile_dir = os.path.join(os.path.dirname(__file__), purpose, 'profiles', platform)
        os.makedirs(profile_dir, exist_ok=True)

        chrome_options = Options()
        chrome_options.add_argument(f"user-data-dir={os.path.abspath(profile_dir)}")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        if platform == 'poshmark':
            driver.get('https://poshmark.com/login')
        elif platform == 'mercari':
            driver.get('https://www.mercari.com/login')

        open_browsers[browser_key] = driver
        return jsonify({
            'success': True,
            'message': f'{platform.capitalize()} {purpose} profile opened',
            'instructions': f'1. Log in to {platform.capitalize()}\n2. Check "Remember Me"\n3. Close browser when done',
        })
    except Exception as e:
        logger.error(f"Error opening Chrome profile: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/chrome/close-profile/<platform>/<purpose>', methods=['POST'])
def close_chrome_profile(platform, purpose):
    global open_browsers
    browser_key = f"{platform}_{purpose}"
    if browser_key in open_browsers:
        try:
            open_browsers[browser_key].quit()
            del open_browsers[browser_key]
            return jsonify({'success': True, 'message': f'{platform.capitalize()} {purpose} browser closed'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    return jsonify({'message': 'Browser not open or already closed'})


@app.route('/api/chrome/profile-status', methods=['GET'])
def get_profile_status():
    global open_browsers
    return jsonify({
        'poshmark_delisting_open': 'poshmark_delisting' in open_browsers,
        'poshmark_crosslisting_open': 'poshmark_crosslisting' in open_browsers,
        'mercari_delisting_open': 'mercari_delisting' in open_browsers,
        'mercari_crosslisting_open': 'mercari_crosslisting' in open_browsers,
        'open_browsers': list(open_browsers.keys()),
    })


# ============================================
# CROSS-LISTING ENDPOINTS
# ============================================

from src.services.crosslisting.crosslist_service import CrosslistService


@app.route('/api/crosslist/unit/<unit_id>', methods=['POST'])
def crosslist_unit(unit_id):
    conn = acquire_conn()
    try:
        result = CrosslistService(conn).check_and_crosslist(unit_id)
        if result.get('errors'):
            return jsonify({'message': 'Cross-listing completed with errors', 'result': result}), 207
        return jsonify({'message': 'Cross-listing completed', 'result': result})
    finally:
        release_conn(conn)


@app.route('/api/crosslist/bulk', methods=['POST'])
def crosslist_bulk():
    conn = acquire_conn()
    try:
        data = request.get_json()
        unit_ids = data.get('unit_ids', [])
        if not unit_ids:
            return jsonify({'error': 'No unit IDs provided'}), 400
        results = CrosslistService(conn).bulk_crosslist(unit_ids)
        return jsonify({'message': f'Processed {results["processed"]} units', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/crosslist/auto-check', methods=['POST'])
def auto_check_crosslisting():
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM units WHERE status = 'listed'")
            unit_ids = [str(r[0]) for r in cur.fetchall()]

        if not unit_ids:
            return jsonify({'message': 'No listed units found', 'results': {'total': 0}})

        results = CrosslistService(conn).bulk_crosslist(unit_ids)
        return jsonify({'message': f'Auto cross-listing checked {len(unit_ids)} units', 'results': results})
    finally:
        release_conn(conn)


@app.route('/api/crosslist/status/<unit_id>', methods=['GET'])
def get_crosslist_status(unit_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, unit_code, status FROM units WHERE id = %s", [unit_id])
            unit = cur.fetchone()
            if not unit:
                return jsonify({'error': 'Unit not found'}), 404
            unit = dict(unit)

            cur.execute(
                """
                SELECT l.id, l.channel_listing_id, l.status, l.current_price,
                       LOWER(c.name) AS channel_name
                FROM listings l
                JOIN listing_units lu ON lu.listing_id = l.id
                JOIN channels c ON c.id = l.channel_id
                WHERE lu.unit_id = %s
                """,
                [unit_id]
            )
            listings = [dict(r) for r in cur.fetchall()]

        platforms = {}
        for listing in listings:
            platform = listing['channel_name']
            platforms[platform] = {
                'listed': True,
                'listing_id': str(listing['id']),
                'channel_listing_id': listing['channel_listing_id'],
                'status': listing['status'],
                'price': float(listing['current_price']) if listing['current_price'] else None,
            }

        for platform in ['ebay', 'poshmark', 'mercari']:
            if platform not in platforms:
                platforms[platform] = {'listed': False, 'can_crosslist': unit['status'] == 'listed'}

        return jsonify({'unit_code': unit['unit_code'], 'unit_status': unit['status'], 'platforms': platforms})
    finally:
        release_conn(conn)


@app.route('/api/crosslist/stats', methods=['GET'])
def get_crosslist_stats():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM units WHERE status = 'listed'")
            total_listed = cur.fetchone()[0]

            platform_counts = {}
            for platform in ['ebay', 'poshmark', 'mercari']:
                cur.execute(
                    "SELECT COUNT(l.id) FROM listings l "
                    "JOIN channels c ON c.id = l.channel_id "
                    "WHERE LOWER(c.name) = %s AND l.status = 'active'",
                    [platform]
                )
                platform_counts[platform] = cur.fetchone()[0]

            cur.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT lu.unit_id
                    FROM listing_units lu
                    JOIN listings l ON l.id = lu.listing_id
                    JOIN units u ON u.id = lu.unit_id
                    WHERE l.status = 'active' AND u.status = 'listed'
                    GROUP BY lu.unit_id
                    HAVING COUNT(l.id) >= 3
                ) sub
                """
            )
            fully_crosslisted = cur.fetchone()[0]

        return jsonify({
            'stats': {
                'total_listed_units': total_listed,
                'fully_crosslisted': fully_crosslisted,
                'platform_counts': platform_counts,
                'needs_crosslisting': total_listed - fully_crosslisted,
            }
        })
    finally:
        release_conn(conn)


# ============================================
# RETURN TRACKING ENDPOINTS
# ============================================

from src.services.returns.ebay_return_parser import EbayReturnParser
from src.services.returns.return_service import ReturnService
from src.services.returns.email_processing_service import EmailProcessingService


@app.route('/api/returns', methods=['GET'])
def get_returns():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]

        status = request.args.get('status')
        outcome = request.args.get('outcome')
        bucket = request.args.get('bucket')
        brand = request.args.get('brand')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        matched = request.args.get('matched')

        if status:
            where.append("status_current = %s")
            params.append(status)
        if outcome:
            where.append("final_outcome = %s")
            params.append(outcome)
        if bucket:
            where.append("internal_bucket = %s")
            params.append(bucket)
        if brand:
            where.append("brand ILIKE %s")
            params.append(f'%{brand}%')
        if start_date:
            where.append("opened_at >= %s")
            params.append(start_date)
        if end_date:
            where.append("opened_at <= %s")
            params.append(end_date)
        if matched:
            if matched.lower() == 'true':
                where.append("internal_order_id IS NOT NULL")
            else:
                where.append("internal_order_id IS NULL")

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT id, return_id, order_number, buyer_username, item_title, brand, sku, "
                f"external_listing_id, status_current, final_outcome, internal_bucket, "
                f"opened_at, request_amount, internal_order_id "
                f"FROM returns WHERE {' AND '.join(where)} ORDER BY created_at DESC",
                params
            )
            returns = [dict(r) for r in cur.fetchall()]

        total = len(returns)
        matched_count = sum(1 for r in returns if r['internal_order_id'])
        return jsonify({
            'returns': [{
                'id': str(r['id']),
                'return_id': r['return_id'],
                'order_number': r['order_number'],
                'buyer_username': r['buyer_username'],
                'item_title': r['item_title'],
                'brand': r['brand'],
                'sku': r['sku'],
                'external_listing_id': r['external_listing_id'],
                'status_current': r['status_current'],
                'final_outcome': r['final_outcome'],
                'internal_bucket': r['internal_bucket'],
                'opened_at': r['opened_at'].isoformat() if r['opened_at'] else None,
                'request_amount': float(r['request_amount']) if r['request_amount'] else None,
                'matched': r['internal_order_id'] is not None,
            } for r in returns],
            'total': total,
            'matched': matched_count,
            'unmatched': total - matched_count,
        })
    finally:
        release_conn(conn)


@app.route('/api/returns/<return_id>', methods=['GET'])
def get_return_details(return_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM returns WHERE id = %s", [return_id])
            r = cur.fetchone()
            if not r:
                return jsonify({'error': 'Return not found'}), 404
            r = dict(r)

            cur.execute(
                "SELECT id, event_type, event_timestamp, source_type, email_subject, created_at "
                "FROM return_events WHERE return_id = %s ORDER BY created_at",
                [return_id]
            )
            events = [dict(e) for e in cur.fetchall()]

            matched_unit = None
            if r.get('internal_order_id'):
                cur.execute(
                    "SELECT u.id, u.unit_code, u.sold_at, u.sold_price, "
                    "p.brand, p.model, p.size "
                    "FROM units u LEFT JOIN products p ON p.id = u.product_id "
                    "WHERE u.id = %s",
                    [str(r['internal_order_id'])]
                )
                unit_row = cur.fetchone()
                if unit_row:
                    unit_row = dict(unit_row)
                    matched_unit = {
                        'id': str(unit_row['id']),
                        'unit_code': unit_row['unit_code'],
                        'product': {'brand': unit_row['brand'], 'model': unit_row['model'],
                                    'size': unit_row['size']} if unit_row['brand'] else None,
                        'sold_at': unit_row['sold_at'].isoformat() if unit_row['sold_at'] else None,
                        'sold_price': float(unit_row['sold_price']) if unit_row['sold_price'] else None,
                    }

        def _iso(v):
            return v.isoformat() if v else None

        return jsonify({
            'return': {
                'id': str(r['id']),
                'return_id': r['return_id'],
                'order_number': r['order_number'],
                'buyer_username': r['buyer_username'],
                'item_title': r['item_title'],
                'brand': r['brand'],
                'sku': r['sku'],
                'external_listing_id': r['external_listing_id'],
                'return_reason_ebay': r.get('return_reason_ebay'),
                'buyer_comment': r.get('buyer_comment'),
                'request_amount': float(r['request_amount']) if r['request_amount'] else None,
                'opened_at': _iso(r.get('opened_at')),
                'buyer_ship_by_date': _iso(r.get('buyer_ship_by_date')),
                'buyer_shipped_at': _iso(r.get('buyer_shipped_at')),
                'tracking_number': r.get('tracking_number'),
                'item_delivered_back_at': _iso(r.get('item_delivered_back_at')),
                'refund_issued_at': _iso(r.get('refund_issued_at')),
                'closed_at': _iso(r.get('closed_at')),
                'status_current': r['status_current'],
                'final_outcome': r['final_outcome'],
                'internal_bucket': r['internal_bucket'],
                'recommended_fix': r.get('recommended_fix'),
                'classifier_source': r.get('classifier_source'),
                'classifier_confidence': float(r['classifier_confidence']) if r.get('classifier_confidence') else None,
            },
            'events': [{
                'id': str(e['id']),
                'event_type': e['event_type'],
                'event_timestamp': _iso(e.get('event_timestamp')),
                'source_type': e['source_type'],
                'email_subject': e['email_subject'],
                'created_at': _iso(e.get('created_at')),
            } for e in events],
            'matched_unit': matched_unit,
        })
    finally:
        release_conn(conn)


@app.route('/api/returns/stats', methods=['GET'])
def get_return_stats():
    conn = acquire_conn()
    try:
        params = []
        where = ["1=1"]
        if request.args.get('start_date'):
            where.append("opened_at >= %s")
            params.append(request.args['start_date'])
        if request.args.get('end_date'):
            where.append("opened_at <= %s")
            params.append(request.args['end_date'])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT final_outcome, internal_bucket, internal_order_id "
                f"FROM returns WHERE {' AND '.join(where)}",
                params
            )
            returns = [dict(r) for r in cur.fetchall()]

        total_returns = len(returns)
        total_refunded = sum(1 for r in returns if 'refunded' in (r['final_outcome'] or ''))
        total_closed_bns = sum(1 for r in returns if r['final_outcome'] == 'closed_buyer_never_shipped')
        matched_returns = sum(1 for r in returns if r['internal_order_id'])

        by_bucket = {}
        for r in returns:
            b = r['internal_bucket'] or 'Unknown'
            by_bucket[b] = by_bucket.get(b, 0) + 1

        by_outcome = {}
        for r in returns:
            o = r['final_outcome'] or 'Unknown'
            by_outcome[o] = by_outcome.get(o, 0) + 1

        return jsonify({
            'summary': {
                'total_returns': total_returns,
                'total_refunded': total_refunded,
                'total_closed_buyer_never_shipped': total_closed_bns,
                'percent_closed_buyer_never_shipped': round((total_closed_bns / total_returns * 100) if total_returns > 0 else 0, 2),
                'matched_returns': matched_returns,
                'unmatched_returns': total_returns - matched_returns,
            },
            'by_bucket': sorted(
                [{'bucket': b, 'count': c,
                  'percent': round(c / total_returns * 100, 2) if total_returns > 0 else 0}
                 for b, c in by_bucket.items()],
                key=lambda x: x['count'], reverse=True
            ),
            'by_outcome': sorted(
                [{'outcome': o, 'count': c} for o, c in by_outcome.items()],
                key=lambda x: x['count'], reverse=True
            ),
        })
    finally:
        release_conn(conn)


@app.route('/api/returns/by-brand', methods=['GET'])
def get_returns_by_brand():
    conn = acquire_conn()
    try:
        params = []
        where = ["brand IS NOT NULL"]
        if request.args.get('start_date'):
            where.append("opened_at >= %s")
            params.append(request.args['start_date'])
        if request.args.get('end_date'):
            where.append("opened_at <= %s")
            params.append(request.args['end_date'])

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT brand, final_outcome FROM returns WHERE {' AND '.join(where)}",
                params
            )
            returns = [dict(r) for r in cur.fetchall()]

        brand_data = {}
        for r in returns:
            b = r['brand']
            if b not in brand_data:
                brand_data[b] = {'total_returns': 0, 'refunded_after_return': 0,
                                 'refunded_without_return': 0, 'closed_buyer_never_shipped': 0}
            brand_data[b]['total_returns'] += 1
            if r['final_outcome'] == 'refunded_after_return_received':
                brand_data[b]['refunded_after_return'] += 1
            elif r['final_outcome'] == 'refunded_without_return_received':
                brand_data[b]['refunded_without_return'] += 1
            elif r['final_outcome'] == 'closed_buyer_never_shipped':
                brand_data[b]['closed_buyer_never_shipped'] += 1

        brands = sorted([
            {'brand': b, **d,
             'percent_closed_buyer_never_shipped': round(d['closed_buyer_never_shipped'] / d['total_returns'] * 100, 2) if d['total_returns'] > 0 else 0}
            for b, d in brand_data.items()
        ], key=lambda x: x['total_returns'], reverse=True)

        return jsonify({'brands': brands})
    finally:
        release_conn(conn)


@app.route('/api/returns/check-emails', methods=['POST'])
def manual_check_return_emails():
    conn = acquire_conn()
    try:
        gmail = GmailService()
        if not gmail.is_connected():
            return jsonify({'error': 'Gmail not connected'}), 400

        parser = EbayReturnParser()
        return_service = ReturnService(conn)
        email_processing = EmailProcessingService(conn)

        label_name = os.getenv('EBAY_RETURNS_GMAIL_LABEL', 'EBAY_RETURNS_TRACKING')
        emails = gmail.get_emails_from_label(label_name, max_results=100)

        results = {'emails_found': len(emails), 'processed': 0, 'skipped': 0, 'errors': 0, 'details': []}

        for email in emails:
            message_id = email.get('message_id')
            if email_processing.is_email_processed(message_id):
                results['skipped'] += 1
                continue
            try:
                parsed = parser.parse(email)
                if not parsed:
                    email_processing.mark_email_processed(
                        message_id, 'failed',
                        f"Failed to parse. Subject: {email.get('subject', 'N/A')}",
                        email.get('subject'), email.get('from')
                    )
                    results['errors'] += 1
                    continue

                result = return_service.process_return_email(parsed)
                if result.get('success'):
                    email_processing.mark_email_processed(
                        message_id, 'success',
                        f"Return {result.get('return_id')} {result.get('action')}",
                        email.get('subject'), email.get('from')
                    )
                    results['processed'] += 1
                    results['details'].append({'return_id': result.get('return_id'),
                                               'action': result.get('action'),
                                               'event_type': result.get('event_type')})
                else:
                    email_processing.mark_email_processed(
                        message_id, 'failed', result.get('error'),
                        email.get('subject'), email.get('from')
                    )
                    results['errors'] += 1
            except Exception as e:
                logger.error(f"Error processing email: {e}")
                email_processing.mark_email_processed(
                    message_id, 'failed', str(e), email.get('subject'), email.get('from')
                )
                results['errors'] += 1

        message = (f"Checked {results['emails_found']} emails: {results['processed']} processed, "
                   f"{results['skipped']} skipped, {results['errors']} errors")
        return jsonify({'success': True, 'message': message, 'results': results})

    except Exception as e:
        logger.error(f"Error in manual_check_return_emails: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/returns/processing-stats', methods=['GET'])
def get_return_processing_stats():
    conn = acquire_conn()
    try:
        return jsonify(EmailProcessingService(conn).get_processing_stats())
    finally:
        release_conn(conn)


@app.route('/api/returns/processed-emails', methods=['GET'])
def get_processed_emails():
    conn = acquire_conn()
    try:
        limit = request.args.get('limit', 100, type=int)
        status = request.args.get('status')

        params = []
        where = ["1=1"]
        if status:
            where.append("processing_status = %s")
            params.append(status)
        params.append(limit)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT id, email_message_id, email_subject, email_sender, "
                f"received_date, processed_at, processing_status, processing_notes "
                f"FROM email_processing_logs WHERE {' AND '.join(where)} "
                f"ORDER BY processed_at DESC LIMIT %s",
                params
            )
            emails = [dict(r) for r in cur.fetchall()]

        return jsonify({
            'total': len(emails),
            'emails': [{
                'id': str(e['id']),
                'email_message_id': e['email_message_id'],
                'email_subject': e['email_subject'],
                'email_sender': e['email_sender'],
                'received_date': e['received_date'].isoformat() if e['received_date'] else None,
                'processed_at': e['processed_at'].isoformat() if e['processed_at'] else None,
                'processing_status': e['processing_status'],
                'processing_notes': e['processing_notes'],
            } for e in emails]
        })
    finally:
        release_conn(conn)


@app.route('/api/returns/processed-emails/<message_id>', methods=['GET'])
def get_processed_email_details(message_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email_message_id, email_subject, email_sender, "
                "received_date, processed_at, processing_status, processing_notes "
                "FROM email_processing_logs WHERE email_message_id = %s",
                [message_id]
            )
            log = cur.fetchone()
            if not log:
                return jsonify({'error': 'Processed email not found'}), 404
            log = dict(log)

            cur.execute(
                "SELECT id, return_id, event_type, event_timestamp, source_type, "
                "email_subject, raw_payload, parsed_data, created_at "
                "FROM return_events WHERE email_message_id = %s ORDER BY created_at DESC",
                [message_id]
            )
            events = [dict(e) for e in cur.fetchall()]

            return_ids = [str(e['return_id']) for e in events if e['return_id']]
            returns = []
            if return_ids:
                cur.execute(
                    "SELECT id, return_id, order_number, buyer_username, brand, sku, "
                    "status_current, final_outcome, opened_at, closed_at, "
                    "internal_bucket, recommended_fix "
                    "FROM returns WHERE id = ANY(%s::uuid[])",
                    [return_ids]
                )
                returns = [{
                    'id': str(r['id']),
                    'return_id': r['return_id'],
                    'order_number': r['order_number'],
                    'buyer_username': r['buyer_username'],
                    'brand': r['brand'],
                    'sku': r['sku'],
                    'status_current': r['status_current'],
                    'final_outcome': r['final_outcome'],
                    'opened_at': r['opened_at'].isoformat() if r['opened_at'] else None,
                    'closed_at': r['closed_at'].isoformat() if r['closed_at'] else None,
                    'internal_bucket': r['internal_bucket'],
                    'recommended_fix': r['recommended_fix'],
                } for r in cur.fetchall()]

        return jsonify({
            'email': {
                'id': str(log['id']),
                'email_message_id': log['email_message_id'],
                'email_subject': log['email_subject'],
                'email_sender': log['email_sender'],
                'received_date': log['received_date'].isoformat() if log['received_date'] else None,
                'processed_at': log['processed_at'].isoformat() if log['processed_at'] else None,
                'processing_status': log['processing_status'],
                'processing_notes': log['processing_notes'],
            },
            'events': [{
                'id': str(e['id']),
                'return_id': str(e['return_id']) if e['return_id'] else None,
                'event_type': e['event_type'],
                'event_timestamp': e['event_timestamp'].isoformat() if e['event_timestamp'] else None,
                'source_type': e['source_type'],
                'email_subject': e['email_subject'],
                'raw_payload': e['raw_payload'],
                'parsed_data': e['parsed_data'],
                'created_at': e['created_at'].isoformat() if e['created_at'] else None,
            } for e in events],
            'returns': returns,
        })
    finally:
        release_conn(conn)


@app.route('/api/returns/<return_id>/match', methods=['POST'])
def manual_match_return(return_id):
    conn = acquire_conn()
    try:
        data = request.get_json()
        unit_id = data.get('unit_id')
        if not unit_id:
            return jsonify({'error': 'unit_id required'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM returns WHERE id = %s LIMIT 1", [return_id])
            if not cur.fetchone():
                return jsonify({'error': 'Return not found'}), 404

            cur.execute(
                "SELECT u.id, u.unit_code, p.brand FROM units u "
                "LEFT JOIN products p ON p.id = u.product_id WHERE u.id = %s",
                [unit_id]
            )
            unit = cur.fetchone()
            if not unit:
                return jsonify({'error': 'Unit not found'}), 404
            unit = dict(unit)

            cur.execute(
                "UPDATE returns SET internal_order_id = %s, sku = %s, brand = %s, "
                "updated_at = now() WHERE id = %s RETURNING return_id",
                [unit_id, unit['unit_code'], unit['brand'], return_id]
            )
            row = cur.fetchone()

        conn.commit()
        logger.info(f"Manually matched return {row['return_id']} to unit {unit['unit_code']}")
        return jsonify({'success': True, 'message': 'Return matched to unit',
                        'return_id': row['return_id'], 'unit_code': unit['unit_code'],
                        'brand': unit['brand']})
    except Exception as e:
        conn.rollback()
        logger.error(f"Error matching return: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/returns/<return_id>/unmatch', methods=['POST'])
def unmatch_return(return_id):
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM returns WHERE id = %s LIMIT 1", [return_id])
            if not cur.fetchone():
                return jsonify({'error': 'Return not found'}), 404
            cur.execute(
                "UPDATE returns SET internal_order_id = NULL, updated_at = now() WHERE id = %s",
                [return_id]
            )
        conn.commit()
        return jsonify({'success': True, 'message': 'Return unmatched'})
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        release_conn(conn)


@app.route('/api/returns/unmatched', methods=['GET'])
def get_unmatched_returns():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, return_id, item_title, brand, buyer_username, "
                "opened_at, request_amount, status_current "
                "FROM returns WHERE internal_order_id IS NULL ORDER BY created_at DESC"
            )
            unmatched = [dict(r) for r in cur.fetchall()]
        return jsonify({
            'unmatched_returns': [{
                'id': str(r['id']),
                'return_id': r['return_id'],
                'item_title': r['item_title'],
                'brand': r['brand'],
                'buyer_username': r['buyer_username'],
                'opened_at': r['opened_at'].isoformat() if r['opened_at'] else None,
                'request_amount': float(r['request_amount']) if r['request_amount'] else None,
                'status_current': r['status_current'],
            } for r in unmatched],
            'total': len(unmatched),
        })
    finally:
        release_conn(conn)


@app.route('/api/scheduler/toggle-return-monitoring', methods=['POST'])
def toggle_return_monitoring():
    try:
        status = sync_scheduler.get_return_monitoring_status()
        if status.get('running'):
            sync_scheduler.stop_return_monitoring()
            status = sync_scheduler.get_return_monitoring_status()
            return jsonify({'success': True, 'status': 'stopped', 'message': 'Return monitoring stopped',
                            'interval_hours': status.get('interval_hours'), 'next_run': status.get('next_run')})
        sync_scheduler.start_return_monitoring()
        status = sync_scheduler.get_return_monitoring_status()
        return jsonify({'success': True, 'status': 'running', 'message': 'Return monitoring started',
                        'interval_hours': status.get('interval_hours'), 'next_run': status.get('next_run')})
    except Exception as e:
        logger.error(f"Error toggling return monitoring: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/scheduler/return-monitoring-status', methods=['GET'])
def get_return_monitoring_status():
    try:
        status = sync_scheduler.get_return_monitoring_status()
        return jsonify({'success': True, 'status': 'running' if status.get('running') else 'stopped', **status})
    except Exception as e:
        logger.error(f"Error getting return monitoring status: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/returns-dashboard')
def returns_dashboard():
    return send_from_directory(STATIC_DIR, 'returns_dashboard.html')


# ============================================
# INITIALIZE & RUN
# ============================================

if __name__ == '__main__':
    logger.info("Initializing database...")
    init_db()

    auto_sync_enabled = os.getenv('AUTO_SYNC_ENABLED', 'false').lower() == 'true'
    if auto_sync_enabled and ebay_api.is_configured():
        sync_scheduler.scheduler.start()

        from datetime import datetime, timedelta
        start_time = datetime.now() + timedelta(seconds=5)

        sync_scheduler.scheduler.add_job(
            func=run_scheduled_sync,
            trigger='interval',
            minutes=int(os.getenv('SYNC_INTERVAL_MINUTES', '60')),
            id='ebay_sync_job',
            name='eBay Sync Job',
            replace_existing=True,
            next_run_time=start_time,
            max_instances=1,
        )
        logger.info("✅ eBay sync scheduler configured")

        if os.getenv('AUTO_DELIST_ENABLED', 'false').lower() == 'true':
            sync_scheduler.start_email_monitoring()
            logger.info("✅ Email monitoring enabled")

        if os.getenv('AUTO_CROSSLIST_ENABLED') == 'true':
            sync_scheduler.start_crosslist_monitoring()
            logger.info("✅ Crosslisting monitoring enabled")
    else:
        logger.info("Automated sync disabled")

    port = int(9500)
    host = os.getenv('API_HOST', '0.0.0.0')
    logger.info(f"Starting API server on {host}:{port}")
    app.run(host=host, port=port, debug=False)
