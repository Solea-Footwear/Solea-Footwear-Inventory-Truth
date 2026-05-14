"""
Flask API Server
Main API endpoints for inventory management system
"""
import os
import logging
import psycopg2.extras
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from dotenv import load_dotenv
from src.jobs.scheduler import sync_scheduler
from src.services.template_service import TemplateService
from src.services.audit_service import AuditService
from src.services.bulk_import_service import BulkImportService

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

app = Flask(__name__)
CORS(app)
from src.backend.config import SECRET_KEY
app.config['SECRET_KEY'] = SECRET_KEY

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
                'unresolved_alerts': unresolved_alerts
            },
            'recent_syncs': [{
                'id': str(s['id']),
                'sync_type': s['sync_type'],
                'status': s['status'],
                'records_processed': s['records_processed'],
                'started_at': s['started_at'].isoformat() if s['started_at'] else None,
                'completed_at': s['completed_at'].isoformat() if s['completed_at'] else None
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
        brand = request.args.get('brand')
        size = request.args.get('size')
        category_id = request.args.get('category_id')

        sql = """
            SELECT p.id, p.brand, p.model, p.colorway, p.size, p.gender,
                   p.default_price_ebay, p.created_at,
                   c.display_name AS category_name,
                   cg.display_name AS condition_name
            FROM products p
            LEFT JOIN categories c ON c.id = p.category_id
            LEFT JOIN condition_grades cg ON cg.id = p.condition_grade_id
            WHERE 1=1
        """
        params = []
        if brand:
            sql += " AND p.brand ILIKE %s"
            params.append(f'%{brand}%')
        if size:
            sql += " AND p.size = %s"
            params.append(size)
        if category_id:
            sql += " AND p.category_id = %s"
            params.append(category_id)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            products = cur.fetchall()

        return jsonify({
            'products': [{
                'id': str(p['id']),
                'brand': p['brand'],
                'model': p['model'],
                'colorway': p['colorway'],
                'size': p['size'],
                'gender': p['gender'],
                'category': p['category_name'],
                'condition_grade': p['condition_name'],
                'default_price_ebay': float(p['default_price_ebay']) if p['default_price_ebay'] else None,
                'created_at': p['created_at'].isoformat() if p['created_at'] else None
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
                INSERT INTO products (id, brand, model, colorway, size, gender,
                    category_id, condition_grade_id, default_price_ebay, sku_prefix, notes)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                [data['brand'], data['model'], data.get('colorway'), data['size'],
                 data.get('gender'), data.get('category_id'), data.get('condition_grade_id'),
                 data.get('default_price_ebay'), data.get('sku_prefix'), data.get('notes')]
            )
            product = dict(cur.fetchone())
        conn.commit()
        logger.info(f"Created product: {product['brand']} {product['model']}")
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
            cur.execute("SELECT * FROM products WHERE id = %s", [product_id])
            product = cur.fetchone()
        if not product:
            return jsonify({'error': 'Product not found'}), 404

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.unit_code, u.status, u.cost_basis, u.created_at,
                       l.code AS location_code,
                       cg.display_name AS condition_name
                FROM units u
                LEFT JOIN locations l ON l.id = u.location_id
                LEFT JOIN condition_grades cg ON cg.id = u.condition_grade_id
                WHERE u.product_id = %s
                """,
                [product_id]
            )
            units = cur.fetchall()

        return jsonify({
            'product': {
                'id': str(product['id']), 'brand': product['brand'],
                'model': product['model'], 'colorway': product['colorway'],
                'size': product['size'], 'gender': product['gender'],
                'default_price_ebay': float(product['default_price_ebay']) if product['default_price_ebay'] else None,
                'notes': product['notes']
            },
            'units': [{
                'id': str(u['id']), 'unit_code': u['unit_code'],
                'status': u['status'], 'location_code': u['location_code'],
                'condition': u['condition_name'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'created_at': u['created_at'].isoformat() if u['created_at'] else None
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
        status = request.args.get('status')
        product_id = request.args.get('product_id')
        location_id = request.args.get('location_id')
        unit_code = request.args.get('unit_code')

        sql = """
            SELECT u.id, u.unit_code, u.status, u.cost_basis, u.created_at,
                   p.brand, p.model, p.size,
                   l.code AS location_code,
                   cg.display_name AS condition_name
            FROM units u
            LEFT JOIN products p ON p.id = u.product_id
            LEFT JOIN locations l ON l.id = u.location_id
            LEFT JOIN condition_grades cg ON cg.id = u.condition_grade_id
            WHERE 1=1
        """
        params = []
        if status:
            sql += " AND u.status = %s"
            params.append(status)
        if product_id:
            sql += " AND u.product_id = %s"
            params.append(product_id)
        if location_id:
            sql += " AND u.location_id = %s"
            params.append(location_id)
        if unit_code:
            sql += " AND u.unit_code ILIKE %s"
            params.append(f'%{unit_code}%')

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            units = cur.fetchall()

        return jsonify({
            'units': [{
                'id': str(u['id']), 'unit_code': u['unit_code'], 'status': u['status'],
                'product': {'brand': u['brand'], 'model': u['model'], 'size': u['size']} if u['brand'] else None,
                'location_code': u['location_code'],
                'condition': u['condition_name'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'created_at': u['created_at'].isoformat() if u['created_at'] else None
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

        with conn.cursor() as cur:
            cur.execute("SELECT id FROM units WHERE unit_code = %s", [data['unit_code']])
            if cur.fetchone():
                return jsonify({'error': 'Unit code already exists'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO units (id, unit_code, product_id, location_id, condition_grade_id,
                    status, cost_basis, notes)
                VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s)
                RETURNING *
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

@app.route('/api/units/<unit_id>', methods=['PUT'])
def update_unit(unit_id):
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM units WHERE id = %s", [unit_id])
            if not cur.fetchone():
                return jsonify({'error': 'Unit not found'}), 404

        data = request.json
        updates = {}
        for field in ('location_id', 'status', 'condition_grade_id', 'cost_basis', 'notes'):
            if field in data:
                updates[field] = data[field]

        if updates:
            set_clause = ', '.join(f"{k} = %s" for k in updates)
            params = list(updates.values()) + [unit_id]
            with conn.cursor() as cur:
                cur.execute(f"UPDATE units SET {set_clause} WHERE id = %s", params)
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
                       l.code AS location_code, l.description AS location_description,
                       cg.display_name AS condition_name
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

        # Get listing info
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT li.channel_listing_id, li.title, li.current_price,
                       li.status, li.listing_url,
                       ch.display_name AS channel_display_name
                FROM listing_units lu
                JOIN listings li ON li.id = lu.listing_id
                JOIN channels ch ON ch.id = li.channel_id
                WHERE lu.unit_id = %s
                LIMIT 1
                """,
                [unit['id']]
            )
            listing_row = cur.fetchone()

        listing_info = None
        if listing_row:
            listing_info = {
                'channel': listing_row['channel_display_name'],
                'listing_id': listing_row['channel_listing_id'],
                'title': listing_row['title'],
                'price': float(listing_row['current_price']) if listing_row['current_price'] else None,
                'status': listing_row['status'],
                'url': listing_row['listing_url']
            }

        return jsonify({
            'unit': {
                'id': str(unit['id']), 'unit_code': unit['unit_code'], 'status': unit['status'],
                'product': {'brand': unit['brand'], 'model': unit['model'],
                            'colorway': unit['colorway'], 'size': unit['size']},
                'location': {'code': unit['location_code'], 'description': unit['location_description']}
                            if unit['location_code'] else None,
                'condition': unit['condition_name'],
                'cost_basis': float(unit['cost_basis']) if unit['cost_basis'] else None,
                'listing': listing_info,
                'created_at': unit['created_at'].isoformat() if unit['created_at'] else None
            }
        })
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
            locations = cur.fetchall()
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

        with conn.cursor() as cur:
            cur.execute("SELECT id FROM locations WHERE code = %s", [data['code']])
            if cur.fetchone():
                return jsonify({'error': 'Location code already exists'}), 400

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO locations (id, code, description, is_active) "
                "VALUES (gen_random_uuid(), %s, %s, %s) RETURNING *",
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
            categories = cur.fetchall()
        return jsonify({
            'categories': [{'id': str(c['id']), 'internal_name': c['internal_name'],
                            'display_name': c['display_name'], 'ebay_category_id': c['ebay_category_id']}
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
            grades = cur.fetchall()
        return jsonify({
            'condition_grades': [{'id': str(g['id']), 'internal_code': g['internal_code'],
                                  'display_name': g['display_name'], 'ebay_condition_id': g['ebay_condition_id'],
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
            logs = cur.fetchall()
        return jsonify({
            'logs': [{
                'id': str(log['id']), 'sync_type': log['sync_type'], 'status': log['status'],
                'records_processed': log['records_processed'],
                'records_updated': log['records_updated'],
                'records_created': log['records_created'],
                'errors': log['errors'],
                'started_at': log['started_at'].isoformat() if log['started_at'] else None,
                'completed_at': log['completed_at'].isoformat() if log['completed_at'] else None
            } for log in logs]
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
        resolved = request.args.get('resolved')
        sql = "SELECT id, alert_type, severity, title, message, is_resolved, created_at FROM alerts WHERE 1=1"
        params = []
        if resolved:
            sql += " AND is_resolved = %s"
            params.append(resolved.lower() == 'true')
        sql += " ORDER BY created_at DESC LIMIT 50"

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            alerts = cur.fetchall()

        return jsonify({
            'alerts': [{
                'id': str(a['id']), 'alert_type': a['alert_type'], 'severity': a['severity'],
                'title': a['title'], 'message': a['message'], 'is_resolved': a['is_resolved'],
                'created_at': a['created_at'].isoformat() if a['created_at'] else None
            } for a in alerts]
        })
    finally:
        release_conn(conn)

@app.route('/api/alerts/<alert_id>/resolve', methods=['POST'])
def resolve_alert(alert_id):
    conn = acquire_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM alerts WHERE id = %s", [alert_id])
            if not cur.fetchone():
                return jsonify({'error': 'Alert not found'}), 404
            cur.execute(
                "UPDATE alerts SET is_resolved = TRUE, resolved_at = %s WHERE id = %s",
                [datetime.utcnow(), alert_id]
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
        status = request.args.get('status')
        sql = """
            SELECT li.id, li.channel_listing_id, li.title, li.current_price,
                   li.status, li.listing_url, li.created_at,
                   ch.display_name AS channel_name,
                   p.brand, p.model, p.size
            FROM listings li
            LEFT JOIN channels ch ON ch.id = li.channel_id
            LEFT JOIN products p ON p.id = li.product_id
            WHERE 1=1
        """
        params = []
        if status:
            sql += " AND li.status = %s"
            params.append(status)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            listings = cur.fetchall()

        return jsonify({
            'listings': [{
                'id': str(l['id']), 'channel_listing_id': l['channel_listing_id'],
                'title': l['title'],
                'current_price': float(l['current_price']) if l['current_price'] else None,
                'status': l['status'], 'listing_url': l['listing_url'],
                'channel': l['channel_name'],
                'product': {'brand': l['brand'], 'model': l['model'], 'size': l['size']} if l['brand'] else None,
                'created_at': l['created_at'].isoformat() if l['created_at'] else None
            } for l in listings]
        })
    finally:
        release_conn(conn)


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
        status = sync_scheduler.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/start', methods=['POST'])
def start_scheduler():
    try:
        if ebay_api.is_configured():
            success = sync_scheduler.start(run_scheduled_sync)
            if success:
                return jsonify({'message': 'Scheduler started successfully', 'status': sync_scheduler.get_status()})
            else:
                return jsonify({'error': 'Failed to start scheduler'}), 500
        else:
            return jsonify({'error': 'eBay API not configured'}), 400
    except Exception as e:
        logger.error(f"Error starting scheduler: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/stop', methods=['POST'])
def stop_scheduler():
    try:
        success = sync_scheduler.stop()
        if success:
            return jsonify({'message': 'Scheduler stopped successfully', 'status': sync_scheduler.get_status()})
        else:
            return jsonify({'error': 'Failed to stop scheduler'}), 500
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/trigger', methods=['POST'])
def trigger_sync_now():
    try:
        success = sync_scheduler.trigger_now()
        if success:
            return jsonify({'message': 'Sync triggered successfully'})
        else:
            return jsonify({'error': 'Failed to trigger sync'}), 500
    except Exception as e:
        logger.error(f"Error triggering sync: {e}")
        return jsonify({'error': str(e)}), 500

# ============================================
# SOLD ITEMS ENDPOINTS
# ============================================

@app.route('/api/sold-items', methods=['GET'])
def get_sold_items():
    conn = acquire_conn()
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        platform = request.args.get('platform')

        sql = """
            SELECT u.id, u.unit_code, u.sold_at, u.sold_price, u.sold_platform,
                   u.cost_basis,
                   p.brand, p.model, p.size, p.colorway,
                   l.code AS location_code
            FROM units u
            LEFT JOIN products p ON p.id = u.product_id
            LEFT JOIN locations l ON l.id = u.location_id
            WHERE u.status = 'sold'
        """
        params = []
        if start_date:
            sql += " AND u.sold_at >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND u.sold_at <= %s"
            params.append(end_date)
        if platform:
            sql += " AND u.sold_platform = %s"
            params.append(platform)
        sql += " ORDER BY u.sold_at DESC"

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            units = cur.fetchall()

        return jsonify({
            'sold_items': [{
                'id': str(u['id']), 'unit_code': u['unit_code'],
                'product': {'brand': u['brand'], 'model': u['model'],
                            'size': u['size'], 'colorway': u['colorway']} if u['brand'] else None,
                'sold_at': u['sold_at'].isoformat() if u['sold_at'] else None,
                'sold_price': float(u['sold_price']) if u['sold_price'] else None,
                'sold_platform': u['sold_platform'],
                'cost_basis': float(u['cost_basis']) if u['cost_basis'] else None,
                'profit': float(u['sold_price'] - u['cost_basis'])
                          if (u['sold_price'] and u['cost_basis']) else None,
                'location_code': u['location_code']
            } for u in units]
        })
    finally:
        release_conn(conn)

@app.route('/api/sales/stats', methods=['GET'])
def get_sales_stats():
    conn = acquire_conn()
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        sql = "SELECT sold_price, cost_basis, sold_platform FROM units WHERE status = 'sold' AND sold_at IS NOT NULL"
        params = []
        if start_date:
            sql += " AND sold_at >= %s"
            params.append(start_date)
        if end_date:
            sql += " AND sold_at <= %s"
            params.append(end_date)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        total_sales = len(rows)
        total_revenue = sum(float(r['sold_price']) for r in rows if r['sold_price'])
        total_cost = sum(float(r['cost_basis']) for r in rows if r['cost_basis'])
        total_profit = total_revenue - total_cost
        avg_sale_price = total_revenue / total_sales if total_sales else 0
        avg_profit = total_profit / total_sales if total_sales else 0
        profit_margin = (total_profit / total_revenue * 100) if total_revenue else 0

        platform_breakdown = {}
        for r in rows:
            p = r['sold_platform'] or 'unknown'
            platform_breakdown.setdefault(p, {'count': 0, 'revenue': 0})
            platform_breakdown[p]['count'] += 1
            platform_breakdown[p]['revenue'] += float(r['sold_price']) if r['sold_price'] else 0

        return jsonify({
            'stats': {
                'total_sales': total_sales,
                'total_revenue': float(total_revenue),
                'total_profit': float(total_profit),
                'avg_sale_price': float(avg_sale_price),
                'avg_profit': float(avg_profit),
                'profit_margin_percent': float(profit_margin)
            },
            'platform_breakdown': platform_breakdown
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
        else:
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
            recent_sales = cur.fetchall()

        with conn.cursor() as cur:
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
                'platform': u['sold_platform']
            } for u in recent_sales],
            'today_sales_count': today_sales_count
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
        validated_only = request.args.get('validated') == 'true'
        product_id = request.args.get('product_id')

        sql = """
            SELECT t.id, t.product_id, t.title, t.base_price, t.photos, t.pricing,
                   t.category_mappings, t.is_validated, t.validation_errors,
                   t.template_version, t.last_synced_at,
                   p.brand, p.model, p.size
            FROM listing_templates t
            LEFT JOIN products p ON p.id = t.product_id
            WHERE 1=1
        """
        params = []
        if validated_only:
            sql += " AND t.is_validated = TRUE"
        if product_id:
            sql += " AND t.product_id = %s"
            params.append(product_id)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            templates = cur.fetchall()

        return jsonify({
            'templates': [{
                'id': str(t['id']), 'product_id': str(t['product_id']),
                'product': {'brand': t['brand'], 'model': t['model'], 'size': t['size']} if t['brand'] else None,
                'title': t['title'],
                'base_price': float(t['base_price']) if t['base_price'] else None,
                'photos_count': len(t['photos']) if t['photos'] else 0,
                'pricing': t['pricing'], 'category_mappings': t['category_mappings'],
                'is_validated': t['is_validated'], 'validation_errors': t['validation_errors'],
                'template_version': t['template_version'],
                'last_synced_at': t['last_synced_at'].isoformat() if t['last_synced_at'] else None
            } for t in templates]
        })
    finally:
        release_conn(conn)

@app.route('/api/templates/<template_id>', methods=['GET'])
def get_template(template_id):
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM listing_templates WHERE id = %s", [template_id])
            template = cur.fetchone()
        if not template:
            return jsonify({'error': 'Template not found'}), 404
        return jsonify({
            'id': str(template['id']), 'product_id': str(template['product_id']),
            'title': template['title'], 'description': template['description'],
            'photos': template['photos'], 'photo_metadata': template['photo_metadata'],
            'item_specifics': template['item_specifics'],
            'base_price': float(template['base_price']) if template['base_price'] else None,
            'pricing': template['pricing'], 'category_mappings': template['category_mappings'],
            'seo_keywords': template['seo_keywords'],
            'is_validated': template['is_validated'], 'validation_errors': template['validation_errors'],
            'template_version': template['template_version'],
            'last_synced_at': template['last_synced_at'].isoformat() if template['last_synced_at'] else None
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
            cur.execute("SELECT * FROM listing_templates WHERE id = %s", [template_id])
            template = cur.fetchone()
        if not template:
            return jsonify({'error': 'Template not found'}), 404

        template_service = TemplateService(conn)
        result = template_service.validate_template(template)

        with conn.cursor() as cur:
            cur.execute(
                "UPDATE listing_templates SET is_validated = %s, validation_errors = %s WHERE id = %s",
                [result['valid'], result.get('errors'), template_id]
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
            cur.execute("SELECT COUNT(*) FROM listing_templates")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM listing_templates WHERE is_validated = TRUE")
            validated = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM listing_templates WHERE is_validated = TRUE AND template_version >= 2")
            ready = cur.fetchone()[0]
        return jsonify({
            'stats': {
                'total': total, 'validated': validated, 'invalid': total - validated,
                'ready_for_crosslisting': ready,
                'validation_rate': round((validated / total * 100) if total > 0 else 0, 1)
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
        summary = audit_service.get_audit_summary()
        return jsonify(summary)
    finally:
        release_conn(conn)

@app.route('/api/audit/sku-issues', methods=['GET'])
def get_sku_issues():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        return jsonify(audit_service.audit_sku_issues())
    finally:
        release_conn(conn)

@app.route('/api/audit/inventory-mismatches', methods=['GET'])
def get_inventory_mismatches():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        return jsonify(audit_service.audit_inventory_mismatches())
    finally:
        release_conn(conn)

@app.route('/api/audit/template-issues', methods=['GET'])
def get_template_issues():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        return jsonify(audit_service.audit_template_issues())
    finally:
        release_conn(conn)

@app.route('/api/audit/pricing-issues', methods=['GET'])
def get_pricing_issues():
    conn = acquire_conn()
    try:
        audit_service = AuditService(conn)
        return jsonify(audit_service.audit_pricing_issues())
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

        now = datetime.utcnow()
        updated = 0
        with conn.cursor() as cur:
            for alert_id in alert_ids:
                cur.execute(
                    "UPDATE alerts SET is_resolved = TRUE, resolved_at = %s WHERE id = %s",
                    [now, alert_id]
                )
                updated += cur.rowcount
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
                    'duplicate_skus': sku_issues['duplicate_skus'][:10]
                },
                'inventory': {
                    'units_without_listings': inventory_issues['units_without_listings'][:10],
                    'listings_without_units': inventory_issues['listings_without_units'][:10]
                },
                'templates': {'invalid_templates': template_issues['invalid_templates'][:10]}
            },
            'issue_counts': {
                'sku_issues': sku_issues['total'],
                'inventory_mismatches': inventory_issues['total'],
                'template_issues': template_issues['total']
            }
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
        bulk_import = BulkImportService(conn)
        results = bulk_import.parse_products_csv(csv_content)
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
        bulk_import = BulkImportService(conn)
        results = bulk_import.import_products(valid_rows)
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
        bulk_import = BulkImportService(conn)
        results = bulk_import.parse_units_csv(csv_content)
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
        bulk_import = BulkImportService(conn)
        results = bulk_import.import_units(valid_rows)
        return jsonify({'message': 'Import completed', 'results': results})
    finally:
        release_conn(conn)

@app.route('/api/import/templates/products', methods=['GET'])
def download_products_template():
    conn = acquire_conn()
    try:
        bulk_import = BulkImportService(conn)
        template = bulk_import.generate_products_template()
        return Response(template, mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment;filename=products_template.csv'})
    finally:
        release_conn(conn)

@app.route('/api/import/templates/units', methods=['GET'])
def download_units_template():
    conn = acquire_conn()
    try:
        bulk_import = BulkImportService(conn)
        template = bulk_import.generate_units_template()
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

        since_minutes = 43200
        logger.info(f"Checking emails for delisting since last {since_minutes} minutes")
        emails = gmail.get_sale_emails(since_minutes=since_minutes)

        results = {
            'emails_found': len(emails), 'emails_processed': 0,
            'total_items': 0, 'processed': [], 'errors': []
        }

        for email in emails:
            try:
                sale_items = parser.parse_sale_email(email)
                if not sale_items:
                    results['errors'].append({'email': email.get('subject', 'Unknown')[:50], 'error': 'Failed to parse email'})
                    continue

                email_items_processed = 0
                results['total_items'] += len(sale_items)

                for i, item in enumerate(sale_items, 1):
                    try:
                        result = delist_service.process_sale(item)
                        if result.get('success'):
                            email_items_processed += 1
                            sku = item.get('sku') or (item.get('skus', [None])[0] if item.get('skus') else None)
                            results['processed'].append({
                                'platform': item.get('platform'), 'sku': sku,
                                'unit_code': result.get('unit_code'),
                                'delisted_from': [d.get('platform') for d in result.get('delisted', [])],
                                'delisted_count': len(result.get('delisted', []))
                            })
                        else:
                            results['errors'].append({
                                'email': email.get('subject', 'Unknown')[:50],
                                'item': i, 'sku': item.get('sku'), 'errors': result.get('errors')
                            })
                    except Exception as e:
                        results['errors'].append({
                            'email': email.get('subject', 'Unknown')[:50],
                            'item': i, 'sku': item.get('sku'), 'error': str(e)
                        })

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
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, created_at, status, message, details FROM sync_logs "
                "WHERE log_type = 'delist' ORDER BY created_at DESC LIMIT 50"
            )
            logs = cur.fetchall()
        return jsonify({
            'history': [{
                'id': str(log['id']),
                'timestamp': log['created_at'].isoformat() if log['created_at'] else None,
                'status': log['status'], 'message': log['message'], 'details': log['details']
            } for log in logs]
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
                "SELECT sold_platform, COUNT(*) AS cnt FROM units "
                "WHERE status = 'sold' AND sold_at >= %s GROUP BY sold_platform",
                [thirty_days_ago]
            )
            sold_rows = cur.fetchall()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM listings WHERE status = 'ended' AND ended_at >= %s",
                [thirty_days_ago]
            )
            delisted_count = cur.fetchone()[0]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT unit_code, sold_platform, sold_price, sold_at FROM units "
                "WHERE status = 'sold' ORDER BY sold_at DESC LIMIT 10"
            )
            recent_sales = cur.fetchall()

        sold_by_platform = {r['sold_platform']: r['cnt'] for r in sold_rows}
        return jsonify({
            'stats': {
                'sold_by_platform': sold_by_platform,
                'total_sold_30_days': sum(sold_by_platform.values()),
                'total_delisted_30_days': delisted_count
            },
            'recent_sales': [{
                'unit_code': s['unit_code'], 'sold_platform': s['sold_platform'],
                'sold_price': float(s['sold_price']) if s['sold_price'] else None,
                'sold_at': s['sold_at'].isoformat() if s['sold_at'] else None
            } for s in recent_sales]
        })
    finally:
        release_conn(conn)

@app.route('/api/delist/gmail-status', methods=['GET'])
def get_gmail_status():
    try:
        gmail = GmailService()
        return jsonify(gmail.get_test_connection())
    except Exception as e:
        return jsonify({'connected': False, 'error': str(e)}), 500

@app.route('/api/delist/test-parse', methods=['POST'])
def test_email_parsing():
    try:
        data = request.get_json()
        if not data or 'email_data' not in data:
            return jsonify({'error': 'email_data required'}), 400
        parser = EmailParserService()
        parsed = parser.parse_sale_email(data['email_data'])
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

        url = 'https://poshmark.com/login' if platform == 'poshmark' else 'https://www.mercari.com/login'
        driver.get(url)
        open_browsers[browser_key] = driver

        return jsonify({
            'success': True,
            'message': f'{platform.capitalize()} {purpose} profile opened',
            'instructions': f'Log in to {platform.capitalize()}, check "Remember Me", then close.'
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
        'open_browsers': list(open_browsers.keys())
    })

# ============================================
# CROSS-LISTING ENDPOINTS
# ============================================

from src.services.crosslisting.crosslist_service import CrosslistService

@app.route('/api/crosslist/unit/<unit_id>', methods=['POST'])
def crosslist_unit(unit_id):
    conn = acquire_conn()
    try:
        crosslist_service = CrosslistService(conn)
        result = crosslist_service.check_and_crosslist(unit_id)
        status_code = 207 if result.get('errors') else 200
        return jsonify({'message': 'Cross-listing completed', 'result': result}), status_code
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
        crosslist_service = CrosslistService(conn)
        results = crosslist_service.bulk_crosslist(unit_ids)
        return jsonify({'message': f'Processed {results["processed"]} units', 'results': results})
    finally:
        release_conn(conn)

@app.route('/api/crosslist/auto-check', methods=['POST'])
def auto_check_crosslisting():
    conn = acquire_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM units WHERE status = 'listed'")
            unit_ids = [str(r['id']) for r in cur.fetchall()]

        if not unit_ids:
            return jsonify({'message': 'No listed units found', 'results': {'total': 0}})

        crosslist_service = CrosslistService(conn)
        results = crosslist_service.bulk_crosslist(unit_ids)
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

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT li.id, li.channel_listing_id, li.status, li.current_price,
                       ch.name AS channel_name
                FROM listing_units lu
                JOIN listings li ON li.id = lu.listing_id
                JOIN channels ch ON ch.id = li.channel_id
                WHERE lu.unit_id = %s
                """,
                [unit_id]
            )
            listings = cur.fetchall()

        platforms = {}
        for listing in listings:
            platform = listing['channel_name'].lower()
            platforms[platform] = {
                'listed': True, 'listing_id': str(listing['id']),
                'channel_listing_id': listing['channel_listing_id'],
                'status': listing['status'],
                'price': float(listing['current_price']) if listing['current_price'] else None
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
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM units WHERE status = 'listed'")
            total_listed = cur.fetchone()[0]

        platform_counts = {}
        for platform in ['ebay', 'poshmark', 'mercari']:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM listings li
                    JOIN channels ch ON ch.id = li.channel_id
                    WHERE LOWER(ch.name) = %s AND li.status = 'active'
                    """,
                    [platform]
                )
                platform_counts[platform] = cur.fetchone()[0]

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM units WHERE status = 'listed'")
            listed_ids = [r['id'] for r in cur.fetchall()]

        fully_crosslisted = 0
        with conn.cursor() as cur:
            for uid in listed_ids:
                cur.execute(
                    "SELECT COUNT(*) FROM listing_units lu JOIN listings li ON li.id = lu.listing_id "
                    "WHERE lu.unit_id = %s AND li.status = 'active'",
                    [uid]
                )
                if cur.fetchone()[0] >= 3:
                    fully_crosslisted += 1

        return jsonify({
            'stats': {
                'total_listed_units': total_listed,
                'fully_crosslisted': fully_crosslisted,
                'platform_counts': platform_counts,
                'needs_crosslisting': total_listed - fully_crosslisted
            }
        })
    finally:
        release_conn(conn)

# ============================================
# INITIALIZE & RUN
# ============================================

if __name__ == '__main__':
    logger.info("Initializing database...")
    init_db()
    port = int(os.getenv('API_PORT', 5000))
    host = os.getenv('API_HOST', '0.0.0.0')
    logger.info(f"Starting API server on {host}:{port}")
    app.run(host=host, port=port, debug=False)
