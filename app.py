"""
Flask API Server
Main API endpoints for inventory management system
"""
import os
import logging
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from dotenv import load_dotenv
from sqlalchemy import func, desc
from sqlalchemy.orm import joinedload
from scheduler import sync_scheduler
from template_service import TemplateService
from audit_service import AuditService
from bulk_import_service import BulkImportService
from flask import request, Response


# from delisitng
from delisting.gmail_service import GmailService
from delisting.email_parser_service import EmailParserService
from delisting.delist_service import DelistService


# Load environment variables
load_dotenv()

# Import database models and services
from database import (
    get_db, init_db, Product, Unit, Location, Category, 
    ConditionGrade, Listing, Channel, Alert, SyncLog,
    ListingTemplate
)
from ebay_api import ebay_api
from sync_service import SyncService

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)
CORS(app)
app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'dev-secret-key')

# ============================================
# HEALTH & STATUS ENDPOINTS
# ============================================

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'ebay_configured': ebay_api.is_configured()
    })

@app.route('/api/dashboard', methods=['GET'])
def get_dashboard():
    """Get dashboard overview statistics"""
    db = next(get_db())
    
    try:
        # Count units by status
        total_units = db.query(Unit).count()
        ready_to_list = db.query(Unit).filter(Unit.status == 'ready_to_list').count()
        listed = db.query(Unit).filter(Unit.status == 'listed').count()
        sold = db.query(Unit).filter(Unit.status == 'sold').count()
        shipped = db.query(Unit).filter(Unit.status == 'shipped').count()
        
        # Count products
        total_products = db.query(Product).count()
        
        # Count active listings
        active_listings = db.query(Listing).filter(Listing.status == 'active').count()
        
        # Count unresolved alerts
        unresolved_alerts = db.query(Alert).filter(Alert.is_resolved == False).count()
        
        # Recent sync logs
        recent_syncs = db.query(SyncLog).order_by(desc(SyncLog.started_at)).limit(5).all()
        
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
                'id': str(sync.id),
                'sync_type': sync.sync_type,
                'status': sync.status,
                'records_processed': sync.records_processed,
                'started_at': sync.started_at.isoformat() if sync.started_at else None,
                'completed_at': sync.completed_at.isoformat() if sync.completed_at else None
            } for sync in recent_syncs]
        })
    finally:
        db.close()

# ============================================
# PRODUCT ENDPOINTS
# ============================================

@app.route('/api/products', methods=['GET'])
def get_products():
    """Get all products with optional filtering"""
    db = next(get_db())
    
    try:
        query = db.query(Product).options(
            joinedload(Product.category),
            joinedload(Product.condition_grade)
        )
        
        # Filters
        brand = request.args.get('brand')
        size = request.args.get('size')
        category_id = request.args.get('category_id')
        
        if brand:
            query = query.filter(Product.brand.ilike(f'%{brand}%'))
        if size:
            query = query.filter(Product.size == size)
        if category_id:
            query = query.filter(Product.category_id == category_id)
        
        products = query.all()
        
        return jsonify({
            'products': [{
                'id': str(p.id),
                'brand': p.brand,
                'model': p.model,
                'colorway': p.colorway,
                'size': p.size,
                'gender': p.gender,
                'category': p.category.display_name if p.category else None,
                'condition_grade': p.condition_grade.display_name if p.condition_grade else None,
                'default_price_ebay': float(p.default_price_ebay) if p.default_price_ebay else None,
                'created_at': p.created_at.isoformat() if p.created_at else None
            } for p in products]
        })
    finally:
        db.close()

@app.route('/api/products', methods=['POST'])
def create_product():
    """Create a new product"""
    db = next(get_db())
    
    try:
        data = request.json
        
        # Validate required fields
        required_fields = ['brand', 'model', 'size']
        for field in required_fields:
            if not data.get(field):
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        # Create product
        product = Product(
            brand=data['brand'],
            model=data['model'],
            colorway=data.get('colorway'),
            size=data['size'],
            gender=data.get('gender'),
            category_id=data.get('category_id'),
            condition_grade_id=data.get('condition_grade_id'),
            default_price_ebay=data.get('default_price_ebay'),
            sku_prefix=data.get('sku_prefix'),
            notes=data.get('notes')
        )
        
        db.add(product)
        db.commit()
        db.refresh(product)
        
        logger.info(f"Created product: {product.brand} {product.model}")
        
        return jsonify({
            'message': 'Product created successfully',
            'product': {
                'id': str(product.id),
                'brand': product.brand,
                'model': product.model,
                'size': product.size
            }
        }), 201
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating product: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/api/products/<product_id>', methods=['GET'])
def get_product(product_id):
    """Get product details with units"""
    db = next(get_db())
    
    try:
        product = db.query(Product).filter(Product.id == product_id).first()
        
        if not product:
            return jsonify({'error': 'Product not found'}), 404
        
        # Get units for this product
        units = db.query(Unit).filter(Unit.product_id == product_id).all()
        
        return jsonify({
            'product': {
                'id': str(product.id),
                'brand': product.brand,
                'model': product.model,
                'colorway': product.colorway,
                'size': product.size,
                'gender': product.gender,
                'default_price_ebay': float(product.default_price_ebay) if product.default_price_ebay else None,
                'notes': product.notes
            },
            'units': [{
                'id': str(u.id),
                'unit_code': u.unit_code,
                'status': u.status,
                'location_code': u.location.code if u.location else None,
                'condition': u.condition_grade.display_name if u.condition_grade else None,
                'cost_basis': float(u.cost_basis) if u.cost_basis else None,
                'created_at': u.created_at.isoformat() if u.created_at else None
            } for u in units]
        })
    finally:
        db.close()

# ============================================
# UNIT ENDPOINTS
# ============================================

@app.route('/api/units', methods=['GET'])
def get_units():
    """Get all units with optional filtering"""
    db = next(get_db())
    
    try:
        query = db.query(Unit).options(
            joinedload(Unit.product),
            joinedload(Unit.location),
            joinedload(Unit.condition_grade)
        )
        
        # Filters
        status = request.args.get('status')
        product_id = request.args.get('product_id')
        location_id = request.args.get('location_id')
        unit_code = request.args.get('unit_code')
        
        if status:
            query = query.filter(Unit.status == status)
        if product_id:
            query = query.filter(Unit.product_id == product_id)
        if location_id:
            query = query.filter(Unit.location_id == location_id)
        if unit_code:
            query = query.filter(Unit.unit_code.ilike(f'%{unit_code}%'))
        
        units = query.all()
        
        return jsonify({
            'units': [{
                'id': str(u.id),
                'unit_code': u.unit_code,
                'status': u.status,
                'product': {
                    'brand': u.product.brand,
                    'model': u.product.model,
                    'size': u.product.size
                } if u.product else None,
                'location_code': u.location.code if u.location else None,
                'condition': u.condition_grade.display_name if u.condition_grade else None,
                'cost_basis': float(u.cost_basis) if u.cost_basis else None,
                'created_at': u.created_at.isoformat() if u.created_at else None
            } for u in units]
        })
    finally:
        db.close()

@app.route('/api/units', methods=['POST'])
def create_unit():
    """Create a new unit (physical shoe)"""
    db = next(get_db())
    
    try:
        data = request.json
        
        # Validate required fields
        if not data.get('unit_code'):
            return jsonify({'error': 'Missing required field: unit_code'}), 400
        if not data.get('product_id'):
            return jsonify({'error': 'Missing required field: product_id'}), 400
        
        # Check if unit_code already exists
        existing = db.query(Unit).filter(Unit.unit_code == data['unit_code']).first()
        if existing:
            return jsonify({'error': 'Unit code already exists'}), 400
        
        # Create unit
        unit = Unit(
            unit_code=data['unit_code'],
            product_id=data['product_id'],
            location_id=data.get('location_id'),
            condition_grade_id=data.get('condition_grade_id'),
            status=data.get('status', 'ready_to_list'),
            cost_basis=data.get('cost_basis'),
            notes=data.get('notes')
        )
        
        db.add(unit)
        db.commit()
        db.refresh(unit)
        
        logger.info(f"Created unit: {unit.unit_code}")
        
        return jsonify({
            'message': 'Unit created successfully',
            'unit': {
                'id': str(unit.id),
                'unit_code': unit.unit_code,
                'status': unit.status
            }
        }), 201
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating unit: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/api/units/<unit_id>', methods=['PUT'])
def update_unit(unit_id):
    """Update unit details"""
    db = next(get_db())
    
    try:
        unit = db.query(Unit).filter(Unit.id == unit_id).first()
        
        if not unit:
            return jsonify({'error': 'Unit not found'}), 404
        
        data = request.json
        
        # Update fields
        if 'location_id' in data:
            unit.location_id = data['location_id']
        if 'status' in data:
            unit.status = data['status']
        if 'condition_grade_id' in data:
            unit.condition_grade_id = data['condition_grade_id']
        if 'cost_basis' in data:
            unit.cost_basis = data['cost_basis']
        if 'notes' in data:
            unit.notes = data['notes']
        
        db.commit()
        
        return jsonify({'message': 'Unit updated successfully'})
    except Exception as e:
        db.rollback()
        logger.error(f"Error updating unit: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

@app.route('/api/units/search/<unit_code>', methods=['GET'])
def search_unit(unit_code):
    """Search for a unit by barcode"""
    db = next(get_db())
    
    try:
        unit = db.query(Unit).filter(Unit.unit_code == unit_code).first()
        
        if not unit:
            return jsonify({'error': 'Unit not found'}), 404
        
        # Get listing info if exists
        listing_info = None
        if unit.listing_units:
            listing = unit.listing_units[0].listing
            listing_info = {
                'channel': listing.channel.display_name,
                'listing_id': listing.channel_listing_id,
                'title': listing.title,
                'price': float(listing.current_price) if listing.current_price else None,
                'status': listing.status,
                'url': listing.listing_url
            }
        
        return jsonify({
            'unit': {
                'id': str(unit.id),
                'unit_code': unit.unit_code,
                'status': unit.status,
                'product': {
                    'brand': unit.product.brand,
                    'model': unit.product.model,
                    'colorway': unit.product.colorway,
                    'size': unit.product.size
                },
                'location': {
                    'code': unit.location.code,
                    'description': unit.location.description
                } if unit.location else None,
                'condition': unit.condition_grade.display_name if unit.condition_grade else None,
                'cost_basis': float(unit.cost_basis) if unit.cost_basis else None,
                'listing': listing_info,
                'created_at': unit.created_at.isoformat() if unit.created_at else None
            }
        })
    finally:
        db.close()

# ============================================
# LOCATION ENDPOINTS
# ============================================

@app.route('/api/locations', methods=['GET'])
def get_locations():
    """Get all locations"""
    db = next(get_db())
    
    try:
        locations = db.query(Location).filter(Location.is_active == True).all()
        
        return jsonify({
            'locations': [{
                'id': str(l.id),
                'code': l.code,
                'description': l.description,
                'is_active': l.is_active
            } for l in locations]
        })
    finally:
        db.close()

@app.route('/api/locations', methods=['POST'])
def create_location():
    """Create a new location"""
    db = next(get_db())
    
    try:
        data = request.json
        
        if not data.get('code'):
            return jsonify({'error': 'Missing required field: code'}), 400
        
        # Check if code already exists
        existing = db.query(Location).filter(Location.code == data['code']).first()
        if existing:
            return jsonify({'error': 'Location code already exists'}), 400
        
        location = Location(
            code=data['code'],
            description=data.get('description'),
            is_active=data.get('is_active', True)
        )
        
        db.add(location)
        db.commit()
        db.refresh(location)
        
        return jsonify({
            'message': 'Location created successfully',
            'location': {
                'id': str(location.id),
                'code': location.code
            }
        }), 201
    except Exception as e:
        db.rollback()
        logger.error(f"Error creating location: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

# ============================================
# CATEGORY & CONDITION ENDPOINTS
# ============================================

@app.route('/api/categories', methods=['GET'])
def get_categories():
    """Get all categories"""
    db = next(get_db())
    
    try:
        categories = db.query(Category).all()
        
        return jsonify({
            'categories': [{
                'id': str(c.id),
                'internal_name': c.internal_name,
                'display_name': c.display_name,
                'ebay_category_id': c.ebay_category_id
            } for c in categories]
        })
    finally:
        db.close()

@app.route('/api/condition-grades', methods=['GET'])
def get_condition_grades():
    """Get all condition grades"""
    db = next(get_db())
    
    try:
        grades = db.query(ConditionGrade).order_by(ConditionGrade.sort_order).all()
        
        return jsonify({
            'condition_grades': [{
                'id': str(g.id),
                'internal_code': g.internal_code,
                'display_name': g.display_name,
                'ebay_condition_id': g.ebay_condition_id,
                'ebay_condition_name': g.ebay_condition_name
            } for g in grades]
        })
    finally:
        db.close()

# ============================================
# SYNC ENDPOINTS
# ============================================

@app.route('/api/sync/ebay', methods=['POST'])
def sync_ebay():
    """Trigger eBay sync manually"""
    db = next(get_db())
    
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        

        sync_service = SyncService(db)

        # sync_ebay_listings():
        #         Fetches active eBay listings
        #         Matches to units by SKU
        #         Creates listing records
        #         Updates unit status to "listed"
        #         Creates templates

        # sync_sold_items():
        #         Fetches sold eBay orders (last 30 days)
        #         Finds units by SKU
        #         Updates unit: status→"sold", sold_at, sold_price, sold_platform
        #         Updates listing: status→"sold", sold_at, sold_price

        # Regular listings sync (matches eBay listings to units, updates status)
        result = sync_service.sync_ebay_listings()

        # Sold items sync (fetches eBay sold orders, updates sold_at/sold_price)
        result2 = sync_service.sync_sold_items()

        if result['success']:
            return jsonify({
                'message': 'Sync completed successfully',
                'results': result.get('results')
            })
        else:
            return jsonify({
                'error': 'Sync failed',
                'details': result.get('error')
            }), 500
    finally:
        db.close()


@app.route('/api/sync/logs', methods=['GET'])
def get_sync_logs():
    """Get sync logs"""
    db = next(get_db())
    
    try:
        logs = db.query(SyncLog).order_by(desc(SyncLog.started_at)).limit(20).all()
        
        return jsonify({
            'logs': [{
                'id': str(log.id),
                'sync_type': log.sync_type,
                'status': log.status,
                'records_processed': log.records_processed,
                'records_updated': log.records_updated,
                'records_created': log.records_created,
                'errors': log.errors,
                'started_at': log.started_at.isoformat() if log.started_at else None,
                'completed_at': log.completed_at.isoformat() if log.completed_at else None
            } for log in logs]
        })
    finally:
        db.close()

# ============================================
# ALERT ENDPOINTS
# ============================================

@app.route('/api/alerts', methods=['GET'])
def get_alerts():
    """Get all alerts"""
    db = next(get_db())
    
    try:
        query = db.query(Alert).order_by(desc(Alert.created_at))
        
        # Filter by resolved status
        resolved = request.args.get('resolved')
        if resolved:
            query = query.filter(Alert.is_resolved == (resolved.lower() == 'true'))
        
        alerts = query.limit(50).all()
        
        return jsonify({
            'alerts': [{
                'id': str(a.id),
                'alert_type': a.alert_type,
                'severity': a.severity,
                'title': a.title,
                'message': a.message,
                'is_resolved': a.is_resolved,
                'created_at': a.created_at.isoformat() if a.created_at else None
            } for a in alerts]
        })
    finally:
        db.close()

@app.route('/api/alerts/<alert_id>/resolve', methods=['POST'])
def resolve_alert(alert_id):
    """Mark alert as resolved"""
    db = next(get_db())
    
    try:
        alert = db.query(Alert).filter(Alert.id == alert_id).first()
        
        if not alert:
            return jsonify({'error': 'Alert not found'}), 404
        
        alert.is_resolved = True
        alert.resolved_at = datetime.utcnow()
        
        db.commit()
        
        return jsonify({'message': 'Alert resolved'})
    except Exception as e:
        db.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db.close()

# ============================================
# LISTING ENDPOINTS
# ============================================

@app.route('/api/listings', methods=['GET'])
def get_listings():
    """Get all listings"""
    db = next(get_db())
    
    try:
        query = db.query(Listing).options(
            joinedload(Listing.product),
            joinedload(Listing.channel)
        )
        
        # Filter by status
        status = request.args.get('status')
        if status:
            query = query.filter(Listing.status == status)
        
        listings = query.all()
        
        return jsonify({
            'listings': [{
                'id': str(l.id),
                'channel_listing_id': l.channel_listing_id,
                'title': l.title,
                'current_price': float(l.current_price) if l.current_price else None,
                'status': l.status,
                'listing_url': l.listing_url,
                'channel': l.channel.display_name if l.channel else None,
                'product': {
                    'brand': l.product.brand,
                    'model': l.product.model,
                    'size': l.product.size
                } if l.product else None,
                'created_at': l.created_at.isoformat() if l.created_at else None
            } for l in listings]
        })
    finally:
        db.close()


# ! scheduler to run ebay sync every hour endpoints
def run_scheduled_sync():
    """
    Wrapper function for scheduled sync (runs without HTTP request context)
    """
    db = next(get_db())
    try:
        from sync_service import SyncService

        sync_service = SyncService(db)

        # sync_ebay_listings():
        #         Fetches active eBay listings
        #         Matches to units by SKU
        #         Creates listing records
        #         Updates unit status to "listed"
        #         Creates templates

        # sync_sold_items():
        #         Fetches sold eBay orders (last 30 days)
        #         Finds units by SKU
        #         Updates unit: status→"sold", sold_at, sold_price, sold_platform
        #         Updates listing: status→"sold", sold_at, sold_price

        # Regular listings sync (matches eBay listings to units, updates status)
        result = sync_service.sync_ebay_listings()

        # Sold items sync (fetches eBay sold orders, updates sold_at/sold_price)
        sync_service.sync_sold_items()

        logger.info(f"Scheduled sync completed: {result}")
    except Exception as e:
        logger.error(f"Scheduled sync failed: {e}")
    finally:
        db.close()


@app.route('/api/scheduler/status', methods=['GET'])
def get_scheduler_status():
    """Get scheduler status"""
    try:
        status = sync_scheduler.get_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"Error getting scheduler status: {e}")
        return jsonify({'error': str(e)}), 500

    

@app.route('/api/scheduler/start', methods=['POST'])
def start_scheduler():
    """Start automated sync scheduler"""
    try:
        if ebay_api.is_configured():
            success = sync_scheduler.start(run_scheduled_sync)
            if success:
                return jsonify({
                    'message': 'Scheduler started successfully',
                    'status': sync_scheduler.get_status()
                })
            else:
                return jsonify({'error': 'Failed to start scheduler'}), 500
        else:
            return jsonify({'error': 'eBay API not configured'}), 400
    except Exception as e:
        logger.error(f"Error starting scheduler: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/stop', methods=['POST'])
def stop_scheduler():
    """Stop automated sync scheduler"""
    try:
        success = sync_scheduler.stop()
        if success:
            return jsonify({
                'message': 'Scheduler stopped successfully',
                'status': sync_scheduler.get_status()
            })
        else:
            return jsonify({'error': 'Failed to stop scheduler'}), 500
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/scheduler/trigger', methods=['POST'])
def trigger_sync_now():
    """Trigger sync immediately (in addition to scheduled runs)"""
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
# ! SOLD ITEMS ENDPOINTS
# ============================================

# checks for sales of units on our system dabase
@app.route('/api/sold-items', methods=['GET'])
def get_sold_items():
    """Get all sold items with filters"""
    db = next(get_db())
    
    try:
        query = db.query(Unit).options(
            joinedload(Unit.product),
            joinedload(Unit.location)
        ).filter(Unit.status == 'sold')
        
        # Filters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        platform = request.args.get('platform')
        
        if start_date:
            query = query.filter(Unit.sold_at >= start_date)
        if end_date:
            query = query.filter(Unit.sold_at <= end_date)
        if platform:
            query = query.filter(Unit.sold_platform == platform)
        
        # Order by sold date (newest first)
        units = query.order_by(desc(Unit.sold_at)).all()
        
        return jsonify({
            'sold_items': [{
                'id': str(u.id),
                'unit_code': u.unit_code,
                'product': {
                    'brand': u.product.brand,
                    'model': u.product.model,
                    'size': u.product.size,
                    'colorway': u.product.colorway
                } if u.product else None,
                'sold_at': u.sold_at.isoformat() if u.sold_at else None,
                'sold_price': float(u.sold_price) if u.sold_price else None,
                'sold_platform': u.sold_platform,
                'cost_basis': float(u.cost_basis) if u.cost_basis else None,
                'profit': float(u.sold_price - u.cost_basis) if (u.sold_price and u.cost_basis) else None,
                'location_code': u.location.code if u.location else None
            } for u in units]
        })
    finally:
        db.close()


# checks for sales of units on our system dabase
@app.route('/api/sales/stats', methods=['GET'])
def get_sales_stats():
    """Get sales statistics"""
    db = next(get_db())
    
    try:
        # Date filters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Query sold items
        query = db.query(Unit).filter(
            Unit.status == 'sold',
            Unit.sold_at.isnot(None)
        )
        
        if start_date:
            query = query.filter(Unit.sold_at >= start_date)
        if end_date:
            query = query.filter(Unit.sold_at <= end_date)
        
        sold_items = query.all()
        
        # Calculate stats
        total_sales = len(sold_items)
        total_revenue = sum(u.sold_price for u in sold_items if u.sold_price)
        total_cost = sum(u.cost_basis for u in sold_items if u.cost_basis)
        total_profit = total_revenue - total_cost
        
        avg_sale_price = total_revenue / total_sales if total_sales > 0 else 0
        avg_profit = total_profit / total_sales if total_sales > 0 else 0
        profit_margin = (total_profit / total_revenue * 100) if total_revenue > 0 else 0
        
        # Sales by platform
        platform_breakdown = {}
        for unit in sold_items:
            platform = unit.sold_platform or 'unknown'
            if platform not in platform_breakdown:
                platform_breakdown[platform] = {
                    'count': 0,
                    'revenue': 0
                }
            platform_breakdown[platform]['count'] += 1
            platform_breakdown[platform]['revenue'] += unit.sold_price if unit.sold_price else 0
        
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
        db.close()


# this endpoins checks all listsings sales that are on ebay 
@app.route('/api/sync/sold-items', methods=['POST'])
def sync_sold_items():
    """Trigger sold items sync"""
    db = next(get_db())
    
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        
        sync_service = SyncService(db)
        result = sync_service.sync_sold_items()
        
        if result['success']:
            return jsonify({
                'message': 'Sold items sync completed successfully',
                'results': result.get('results')
            })
        else:
            return jsonify({
                'error': 'Sold items sync failed',
                'details': result.get('error')
            }), 500
    finally:
        db.close()


# this checks only listings that are in our system and then tries to get thier data from ebay api to check if sold or not . then updates the data back on our systme.
@app.route('/api/sync/check-sold', methods=['POST'])
def check_for_sold():
    """Quick check of active listings for sold items"""
    db = next(get_db())
    
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        
        sync_service = SyncService(db)
        results = sync_service.check_active_listings_for_sold()
        
        return jsonify({
            'message': 'Check completed',
            'results': results
        })
    finally:
        db.close()



@app.route('/api/dashboard/sales', methods=['GET'])
def get_dashboard_sales():
    """Get sales data for dashboard"""
    db = next(get_db())
    
    try:
        from datetime import datetime, timedelta
        
        # Last 30 days
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        
        recent_sales = db.query(Unit).filter(
            Unit.status == 'sold',
            Unit.sold_at >= thirty_days_ago
        ).order_by(desc(Unit.sold_at)).limit(10).all()
        
        # Today's sales
        today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        today_sales_count = db.query(Unit).filter(
            Unit.status == 'sold',
            Unit.sold_at >= today_start
        ).count()
        
        return jsonify({
            'recent_sales': [{
                'unit_code': u.unit_code,
                'product': f"{u.product.brand} {u.product.model}" if u.product else '-',
                'sold_price': float(u.sold_price) if u.sold_price else 0,
                'sold_at': u.sold_at.isoformat() if u.sold_at else None,
                'platform': u.sold_platform
            } for u in recent_sales],
            'today_sales_count': today_sales_count
        })
    finally:
        db.close()




"""
API ENDPOINTS FOR ENHANCED LISTING TEMPLATES

Add these endpoints to app.py:
"""

# ============================================
# ! TEMPLATE ENDPOINTS
# ============================================

@app.route('/api/templates', methods=['GET'])
def get_templates():
    """Get all listing templates with filters"""
    db = next(get_db())
    try:
        from database import ListingTemplate, Product
        from sqlalchemy.orm import joinedload
        
        query = db.query(ListingTemplate).options(
            joinedload(ListingTemplate.product)
        )
        
        # Filters
        validated_only = request.args.get('validated') == 'true'
        product_id = request.args.get('product_id')
        
        if validated_only:
            query = query.filter(ListingTemplate.is_validated == True)
        
        if product_id:
            query = query.filter(ListingTemplate.product_id == product_id)
        
        templates = query.all()
        
        return jsonify({
            'templates': [{
                'id': str(t.id),
                'product_id': str(t.product_id),
                'product': {
                    'brand': t.product.brand,
                    'model': t.product.model,
                    'size': t.product.size
                } if t.product else None,
                'title': t.title,
                'base_price': float(t.base_price) if t.base_price else None,
                'photos_count': len(t.photos) if t.photos else 0,
                'pricing': t.pricing,
                'category_mappings': t.category_mappings,
                'is_validated': t.is_validated,
                'validation_errors': t.validation_errors,
                'template_version': t.template_version,
                'last_synced_at': t.last_synced_at.isoformat() if t.last_synced_at else None
            } for t in templates]
        })
    finally:
        db.close()


@app.route('/api/templates/<template_id>', methods=['GET'])
def get_template(template_id):
    """Get single template with full details"""
    db = next(get_db())
    
    try:
        from database import ListingTemplate
        
        template = db.query(ListingTemplate).filter(
            ListingTemplate.id == template_id
        ).first()
        
        if not template:
            return jsonify({'error': 'Template not found'}), 404
        
        return jsonify({
            'id': str(template.id),
            'product_id': str(template.product_id),
            'title': template.title,
            'description': template.description,
            'photos': template.photos,
            'photo_metadata': template.photo_metadata,
            'item_specifics': template.item_specifics,
            'base_price': float(template.base_price) if template.base_price else None,
            'pricing': template.pricing,
            'category_mappings': template.category_mappings,
            'seo_keywords': template.seo_keywords,
            'is_validated': template.is_validated,
            'validation_errors': template.validation_errors,
            'template_version': template.template_version,
            'last_synced_at': template.last_synced_at.isoformat() if template.last_synced_at else None
        })
    finally:
        db.close()

@app.route('/api/templates/<template_id>/platform/<platform>', methods=['GET'])
def get_template_for_platform(template_id, platform):
    """Get template formatted for specific platform"""
    db = next(get_db())
    
    try:
        template_service = TemplateService(db)
        formatted = template_service.get_template_for_platform(template_id, platform)
        
        if not formatted:
            return jsonify({'error': 'Template not found'}), 404
        
        return jsonify(formatted)
    finally:
        db.close()

@app.route('/api/templates/<template_id>/validate', methods=['POST'])
def validate_template(template_id):
    """Validate a template"""
    db = next(get_db())
    
    try:
        from database import ListingTemplate
        
        template = db.query(ListingTemplate).filter(
            ListingTemplate.id == template_id
        ).first()
        
        if not template:
            return jsonify({'error': 'Template not found'}), 404
        
        template_service = TemplateService(db)
        result = template_service.validate_template(template)
        
        # Update template
        template.is_validated = result['valid']
        template.validation_errors = result.get('errors')
        db.commit()
        
        return jsonify(result)
    finally:
        db.close()

@app.route('/api/templates/validate-all', methods=['POST'])
def validate_all_templates():
    """Bulk validate all templates"""
    db = next(get_db())
    
    try:
        template_service = TemplateService(db)
        results = template_service.bulk_validate_templates()
        
        return jsonify({
            'message': 'Bulk validation completed',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/templates/refresh', methods=['POST'])
def refresh_templates():
    """Refresh templates from eBay"""
    db = next(get_db())
    
    try:
        if not ebay_api.is_configured():
            return jsonify({'error': 'eBay API not configured'}), 400
        
        from sync_service import SyncService
        
        sync_service = SyncService(db)
        results = sync_service.refresh_templates()
        
        return jsonify({
            'message': 'Templates refreshed',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/templates/stats', methods=['GET'])
def get_template_stats():
    """Get template statistics"""
    db = next(get_db())
    
    try:
        from database import ListingTemplate
        
        total = db.query(ListingTemplate).count()
        validated = db.query(ListingTemplate).filter(
            ListingTemplate.is_validated == True
        ).count()
        invalid = total - validated
        
        # Templates ready for cross-listing
        ready = db.query(ListingTemplate).filter(
            ListingTemplate.is_validated == True,
            ListingTemplate.template_version >= 2
        ).count()
        
        return jsonify({
            'stats': {
                'total': total,
                'validated': validated,
                'invalid': invalid,
                'ready_for_crosslisting': ready,
                'validation_rate': round((validated / total * 100) if total > 0 else 0, 1)
            }
        })
    finally:
        db.close()




"""
API ENDPOINTS FOR AUDIT DASHBOARD

Add these endpoints to app.py:
"""
# ============================================
# ! AUDIT ENDPOINTS
# ============================================

@app.route('/api/audit/full', methods=['POST'])
def run_full_audit():
    """Run comprehensive audit"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        results = audit_service.run_full_audit()
        
        return jsonify({
            'message': 'Audit completed',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/audit/summary', methods=['GET'])
def get_audit_summary():
    """Get quick audit summary"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        summary = audit_service.get_audit_summary()
        
        return jsonify(summary)
    finally:
        db.close()

@app.route('/api/audit/sku-issues', methods=['GET'])
def get_sku_issues():
    """Get SKU-related issues only"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        issues = audit_service.audit_sku_issues()
        
        return jsonify(issues)
    finally:
        db.close()

@app.route('/api/audit/inventory-mismatches', methods=['GET'])
def get_inventory_mismatches():
    """Get inventory mismatch issues"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        issues = audit_service.audit_inventory_mismatches()
        
        return jsonify(issues)
    finally:
        db.close()

@app.route('/api/audit/template-issues', methods=['GET'])
def get_template_issues():
    """Get template validation issues"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        issues = audit_service.audit_template_issues()
        
        return jsonify(issues)
    finally:
        db.close()

@app.route('/api/audit/pricing-issues', methods=['GET'])
def get_pricing_issues():
    """Get pricing-related issues"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        issues = audit_service.audit_pricing_issues()
        
        return jsonify(issues)
    finally:
        db.close()

@app.route('/api/audit/export', methods=['POST'])
def export_audit_report():
    """Export audit report as CSV"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        
        # Run audit
        results = audit_service.run_full_audit()
        
        # Export to CSV
        csv_content = audit_service.export_audit_report(results)
        
        # Return as downloadable file
        from flask import Response
        return Response(
            csv_content,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment;filename=audit_report.csv'}
        )
    finally:
        db.close()

@app.route('/api/audit/issues/resolve-bulk', methods=['POST'])
def resolve_bulk_issues():
    """
    Bulk resolve alerts/issues
    Body: { "alert_ids": ["id1", "id2", ...] }
    """
    db = next(get_db())
    
    try:
        from database import Alert
        
        data = request.get_json()
        alert_ids = data.get('alert_ids', [])
        
        if not alert_ids:
            return jsonify({'error': 'No alert IDs provided'}), 400
        
        # Update alerts
        updated = 0
        for alert_id in alert_ids:
            alert = db.query(Alert).filter(Alert.id == alert_id).first()
            if alert:
                alert.is_resolved = True
                alert.resolved_at = datetime.utcnow()
                updated += 1
        
        db.commit()
        
        return jsonify({
            'message': f'{updated} alerts resolved',
            'updated': updated
        })
    finally:
        db.close()

@app.route('/api/audit/dashboard', methods=['GET'])
def get_audit_dashboard():
    """Get complete audit dashboard data"""
    db = next(get_db())
    
    try:
        audit_service = AuditService(db)
        
        # Get summary
        summary = audit_service.get_audit_summary()
        
        # Get recent issues (top 10 of each type)
        sku_issues = audit_service.audit_sku_issues()
        inventory_issues = audit_service.audit_inventory_mismatches()
        template_issues = audit_service.audit_template_issues()
        
        # Limit to top 10 each for dashboard
        dashboard_data = {
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
                'templates': {
                    'invalid_templates': template_issues['invalid_templates'][:10]
                }
            },
            'issue_counts': {
                'sku_issues': sku_issues['total'],
                'inventory_mismatches': inventory_issues['total'],
                'template_issues': template_issues['total']
            }
        }
        
        return jsonify(dashboard_data)
    finally:
        db.close()


"""
API ENDPOINTS FOR BULK IMPORT

Add these endpoints to app.py:
"""
# ============================================
#! BULK IMPORT ENDPOINTS
# ============================================

@app.route('/api/import/products/preview', methods=['POST'])
def preview_products_import():
    """
    Preview products CSV before import
    Body: { "csv_content": "..." }
    """
    db = next(get_db())
    
    try:
        data = request.get_json()
        csv_content = data.get('csv_content')
        
        if not csv_content:
            return jsonify({'error': 'No CSV content provided'}), 400
        
        bulk_import = BulkImportService(db)
        results = bulk_import.parse_products_csv(csv_content)
        
        return jsonify({
            'message': 'CSV parsed successfully',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/import/products/execute', methods=['POST'])
def execute_products_import():
    """
    Execute products import
    Body: { "valid_rows": [...] }
    """
    db = next(get_db())
    
    try:
        data = request.get_json()
        valid_rows = data.get('valid_rows', [])
        
        if not valid_rows:
            return jsonify({'error': 'No valid rows to import'}), 400
        
        bulk_import = BulkImportService(db)
        results = bulk_import.import_products(valid_rows)
        
        return jsonify({
            'message': 'Import completed',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/import/units/preview', methods=['POST'])
def preview_units_import():
    """
    Preview units CSV before import
    Body: { "csv_content": "..." }
    """
    db = next(get_db())
    
    try:
        data = request.get_json()
        csv_content = data.get('csv_content')
        
        if not csv_content:
            return jsonify({'error': 'No CSV content provided'}), 400
        
        bulk_import = BulkImportService(db)
        results = bulk_import.parse_units_csv(csv_content)
        
        return jsonify({
            'message': 'CSV parsed successfully',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/import/units/execute', methods=['POST'])
def execute_units_import():
    """
    Execute units import
    Body: { "valid_rows": [...] }
    """
    db = next(get_db())
    
    try:
        data = request.get_json()
        valid_rows = data.get('valid_rows', [])
        
        if not valid_rows:
            return jsonify({'error': 'No valid rows to import'}), 400
        
        bulk_import = BulkImportService(db)
        results = bulk_import.import_units(valid_rows)
        
        return jsonify({
            'message': 'Import completed',
            'results': results
        })
    finally:
        db.close()

@app.route('/api/import/templates/products', methods=['GET'])
def download_products_template():
    """Download products CSV template"""
    db = next(get_db())
    
    try:
        bulk_import = BulkImportService(db)
        template = bulk_import.generate_products_template()
        
        return Response(
            template,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment;filename=products_template.csv'}
        )
    finally:
        db.close()

@app.route('/api/import/templates/units', methods=['GET'])
def download_units_template():
    """Download units CSV template"""
    db = next(get_db())
    
    try:
        bulk_import = BulkImportService(db)
        template = bulk_import.generate_units_template()
        
        return Response(
            template,
            mimetype='text/csv',
            headers={'Content-Disposition': 'attachment;filename=units_template.csv'}
        )
    finally:
        db.close()



# # ============================================
# # ! DELISTING ENDPOINTS
# # ============================================
# @app.route('/api/delist/check-emails', methods=['POST'])
# def manual_check_emails():
#     """Manually trigger email check and processing"""
#     db = next(get_db())
    
#     try:
#         gmail = GmailService()
        
#         if not gmail.is_connected():
#             return jsonify({'error': 'Gmail not connected'}), 400
        
#         parser = EmailParserService()
#         delist_service = DelistService(db)
        
#         # Get emails from last hour
#         since_minutes = request.json.get('since_minutes', 60) if request.json else 60
#         print(f"Checking emails for delisting since last {since_minutes} Minutes")
#         emails = gmail.get_sale_emails(since_minutes=since_minutes)
        
#         results = {
#             'emails_found': len(emails),
#             'processed': [],
#             'errors': []
#         }
        
#         for email in emails:
#             try:
#                 parsed = parser.parse_sale_email(email)
                
#                 if parsed:
#                     result = delist_service.process_sale(parsed)
                    
#                     if result.get('success'):
#                         gmail.mark_as_read(email.get('message_id'))
#                         results['processed'].append({
#                             'platform': parsed.get('platform'),
#                             'sku': parsed.get('sku'),
#                             'unit_code': result.get('unit_code'),
#                             'delisted_count': len(result.get('delisted', []))
#                         })
#                     else:
#                         results['errors'].append({
#                             'email': email.get('subject'),
#                             'errors': result.get('errors')
#                         })
#             except Exception as e:
#                 results['errors'].append({
#                     'email': email.get('subject'),
#                     'error': str(e)
#                 })
        
#         return jsonify({
#             'message': f'Checked {results["emails_found"]} emails',
#             'results': results
#         })
        
#     finally:
#         db.close()


# ============================================
# ! DELISTING ENDPOINTS
# ============================================
@app.route('/api/delist/check-emails', methods=['POST'])
def manual_check_emails():
    """Manually trigger email check and processing (handles bundles)"""
    db = next(get_db())
    
    try:
        gmail = GmailService()
        
        if not gmail.is_connected():
            return jsonify({'error': 'Gmail not connected'}), 400
        
        parser = EmailParserService()
        delist_service = DelistService(db)
        
        # Get emails from last hour (or custom time range)
        since_minutes = request.json.get('since_minutes', 60) if request.json else 60
        logger.info(f"Checking emails for delisting since last {since_minutes} minutes")
        emails = gmail.get_sale_emails(since_minutes=since_minutes)
        
        results = {
            'emails_found': len(emails),
            'emails_processed': 0,
            'total_items': 0,
            'processed': [],
            'errors': []
        }
        
        for email in emails:
            try:
                # Parse email - returns LIST of items (handles bundles)
                sale_items = parser.parse_sale_email(email)
                
                if not sale_items:
                    results['errors'].append({
                        'email': email.get('subject', 'Unknown')[:50],
                        'error': 'Failed to parse email'
                    })
                    continue
                
                # Track items in this email
                email_items_processed = 0
                email_items_failed = 0
                results['total_items'] += len(sale_items)
                
                # Process EACH item
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
                                'delisted_count': len(result.get('delisted', []))
                            })
                        else:
                            email_items_failed += 1
                            results['errors'].append({
                                'email': email.get('subject', 'Unknown')[:50],
                                'item': i,
                                'sku': item.get('sku'),
                                'errors': result.get('errors')
                            })
                    
                    except Exception as e:
                        email_items_failed += 1
                        results['errors'].append({
                            'email': email.get('subject', 'Unknown')[:50],
                            'item': i,
                            'sku': item.get('sku'),
                            'error': str(e)
                        })
                
                # Mark email as read if at least one item succeeded
                if email_items_processed > 0:
                    gmail.mark_as_read(email.get('message_id'))
                    results['emails_processed'] += 1
                    
                    if len(sale_items) > 1:
                        logger.info(f"Bundle processed: {email_items_processed}/{len(sale_items)} items successful")
                    else:
                        logger.info(f"Single sale processed successfully")
                else:
                    logger.error(f"Email failed: No items processed successfully")
            
            except Exception as e:
                results['errors'].append({
                    'email': email.get('subject', 'Unknown')[:50],
                    'error': str(e)
                })
        
        # Build response message
        message = f"Checked {results['emails_found']} email(s): "
        message += f"{results['emails_processed']} processed, "
        message += f"{len(results['processed'])} item(s) delisted"
        
        if results['errors']:
            message += f", {len(results['errors'])} error(s)"
        
        return jsonify({
            'success': True,
            'message': message,
            'results': results
        })
        
    except Exception as e:
        logger.error(f"Error in manual_check_emails: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
    
    finally:
        db.close()



@app.route('/api/delist/history', methods=['GET'])
def get_delist_history():
    """Get delisting history from sync logs"""
    db = next(get_db())
    
    try:
        from database import SyncLog
        from sqlalchemy import desc
        
        # Get recent delist logs (you can add a specific log_type for delist)
        logs = db.query(SyncLog).filter(
            SyncLog.log_type == 'delist'
        ).order_by(desc(SyncLog.created_at)).limit(50).all()
        
        return jsonify({
            'history': [{
                'id': str(log.id),
                'timestamp': log.created_at.isoformat(),
                'status': log.status,
                'message': log.message,
                'details': log.details
            } for log in logs]
        })
        
    finally:
        db.close()

@app.route('/api/delist/stats', methods=['GET'])
def get_delist_stats():
    """Get delisting statistics"""
    db = next(get_db())
    
    try:
        from database import Unit, Listing
        from sqlalchemy import func
        from datetime import datetime, timedelta
        
        # Units sold in last 30 days
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        
        sold_by_platform = db.query(
            Unit.sold_platform,
            func.count(Unit.id).label('count')
        ).filter(
            Unit.status == 'sold',
            Unit.sold_at >= thirty_days_ago
        ).group_by(Unit.sold_platform).all()
        
        # Listings delisted (ended) in last 30 days
        delisted_count = db.query(Listing).filter(
            Listing.status == 'ended',
            Listing.ended_at >= thirty_days_ago
        ).count()
        
        # Recent sales
        recent_sales = db.query(Unit).filter(
            Unit.status == 'sold'
        ).order_by(Unit.sold_at.desc()).limit(10).all()
        
        return jsonify({
            'stats': {
                'sold_by_platform': {
                    platform: count for platform, count in sold_by_platform
                },
                'total_sold_30_days': sum(count for _, count in sold_by_platform),
                'total_delisted_30_days': delisted_count
            },
            'recent_sales': [{
                'unit_code': sale.unit_code,
                'sold_platform': sale.sold_platform,
                'sold_price': float(sale.sold_price) if sale.sold_price else None,
                'sold_at': sale.sold_at.isoformat() if sale.sold_at else None
            } for sale in recent_sales]
        })
        
    finally:
        db.close()

@app.route('/api/delist/gmail-status', methods=['GET'])
def get_gmail_status():
    """Check Gmail connection status"""
    try:
        gmail = GmailService()
        status = gmail.get_test_connection()
        
        return jsonify(status)
        
    except Exception as e:
        return jsonify({
            'connected': False,
            'error': str(e)
        }), 500

@app.route('/api/delist/test-parse', methods=['POST'])
def test_email_parsing():
    """Test email parsing with sample email data"""
    try:
        data = request.get_json()
        
        if not data or 'email_data' not in data:
            return jsonify({'error': 'email_data required'}), 400
        
        parser = EmailParserService()
        parsed = parser.parse_sale_email(data['email_data'])
        
        return jsonify({
            'parsed': parsed,
            'success': parsed is not None
        })
        
    except Exception as e:
        return jsonify({
            'error': str(e),
            'success': False
        }), 500



# ============================================
# ! CHROME PROFILES OPENING ENDPOINTS FOR DELISTING PLATFORMS PRE LOGIN 
"""
API ENDPOINT FOR CHROME PROFILE SETUP

Add this to app.py
"""
# ============================================

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os

# Global to track open browsers
# open_browsers = {}

# @app.route('/api/chrome/open-profile/<platform>', methods=['POST'])
# def open_chrome_profile(platform):
#     """
#     Open Chrome with specific profile for manual setup/login
#     Args:
#         platform (str): 'poshmark' or 'mercari'
#     """
#     global open_browsers
    
#     if platform not in ['poshmark', 'mercari']:
#         return jsonify({'error': 'Invalid platform. Use poshmark or mercari'}), 400
    
#     try:
#         # Profile path (relative to app.py)
#         profile_dir = os.path.join(os.path.dirname(__file__), 'delisting', 'profiles', platform)
        
#         # Create profile dir if doesn't exist
#         os.makedirs(profile_dir, exist_ok=True)
        
#         # Chrome options
#         chrome_options = Options()
#         chrome_options.add_argument(f"user-data-dir={os.path.abspath(profile_dir)}")
        
#         # Anti-detection options
#         chrome_options.add_argument('--disable-blink-features=AutomationControlled')
#         chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
#         chrome_options.add_experimental_option('useAutomationExtension', False)
#         chrome_options.add_argument('--no-sandbox')
#         chrome_options.add_argument('--disable-dev-shm-usage')
        
#         # Initialize driver
#         service = Service(ChromeDriverManager().install())
#         driver = webdriver.Chrome( options=chrome_options)

#         # Remove webdriver property
#         driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
#         # Navigate to platform
#         if platform == 'poshmark':
#             driver.get('https://poshmark.com')
#         elif platform == 'mercari':
#             driver.get('https://www.mercari.com')
        
#         # Store driver reference
#         open_browsers[platform] = driver
        
#         return jsonify({
#             'success': True,
#             'message': f'{platform.capitalize()} profile opened',
#             'instructions': f'Log in to {platform.capitalize()}, check "Remember Me", then close the browser window manually.'
#         })
        
#     except Exception as e:
#         logger.error(f"Error opening Chrome profile: {e}")
#         return jsonify({
#             'error': str(e)
#         }), 500

# @app.route('/api/chrome/close-profile/<platform>', methods=['POST'])
# def close_chrome_profile(platform):
#     """Close Chrome profile browser"""
#     global open_browsers
    
#     if platform in open_browsers:
#         try:
#             open_browsers[platform].quit()
#             del open_browsers[platform]
#             return jsonify({
#                 'success': True,
#                 'message': f'{platform.capitalize()} browser closed'
#             })
#         except Exception as e:
#             return jsonify({
#                 'error': str(e)
#             }), 500
    
#     return jsonify({
#         'message': 'Browser not open or already closed'
#     })

# @app.route('/api/chrome/profile-status', methods=['GET'])
# def get_profile_status():
#     """Check which profiles are currently open"""
#     global open_browsers
    
#     return jsonify({
#         'poshmark_open': 'poshmark' in open_browsers,
#         'mercari_open': 'mercari' in open_browsers,
#         'open_browsers': list(open_browsers.keys())
#     })


open_browsers = {}
@app.route('/api/chrome/open-profile/<platform>/<purpose>', methods=['POST'])
def open_chrome_profile(platform, purpose):
    """
    Open Chrome with specific profile for manual setup/login
    
    Args:
        platform (str): 'poshmark' or 'mercari'
        purpose (str): 'delisting' or 'crosslisting'
    """
    global open_browsers
    
    if platform not in ['poshmark', 'mercari']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    if purpose not in ['delisting', 'crosslisting']:
        return jsonify({'error': 'Invalid purpose'}), 400
    
    # Create unique key for tracking
    browser_key = f"{platform}_{purpose}"
    
    # Check if already open
    if browser_key in open_browsers:
        return jsonify({
            'error': f'{platform.capitalize()} {purpose} profile is already open. Close it first.'
        }), 400
    
    try:
        # Separate profile per purpose
        profile_dir = os.path.join(
            os.path.dirname(__file__), 
            purpose,  # 'delisting' or 'crosslisting'
            'profiles', 
            platform
        )
        
        # Create profile dir if doesn't exist
        os.makedirs(profile_dir, exist_ok=True)
        
        logger.info(f"Opening Chrome {purpose} profile for {platform} at: {profile_dir}")
        
        # Chrome options
        chrome_options = Options()
        chrome_options.add_argument(f"user-data-dir={os.path.abspath(profile_dir)}")
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # Initialize driver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # Navigate to platform
        if platform == 'poshmark':
            driver.get('https://poshmark.com/login')
        elif platform == 'mercari':
            driver.get('https://www.mercari.com/login')
        
        # Store driver reference with compound key
        open_browsers[browser_key] = driver
        
        return jsonify({
            'success': True,
            'message': f'{platform.capitalize()} {purpose} profile opened',
            'instructions': f'1. Log in to {platform.capitalize()}\n2. Check "Remember Me"\n3. Close browser when done\n\nThis profile is for {purpose} only.'
        })
        
    except Exception as e:
        logger.error(f"Error opening Chrome profile: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/chrome/close-profile/<platform>/<purpose>', methods=['POST'])
def close_chrome_profile(platform, purpose):
    """Close Chrome profile browser"""
    global open_browsers
    
    browser_key = f"{platform}_{purpose}"
    
    if browser_key in open_browsers:
        try:
            open_browsers[browser_key].quit()
            del open_browsers[browser_key]
            return jsonify({
                'success': True,
                'message': f'{platform.capitalize()} {purpose} browser closed'
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    return jsonify({'message': 'Browser not open or already closed'})


@app.route('/api/chrome/profile-status', methods=['GET'])
def get_profile_status():
    """Check which profiles are currently open"""
    global open_browsers
    
    return jsonify({
        'poshmark_delisting_open': 'poshmark_delisting' in open_browsers,
        'poshmark_crosslisting_open': 'poshmark_crosslisting' in open_browsers,
        'mercari_delisting_open': 'mercari_delisting' in open_browsers,
        'mercari_crosslisting_open': 'mercari_crosslisting' in open_browsers,
        'open_browsers': list(open_browsers.keys())
    })



# ============================================
# ! CROSS-LISTING ENDPOINTS
# ============================================

from crosslisting.crosslist_service import CrosslistService

@app.route('/api/crosslist/unit/<unit_id>', methods=['POST'])
def crosslist_unit(unit_id):
    """Cross-list a single unit to other platforms"""
    db = next(get_db())
    
    try:
        crosslist_service = CrosslistService(db)
        result = crosslist_service.check_and_crosslist(unit_id)
        
        if result.get('errors'):
            return jsonify({
                'message': 'Cross-listing completed with errors',
                'result': result
            }), 207  # Multi-Status
        
        return jsonify({
            'message': 'Cross-listing completed',
            'result': result
        })
        
    finally:
        db.close()

@app.route('/api/crosslist/bulk', methods=['POST'])
def crosslist_bulk():
    """
    Cross-list multiple units
    Body: { "unit_ids": ["id1", "id2", ...] }
    """
    db = next(get_db())
    
    try:
        data = request.get_json()
        unit_ids = data.get('unit_ids', [])
        
        if not unit_ids:
            return jsonify({'error': 'No unit IDs provided'}), 400
        
        crosslist_service = CrosslistService(db)
        results = crosslist_service.bulk_crosslist(unit_ids)
        
        return jsonify({
            'message': f'Processed {results["processed"]} units',
            'results': results
        })
        
    finally:
        db.close()

@app.route('/api/crosslist/auto-check', methods=['POST'])
def auto_check_crosslisting():
    """
    Check all listed units and auto cross-list if needed
    This can be triggered manually or by scheduler
    """
    db = next(get_db())
    
    try:
        from database import Unit
        
        # Get all listed units
        listed_units = db.query(Unit).filter(
            Unit.status == 'listed'
        ).all()
        
        unit_ids = [unit.id for unit in listed_units]
        
        if not unit_ids:
            return jsonify({
                'message': 'No listed units found',
                'results': {'total': 0}
            })
        
        crosslist_service = CrosslistService(db)
        results = crosslist_service.bulk_crosslist(unit_ids)
        
        return jsonify({
            'message': f'Auto cross-listing checked {len(unit_ids)} units',
            'results': results
        })
        
    finally:
        db.close()

@app.route('/api/crosslist/status/<unit_id>', methods=['GET'])
def get_crosslist_status(unit_id):
    """Get cross-listing status for a unit"""
    db = next(get_db())
    
    try:
        from database import Unit, Listing, ListingUnit, Channel
        
        unit = db.query(Unit).filter(Unit.id == unit_id).first()
        
        if not unit:
            return jsonify({'error': 'Unit not found'}), 404
        
        # Get all listings for this unit
        listings = db.query(Listing).join(ListingUnit).join(Channel).filter(
            ListingUnit.unit_id == unit.id
        ).all()
        
        platforms = {}
        for listing in listings:
            if listing.channel:
                platform = listing.channel.name.lower()
                platforms[platform] = {
                    'listed': True,
                    'listing_id': str(listing.id),
                    'channel_listing_id': listing.channel_listing_id,
                    'status': listing.status,
                    'price': float(listing.current_price) if listing.current_price else None
                }
        
        # Check which platforms are missing
        all_platforms = ['ebay', 'poshmark', 'mercari']
        for platform in all_platforms:
            if platform not in platforms:
                platforms[platform] = {
                    'listed': False,
                    'can_crosslist': unit.status == 'listed'
                }
        
        return jsonify({
            'unit_code': unit.unit_code,
            'unit_status': unit.status,
            'platforms': platforms
        })
        
    finally:
        db.close()


@app.route('/api/crosslist/stats', methods=['GET'])
def get_crosslist_stats():
    """Get cross-listing statistics"""
    db = next(get_db())
    
    try:
        from database import Unit, Listing, ListingUnit, Channel
        from sqlalchemy import func
        
        # Total listed units
        total_listed = db.query(Unit).filter(Unit.status == 'listed').count()
        
        # Units listed on each platform
        platform_counts = {}
        for platform in ['ebay', 'poshmark', 'mercari']:
            channel = db.query(Channel).filter(Channel.name == platform).first()
            if channel:
                count = db.query(Listing).filter(
                    Listing.channel_id == channel.id,
                    Listing.status == 'active'
                ).count()
                platform_counts[platform] = count
        
        # Units on all 3 platforms
        units_on_all_platforms = db.query(Unit).filter(
            Unit.status == 'listed'
        ).all()
        
        fully_crosslisted = 0
        for unit in units_on_all_platforms:
            listings = db.query(Listing).join(ListingUnit).filter(
                ListingUnit.unit_id == unit.id,
                Listing.status == 'active'
            ).count()
            
            if listings >= 3:
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
        db.close()


# ============================================
# ! INITIALIZE & RUN
# ============================================

# if __name__ == '__main__':

#     # Initialize database
#     logger.info("Initializing database...")
#     init_db()

#     # Start sync scheduler if enabled
#     auto_sync_enabled = os.getenv('AUTO_SYNC_ENABLED', 'false').lower() == 'true'
#     if auto_sync_enabled and ebay_api.is_configured():
#         logger.info("✅ Starting automated sync & Sold items sync scheduler ...")

#         sync_scheduler.start(run_scheduled_sync) # sync scheduler handle both sync items and sold sync items, so stopping it form ui will stop both normal sync items + sold sync items 
        
#         # Also start email monitoring if delisting enabled
#         auto_delist_enabled = os.getenv('AUTO_DELIST_ENABLED', 'false').lower() == 'true'
#         if auto_delist_enabled:
#             sync_scheduler.start_email_monitoring()
#             # logger.info("Email monitoring enabled for auto-delisting")
#             logger.info("✅ Starting Email monitoring service for Delisting....")

#         if os.getenv('AUTO_CROSSLIST_ENABLED') == 'true':
#            sync_scheduler.start_crosslist_monitoring()
#            logger.info("Auto Crosslisting enabled and Schedulder will do crosslisting for desired units after specific interval mentioned in env.")
#            logger.info(" ✅ Starting AutoCrosslisting Monitoring Service....")

#     else:
#         logger.info("Automated sync disabled (set AUTO_SYNC_ENABLED=true to enable)")
    
#     # Start Flask server
#     port = int(os.getenv('API_PORT', 5000))
#     host = os.getenv('API_HOST', '0.0.0.0')
    
#     logger.info(f"Starting API server on {host}:{port}")
#     app.run(host=host, port=port, debug=False)



if __name__ == '__main__':
    # Initialize database
    logger.info("Initializing database...")
    init_db()

    # Setup schedulers (but don't run initial sync yet)
    auto_sync_enabled = os.getenv('AUTO_SYNC_ENABLED', 'false').lower() == 'true'
    if auto_sync_enabled and ebay_api.is_configured():
        
        # # Start schedulers WITHOUT immediate run
        sync_scheduler.scheduler.start()  # Start APScheduler
        
        # Add jobs with run_date for delayed start (5 seconds after server starts)
        from datetime import datetime, timedelta
        start_time = datetime.now() + timedelta(seconds=5)
        
        # # # eBay sync job
        sync_scheduler.scheduler.add_job(
            func=run_scheduled_sync,
            trigger='interval',
            minutes=int(os.getenv('SYNC_INTERVAL_MINUTES', '60')),
            id='ebay_sync_job',
            name='eBay Sync Job',
            replace_existing=True,
            next_run_time=start_time, # Run 5 seconds after startup,
             max_instances=1
        )
        
        logger.info("✅ eBay sync scheduler configured")
        
        # Email monitoring
        auto_delist_enabled = os.getenv('AUTO_DELIST_ENABLED', 'false').lower() == 'true'
        if auto_delist_enabled:
            sync_scheduler.start_email_monitoring()
            logger.info("✅ Email monitoring enabled")
        
        # Crosslisting
        if os.getenv('AUTO_CROSSLIST_ENABLED') == 'true':
            sync_scheduler.start_crosslist_monitoring()
            logger.info("✅ Crosslisting monitoring enabled")
    
    else:
        logger.info("Automated sync disabled")
    
    # Start Flask server (NOW STARTS IMMEDIATELY!)
    # port = int(os.getenv('API_PORT', 5000))
    port = int(9500)
    host = os.getenv('API_HOST', '0.0.0.0')
    
    logger.info(f"🚀 Starting API server on {host}:{port}")
    app.run(host=host, port=port, debug=False)