# 📖 API Documentation

Base URL: `http://localhost:5000/api`

## Authentication

Phase 1 does not include authentication. This will be added in future phases.

---

## Endpoints

### Health Check

#### GET `/health`

Check if the API is running and eBay is configured.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-11-27T10:30:00.000Z",
  "ebay_configured": true
}
```

---

### Dashboard

#### GET `/api/dashboard`

Get dashboard overview with statistics and recent sync logs.

**Response:**
```json
{
  "summary": {
    "total_products": 50,
    "total_units": 247,
    "ready_to_list": 45,
    "listed": 180,
    "sold": 20,
    "shipped": 15,
    "active_listings": 180,
    "unresolved_alerts": 3
  },
  "recent_syncs": [
    {
      "id": "uuid",
      "sync_type": "active_listings",
      "status": "completed",
      "records_processed": 180,
      "started_at": "2024-11-27T10:00:00.000Z",
      "completed_at": "2024-11-27T10:02:30.000Z"
    }
  ]
}
```

---

## Products

### GET `/api/products`

List all products with optional filtering.

**Query Parameters:**
- `brand` (string, optional) - Filter by brand
- `size` (string, optional) - Filter by size
- `category_id` (uuid, optional) - Filter by category

**Response:**
```json
{
  "products": [
    {
      "id": "uuid",
      "brand": "Nike",
      "model": "Air Jordan 1 Retro High OG",
      "colorway": "Black/Red",
      "size": "10",
      "gender": "Men",
      "category": "Athletic Shoes",
      "condition_grade": "Excellent Pre-Owned",
      "default_price_ebay": 150.00,
      "created_at": "2024-11-20T10:00:00.000Z"
    }
  ]
}
```

### POST `/api/products`

Create a new product.

**Request Body:**
```json
{
  "brand": "Nike",
  "model": "Air Jordan 1 Retro High OG",
  "colorway": "Black/Red",
  "size": "10",
  "gender": "Men",
  "category_id": "uuid",
  "condition_grade_id": "uuid",
  "default_price_ebay": 150.00,
  "sku_prefix": "AJ1",
  "notes": "Popular colorway"
}
```

**Required fields:** `brand`, `model`, `size`

**Response:**
```json
{
  "message": "Product created successfully",
  "product": {
    "id": "uuid",
    "brand": "Nike",
    "model": "Air Jordan 1 Retro High OG",
    "size": "10"
  }
}
```

### GET `/api/products/<product_id>`

Get product details with all units.

**Response:**
```json
{
  "product": {
    "id": "uuid",
    "brand": "Nike",
    "model": "Air Jordan 1 Retro High OG",
    "colorway": "Black/Red",
    "size": "10",
    "gender": "Men",
    "default_price_ebay": 150.00,
    "notes": "Popular colorway"
  },
  "units": [
    {
      "id": "uuid",
      "unit_code": "SHOE-12345",
      "status": "listed",
      "location_code": "A1-01-06-03",
      "condition": "Excellent Pre-Owned",
      "cost_basis": 80.00,
      "created_at": "2024-11-25T10:00:00.000Z"
    }
  ]
}
```

---

## Units

### GET `/api/units`

List all units with optional filtering.

**Query Parameters:**
- `status` (string, optional) - Filter by status
- `product_id` (uuid, optional) - Filter by product
- `location_id` (uuid, optional) - Filter by location
- `unit_code` (string, optional) - Search by unit code

**Response:**
```json
{
  "units": [
    {
      "id": "uuid",
      "unit_code": "SHOE-12345",
      "status": "listed",
      "product": {
        "brand": "Nike",
        "model": "Air Jordan 1 Retro High OG",
        "size": "10"
      },
      "location_code": "A1-01-06-03",
      "condition": "Excellent Pre-Owned",
      "cost_basis": 80.00,
      "created_at": "2024-11-25T10:00:00.000Z"
    }
  ]
}
```

### POST `/api/units`

Create a new unit (physical shoe).

**Request Body:**
```json
{
  "unit_code": "SHOE-12345",
  "product_id": "uuid",
  "location_id": "uuid",
  "condition_grade_id": "uuid",
  "status": "ready_to_list",
  "cost_basis": 80.00,
  "notes": "Good condition"
}
```

**Required fields:** `unit_code`, `product_id`

**Response:**
```json
{
  "message": "Unit created successfully",
  "unit": {
    "id": "uuid",
    "unit_code": "SHOE-12345",
    "status": "ready_to_list"
  }
}
```

### PUT `/api/units/<unit_id>`

Update unit details.

**Request Body:**
```json
{
  "location_id": "uuid",
  "status": "listed",
  "condition_grade_id": "uuid",
  "cost_basis": 80.00,
  "notes": "Updated notes"
}
```

**Response:**
```json
{
  "message": "Unit updated successfully"
}
```

### GET `/api/units/search/<unit_code>`

Search for a unit by barcode.

**Example:** `GET /api/units/search/SHOE-12345`

**Response:**
```json
{
  "unit": {
    "id": "uuid",
    "unit_code": "SHOE-12345",
    "status": "listed",
    "product": {
      "brand": "Nike",
      "model": "Air Jordan 1 Retro High OG",
      "colorway": "Black/Red",
      "size": "10"
    },
    "location": {
      "code": "A1-01-06-03",
      "description": "Aisle A1, Row 01, Column 06, Shelf 03"
    },
    "condition": "Excellent Pre-Owned",
    "cost_basis": 80.00,
    "listing": {
      "channel": "eBay",
      "listing_id": "293847562910",
      "title": "Nike Air Jordan 1 Retro High OG Black Red Size 10",
      "price": 150.00,
      "status": "active",
      "url": "https://www.ebay.com/itm/293847562910"
    },
    "created_at": "2024-11-25T10:00:00.000Z"
  }
}
```

**404 Response:**
```json
{
  "error": "Unit not found"
}
```

---

## Locations

### GET `/api/locations`

List all active locations.

**Response:**
```json
{
  "locations": [
    {
      "id": "uuid",
      "code": "A1-01-06-03",
      "description": "Aisle A1, Row 01, Column 06, Shelf 03",
      "is_active": true
    }
  ]
}
```

### POST `/api/locations`

Create a new location.

**Request Body:**
```json
{
  "code": "A1-01-06-03",
  "description": "Aisle A1, Row 01, Column 06, Shelf 03",
  "is_active": true
}
```

**Required fields:** `code`

**Response:**
```json
{
  "message": "Location created successfully",
  "location": {
    "id": "uuid",
    "code": "A1-01-06-03"
  }
}
```

---

## Categories & Conditions

### GET `/api/categories`

List all categories.

**Response:**
```json
{
  "categories": [
    {
      "id": "uuid",
      "internal_name": "athletic_shoes",
      "display_name": "Athletic Shoes",
      "ebay_category_id": "15709"
    }
  ]
}
```

### GET `/api/condition-grades`

List all condition grades.

**Response:**
```json
{
  "condition_grades": [
    {
      "id": "uuid",
      "internal_code": "excellent",
      "display_name": "Excellent Pre-Owned",
      "ebay_condition_id": 2750,
      "ebay_condition_name": "Pre-owned"
    }
  ]
}
```

---

## Listings

### GET `/api/listings`

List all listings.

**Query Parameters:**
- `status` (string, optional) - Filter by status (active, sold, ended)

**Response:**
```json
{
  "listings": [
    {
      "id": "uuid",
      "channel_listing_id": "293847562910",
      "title": "Nike Air Jordan 1 Retro High OG Black Red Size 10",
      "current_price": 150.00,
      "status": "active",
      "listing_url": "https://www.ebay.com/itm/293847562910",
      "channel": "eBay",
      "product": {
        "brand": "Nike",
        "model": "Air Jordan 1 Retro High OG",
        "size": "10"
      },
      "created_at": "2024-11-25T10:00:00.000Z"
    }
  ]
}
```

---

## Sync

### POST `/api/sync/ebay`

Trigger manual eBay sync.

**Response (Success):**
```json
{
  "message": "Sync completed successfully",
  "results": {
    "processed": 180,
    "matched": 175,
    "created": 10,
    "updated": 165,
    "unmatched_skus": ["SHOE-99999"],
    "missing_skus": ["293847562910"],
    "errors": []
  }
}
```

**Response (Error):**
```json
{
  "error": "Sync failed",
  "details": "eBay API connection error"
}
```

### GET `/api/sync/logs`

Get recent sync logs.

**Response:**
```json
{
  "logs": [
    {
      "id": "uuid",
      "sync_type": "active_listings",
      "status": "completed",
      "records_processed": 180,
      "records_updated": 165,
      "records_created": 10,
      "errors": null,
      "started_at": "2024-11-27T10:00:00.000Z",
      "completed_at": "2024-11-27T10:02:30.000Z"
    }
  ]
}
```

---

## Alerts

### GET `/api/alerts`

List all alerts.

**Query Parameters:**
- `resolved` (boolean, optional) - Filter by resolved status

**Response:**
```json
{
  "alerts": [
    {
      "id": "uuid",
      "alert_type": "unmatched_sku",
      "severity": "error",
      "title": "5 SKUs not found in inventory",
      "message": "SKUs: SHOE-99999, SHOE-88888...",
      "is_resolved": false,
      "created_at": "2024-11-27T10:00:00.000Z"
    }
  ]
}
```

### POST `/api/alerts/<alert_id>/resolve`

Mark an alert as resolved.

**Response:**
```json
{
  "message": "Alert resolved"
}
```

---

## Status Values

### Unit Status
- `ready_to_list` - In warehouse, ready to be listed
- `listed` - Currently listed on a platform
- `sold` - Sold, pending shipment
- `shipped` - Shipped to customer
- `returned` - Returned by customer
- `damaged` - Damaged, cannot sell
- `reserved` - Reserved, do not list

### Listing Status
- `active` - Currently active
- `sold` - Sold
- `ended` - Ended/delisted
- `draft` - Draft, not published

### Alert Severity
- `info` - Informational
- `warning` - Warning, needs attention
- `error` - Error, requires action
- `critical` - Critical, immediate action needed

---

## Error Responses

### 400 Bad Request
```json
{
  "error": "Missing required field: brand"
}
```

### 404 Not Found
```json
{
  "error": "Unit not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "Database connection failed"
}
```

---

## Rate Limits

Currently no rate limits in Phase 1. Will be implemented in production.

---

## Testing with cURL

### Create a product:
```bash
curl -X POST http://localhost:5000/api/products \
  -H "Content-Type: application/json" \
  -d '{
    "brand": "Nike",
    "model": "Air Jordan 1",
    "size": "10",
    "default_price_ebay": 150
  }'
```

### Search for a unit:
```bash
curl http://localhost:5000/api/units/search/SHOE-12345
```

### Trigger sync:
```bash
curl -X POST http://localhost:5000/api/sync/ebay
```

---

## Postman Collection

Import this collection for easy testing:

```json
{
  "info": {
    "name": "Inventory Management API",
    "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
  },
  "item": [
    {
      "name": "Health Check",
      "request": {
        "method": "GET",
        "url": "http://localhost:5000/health"
      }
    },
    {
      "name": "Get Dashboard",
      "request": {
        "method": "GET",
        "url": "http://localhost:5000/api/dashboard"
      }
    },
    {
      "name": "Create Product",
      "request": {
        "method": "POST",
        "url": "http://localhost:5000/api/products",
        "header": [
          {
            "key": "Content-Type",
            "value": "application/json"
          }
        ],
        "body": {
          "mode": "raw",
          "raw": "{\n  \"brand\": \"Nike\",\n  \"model\": \"Air Jordan 1\",\n  \"size\": \"10\",\n  \"default_price_ebay\": 150\n}"
        }
      }
    }
  ]
}
```
