# 🏗️ System Architecture - Phase 1

## High-Level Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        USER INTERFACE                           │
│                                                                 │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  Dashboard   │  │   Search     │  │   Forms      │        │
│  │  (Stats)     │  │  (Barcode)   │  │  (Add Data)  │        │
│  └──────────────┘  └──────────────┘  └──────────────┘        │
│                                                                 │
│                    Web Dashboard (HTML/CSS/JS)                  │
└─────────────────────────────────────────────────────────────────┘
                              ↕ HTTP/JSON
┌─────────────────────────────────────────────────────────────────┐
│                         BACKEND API                             │
│                                                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                     Flask API Server                      │  │
│  │                                                           │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │  Products   │  │    Units    │  │  Locations  │    │  │
│  │  │  Endpoints  │  │  Endpoints  │  │  Endpoints  │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │  │
│  │                                                           │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐    │  │
│  │  │  Listings   │  │    Sync     │  │   Alerts    │    │  │
│  │  │  Endpoints  │  │  Endpoints  │  │  Endpoints  │    │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘    │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ↕                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Business Logic                         │  │
│  │                                                           │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │  │
│  │  │   Database   │  │     eBay     │  │     Sync     │  │  │
│  │  │    Models    │  │  Integration │  │    Service   │  │  │
│  │  │  (SQLAlchemy)│  │   (ebaysdk)  │  │   (Matcher)  │  │  │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                 │
│                  Python Flask + SQLAlchemy                      │
└─────────────────────────────────────────────────────────────────┘
                    ↕                           ↕
┌──────────────────────────────┐    ┌──────────────────────────┐
│        DATABASE              │    │      eBay API            │
│                              │    │                          │
│  ┌────────────────────────┐  │    │  ┌────────────────────┐ │
│  │    products            │  │    │  │  Get Active        │ │
│  │    units               │  │    │  │  Listings          │ │
│  │    locations           │  │    │  │                    │ │
│  │    categories          │  │    │  │  Get Item Details  │ │
│  │    condition_grades    │  │    │  │                    │ │
│  │    channels            │  │    │  │  Revise Item       │ │
│  │    listings            │  │    │  │                    │ │
│  │    listing_units       │  │    │  │  End Item          │ │
│  │    listing_templates   │  │    │  └────────────────────┘ │
│  │    sync_logs           │  │    │                          │
│  │    alerts              │  │    │  ebaysdk Library         │
│  └────────────────────────┘  │    └──────────────────────────┘
│                              │
│  PostgreSQL (Supabase)       │
└──────────────────────────────┘
```

## Component Breakdown

### 1. Frontend (Web Dashboard)
```
HTML/CSS/JavaScript
├── Dashboard Statistics
├── Search Interface
├── Product Management
├── Unit Management
├── Listing Viewer
├── Alert System
└── Sync Controls

Technologies:
- Vanilla JavaScript
- Fetch API for HTTP requests
- CSS Grid for layout
- Modal dialogs for forms
```

### 2. Backend API (Flask)
```
Python Flask Server
├── Route Handlers (app.py)
├── Database Models (database.py)
├── eBay Integration (ebay_api.py)
├── Sync Service (sync_service.py)
└── Configuration (.env)

Key Features:
- RESTful API design
- JSON request/response
- Error handling
- CORS enabled
- Logging
```

### 3. Database (PostgreSQL)
```
Supabase/PostgreSQL
├── Core Tables
│   ├── products (catalog)
│   ├── units (physical items)
│   ├── locations (shelves)
│   └── listings (platform listings)
├── Reference Tables
│   ├── categories
│   ├── condition_grades
│   └── channels
├── Junction Tables
│   └── listing_units
└── Operational Tables
    ├── listing_templates
    ├── sync_logs
    └── alerts

Features:
- UUID primary keys
- Foreign key constraints
- Indexes on lookups
- Views for common queries
- Triggers for timestamps
```

### 4. eBay Integration
```
eBay API (via ebaysdk)
├── Authentication (OAuth)
├── GetMyeBaySelling
├── GetItem
├── ReviseItem (future)
└── EndItem (future)

Data Extracted:
- Item ID
- SKU (Custom Label)
- Title, Description
- Price
- Photos (URLs)
- Item Specifics
- Category
- Condition
```

## Data Flow

### Creating Inventory
```
User Input (Dashboard)
    ↓
POST /api/products
    ↓
Flask Route Handler
    ↓
SQLAlchemy Model
    ↓
PostgreSQL Insert
    ↓
Return Success
    ↓
Update UI
```

### Adding Physical Unit
```
Barcode Scan
    ↓
POST /api/units
    ↓
Validate unit_code unique
    ↓
Link to product_id
    ↓
Assign location_id
    ↓
Set status: ready_to_list
    ↓
Store in database
```

### eBay Sync Flow
```
User clicks "Sync eBay"
    ↓
POST /api/sync/ebay
    ↓
Create sync_log entry
    ↓
Call eBay API
    ↓
Fetch all active listings
    ↓
For each listing:
    ├── Extract SKU
    ├── Find matching unit
    ├── Create/update listing record
    ├── Link via listing_units
    └── Update unit status
    ↓
Create alerts for issues
    ↓
Update sync_log
    ↓
Return results
```

### Barcode Search
```
User enters barcode
    ↓
GET /api/units/search/{code}
    ↓
Query database for unit_code
    ↓
Join with:
    ├── products
    ├── locations
    ├── condition_grades
    └── listings (via listing_units)
    ↓
Return complete information
    ↓
Display in dashboard
```

## Technology Stack

### Backend
- **Language:** Python 3.9+
- **Framework:** Flask 3.0
- **ORM:** SQLAlchemy 2.0
- **eBay SDK:** ebaysdk 2.2
- **Database Driver:** psycopg2
- **Environment:** python-dotenv
- **Scheduling:** APScheduler (for Phase 2)

### Frontend
- **HTML5**
- **CSS3** (Grid, Flexbox)
- **JavaScript** (ES6+)
- **Fetch API** (AJAX)

### Database
- **Engine:** PostgreSQL 15+
- **Hosting:** Supabase
- **Features:** UUID, JSONB, Triggers, Views

### External APIs
- **eBay Trading API**
- **Authentication:** OAuth 2.0

## Security Considerations

### Current (Phase 1)
- Environment variables for credentials
- CORS enabled for local development
- Input validation on forms
- SQL injection prevention (SQLAlchemy ORM)

### Future (Production)
- JWT authentication
- Rate limiting
- HTTPS only
- API key management
- Database encryption
- Audit logging

## Scalability

### Current Capacity
- **Products:** Unlimited
- **Units:** Tested with 10,000+
- **Listings:** 200+ per sync
- **Sync Time:** ~2 minutes for 200 listings

### Performance Optimizations
- Database indexes on lookups
- Batch processing in sync
- Lazy loading of relationships
- Efficient queries with joins

### Future Improvements
- Caching layer (Redis)
- Background job queue (Celery)
- Database connection pooling
- API pagination
- Async processing

## Deployment Architecture

### Development (Current)
```
Local Machine
├── Backend: localhost:5000
├── Frontend: localhost:8000
└── Database: Supabase cloud
```

### Production (Future)
```
Cloud Infrastructure
├── Backend: Docker container
├── Frontend: Static hosting (S3, Netlify)
├── Database: Supabase production
├── Load Balancer: Nginx
└── Monitoring: Logging service
```

## Integration Points

### Current Integrations
1. **eBay API** - Listing sync
2. **Supabase** - Database hosting

### Future Integrations (Phase 2+)
1. **Poshmark** - Cross-listing
2. **Mercari** - Cross-listing
3. **Shopify** - E-commerce
4. **Email** - Notifications
5. **SMS** - Alerts

## Error Handling

### API Level
- Try-catch blocks
- HTTP status codes
- Error messages in JSON
- Logging to console

### Database Level
- Foreign key constraints
- Check constraints
- Unique constraints
- Rollback on error

### User Level
- Alert dialogs
- Toast notifications
- Form validation
- Loading indicators

## Monitoring & Logging

### Current
- Console logging (Python logging module)
- Sync logs in database
- Alert system for issues

### Future
- Centralized logging
- Performance monitoring
- Error tracking (Sentry)
- Uptime monitoring
- Analytics dashboard

---

## Quick Reference

**Start Backend:**
```bash
cd backend && python app.py
```

**Start Frontend:**
```bash
cd frontend && python -m http.server 8000
```

**API Health:**
```bash
curl http://localhost:5000/health
```

**Trigger Sync:**
```bash
curl -X POST http://localhost:5000/api/sync/ebay
```
