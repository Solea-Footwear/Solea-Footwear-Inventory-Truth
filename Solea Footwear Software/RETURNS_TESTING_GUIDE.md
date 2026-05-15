# eBay Returns Tracking System - Testing Guide

## Table of Contents
1. [System Overview](#system-overview)
2. [How It Works](#how-it-works)
3. [Setup Instructions](#setup-instructions)
4. [Testing the System](#testing-the-system)
5. [Dashboard Guide](#dashboard-guide)
6. [API Documentation](#api-documentation)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start Checklist

Before you begin, make sure you have:

- [ ] Docker and Docker Compose installed
- [ ] eBay Developer Account with OAuth token
- [ ] Gmail account with API credentials (`credentials.json`)
- [ ] `.env` file configured (copy from `.env.example`)
- [ ] Gmail label `EBAY_RETURNS_TRACKING` created
- [ ] Gmail token generated (`gmail_token.pickle`)

**Files You Need:**
- `.env` - Environment configuration (in project root)
- `credentials.json` - Gmail API credentials (in project root)
- `gmail_token.pickle` - Gmail authentication token (auto-generated)
- `docker-compose.yml` - Docker services configuration (already exists)
- `returns_dashboard.html` - Dashboard UI (already exists, open directly in browser)

**Dashboard Access:**
- The dashboard HTML file can be opened directly in your browser
- Or add a Flask route to serve it (see Setup Instructions → Step 4)

---

## System Overview

The eBay Returns Tracking System automatically processes eBay return notification emails, matches them to your internal orders, classifies returns into actionable buckets, and provides comprehensive reporting on return metrics.

### Key Features
- ✅ Automatic email processing from Gmail
- ✅ One master record per return (no duplicates)
- ✅ Automatic order matching and brand enrichment
- ✅ Return classification into internal buckets
- ✅ Buyer-no-ship closure tracking
- ✅ Real-time dashboard with detailed views
- ✅ Complete audit trail of all events

---

## How It Works

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                         GMAIL INBOX                              │
│  eBay sends return notification emails → Gmail Label Filter     │
│                    (EBAY_RETURNS_TRACKING)                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                    EMAIL PROCESSING SERVICE                      │
│  • Polls Gmail label once per day (or on-demand)                │
│  • Deduplicates by email message ID                             │
│  • Parses return data from email body                           │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                      RETURN SERVICE                              │
│  • Creates/updates ONE master return record per return_id       │
│  • Matches to internal order by order number                    │
│  • Enriches with eBay API data (item title, etc.)               │
│  • Classifies into internal buckets                             │
│  • Tracks status changes (opened → shipped → closed)            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                         DATABASE                                 │
│  • returns table (master records)                               │
│  • return_events table (audit trail)                            │
│  • email_processing_log table (email tracking)                  │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                    DASHBOARD & REPORTING                         │
│  • Real-time metrics and statistics                             │
│  • Email processing logs                                        │
│  • Detailed return views                                        │
│  • Brand return rate analysis                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
Email Received
     │
     ↓
Parse Email ──────────────→ Extract Fields:
     │                      • return_id
     │                      • order_number
     │                      • buyer_username
     │                      • return_reason
     │                      • dates, amounts, etc.
     ↓
Check if return_id exists?
     │
     ├─→ YES → Update existing return record
     │
     └─→ NO  → Create new return record
     │
     ↓
Match to Internal Order ──→ Find by order_number
     │                      • Get SKU
     │                      • Get Brand
     │                      • Link to Unit
     ↓
Enrich with eBay API ─────→ Get missing data:
     │                      • Item title
     │                      • Return details
     ↓
Classify Return ──────────→ Determine:
     │                      • Internal bucket
     │                      • Recommended fix
     │                      • Confidence score
     ↓
Update Status ────────────→ Set:
     │                      • status_current
     │                      • final_outcome
     ↓
Create Event Record ──────→ Audit trail entry
     │
     ↓
Display in Dashboard
```

### Return Lifecycle

```
┌──────────────┐
│ Return       │
│ Opened       │  Email: "Return request from buyer"
└──────┬───────┘
       │
       ↓
┌──────────────┐
│ Buyer        │
│ Shipped      │  Email: "Buyer shipped your return"
└──────┬───────┘  Status: buyer_shipped
       │
       ↓
┌──────────────┐
│ Delivered    │
│ Back         │  Email: "Item delivered back"
└──────┬───────┘  Status: delivered_back
       │
       ├─────────────────────────────────┐
       │                                 │
       ↓                                 ↓
┌──────────────┐              ┌──────────────────┐
│ Refund       │              │ Closed           │
│ Issued       │              │ No Shipment      │
└──────────────┘              └──────────────────┘
Outcome:                      Outcome:
refunded_after_return         closed_buyer_never_shipped
```

---

## Setup Instructions

### File Structure Overview

```
Project Root Directory
│
├── .env                          ← YOU CREATE THIS (copy from .env.example)
├── .env.example                  ← Template provided
├── credentials.json              ← Download from Google Cloud Console
├── gmail_token.pickle            ← Auto-generated (don't create manually)
│
├── docker-compose.yml            ← Docker configuration (already exists)
├── app.py                        ← Main application (already exists)
├── requirements.txt              ← Dependencies (already exists)
│
├── returns/                      ← Returns system code
│   ├── ebay_return_parser.py
│   ├── return_service.py
│   ├── email_processing_service.py
│   └── return_classifier.py
│
├── returns_dashboard.html        ← Web dashboard
├── schema.sql                    ← Database schema
├── seed_test_data.sql           ← Test data
│
└── RETURNS_TESTING_GUIDE.md     ← This guide
```

### Prerequisites
- Docker and Docker Compose installed
- Gmail account with API access configured
- eBay API credentials (IAF token)
- Python 3.9+ (if running without Docker)

### Step 1: Configure Gmail

1. **Create Gmail Label**
   - Open Gmail
   - Create a new label: `EBAY_RETURNS_TRACKING`
   - Set up Gmail filters to automatically label eBay return emails

2. **Gmail Filter Example**
   ```
   From: ebay@ebay.com OR ebay@reply.ebay.com
   Subject: (return OR refund OR "buyer shipped")
   → Apply label: EBAY_RETURNS_TRACKING
   ```

3. **Gmail API Setup**
   - Place your `credentials.json` in the project root
   - Run `python create_pickle_token.py` to generate `gmail_token.pickle`
   - This authenticates the app to access your Gmail

### Step 2: Configure Environment Variables

The system uses a `.env` file in the **project root directory** to store configuration.

#### Location
```
/Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/.env
```

#### Setup Instructions

1. **Copy the example file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit the `.env` file** with your actual credentials:
   ```bash
   nano .env
   # or use any text editor
   ```

3. **Required Variables for Returns Tracking:**

```bash
# ============================================
# DATABASE CONFIGURATION
# ============================================
# Full database connection URL (recommended)
DATABASE_URL=postgresql://postgres:postgres@db:5432/inventory_db

# Note: When using Docker, use 'db' as hostname
# When running locally, use 'localhost'

# ============================================
# EBAY API CONFIGURATION
# ============================================
# Legacy Auth Token (for Trading API)
EBAY_AUTH_TOKEN=your_legacy_auth_token_here

# OAuth 2.0 Token (REQUIRED for Return API)
# This is the IAF token format: v^1.1#i^1#I^3#r^1#f^0#p^3#t^...
EBAY_OAUTH_TOKEN=v^1.1#i^1#I^3#r^1#f^0#p^3#t^...your_actual_token_here

# OAuth Refresh Token (for automatic token renewal)
EBAY_OAUTH_REFRESH_TOKEN=your_refresh_token_here

# eBay App Credentials
EBAY_APP_ID=your_ebay_app_id
EBAY_CERT_ID=your_ebay_cert_id
EBAY_DEV_ID=your_ebay_dev_id

# Environment: 'production' or 'sandbox'
EBAY_ENVIRONMENT=production

# ============================================
# GMAIL API CONFIGURATION
# ============================================
# Gmail label to monitor for return emails
EBAY_RETURNS_GMAIL_LABEL=EBAY_RETURNS_TRACKING

# Path to Gmail token file (auto-generated)
GMAIL_TOKEN_PATH=./gmail_token.pickle

# ============================================
# RETURN TRACKING CONFIGURATION
# ============================================
# Enable/disable automatic return processing
RETURN_PROCESSING_ENABLED=true

# How often to check for new return emails (in hours)
RETURN_CHECK_INTERVAL_HOURS=24

# ============================================
# SCHEDULER CONFIGURATION
# ============================================
# Enable automatic email checking
AUTO_SYNC_ENABLED=true

# How often to check emails (in minutes)
EMAIL_CHECK_INTERVAL_MINUTES=1440  # 24 hours = 1440 minutes

# ============================================
# API SERVER CONFIGURATION
# ============================================
# Port for the API server
API_PORT=9500

# Host binding (0.0.0.0 allows external access)
API_HOST=0.0.0.0

# Flask secret key (generate a random string)
SECRET_KEY=your_random_secret_key_here

# ============================================
# OPTIONAL: PGADMIN (Database Management UI)
# ============================================
PGADMIN_DEFAULT_EMAIL=admin@gmail.com
PGADMIN_DEFAULT_PASSWORD=admin
```

#### Important Notes

**For eBay OAuth Token:**
- The `EBAY_OAUTH_TOKEN` is the IAF (Identity and Access Framework) token
- Format: `v^1.1#i^1#I^3#r^1#f^0#p^3#t^...` (very long string)
- This token expires after 2 hours and needs to be refreshed
- Get it from: eBay Developer Portal → Your App → User Tokens

**For Gmail:**
- The `credentials.json` file must be in the project root
- Run `python create_pickle_token.py` to generate `gmail_token.pickle`
- The pickle file is auto-generated and should not be edited manually

**For Database:**
- When using Docker: use `db` as hostname (service name)
- When running locally: use `localhost` as hostname
- Default credentials in docker-compose.yml: `postgres:postgres`

#### Verification

After setting up `.env`, verify it's loaded correctly:

```bash
# Check if environment variables are set
docker-compose config

# Or check in Python
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('EBAY_OAUTH_TOKEN:', os.getenv('EBAY_OAUTH_TOKEN')[:20] + '...')"
```

### Step 3: Start the System

**Using Docker (Recommended):**
```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f app

# Stop services
docker-compose down
```

**Without Docker:**
```bash
# Install dependencies
pip install -r requirements.txt

# Run database migrations
python -c "from database import init_db; init_db()"

# Start the application
python app.py
```

### Step 4: Verify Setup

1. **Check API is running:**
   ```bash
   curl http://localhost:9500/health
   ```
   Expected response: `{"status": "healthy"}`

2. **Check database connection:**
   ```bash
   curl http://localhost:9500/api/returns
   ```
   Expected response: `{"returns": [], "total": 0, ...}`

3. **Open Dashboard:**
   
   The dashboard is a standalone HTML file. You have two options to access it:
   
   **Option 1: Open HTML file directly (Simplest)**
   ```bash
   # Open in your default browser
   open returns_dashboard.html
   
   # Or on Linux
   xdg-open returns_dashboard.html
   
   # Or manually navigate to:
   file:///Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/returns_dashboard.html
   ```
   
   **Option 2: Add a route to serve it via Flask (Recommended for production)**
   
   Add this code to `app.py` (after the imports section):
   ```python
   from flask import send_file
   
   @app.route('/returns-dashboard')
   def returns_dashboard():
       """Serve the returns tracking dashboard"""
       return send_file('returns_dashboard.html')
   ```
   
   Then restart the app and navigate to: `http://localhost:9500/returns-dashboard`
   
   **Note:** The HTML file is located at:
   ```
   /Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/returns_dashboard.html
   ```

---

## Testing the System

### Test Scenario 1: Process Test Email

1. **Forward a test email** to your Gmail account with the `EBAY_RETURNS_TRACKING` label

2. **Trigger email processing** via Dashboard:
   - Click "Process Return Emails" button
   - Wait for confirmation message

3. **Verify in Email Processing Log:**
   - Check the "Email Processing Log" section
   - Click on the email row to see details
   - Status should be "SUCCESS" if parsed correctly
   - Status will be "FAILED" if parsing failed (check notes for reason)

4. **Check Returns Table:**
   - Scroll to "Recent Returns" section
   - You should see the new return record
   - Click on the row to see full details

### Test Scenario 2: API Testing

**Process Emails via API:**
```bash
curl -X POST http://localhost:9500/api/returns/check-emails
```

**Get All Returns:**
```bash
curl http://localhost:9500/api/returns
```

**Get Specific Return:**
```bash
curl http://localhost:9500/api/returns/{return_id}
```

**Get Email Processing Logs:**
```bash
curl http://localhost:9500/api/returns/processed-emails
```

### Test Scenario 3: Verify Matching

1. **Ensure test data exists:**
   - The system includes seed data for eBay item `306850784377`
   - Order number: `04-14442-55788`
   - Buyer: `bfuchs226`
   - SKU: `00101733`

2. **Send/forward an email** with this order number

3. **Check matching:**
   - Process the email
   - View the return details
   - "Matched" badge should show "Matched" (green)
   - SKU should be populated
   - Brand should be populated

### Test Scenario 4: Verify Classification

1. **Check internal bucket:**
   - View return details
   - "Internal Bucket" should be assigned (e.g., "Size Issue/Fit")
   - "Recommended Fix" should be populated

2. **Verify classification logic:**
   - Returns with "doesn't fit" → Size Issue/Fit
   - Returns with "wrong item" → Wrong Item
   - Returns with "damaged" → Shipping Damage

---

## Dashboard Guide

### Dashboard Access

The dashboard is a standalone HTML file that can be accessed in two ways:

**Method 1: Direct File Access (Quick Testing)**
- Open `returns_dashboard.html` directly in your browser
- File location: `/Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/returns_dashboard.html`
- No server route needed

**Method 2: Via Flask Route (Production)**
- Add the route to `app.py` (see Setup Instructions)
- Access at: `http://localhost:9500/returns-dashboard`
- Better for deployed environments

### Dashboard URL

**If using Flask route:**
```
http://localhost:9500/returns-dashboard
```

**If opening file directly:**
```
file:///Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/returns_dashboard.html
```

### Dashboard Sections

#### 1. Header
- **Title:** Return Tracking System
- **Description:** Monitor and manage eBay returns efficiently

#### 2. Quick Actions
- **Process Return Emails:** Manually trigger email processing
- **View All Returns:** Refresh and load all returns data

#### 3. Overview Statistics
Four stat cards showing:
- **Total Returns:** Count of all returns in system
- **Matched:** Returns matched to internal orders
- **Open:** Returns still in progress
- **Closed:** Completed returns

#### 4. Email Processing Log
Table showing all processed emails:
- **Status:** SUCCESS / FAILED / SKIPPED
- **Subject:** Email subject line
- **From:** Sender email address
- **Processed At:** Timestamp
- **Notes:** Processing details or error messages

**Click any row** to see full email details:
- Message ID
- Complete sender information
- Received date
- Processing status
- Full processing notes

#### 5. Recent Returns
Table showing all returns:
- **Return ID:** eBay return ID
- **Buyer:** Buyer username
- **Brand:** Product brand
- **Status:** Current status (opened, buyer_shipped, closed)
- **Matched:** Whether matched to internal order
- **Opened:** Date return was opened

**Click any row** to see full return details:
- Basic Information (return ID, order number, buyer, item title, brand, SKU)
- Status & Classification (current status, internal bucket, final outcome, recommended fix)
- Return Details (reason, buyer comment, request amount)
- Timeline (all important dates)
- Shipping (tracking number)
- Matched Unit (linked inventory unit details)
- Events Timeline (complete audit trail)

### Dashboard Features

#### Color-Coded Status Badges
- **Open:** Yellow/Orange gradient
- **Buyer Shipped:** Blue gradient
- **Closed:** Green gradient
- **Success:** Green gradient
- **Failed:** Red gradient
- **Matched:** Green gradient
- **Unmatched:** Red gradient

#### Interactive Elements
- **Hover effects:** Cards and buttons lift on hover
- **Smooth animations:** Expandable details slide down smoothly
- **One-click details:** Click any row to expand/collapse details
- **Auto-close:** Opening a new detail closes the previous one

---

## API Documentation

### Base URL
```
http://localhost:9500
```

### Endpoints

#### 1. Health Check
```http
GET /health
```

**Response:**
```json
{
  "status": "healthy"
}
```

---

#### 2. Process Return Emails
```http
POST /api/returns/check-emails
```

**Description:** Polls Gmail label and processes all unprocessed emails.

**Response:**
```json
{
  "success": true,
  "results": {
    "processed": 5,
    "new_returns": 3,
    "updated_returns": 2,
    "skipped": 0,
    "failed": 0
  }
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Error message here"
}
```

---

#### 3. Get All Returns
```http
GET /api/returns
```

**Query Parameters:**
- `status` (optional): Filter by status (opened, buyer_shipped, closed)
- `matched` (optional): Filter by matched status (true/false)
- `brand` (optional): Filter by brand name

**Response:**
```json
{
  "returns": [
    {
      "id": "uuid",
      "marketplace": "ebay",
      "return_id": "5317157959",
      "order_number": "04-14442-55788",
      "buyer_username": "bfuchs226",
      "item_title": "Ecco ST.1 Hybrid Mens Size 10-10.5...",
      "brand": "Ecco",
      "sku": "00101733",
      "internal_order_id": "uuid",
      "return_reason_ebay": "Doesn't fit",
      "buyer_comment": "Too small",
      "request_amount": 89.99,
      "opened_at": "2026-05-01T10:30:00Z",
      "buyer_ship_by_date": "2026-05-15T23:59:59Z",
      "buyer_shipped_at": null,
      "tracking_number": null,
      "item_delivered_back_at": null,
      "refund_issued_at": null,
      "closed_at": null,
      "status_current": "opened",
      "final_outcome": "still_open",
      "internal_bucket": "Size Issue/Fit",
      "recommended_fix": "Add fit note, width guidance, or brand-specific sizing note.",
      "classifier_source": "rule_based",
      "classifier_confidence": 0.85,
      "matched": true,
      "created_at": "2026-05-01T10:35:00Z",
      "updated_at": "2026-05-01T10:35:00Z"
    }
  ],
  "total": 1,
  "matched": 1,
  "unmatched": 0,
  "still_open": 1
}
```

---

#### 4. Get Single Return
```http
GET /api/returns/{return_id}
```

**Path Parameters:**
- `return_id`: UUID of the return record

**Response:**
```json
{
  "return": {
    "id": "uuid",
    "return_id": "5317157959",
    ...
  },
  "events": [
    {
      "id": "uuid",
      "return_id": "uuid",
      "event_type": "return_opened",
      "event_timestamp": "2026-05-01T10:30:00Z",
      "source_type": "email",
      "email_message_id": "19df69334c9db0d7",
      "email_subject": "Return 5317157959: Return approved",
      "created_at": "2026-05-01T10:35:00Z"
    }
  ],
  "matched_unit": {
    "unit_code": "00101733",
    "sold_at": "2026-04-15T14:20:00Z",
    "sold_price": 89.99,
    "product": {
      "brand": "Ecco",
      "model": "ST.1 Hybrid",
      "size": "10-10.5"
    }
  }
}
```

---

#### 5. Get Email Processing Logs
```http
GET /api/returns/processed-emails
```

**Query Parameters:**
- `status` (optional): Filter by processing status (success, failed, skipped)
- `limit` (optional): Limit number of results (default: 100)

**Response:**
```json
{
  "emails": [
    {
      "id": "uuid",
      "email_message_id": "19df69334c9db0d7",
      "email_subject": "Fwd: Return 5317157959: Return approved",
      "email_sender": "Abdullah Al Saimun <saimun.devxhub@gmail.com>",
      "received_date": null,
      "processed_at": "2026-05-05T10:04:31Z",
      "processing_status": "success",
      "processing_notes": "Successfully created return record",
      "created_at": "2026-05-05T10:04:31Z"
    }
  ],
  "total": 1
}
```

---

#### 6. Get Brand Return Rates
```http
GET /api/returns/brand-stats
```

**Query Parameters:**
- `start_date` (optional): Start date for analysis (YYYY-MM-DD)
- `end_date` (optional): End date for analysis (YYYY-MM-DD)

**Response:**
```json
{
  "brands": [
    {
      "brand": "Ecco",
      "total_sold": 150,
      "total_returns": 12,
      "return_rate": 0.08,
      "return_rate_percent": "8.0%"
    },
    {
      "brand": "Nike",
      "total_sold": 200,
      "total_returns": 5,
      "return_rate": 0.025,
      "return_rate_percent": "2.5%"
    }
  ]
}
```

---

#### 7. Get Return Metrics
```http
GET /api/returns/metrics
```

**Response:**
```json
{
  "total_returns": 50,
  "total_refunded": 30,
  "total_closed_no_shipment": 8,
  "percent_closed_no_shipment": 16.0,
  "by_bucket": {
    "Size Issue/Fit": 20,
    "Condition Mismatch": 5,
    "Wrong Item": 3,
    "Shipping Damage": 2,
    "Low Intent Buyer": 8,
    "Needs Review": 12
  },
  "matched_vs_unmatched": {
    "matched": 45,
    "unmatched": 5
  }
}
```

---

## Troubleshooting

### Issue: Emails Not Being Processed

**Symptoms:**
- Email Processing Log shows "FAILED" status
- Processing notes show "Not a return email" or "Failed to parse email"

**Solutions:**
1. **Check email format:**
   - Ensure email is from eBay (ebay@ebay.com)
   - Subject should contain "return" keyword
   - Body should contain return ID

2. **Check logs:**
   ```bash
   docker-compose logs -f app | grep PARSE
   ```

3. **Verify Gmail label:**
   - Email must be in `EBAY_RETURNS_TRACKING` label
   - Check Gmail filters are working

### Issue: Returns Not Matching to Orders

**Symptoms:**
- "Matched" badge shows "Unmatched" (red)
- SKU and Brand fields are empty

**Solutions:**
1. **Check order number:**
   - Verify order number exists in database
   - Check `listings` table for matching `external_order_id`

2. **Check seed data:**
   ```bash
   docker-compose exec db psql -U user -d inventory -c "SELECT * FROM listings WHERE external_order_id = '04-14442-55788';"
   ```

3. **Verify order format:**
   - eBay order numbers: `XX-XXXXX-XXXXX` format
   - Must match exactly

### Issue: Dashboard Not Loading

**Symptoms:**
- Blank page or loading forever
- Browser console shows errors

**Solutions:**
1. **Check API is running:**
   ```bash
   curl http://localhost:9500/health
   ```

2. **Check browser console:**
   - Open Developer Tools (F12)
   - Look for CORS or network errors

3. **Verify port:**
   - Ensure port 9500 is not in use
   - Check `docker-compose ps` for service status

### Issue: eBay API Enrichment Failing

**Symptoms:**
- Item title shows "N/A" or incomplete
- Logs show "eBay API error"

**Solutions:**
1. **Check eBay token:**
   - Verify `EBAY_API_TOKEN` in `.env`
   - Token format: `v^1.1#i^1#I^3#r^1#f^0#p^3#t^...`

2. **Check token expiration:**
   - IAF tokens expire after 2 hours
   - Generate new token if expired

3. **Check API logs:**
   ```bash
   docker-compose logs -f app | grep "eBay API"
   ```

### Issue: Duplicate Returns Created

**Symptoms:**
- Same return appears multiple times
- Different IDs but same return_id

**Solutions:**
1. **Check deduplication:**
   - System should deduplicate by `return_id`
   - Check logs for "Already exists" messages

2. **Verify email processing:**
   - Each email should only be processed once
   - Check `email_processing_log` table

3. **Manual cleanup:**
   ```sql
   -- Find duplicates
   SELECT return_id, COUNT(*) 
   FROM returns 
   GROUP BY return_id 
   HAVING COUNT(*) > 1;
   ```

### Getting Help

**Check Logs:**
```bash
# All logs
docker-compose logs -f

# App logs only
docker-compose logs -f app

# Database logs
docker-compose logs -f db

# Filter by keyword
docker-compose logs -f app | grep ERROR
```

**Database Access:**
```bash
# Connect to database
docker-compose exec db psql -U user -d inventory

# Check returns
SELECT return_id, buyer_username, status_current, matched FROM returns;

# Check email logs
SELECT email_subject, processing_status, processing_notes FROM email_processing_log;
```

---

## File Locations Reference

### Configuration Files (Project Root)

```
/Users/abdullahalsaimun/Desktop/devxhub/Solea-Footwear-Inventory-Truth/
├── .env                          # Environment variables (YOU MUST CREATE THIS)
├── .env.example                  # Template for .env file
├── credentials.json              # Gmail API credentials (from Google Cloud Console)
├── gmail_token.pickle            # Gmail auth token (auto-generated)
├── docker-compose.yml            # Docker services configuration
├── app.py                        # Main application entry point
├── requirements.txt              # Python dependencies
└── RETURNS_TESTING_GUIDE.md      # This guide
```

### Returns System Files

```
returns/
├── __init__.py
├── ebay_return_parser.py         # Email parsing logic
├── return_service.py             # Return processing and matching
├── email_processing_service.py   # Email polling and deduplication
├── return_classifier.py          # Classification into buckets
└── new requirements.md           # Original requirements document
```

### Dashboard File

```
returns_dashboard.html            # Web dashboard (standalone HTML file)
                                  # Access via: file:// or add Flask route
```

**How to Access:**
1. **Direct:** Open `returns_dashboard.html` in any web browser
2. **Via Flask:** Add route to `app.py` and access at `/returns-dashboard`

**Important:** The dashboard HTML file contains all CSS and JavaScript inline. It makes API calls to `http://localhost:9500` so the Flask app must be running.

### Database Schema

```
schema.sql                        # Database table definitions
seed_test_data.sql               # Test data for eBay item 306850784377
```

### Important: Which Files to Edit

**Files You MUST Create/Edit:**
1. `.env` - Copy from `.env.example` and add your credentials
2. `credentials.json` - Download from Google Cloud Console

**Files Auto-Generated (Don't Edit):**
1. `gmail_token.pickle` - Created by `create_pickle_token.py`
2. `__pycache__/` - Python cache directories

**Files You Should NOT Edit (Unless Customizing):**
1. `docker-compose.yml` - Docker configuration
2. `schema.sql` - Database structure
3. Python source files in `returns/` directory

---

## Summary

The eBay Returns Tracking System provides:
- ✅ Automated email processing with deduplication
- ✅ One master record per return with complete audit trail
- ✅ Automatic order matching and brand enrichment
- ✅ Intelligent classification into actionable buckets
- ✅ Real-time dashboard with detailed views
- ✅ Comprehensive API for integration
- ✅ Buyer-no-ship closure tracking

For additional support or questions, refer to the logs and API documentation above.
