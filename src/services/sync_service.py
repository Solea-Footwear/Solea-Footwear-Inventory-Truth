"""
Sync Service (psycopg2)
Matches eBay listings with inventory units and creates alerts for mismatches.
"""
import json
import logging
import re
from datetime import datetime

import psycopg2
import psycopg2.extras

from src.backend.db.database import _dsn
from src.integrations.ebay.ebay_api import ebay_api

logger = logging.getLogger(__name__)


class SyncService:
    def __init__(self, conn):
        self.conn = conn

    # ------------------------------------------------------------------
    # Public: sync_ebay_listings
    # ------------------------------------------------------------------

    def sync_ebay_listings(self):
        logger.info("Starting eBay sync...")

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM channels WHERE name = 'ebay' LIMIT 1")
            ebay_channel = cur.fetchone()

        if not ebay_channel:
            logger.error("eBay channel not found in database")
            return {"success": False, "error": "eBay channel not configured"}

        channel_id = ebay_channel["id"]

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO sync_logs (id, channel_id, sync_type, status, started_at)
                VALUES (gen_random_uuid(), %s, 'active_listings', 'running', now())
                RETURNING id
                """,
                [channel_id],
            )
            sync_log_id = cur.fetchone()["id"]
        self.conn.commit()

        try:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT u.unit_code
                    FROM units u
                    LEFT JOIN listing_templates lt ON lt.product_id = u.product_id
                    WHERE u.status = 'listed'
                      AND (lt.id IS NULL
                           OR lt.is_validated IS NOT TRUE
                           OR lt.photos IS NULL
                           OR lt.category_mappings IS NULL)
                    LIMIT 500
                    """
                )
                rows = cur.fetchall()

            our_skus = [r["unit_code"] for r in rows if r["unit_code"]]
            logger.info("REPAIR MODE: refreshing %d broken/missing-template SKUs", len(our_skus))

            if not our_skus:
                logger.warning("No SKUs in database to sync")
                self._finish_sync_log(sync_log_id, "completed", 0, 0, 0, None)
                return {"synced": 0, "errors": []}

            self.conn.commit()

            ebay_listings = ebay_api.get_listings_by_skus(our_skus)
            logger.info("Found %d matching eBay listings", len(ebay_listings))

            if not ebay_listings:
                logger.warning("No eBay listings fetched")
                self._finish_sync_log(sync_log_id, "completed", 0, 0, 0, None)
                return {"success": True, "message": "No listings to process"}

            results = {
                "processed": 0, "matched": 0, "created": 0, "updated": 0,
                "unmatched_skus": [], "missing_skus": [], "errors": [],
            }

            for ebay_item in ebay_listings:
                try:
                    result = self._process_ebay_listing(ebay_item, channel_id)
                    results["processed"] += 1
                    if result["matched"]:    results["matched"] += 1
                    if result["created"]:    results["created"] += 1
                    if result["updated"]:    results["updated"] += 1
                    if result["unmatched_sku"]:  results["unmatched_skus"].append(result["unmatched_sku"])
                    if result["missing_sku"]:    results["missing_skus"].append(ebay_item["item_id"])
                except Exception as e:
                    logger.error("Error processing listing %s: %s", ebay_item.get("item_id"), e)
                    results["errors"].append({"item_id": ebay_item.get("item_id"), "error": str(e)})

            self._finish_sync_log(
                sync_log_id, "completed",
                results["processed"], results["updated"], results["created"],
                results["errors"] or None,
            )
            self._create_sync_alerts(results)

            logger.info("Sync completed: %d matched, %d created, %d updated",
                        results["matched"], results["created"], results["updated"])
            return {"success": True, "results": results}

        except Exception as e:
            logger.error("Sync failed: %s", e)
            self._finish_sync_log(sync_log_id, "failed", 0, 0, 0, [{"error": str(e)}])
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Public: sync_sold_items
    # ------------------------------------------------------------------

    def sync_sold_items(self):
        logger.info("Starting sold items sync...")

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM channels WHERE name = 'ebay' LIMIT 1")
            ebay_channel = cur.fetchone()

        if not ebay_channel:
            return {"success": False, "error": "eBay channel not configured"}

        channel_id = ebay_channel["id"]

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO sync_logs (id, channel_id, sync_type, status, started_at)
                VALUES (gen_random_uuid(), %s, 'sold_items', 'running', now())
                RETURNING id
                """,
                [channel_id],
            )
            sync_log_id = cur.fetchone()["id"]
        self.conn.commit()

        try:
            sold_listings = ebay_api.get_all_sold_listings(days_back=30)
            results = {"processed": 0, "updated": 0, "not_found": [], "errors": []}

            for sold_item in sold_listings:
                try:
                    result = self._process_sold_item(sold_item, channel_id)
                    results["processed"] += 1
                    if result["updated"]:    results["updated"] += 1
                    if result["not_found"]:  results["not_found"].append(sold_item["sku"])
                except Exception as e:
                    logger.error("Error processing sold item %s: %s", sold_item.get("item_id"), e)
                    results["errors"].append({"item_id": sold_item.get("item_id"), "error": str(e)})

            self._finish_sync_log(sync_log_id, "completed",
                                  results["processed"], results["updated"], 0,
                                  results["errors"] or None)
            logger.info("Sold items sync completed: %d updated", results["updated"])
            return {"success": True, "results": results}

        except Exception as e:
            logger.error("Sold items sync failed: %s", e)
            self._finish_sync_log(sync_log_id, "failed", 0, 0, 0, [{"error": str(e)}])
            return {"success": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _process_ebay_listing(self, ebay_item, channel_id):
        result = {"matched": False, "created": False, "updated": False,
                  "unmatched_sku": None, "missing_sku": False}

        item_id = ebay_item["item_id"]
        sku = ebay_item.get("sku", "").strip()

        if not sku:
            result["missing_sku"] = True
            return result

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, product_id, status FROM units WHERE unit_code = %s", [sku])
            unit = cur.fetchone()

        if not unit:
            result["unmatched_sku"] = sku
            return result

        logger.info("SKU matched: %s, fetching full details for item %s", sku, item_id)
        full_details = ebay_api.get_item_details(item_id)

        if full_details:
            ebay_item = full_details
            item_specifics = ebay_item.get("item_specifics", {}) or {}
            title = ebay_item.get("title", "")
            title_lower = title.lower()

            size_value = (item_specifics.get("US Shoe Size")
                          or item_specifics.get("Size")
                          or item_specifics.get("Shoe Size") or "")
            if not size_value:
                m = re.search(r'\b(?:Size|Sz)\s+([0-9]{1,2}(?:\.[0-9])?[A-Z]?)\b', title, re.IGNORECASE)
                if m:
                    size_value = m.group(1)

            brand_value = item_specifics.get("Brand") or ebay_item.get("brand", "")
            item_specifics["Brand"] = brand_value
            item_specifics["Size"] = size_value

            category_lower = ebay_item.get("category_name", "").lower()
            department = "Men"
            level_2 = "Shoes"
            if any(x in title_lower for x in ["women", "womens", "ladies"]):
                department = "Women"
            elif any(x in title_lower for x in ["boys","girls","youth","kids","toddler","baby","child"]) \
                 or any(x in category_lower for x in ["boys","girls","kids"]):
                department = "Kids"
                level_2 = "Boys shoes" if "boy" in title_lower or "boy" in category_lower else "Girls shoes"

            level_3 = "Sneakers"
            if "boot" in title_lower:                                         level_3 = "Boots"
            elif any(x in title_lower for x in ["sandal","slide","flip flop"]): level_3 = "Sandals & Flip Flops" if department == "Kids" else "Sandals"
            elif any(x in title_lower for x in ["loafer","slip on","slip-on"]): level_3 = "Loafers & Slip-Ons" if department == "Men" else "Flats & Loafers"
            elif "slipper" in title_lower:                                    level_3 = "Slippers"

            ebay_item["item_specifics"] = item_specifics
            ebay_item["poshmark_data"] = {
                "category": {"level_1": department, "level_2": level_2, "level_3": level_3},
                "condition": "Good", "size": size_value,
                "color": ["Black"], "brand": brand_value,
            }
            ebay_item["mercari_data"] = {}
        else:
            logger.warning("Could not fetch full details for %s, using basic data", item_id)

        result["matched"] = True

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id FROM listings WHERE channel_listing_id = %s AND channel_id = %s",
                [item_id, channel_id],
            )
            existing_listing = cur.fetchone()

        ebay_category_data = {
            "category_id": ebay_item.get("category_id", ""),
            "category_name": ebay_item.get("category_name", ""),
        }

        if existing_listing:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE listings SET title=%s, description=%s, current_price=%s,
                        listing_url=%s, photos=%s, item_specifics=%s,
                        status='active', updated_at=now()
                    WHERE id=%s
                    """,
                    [ebay_item["title"], ebay_item["description"],
                     ebay_item["current_price"], ebay_item["listing_url"],
                     json.dumps(ebay_item["photos"]), json.dumps(ebay_item["item_specifics"]),
                     existing_listing["id"]],
                )
                cur.execute(
                    "SELECT 1 FROM listing_units WHERE listing_id=%s AND unit_id=%s",
                    [existing_listing["id"], unit["id"]],
                )
                if not cur.fetchone():
                    cur.execute(
                        "INSERT INTO listing_units (id, listing_id, unit_id) VALUES (gen_random_uuid(), %s, %s)",
                        [existing_listing["id"], unit["id"]],
                    )
            result["updated"] = True
            self._create_listing_template(ebay_item, unit["product_id"], ebay_category_data)
        else:
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO listings
                        (id, product_id, channel_id, channel_listing_id, title, description,
                         current_price, listing_url, status, mode, photos, item_specifics)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s, %s,
                            'active', 'single_quantity', %s, %s)
                    RETURNING id
                    """,
                    [unit["product_id"], channel_id, item_id,
                     ebay_item["title"], ebay_item["description"],
                     ebay_item["current_price"], ebay_item["listing_url"],
                     json.dumps(ebay_item["photos"]),
                     json.dumps(ebay_item["item_specifics"])],
                )
                new_listing_id = cur.fetchone()["id"]
                cur.execute(
                    "INSERT INTO listing_units (id, listing_id, unit_id) VALUES (gen_random_uuid(), %s, %s)",
                    [new_listing_id, unit["id"]],
                )
            result["created"] = True
            self._create_listing_template(ebay_item, unit["product_id"], ebay_category_data)

        if unit["status"] == "ready_to_list":
            with self.conn.cursor() as cur:
                cur.execute("UPDATE units SET status='listed' WHERE id=%s", [unit["id"]])

        self.conn.commit()
        return result

    def _process_sold_item(self, sold_item, channel_id):
        result = {"updated": False, "not_found": False}
        sku = sold_item.get("sku", "").strip()

        if not sku:
            result["not_found"] = True
            return result

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, status, sold_at FROM units WHERE unit_code = %s", [sku])
            unit = cur.fetchone()

        if not unit:
            result["not_found"] = True
            return result

        if unit["status"] == "sold" and unit["sold_at"]:
            return result

        try:
            sold_at = datetime.fromisoformat(sold_item["sold_at"].replace("Z", "+00:00"))
        except Exception:
            sold_at = datetime.utcnow()

        with self.conn.cursor() as cur:
            cur.execute(
                "UPDATE units SET status='sold', sold_at=%s, sold_price=%s, sold_platform='ebay' WHERE id=%s",
                [sold_at, sold_item["sold_price"], unit["id"]],
            )
            cur.execute(
                """
                UPDATE listings SET status='sold', sold_at=%s, sold_price=%s, ended_at=%s
                WHERE channel_listing_id=%s AND channel_id=%s
                """,
                [sold_at, sold_item["sold_price"], sold_at,
                 sold_item["item_id"], channel_id],
            )
        self.conn.commit()
        result["updated"] = True
        logger.info("Marked unit %s as sold: $%s on %s", sku, sold_item["sold_price"], sold_at)
        return result

    def _create_listing_template(self, listing, product_id, ebay_category_data=None):
        from src.services.template_service import TemplateService

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM channels WHERE name = 'ebay' LIMIT 1")
            row = cur.fetchone()
        ebay_channel_id = row["id"] if row else None

        formatted_data = {
            "title": listing.get("title", ""),
            "description": listing.get("description", ""),
            "photos": listing.get("photos", []),
            "item_specifics": listing.get("item_specifics", {}),
            "current_price": listing.get("current_price", 0),
            "poshmark_data": listing.get("poshmark_data", {}),
            "mercari_data": listing.get("mercari_data", {}),
        }
        template_service = TemplateService(self.conn)
        return template_service.create_enhanced_template(
            product_id=product_id,
            listing_data=formatted_data,
            channel_id=ebay_channel_id,
            ebay_category_data=ebay_category_data,
        )

    def refresh_templates(self):
        from src.services.template_service import TemplateService

        logger.info("Refreshing listing templates...")
        template_service = TemplateService(self.conn)

        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, channel_listing_id, product_id, channel_id FROM listings WHERE status='active'")
            listings = cur.fetchall()

        results = {"processed": 0, "updated": 0, "errors": []}

        for listing in listings:
            try:
                fresh_data = ebay_api.get_item_details(listing["channel_listing_id"])
                if fresh_data:
                    template_service.create_enhanced_template(
                        product_id=listing["product_id"],
                        listing_data=fresh_data,
                        channel_id=listing["channel_id"],
                    )
                    results["updated"] += 1
                results["processed"] += 1
            except Exception as e:
                logger.error("Error refreshing template for listing %s: %s", listing["id"], e)
                results["errors"].append({"listing_id": str(listing["id"]), "error": str(e)})

        logger.info("Template refresh complete: %d updated", results["updated"])
        return results

    def _finish_sync_log(self, sync_log_id, status, processed, updated, created, errors):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE sync_logs SET status=%s, completed_at=now(),
                    records_processed=%s, records_updated=%s, records_created=%s, errors=%s
                WHERE id=%s
                """,
                [status, processed, updated, created,
                 json.dumps(errors) if errors else None, sync_log_id],
            )
        self.conn.commit()

    def _create_sync_alerts(self, results):
        alerts_conn = psycopg2.connect(_dsn())
        try:
            with alerts_conn.cursor() as cur:
                if results["missing_skus"]:
                    ids = results["missing_skus"]
                    cur.execute(
                        """
                        INSERT INTO alerts (id, alert_type, severity, title, message, is_resolved)
                        VALUES (gen_random_uuid(), 'missing_sku', 'warning', %s, %s, false)
                        """,
                        [f"{len(ids)} eBay listings missing SKU",
                         f"eBay Item IDs: {', '.join(ids[:10])}{'...' if len(ids) > 10 else ''}"],
                    )
                if results["unmatched_skus"]:
                    skus = results["unmatched_skus"]
                    cur.execute(
                        """
                        INSERT INTO alerts (id, alert_type, severity, title, message, is_resolved)
                        VALUES (gen_random_uuid(), 'unmatched_sku', 'error', %s, %s, false)
                        """,
                        [f"{len(skus)} SKUs not found in inventory",
                         f"SKUs: {', '.join(skus[:10])}{'...' if len(skus) > 10 else ''}"],
                    )
                if results["errors"]:
                    cur.execute(
                        """
                        INSERT INTO alerts (id, alert_type, severity, title, message, is_resolved)
                        VALUES (gen_random_uuid(), 'sync_error', 'critical', %s, 'Check sync logs for details', false)
                        """,
                        [f"{len(results['errors'])} errors during sync"],
                    )
            alerts_conn.commit()
        except Exception as e:
            logger.error("Failed to save alerts: %s", e)
            alerts_conn.rollback()
        finally:
            alerts_conn.close()
