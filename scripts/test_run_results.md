# E2E Test Run Results

**Run ID:** RUN-05161651  
**Date:** 2026-05-16  

---

## Shoes Intaked (10 units)

| Unit Code | Brand | Model | Size | Condition | Unit ID |
|-----------|-------|-------|------|-----------|---------|
| RUN-05161651-001 | Nike | Air Jordan 1 Retro High OG Chicago | 10 | NEW | fcec92c6 |
| RUN-05161651-002 | Nike | Air Jordan 1 Retro High OG Chicago | 10 | NEW | a0861fa0 |
| RUN-05161651-003 | Nike | Air Jordan 1 Retro High OG Chicago | 10 | NEW | 80dc63fb |
| RUN-05161651-004 | Adidas | Yeezy Boost 350 V2 Zebra | 10.5 | NEW | 244ec958 |
| RUN-05161651-005 | Adidas | Yeezy Boost 350 V2 Zebra | 10.5 | NEW | a1742c51 |
| RUN-05161651-006 | Nike | Dunk Low Retro Panda | 9.5 | LIKE_NEW | befeb764 |
| RUN-05161651-007 | New Balance | 990v5 Made in USA | 11 | EXCELLENT | 01179d32 |
| RUN-05161651-008 | Jordan | Air Jordan 4 Retro Fire Red | 10 | NEW | c43260de |
| RUN-05161651-009 | Nike | Air Force 1 Low 07 | 9 | LIKE_NEW | 7ac6a394 |
| RUN-05161651-010 | Asics | Gel-Kayano 14 | 10.5 | EXCELLENT | 97086ece |

---

## Listings Created

| Listing ID | Title | Mode | Price | Units Assigned |
|------------|-------|------|-------|----------------|
| 6fdd592b | Nike Air Jordan 1 Retro High OG Chicago Size 10 DS | multi_quantity | $320 | 001, 002, 003 |
| 2a78af54 | Adidas Yeezy Boost 350 V2 Zebra Size 10.5 DS | multi_quantity | $380 | 004, 005 |
| 15a9e197 | Nike Dunk Low Retro Panda Size 9.5 Like New | multi_quantity | $145 | 006 |
| 641f7bcb | New Balance 990v5 Made in USA Size 11 Excellent | single_quantity | $210 | 007 |

---

## Sales Simulated

| Unit | Order ID | Sale Price | Platform |
|------|----------|------------|----------|
| RUN-05161651-001 | e15aab62 | $320 | eBay |
| RUN-05161651-002 | 6e8ddcbd | $320 | eBay |
| RUN-05161651-004 | 5e1aa5c7 | $380 | eBay |

---

## Shipment

| Order ID | Unit | Result |
|----------|------|--------|
| e15aab62 | RUN-05161651-001 | Shipped |

---

## Final Unit States

| Unit | Status | Notes |
|------|--------|-------|
| RUN-05161651-001 | shipped | AJ1 Chicago — full lifecycle done |
| RUN-05161651-002 | sold | AJ1 Chicago — sold, awaiting shipment |
| RUN-05161651-003 | listed | AJ1 Chicago — 1 unit left in multi-qty listing |
| RUN-05161651-004 | sold | Yeezy Zebra — sold, awaiting shipment |
| RUN-05161651-005 | listed | Yeezy Zebra — 1 unit left in multi-qty listing |
| RUN-05161651-006 | listed | Dunk Low Panda — single listing active |
| RUN-05161651-007 | listed | NB 990v5 — single listing active |
| RUN-05161651-008 | ready_to_list | Jordan 4 Fire Red — intake only |
| RUN-05161651-009 | ready_to_list | Air Force 1 Low — intake only |
| RUN-05161651-010 | ready_to_list | Asics Gel-Kayano 14 — intake only |

---

## Notes

- All listings are internal inventory records only — nothing was posted to eBay's API.
- A failed first run (RUN-05161649) also exists in the DB with units in listed/ready_to_list states (sale simulation errored before completing).
- The Decimal serialization bug in `simulate_sale` was fixed before the successful run.
