# main.py — FULL REPLACEMENT
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

print("🚀 main.py loaded — sanity build", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

MONGO_DB = os.environ.get("MONGO_DB", "leads")

# ✅ IMPORTANT: your canonical inventory collection
# For your record: it's createdAt and collection is LeadsData
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-1")

# CORS
CORS_ALLOW_ALL = os.environ.get("CORS_ALLOW_ALL", "").strip() == "1"
ALLOWED_ORIGINS = [
    "https://castudios.tv",
    "https://www.castudios.tv",
    "https://yesterdaysleads.onrender.com",
    "http://localhost:3000",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
]

# =========================
# PRICING (Lead Type x Age Bucket)
# =========================
# Keys are lead_type_norm (normalized), buckets are:
# YESTERDAY_72H, DAYS_4_14, DAYS_15_30, DAYS_31_90, DAYS_91_PLUS, CABOOM_RETAIL
DEFAULT_PRICING: Dict[str, Dict[str, float]] = {
    "final_expense": {
        "YESTERDAY_72H": 15.00, "DAYS_4_14": 10.00, "DAYS_15_30": 7.50, "DAYS_31_90": 5.00, "DAYS_91_PLUS": 2.50,
        "CABOOM_RETAIL": 25.00
    },
    "life": {
        "YESTERDAY_72H": 21.00, "DAYS_4_14": 14.00, "DAYS_15_30": 10.00, "DAYS_31_90": 7.00, "DAYS_91_PLUS": 3.50,
        "CABOOM_RETAIL": 35.00
    },
    "veteran_life": {
        "YESTERDAY_72H": 14.00, "DAYS_4_14": 9.00, "DAYS_15_30": 7.00, "DAYS_31_90": 4.00, "DAYS_91_PLUS": 2.00,
        "CABOOM_RETAIL": 23.00
    },
    "home": {
        "YESTERDAY_72H": 16.00, "DAYS_4_14": 11.00, "DAYS_15_30": 8.00, "DAYS_31_90": 5.50, "DAYS_91_PLUS": 3.00,
        "CABOOM_RETAIL": 27.00
    },
    "auto": {
        "YESTERDAY_72H": 16.00, "DAYS_4_14": 11.00, "DAYS_15_30": 8.00, "DAYS_31_90": 5.50, "DAYS_91_PLUS": 3.00,
        "CABOOM_RETAIL": 27.00
    },
    "medicare": {
        "YESTERDAY_72H": 15.00, "DAYS_4_14": 10.00, "DAYS_15_30": 7.50, "DAYS_31_90": 5.00, "DAYS_91_PLUS": 2.50,
        "CABOOM_RETAIL": 25.00
    },
    "health": {
        "YESTERDAY_72H": 16.00, "DAYS_4_14": 11.00, "DAYS_15_30": 8.00, "DAYS_31_90": 5.50, "DAYS_91_PLUS": 3.00,
        "CABOOM_RETAIL": 27.00
    },
    "retirement": {
        "YESTERDAY_72H": 29.00, "DAYS_4_14": 19.00, "DAYS_15_30": 14.00, "DAYS_31_90": 9.00, "DAYS_91_PLUS": 4.50,
        "CABOOM_RETAIL": 50.00
    },
}

def load_pricing() -> Dict[str, Dict[str, float]]:
    raw = os.environ.get("PRICING_JSON")
    if not raw:
        return DEFAULT_PRICING
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            out: Dict[str, Dict[str, float]] = {}
            for k, v in data.items():
                if isinstance(v, dict):
                    out[str(k).strip().lower()] = {str(b).strip().upper(): float(p) for b, p in v.items()}
            return out or DEFAULT_PRICING
    except Exception:
        pass
    return DEFAULT_PRICING

PRICING = load_pricing()

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if CORS_ALLOW_ALL else ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# DB
# =========================
client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]
leads_col = db[MONGO_COLLECTION]

# =========================
# MODELS
# =========================
class LeadsSearchRequest(BaseModel):
    # "boost" selectors (they DO NOT filter inventory; they only affect ranking)
    lead_type_norm: Optional[str] = None
    state: Optional[str] = None
    zip: Optional[str] = None
    bucket: str = Field(default="ALL")  # ALL or YESTERDAY_72H, DAYS_4_14, etc.

    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    # optional behavior
    only_available: bool = Field(default=False)  # if true, hard-filter to Available

# =========================
# HELPERS
# =========================
def norm_state(v: str) -> str:
    return (v or "").strip().upper()

def norm_zip(v: str) -> str:
    z = "".join(ch for ch in (v or "").strip() if ch.isdigit())
    return z[:5] if len(z) >= 5 else z

def norm_type(v: str) -> str:
    # viewer sends strings like "final_expense" / "veteran_life"
    return (v or "").strip().lower()

def norm_bucket(v: str) -> str:
    b = (v or "").strip().upper()
    if not b or b == "ALL":
        return "ALL"
    aliases = {
        "YESTERDAY_72": "YESTERDAY_72H",
        "YESTERDAY": "YESTERDAY_72H",
        "4_14": "DAYS_4_14",
        "15_30": "DAYS_15_30",
        "31_90": "DAYS_31_90",
        "91_PLUS": "DAYS_91_PLUS",
    }
    return aliases.get(b, b)

def price_for(lead_type_norm: str, bucket_up: str) -> Optional[float]:
    lt = (lead_type_norm or "").strip().lower()
    b = norm_bucket(bucket_up)
    if b == "ALL":
        return None
    m = PRICING.get(lt)
    if not m:
        return None
    return m.get(b)

def caboom_retail_for(lead_type_norm: str) -> Optional[float]:
    lt = (lead_type_norm or "").strip().lower()
    m = PRICING.get(lt)
    if not m:
        return None
    return m.get("CABOOM_RETAIL")

def mongo_id_to_str(d: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in d:
        d["id"] = str(d.pop("_id"))
    return d

# =========================
# ROUTES
# =========================
@app.get("/")
async def root():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": VERSION,
        "db": MONGO_DB,
        "collection": MONGO_COLLECTION,
    }

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/__whoami")
async def whoami():
    return {"ok": True, "file": "main.py", "service": SERVICE_NAME, "version": VERSION}

@app.get("/pricing")
async def pricing():
    return {"ok": True, "pricing": PRICING}

@app.get("/meta/lead-types")
async def meta_lead_types():
    # returns normalized types (strings) that exist in inventory
    vals = await leads_col.distinct("lead_type_norm")
    items = sorted([str(v).strip().lower() for v in vals if v is not None and str(v).strip() != ""])
    return {"ok": True, "items": items}

@app.get("/leads")
async def leads():
    # simplest “is Mongo hooked up to the right collection?”
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "collection": MONGO_COLLECTION}
    doc = mongo_id_to_str(doc)
    return {"ok": True, "sample": doc, "collection": MONGO_COLLECTION}

@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    IMPORTANT BEHAVIOR:
    - Inventory always shows (no filtering), unless only_available=True.
    - Filters are "boosts" that push matching leads to the top.
    - Bucket is derived from createdAt (for your record: createdAt is canonical).

    Expected fields (from your current docs):
      - createdAt (date)
      - lead_type_norm (string)
      - state (string)
      - zip_code (string or number)
      - zip5 (string)
      - status (string) optional
    """
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    boost_type = norm_type(body.lead_type_norm) if body.lead_type_norm else ""
    boost_state = norm_state(body.state) if body.state else ""
    boost_zip = norm_zip(body.zip) if body.zip else ""
    boost_bucket = norm_bucket(body.bucket)

    # Base match — keep it light so "inventory always shows"
    base_match: Dict[str, Any] = {}
    if body.only_available:
        base_match["status"] = "Available"

    # Bucket computed from createdAt
    # days <= 3 -> YESTERDAY_72H
    # days <= 14 -> DAYS_4_14
    # days <= 30 -> DAYS_15_30
    # days <= 90 -> DAYS_31_90
    # else -> DAYS_91_PLUS
    add_bucket = {
        "$addFields": {
            "_daysSince": {
                "$dateDiff": {
                    "startDate": "$createdAt",
                    "endDate": "$$NOW",
                    "unit": "day",
                }
            },
            "_bucket": {
                "$switch": {
                    "branches": [
                        {"case": {"$lte": ["$_daysSince", 3]}, "then": "YESTERDAY_72H"},
                        {"case": {"$lte": ["$_daysSince", 14]}, "then": "DAYS_4_14"},
                        {"case": {"$lte": ["$_daysSince", 30]}, "then": "DAYS_15_30"},
                        {"case": {"$lte": ["$_daysSince", 90]}, "then": "DAYS_31_90"},
                    ],
                    "default": "DAYS_91_PLUS",
                }
            },
        }
    }

    # Score boosts (inventory still returns regardless)
    score_parts: List[Dict[str, Any]] = []

    # Always slightly favor available if field exists
    score_parts.append({
        "$cond": [
            {"$eq": ["$status", "Available"]},
            10,
            0
        ]
    })

    if boost_type:
        score_parts.append({
            "$cond": [
                {"$eq": ["$lead_type_norm", boost_type]},
                100,
                0
            ]
        })

    if boost_state:
        score_parts.append({
            "$cond": [
                {"$eq": ["$state", boost_state]},
                50,
                0
            ]
        })

    if boost_zip:
        # compare against either zip5 or zip_code stringified
        score_parts.append({
            "$cond": [
                {
                    "$or": [
                        {"$eq": ["$zip5", boost_zip]},
                        {"$eq": [{"$toString": "$zip_code"}, boost_zip]},
                    ]
                },
                30,
                0
            ]
        })

    if boost_bucket != "ALL":
        score_parts.append({
            "$cond": [
                {"$eq": ["$_bucket", boost_bucket]},
                20,
                0
            ]
        })

    add_score = {
        "$addFields": {
            "_score": {"$add": score_parts},
        }
    }

    # Sort by score desc, then newest createdAt desc
    sort_stage = {"$sort": {"_score": -1, "createdAt": -1}}

    # Facet for pagination + total
    pipeline: List[Dict[str, Any]] = [
        {"$match": base_match},
        add_bucket,
        add_score,
        sort_stage,
        {
            "$facet": {
                "items": [
                    {"$skip": skip},
                    {"$limit": limit},
                ],
                "meta": [
                    {"$count": "total"}
                ]
            }
        }
    ]

    try:
        out = await leads_col.aggregate(pipeline, allowDiskUse=True).to_list(length=1)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregate failed: {e}")

    if not out:
        return {"ok": True, "page": page, "limit": limit, "total": 0, "items": [], "bucket": boost_bucket}

    items = out[0].get("items", [])
    meta = out[0].get("meta", [])
    total = int(meta[0]["total"]) if meta else 0

    # Post-process: id + pricing fields
    final_items: List[Dict[str, Any]] = []
    for d in items:
        d = mongo_id_to_str(d)

        # expose computed bucket for UI labels
        bucket = d.pop("_bucket", None)
        d.pop("_daysSince", None)

        lt = (d.get("lead_type_norm") or "").strip().lower()
        d["computed_bucket"] = bucket
        d["price"] = price_for(lt, bucket or "ALL")
        d["caboom_retail"] = caboom_retail_for(lt)

        # remove internal score
        d.pop("_score", None)
        final_items.append(d)

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "items": final_items,
        "collection": MONGO_COLLECTION,
        "boosts": {
            "lead_type_norm": boost_type or None,
            "state": boost_state or None,
            "zip": boost_zip or None,
            "bucket": boost_bucket,
        },
        "base_match": base_match,
    }
