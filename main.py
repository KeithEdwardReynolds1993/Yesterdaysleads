from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field


print("🚀 main.py loaded — schema-aligned build", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")
# ✅ CANONICAL
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-schema-aligned")

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
# PRICING (Sheet Authority)
# =========================
# Stored here as default; can override with PRICING_JSON env var.
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

# ✅ Map canonical DB lead_type_code -> pricing row key
CODE_TO_PRICING_KEY: Dict[str, str] = {
    "FE": "final_expense",
    "LIFE": "life",
    "VET": "veteran_life",
    "HOME": "home",
    "AUTO": "auto",
    "MED": "medicare",
    "HEALTH": "health",
    "RET": "retirement",
}

VALID_CODES = set(CODE_TO_PRICING_KEY.keys())

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
    # bucket: ALL or YESTERDAY_72H, DAYS_4_14, DAYS_15_30, DAYS_31_90, DAYS_91_PLUS
    bucket: str = Field(default="ALL")
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    # optional filters
    state: Optional[str] = None
    zip: Optional[str] = None
    lead_type_code: Optional[str] = None  # ✅ FE/LIFE/VET/RET/MED/HOME/HEALTH/AUTO

    # behavior toggles
    include_sold: bool = Field(default=False)   # if true, include fully sold (no available tiers)
    only_available: bool = Field(default=False) # if true, require ANY tier_X == "Available"


# =========================
# HELPERS
# =========================
def norm_zip(v: str) -> str:
    z = "".join(ch for ch in (v or "").strip() if ch.isdigit())
    return z[:5] if len(z) >= 5 else z

def norm_state(v: str) -> str:
    return (v or "").strip().upper()

def norm_code(v: str) -> str:
    return (v or "").strip().upper()

def norm_bucket(v: str) -> str:
    b = (v or "").strip()
    if not b:
        return "ALL"
    b = b.upper()
    aliases = {
        "YESTERDAY_72": "YESTERDAY_72H",
        "YESTERDAY": "YESTERDAY_72H",
        "4_14": "DAYS_4_14",
        "15_30": "DAYS_15_30",
        "31_90": "DAYS_31_90",
        "91_PLUS": "DAYS_91_PLUS",
    }
    return aliases.get(b, b)

def mongo_id_to_str(d: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in d:
        d["id"] = str(d.pop("_id"))
    return d

def pricing_key_for_code(code: str) -> Optional[str]:
    c = norm_code(code)
    return CODE_TO_PRICING_KEY.get(c)

def price_for_code(code: str, bucket_up: str) -> Optional[float]:
    key = pricing_key_for_code(code)
    if not key:
        return None
    m = PRICING.get(key)
    if not m:
        return None
    return m.get(norm_bucket(bucket_up))

def caboom_retail_for_code(code: str) -> Optional[float]:
    key = pricing_key_for_code(code)
    if not key:
        return None
    m = PRICING.get(key)
    if not m:
        return None
    return m.get("CABOOM_RETAIL")

def bucket_date_query(bucket_up: str) -> Optional[Dict[str, Any]]:
    """
    Creates a Mongo range filter on createdAt using NOW (UTC).
    Buckets:
      - YESTERDAY_72H: 0-3 days old (<=72 hours)
      - DAYS_4_14
      - DAYS_15_30
      - DAYS_31_90
      - DAYS_91_PLUS: >=91 days old
    """
    b = norm_bucket(bucket_up)
    if b == "ALL":
        return None

    now = datetime.now(timezone.utc)

    if b == "YESTERDAY_72H":
        return {"createdAt": {"$gte": now - timedelta(days=3)}}

    if b == "DAYS_4_14":
        return {"createdAt": {"$gte": now - timedelta(days=14), "$lt": now - timedelta(days=3)}}

    if b == "DAYS_15_30":
        return {"createdAt": {"$gte": now - timedelta(days=30), "$lt": now - timedelta(days=14)}}

    if b == "DAYS_31_90":
        return {"createdAt": {"$gte": now - timedelta(days=90), "$lt": now - timedelta(days=30)}}

    if b == "DAYS_91_PLUS":
        return {"createdAt": {"$lt": now - timedelta(days=90)}}

    # unknown bucket string -> treat as ALL (no filter)
    return None


# =========================
# ROUTES
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": SERVICE_NAME, "version": VERSION, "db": MONGO_DB, "collection": MONGO_COLLECTION}

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/__whoami")
async def whoami():
    return {"ok": True, "file": "main.py", "service": SERVICE_NAME, "version": VERSION}

@app.get("/pricing")
async def pricing():
    # expose pricing as the backend truth (for UI debug)
    return {
        "ok": True,
        "pricing": PRICING,
        "code_to_pricing_key": CODE_TO_PRICING_KEY,
        "valid_codes": sorted(list(VALID_CODES)),
    }

@app.get("/leads")
async def leads():
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0}
    doc = mongo_id_to_str(doc)
    return {"ok": True, "sample": doc}


@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Canonical search over LeadsData:
      - Filters:
          lead_type_code (FE/LIFE/VET/RET/MED/HOME/HEALTH/AUTO)
          state
          zip_code (5 digit)
          age bucket via createdAt
      - Sold logic:
          tier_1..tier_5 each represent sellable availability.
          only_available=True => ANY tier_X == "Available"
          include_sold=False => EXCLUDE leads where ALL tier_X != "Available"
    """
    bucket_up = norm_bucket(body.bucket)
    and_clauses: List[Dict[str, Any]] = []

    # Bucket filtering via createdAt
    bq = bucket_date_query(bucket_up)
    if bq:
        and_clauses.append(bq)

    # Lead type filter (canonical)
    if body.lead_type_code:
        code = norm_code(body.lead_type_code)
        and_clauses.append({"lead_type_code": code})

    # State filter (canonical 'state')
    if body.state:
        st = norm_state(body.state)
        and_clauses.append({"state": st})

    # ZIP filter (canonical 'zip_code' but may be stored as number or string)
    if body.zip:
        z = norm_zip(body.zip)
        if z:
            and_clauses.append({
                "$or": [
                    {"zip_code": z},
                    {"zip_code": int(z)} if z.isdigit() else {"zip_code": z},
                ]
            })

    # Tier availability logic
    tier_any_available = {
        "$or": [
            {"tier_1": "Available"},
            {"tier_2": "Available"},
            {"tier_3": "Available"},
            {"tier_4": "Available"},
            {"tier_5": "Available"},
        ]
    }

    if body.only_available:
        and_clauses.append(tier_any_available)

    if not body.include_sold:
        # exclude fully sold (no available tiers)
        and_clauses.append(tier_any_available)

    # Final query
    if not and_clauses:
        q: Dict[str, Any] = {}
    elif len(and_clauses) == 1:
        q = and_clauses[0]
    else:
        q = {"$and": and_clauses}

    # Pagination
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # Sort newest first (createdAt exists and is Date now)
    sort: List[Tuple[str, int]] = [("createdAt", -1), ("external_id", -1)]

    cursor = leads_col.find(q).sort(sort).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    total = await leads_col.count_documents(q)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d = mongo_id_to_str(d)

        code = norm_code(d.get("lead_type_code") or "")
        if bucket_up != "ALL":
            d["price"] = price_for_code(code, bucket_up)
        else:
            d["price"] = None

        d["caboom_retail"] = caboom_retail_for_code(code)
        d["bucket"] = bucket_up
        items.append(d)

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "items": items,
        "bucket": bucket_up,
        "query": q,
    }
