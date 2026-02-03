from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field


print("🚀 main.py loaded — sanity build", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "leads")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-03-4")

# CORS
# Put your real domains here (and keep localhost for dev).
# If you want to go wide-open temporarily, set CORS_ALLOW_ALL=1
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
# You can override this at deploy-time with env var PRICING_JSON (recommended).
# Format:
# {
#   "final_expense": {"YESTERDAY_72H": 15.0, "DAYS_4_14": 10.0, ... , "CABOOM_RETAIL": 25.0},
#   "life": {...}
# }
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
            # normalize keys
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
    lead_type_norm: Optional[str] = None

    # behavior toggles
    include_sold: bool = Field(default=False)   # if true, allows already-sold-in-tier to show up
    only_available: bool = Field(default=False) # if true, require status == "Available"


# =========================
# HELPERS
# =========================
def norm_zip(v: str) -> str:
    z = "".join(ch for ch in (v or "").strip() if ch.isdigit())
    return z[:5] if len(z) >= 5 else z

def norm_state(v: str) -> str:
    return (v or "").strip().upper()

def norm_type(v: str) -> str:
    return (v or "").strip().lower()

def norm_bucket(v: str) -> str:
    b = (v or "").strip()
    if not b:
        return "ALL"
    b = b.upper()
    # allow friendly inputs
    aliases = {
        "YESTERDAY_72": "YESTERDAY_72H",
        "YESTERDAY": "YESTERDAY_72H",
        "DAYS_4_14": "DAYS_4_14",
        "4_14": "DAYS_4_14",
        "DAYS_15_30": "DAYS_15_30",
        "15_30": "DAYS_15_30",
        "DAYS_31_90": "DAYS_31_90",
        "31_90": "DAYS_31_90",
        "DAYS_91_PLUS": "DAYS_91_PLUS",
        "91_PLUS": "DAYS_91_PLUS",
    }
    return aliases.get(b, b)

def bucket_lower_forms(b_up: str) -> List[str]:
    # docs often store lead_age_bucket lowercase like "days_4_14"
    b_low = b_up.lower()
    return list({b_low, b_low.replace("-", "_"), b_low.replace("_", "-")})

def price_for(lead_type_norm: str, bucket_up: str) -> Optional[float]:
    lt = (lead_type_norm or "").strip().lower()
    b = norm_bucket(bucket_up)
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
    # prevents "Not Found" confusion at base URL
    return {"ok": True, "service": SERVICE_NAME, "version": VERSION}

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/__whoami")
async def whoami():
    return {"ok": True, "file": "main.py", "service": SERVICE_NAME, "version": VERSION}

@app.get("/pricing")
async def pricing():
    # lets Bret / UI confirm backend pricing
    return {"ok": True, "pricing": PRICING}

@app.get("/leads")
async def leads():
    """
    Absolute simplest Mongo test.
    If this works, Mongo + deploy are correct.
    """
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0}
    doc = mongo_id_to_str(doc)
    return {"ok": True, "sample": doc}


@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Tier-aware search with "bought once per tier" logic.

    Fields we support (from your sample):
      - sold_tiers: ["YESTERDAY_72H", ...]  (UPPERCASE)
      - lead_age_bucket: "days_4_14" / "yesterday_72h" (lowercase)
      - state OR state2
      - zip5 OR zip_code
      - lead_type_norm
      - status (optional)
    """
    bucket_up = norm_bucket(body.bucket)

    and_clauses: List[Dict[str, Any]] = []

    # Bucket filtering
    if bucket_up != "ALL":
        # require that doc belongs to that tier (by sold_tiers OR age_bucket)
        or_bucket: List[Dict[str, Any]] = [{"sold_tiers": bucket_up}]
        for b_low in bucket_lower_forms(bucket_up):
            or_bucket.append({"lead_age_bucket": b_low})
            or_bucket.append({"lead_age_bucket": b_low.replace("_", "-")})

        and_clauses.append({"$or": or_bucket})

        # "Bought once per tier": hide leads that already have this tier in sold_tiers
        # (unless include_sold=True)
        if not body.include_sold:
            and_clauses.append({"sold_tiers": {"$ne": bucket_up}})

    # State filter
    if body.state:
        st = norm_state(body.state)
        and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    # ZIP filter
    if body.zip:
        z = norm_zip(body.zip)
        if z:
            and_clauses.append({"$or": [{"zip5": z}, {"zip_code": z}]})

    # Lead type filter
    if body.lead_type_norm:
        lt = norm_type(body.lead_type_norm)
        and_clauses.append({"lead_type_norm": lt})

    # Availability filter (optional toggle)
    if body.only_available:
        and_clauses.append({"status": "Available"})

    # Final query
    q: Dict[str, Any]
    if not and_clauses:
        q = {}
    elif len(and_clauses) == 1:
        q = and_clauses[0]
    else:
        q = {"$and": and_clauses}

    # Pagination
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # Sort newest first (use submittedAt/createdAt if present)
    sort: List[Tuple[str, int]] = [("submittedAt", -1), ("createdAt", -1)]

    cursor = leads_col.find(q).sort(sort).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    total = await leads_col.count_documents(q)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d = mongo_id_to_str(d)

        lt = (d.get("lead_type_norm") or "").strip().lower()
        # If browsing a tier, show tier price. If bucket=ALL, return None (UI can decide).
        if bucket_up != "ALL":
            d["price"] = price_for(lt, bucket_up)
        else:
            d["price"] = None

        d["caboom_retail"] = caboom_retail_for(lt)
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
