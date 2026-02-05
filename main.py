from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field


# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")  # case-sensitive

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-05-minimal")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# PRICING (Lead Type x Age Bucket)
# =========================
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
                    out[str(k).strip().lower()] = {
                        str(bucket).strip().upper(): float(price)
                        for bucket, price in v.items()
                    }
            return out or DEFAULT_PRICING
    except Exception:
        return DEFAULT_PRICING
    return DEFAULT_PRICING

PRICING = load_pricing()

# Map canonical lead_type_code -> pricing key
CODE_TO_KEY = {
    "FE": "final_expense",
    "LIFE": "life",
    "VET": "veteran_life",
    "HOME": "home",
    "AUTO": "auto",
    "MED": "medicare",
    "HEALTH": "health",
    "RET": "retirement",
}

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API (Minimal)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten later
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
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    # hard filters only (frontend can do boost/sort)
    state: Optional[str] = None
    zip: Optional[str] = None
    lead_type_norm: Optional[str] = None
    lead_type_code: Optional[str] = None  # FE/LIFE/VET/HOME/AUTO/MED/HEALTH/RET

    # Optional: restrict to available only
    available_only: bool = Field(default=True)


# =========================
# HELPERS
# =========================
def mongo_id_to_str(d: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in d:
        d["id"] = str(d.pop("_id"))
    return d

def norm_zip(v: str) -> str:
    z = "".join(ch for ch in (v or "").strip() if ch.isdigit())
    return z[:5] if len(z) >= 5 else z

def norm_state(v: str) -> str:
    return (v or "").strip().upper()

def parse_dt(v: Any) -> Optional[datetime]:
    if v is None:
        return None
    if isinstance(v, datetime):
        dt = v
    elif isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def bucket_from_created_at(created_at: Optional[datetime]) -> Optional[str]:
    if not created_at:
        return None
    now = datetime.now(timezone.utc)
    age_days = (now - created_at).total_seconds() / 86400.0

    if age_days <= 3.0:
        return "YESTERDAY_72H"
    if age_days <= 14.0:
        return "DAYS_4_14"
    if age_days <= 30.0:
        return "DAYS_15_30"
    if age_days <= 90.0:
        return "DAYS_31_90"
    return "DAYS_91_PLUS"

def type_key_from_doc(d: Dict[str, Any]) -> Optional[str]:
    code = (d.get("lead_type_code") or "").strip().upper()
    if code in CODE_TO_KEY:
        return CODE_TO_KEY[code]

    ln = (d.get("lead_type_norm") or "").strip().lower().replace(" ", "_")
    if ln in ("veteran", "vet", "veteranlife", "veteran_life"):
        return "veteran_life"
    if ln in ("finalexpense", "final_expense"):
        return "final_expense"
    if ln in ("med", "medicare"):
        return "medicare"
    if ln in ("ret", "retirement"):
        return "retirement"
    if ln in PRICING:
        return ln
    return None

def price_for(type_key: Optional[str], bucket: Optional[str]) -> Optional[float]:
    if not type_key or not bucket:
        return None
    m = PRICING.get(type_key)
    if not m:
        return None
    return m.get(bucket)

def caboom_retail_for(type_key: Optional[str]) -> Optional[float]:
    if not type_key:
        return None
    m = PRICING.get(type_key)
    if not m:
        return None
    return m.get("CABOOM_RETAIL")

def allowlist_item(d: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return only fields the frontend needs (NO PII).
    """
    return {
        "id": d.get("id"),
        "external_id": d.get("external_id"),
        "lead_type_norm": d.get("lead_type_norm"),
        "lead_type_code": d.get("lead_type_code"),
        "state": d.get("state") or d.get("state2"),
        "zip": d.get("zip5") or d.get("zip_code"),
        "createdAt": d.get("createdAt") or d.get("created_at"),
        "tier_1": d.get("tier_1"),
        "tier_2": d.get("tier_2"),
        "tier_3": d.get("tier_3"),
        "tier_4": d.get("tier_4"),
        "tier_5": d.get("tier_5"),
        "age_bucket": d.get("age_bucket"),
        "price": d.get("price"),
        "caboom_retail": d.get("caboom_retail"),
    }


# =========================
# ROUTES
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": SERVICE_NAME, "version": VERSION}

@app.get("/health")
async def health():
    return {"ok": True, "version": VERSION}

@app.get("/__mongo")
async def __mongo():
    try:
        await client.admin.command("ping")
    except Exception as e:
        return {"ok": False, "error": str(e), "version": VERSION}

    total = await leads_col.count_documents({})
    return {
        "ok": True,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "total": total,
        "version": VERSION,
    }

@app.get("/meta/lead-types")
async def meta_lead_types():
    norms = await leads_col.distinct("lead_type_norm")
    codes = await leads_col.distinct("lead_type_code")

    norms_clean = sorted({str(s).strip() for s in norms if s is not None and str(s).strip()})
    codes_clean = sorted({str(s).strip().upper() for s in codes if s is not None and str(s).strip()})
    return {"ok": True, "lead_type_norm": norms_clean, "lead_type_code": codes_clean, "version": VERSION}

@app.get("/pricing")
async def pricing():
    return {"ok": True, "pricing": PRICING, "version": VERSION}

@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Essentials-only:
    - Hard filters only (state/zip/type + available_only)
    - Pagination
    - Returns per-lead age_bucket + price based on each lead's createdAt
    - Does NOT use any UI-selected age range to compute price
    """
    and_clauses: List[Dict[str, Any]] = []

    # Available-only (any tier Available)
    if body.available_only:
        and_clauses.append({
            "$or": [
                {"tier_1": "Available"},
                {"tier_2": "Available"},
                {"tier_3": "Available"},
                {"tier_4": "Available"},
                {"tier_5": "Available"},
            ]
        })

    # State filter
    if body.state and body.state.strip():
        st = norm_state(body.state)
        if st not in ("ALL", "ANY", "ALL STATES"):
            and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    # Zip filter
    if body.zip and body.zip.strip():
        z = norm_zip(body.zip)
        if z:
            or_zip: List[Dict[str, Any]] = [{"zip5": z}, {"zip_code": z}]
            if z.isdigit():
                or_zip.append({"zip_code": int(z)})
            and_clauses.append({"$or": or_zip})

    # Lead type filter (code preferred)
    if body.lead_type_code and body.lead_type_code.strip():
        code = body.lead_type_code.strip().upper()
        and_clauses.append({"lead_type_code": code})

    elif body.lead_type_norm and body.lead_type_norm.strip():
        lt = body.lead_type_norm.strip()
        and_clauses.append({"lead_type_norm": {"$regex": f"^{lt}$", "$options": "i"}})

    # Build query
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

    total = await leads_col.count_documents(q)

    # Stable sort: newest first
    cursor = (
        leads_col.find(q)
        .sort([("createdAt", -1), ("_id", -1)])
        .skip(skip)
        .limit(limit)
    )

    docs = await cursor.to_list(length=limit)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d = mongo_id_to_str(d)

        created = parse_dt(d.get("createdAt")) or parse_dt(d.get("created_at"))
        age_bucket = bucket_from_created_at(created)

        type_key = type_key_from_doc(d)
        d["age_bucket"] = age_bucket
        d["price"] = price_for(type_key, age_bucket)
        d["caboom_retail"] = caboom_retail_for(type_key)

        items.append(allowlist_item(d))

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "items": items,
        "version": VERSION,
    }
