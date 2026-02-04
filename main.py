from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

print("🚀 main.py loaded — pricing+bucket from createdAt", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")

# ✅ IMPORTANT: your real Atlas collection is LeadsData (case-sensitive)
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-createdAt-price")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# PRICING (Lead Type x Age Bucket) — matches your Sheet
# =========================
# Buckets:
# - YESTERDAY_72H
# - DAYS_4_14
# - DAYS_15_30
# - DAYS_31_90
# - DAYS_91_PLUS
#
# Lead Type keys (internal):
# - final_expense, life, veteran_life, home, auto, medicare, health, retirement
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
        pass
    return DEFAULT_PRICING

PRICING = load_pricing()

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

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
print(f"🧠 Mongo configured → DB='{db.name}', Collection='{leads_col.name}'", flush=True)

# =========================
# MODELS
# =========================
class LeadsSearchRequest(BaseModel):
    bucket: str = Field(default="ALL")  # ALL or YESTERDAY_72H, DAYS_4_14, etc.
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    # optional filters
    state: Optional[str] = None
    zip: Optional[str] = None
    lead_type_norm: Optional[str] = None
    lead_type_code: Optional[str] = None  # FE/LIFE/VET/HOME/AUTO/MED/HEALTH/RET (optional)


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

def norm_bucket(v: str) -> str:
    b = (v or "").strip()
    if not b:
        return "ALL"
    b = b.upper()
    aliases = {
        "YESTERDAY_72": "YESTERDAY_72H",
        "YESTERDAY": "YESTERDAY_72H",
        "YESTERDAY_72H": "YESTERDAY_72H",
        "4_14": "DAYS_4_14",
        "DAYS_4_14": "DAYS_4_14",
        "15_30": "DAYS_15_30",
        "DAYS_15_30": "DAYS_15_30",
        "31_90": "DAYS_31_90",
        "DAYS_31_90": "DAYS_31_90",
        "91_PLUS": "DAYS_91_PLUS",
        "DAYS_91_PLUS": "DAYS_91_PLUS",
    }
    return aliases.get(b, b)

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

def norm_type_key_from_doc(d: Dict[str, Any]) -> Optional[str]:
    """
    Prefer lead_type_code (FE/LIFE/...) because it's canonical.
    Fallback to lead_type_norm if code missing.
    """
    code = (d.get("lead_type_code") or "").strip().upper()
    if code and code in CODE_TO_KEY:
        return CODE_TO_KEY[code]

    # fallback lead_type_norm -> pricing key
    # allow: "Final Expense", "final_expense", "Veteran Life", "veteran_life"
    ln = (d.get("lead_type_norm") or "").strip().lower()
    if not ln:
        return None
    ln = ln.replace(" ", "_")
    # normalize some common variations
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
            # supports "2026-01-30T00:00:00.000+00:00" and "2026-01-30"
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except Exception:
            return None
    else:
        return None

    # make tz-aware (assume UTC if naive)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def bucket_from_created_at(created_at: Optional[datetime]) -> Optional[str]:
    if not created_at:
        return None
    now = datetime.now(timezone.utc)
    age_days = (now - created_at).total_seconds() / 86400.0

    if age_days < 0:
        # clock skew / future date
        return "YESTERDAY_72H"

    if age_days <= 3.0:
        return "YESTERDAY_72H"
    if 4.0 <= age_days <= 14.0:
        return "DAYS_4_14"
    if 15.0 <= age_days <= 30.0:
        return "DAYS_15_30"
    if 31.0 <= age_days <= 90.0:
        return "DAYS_31_90"
    if age_days >= 91.0:
        return "DAYS_91_PLUS"
    # gap (3-4, 14-15 etc) — push to nearest sensible tier
    if age_days < 4.0:
        return "YESTERDAY_72H"
    if age_days < 15.0:
        return "DAYS_4_14"
    if age_days < 31.0:
        return "DAYS_15_30"
    if age_days < 91.0:
        return "DAYS_31_90"
    return "DAYS_91_PLUS"

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

def bucket_range_query(bucket_up: str) -> Optional[Dict[str, Any]]:
    """
    Build a Mongo query that filters by createdAt/created_at range.
    """
    b = norm_bucket(bucket_up)
    if b == "ALL":
        return None

    now = datetime.now(timezone.utc)

    def or_created(range_q: Dict[str, Any]) -> Dict[str, Any]:
        # apply to either field if your data has both styles
        return {"$or": [{"createdAt": range_q}, {"created_at": range_q}]}

    if b == "YESTERDAY_72H":
        start = now - timedelta(hours=72)
        return or_created({"$gte": start})
    if b == "DAYS_4_14":
        # between 14 and 4 days ago
        older = now - timedelta(days=4)
        newer = now - timedelta(days=14)
        return or_created({"$lte": older, "$gte": newer})
    if b == "DAYS_15_30":
        older = now - timedelta(days=15)
        newer = now - timedelta(days=30)
        return or_created({"$lte": older, "$gte": newer})
    if b == "DAYS_31_90":
        older = now - timedelta(days=31)
        newer = now - timedelta(days=90)
        return or_created({"$lte": older, "$gte": newer})
    if b == "DAYS_91_PLUS":
        cutoff = now - timedelta(days=91)
        return or_created({"$lte": cutoff})

    return None


# =========================
# ROUTES
# =========================
@app.get("/")
async def root():
    return {"ok": True, "service": SERVICE_NAME, "version": VERSION}

@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/__whoami")
async def whoami():
    return {
        "ok": True,
        "file": "main.py",
        "service": SERVICE_NAME,
        "version": VERSION,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
    }

@app.get("/__mongo")
async def __mongo():
    try:
        await client.admin.command("ping")
    except Exception as e:
        return {"ok": False, "error": str(e), "mongo_db": db.name, "mongo_collection": leads_col.name, "version": VERSION}

    total = await leads_col.count_documents({})
    sample = await leads_col.find_one({})
    if isinstance(sample, dict) and "_id" in sample:
        sample["_id"] = str(sample["_id"])

    return {
        "ok": True,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "total": total,
        "sample_keys": sorted(list(sample.keys())) if isinstance(sample, dict) else [],
        "version": VERSION,
    }

@app.get("/pricing")
async def pricing():
    return {"ok": True, "pricing": PRICING, "version": VERSION}

@app.get("/meta/lead-types")
async def meta_lead_types():
    norms = await leads_col.distinct("lead_type_norm")
    codes = await leads_col.distinct("lead_type_code")

    norms_clean = sorted({str(s).strip() for s in norms if s is not None and str(s).strip() and str(s).strip().lower() != "unknown"})
    codes_clean = sorted({str(s).strip().upper() for s in codes if s is not None and str(s).strip() and str(s).strip().lower() != "unknown"})

    return {"ok": True, "lead_type_norm": norms_clean, "lead_type_code": codes_clean, "version": VERSION}

@app.get("/leads")
async def leads():
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "mongo_db": db.name, "mongo_collection": leads_col.name}

    doc = mongo_id_to_str(doc)
    return {"ok": True, "sample": doc, "mongo_db": db.name, "mongo_collection": leads_col.name}

@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Key behavior:
    - Derive bucket from createdAt (fallback created_at) per-lead
    - Derive type via lead_type_code (fallback lead_type_norm)
    - Compute price = PRICING[type_key][bucket]
    """
    and_clauses: List[Dict[str, Any]] = []

    # ---- bucket filter (optional) ----
    bucket_up = norm_bucket(body.bucket)
    brq = bucket_range_query(bucket_up)
    if brq:
        and_clauses.append(brq)

    # ---- state filter ----
    if body.state and str(body.state).strip():
        st = norm_state(body.state)
        # ignore "All States"
        if st not in ("ALL STATES", "ALL", "ANY"):
            and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    # ---- zip filter ----
    if body.zip and str(body.zip).strip():
        z = norm_zip(body.zip)
        if z:
            # zip_code might be stored as int in some imports
            or_zip: List[Dict[str, Any]] = [{"zip5": z}, {"zip_code": z}]
            if z.isdigit():
                or_zip.append({"zip_code": int(z)})
            and_clauses.append({"$or": or_zip})

    # ---- lead type filter (code preferred) ----
    if body.lead_type_code and str(body.lead_type_code).strip():
        code = str(body.lead_type_code).strip().upper()
        if code in CODE_TO_KEY:
            and_clauses.append({"lead_type_code": code})

    elif body.lead_type_norm and str(body.lead_type_norm).strip():
        lt = str(body.lead_type_norm).strip()
        # case-insensitive exact match
        and_clauses.append({"lead_type_norm": {"$regex": f"^{lt}$", "$options": "i"}})

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

    # Sort newest first (createdAt preferred)
    sort = [("createdAt", -1), ("created_at", -1), ("_id", -1)]

    cursor = leads_col.find(q).sort(sort).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    total = await leads_col.count_documents(q)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d = mongo_id_to_str(d)

        created = parse_dt(d.get("createdAt")) or parse_dt(d.get("created_at"))
        bucket = bucket_from_created_at(created)

        type_key = norm_type_key_from_doc(d)

        d["age_bucket"] = bucket  # ✅ UI can display this
        d["price"] = price_for(type_key, bucket)  # ✅ ALWAYS computed from createdAt+type
        d["caboom_retail"] = caboom_retail_for(type_key)

        # helpful debug fields (keep or remove later)
        d["type_key"] = type_key

        items.append(d)

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "bucket_filter": bucket_up,
        "items": items,
        "query": q,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "version": VERSION,
    }
