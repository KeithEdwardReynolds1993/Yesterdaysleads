from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

print("🚀 main.py loaded — BOOST RANK build", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")

# ✅ IMPORTANT: your real Atlas collection is LeadsData (case-sensitive)
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-BOOST")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # viewer-friendly; tighten later
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
# HELPERS
# =========================
def _s(v: Optional[str]) -> str:
    return (str(v).strip() if v is not None else "")

def _zip5(v: Optional[str]) -> str:
    z = "".join(ch for ch in _s(v) if ch.isdigit())
    return z[:5] if len(z) >= 5 else z

def _upper(v: Optional[str]) -> str:
    return _s(v).upper()

def _bucket_up(v: Optional[str]) -> str:
    b = _s(v)
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
        "91+": "DAYS_91_PLUS",
    }
    return aliases.get(b, b)

def _bucket_low_forms(b_up: str) -> List[str]:
    b = b_up.lower()
    return list({b, b.replace("-", "_"), b.replace("_", "-")})

def _is_allish(v: Optional[str]) -> bool:
    s = _s(v).lower()
    return s in {"", "all", "any", "none", "null", "undefined", "all states", "all ages", "all types"}

def _mongo_id_to_str(d: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in d:
        d["id"] = str(d.pop("_id"))
    return d

def _clean_type_norm(v: Optional[str]) -> str:
    """
    UI may send:
      - "final_expense"
      - "Final Expense"
      - "FE"
    DB may have:
      - lead_type_norm like "Final Expense" OR "final_expense"
      - lead_type_code like "FE"
    We handle both.
    """
    s = _s(v)
    if not s:
        return ""
    return s.strip()

# =========================
# MODELS
# =========================
class LeadsSearchRequest(BaseModel):
    # These are BOOST inputs now (they do not hard-filter inventory)
    bucket: str = Field(default="ALL")     # ALL or YESTERDAY_72H, DAYS_4_14, etc.
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    state: Optional[str] = None           # "AL"
    zip: Optional[str] = None             # "70607"
    lead_type_norm: Optional[str] = None  # "final_expense" OR "Final Expense" etc.

    # Optional toggle if you want: only show Available
    only_available: bool = Field(default=False)

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
    """
    Proves Render is pointing at the same DB/collection you expect.
    """
    try:
        await client.admin.command("ping")
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "mongo_db": db.name,
            "mongo_collection": leads_col.name,
            "version": VERSION,
        }

    total = await leads_col.count_documents({})
    sample = await leads_col.find_one({})
    if isinstance(sample, dict):
        sample = {k: (str(v) if k == "_id" else v) for k, v in sample.items()}

    return {
        "ok": True,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "total": total,
        "sample_keys": sorted(list(sample.keys())) if isinstance(sample, dict) else [],
        "version": VERSION,
    }

@app.get("/leads")
async def leads():
    """
    Simplest Mongo test: returns 1 sample lead.
    """
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "mongo_db": db.name, "mongo_collection": leads_col.name}
    doc = _mongo_id_to_str(doc)
    return {"ok": True, "sample": doc, "mongo_db": db.name, "mongo_collection": leads_col.name}

# ---- META for UI dropdowns ----
@app.get("/meta/states")
async def meta_states():
    # state may live in state or state2 depending on dataset
    s1 = await leads_col.distinct("state")
    s2 = await leads_col.distinct("state2")
    all_states = set()

    for x in (s1 or []):
        v = _upper(x)
        if v and v != "UNKNOWN":
            all_states.add(v)
    for x in (s2 or []):
        v = _upper(x)
        if v and v != "UNKNOWN":
            all_states.add(v)

    out = sorted(all_states)
    return {"ok": True, "states": out, "count": len(out), "version": VERSION}

@app.get("/meta/lead-types")
async def meta_lead_types():
    norms = await leads_col.distinct("lead_type_norm")
    codes = await leads_col.distinct("lead_type_code")

    norms_clean = sorted({str(x).strip() for x in (norms or []) if x is not None and str(x).strip() and str(x).strip().lower() != "unknown"})
    codes_clean = sorted({str(x).strip().upper() for x in (codes or []) if x is not None and str(x).strip() and str(x).strip().lower() != "unknown"})

    return {
        "ok": True,
        "lead_type_norm": norms_clean,
        "lead_type_code": codes_clean,
        "version": VERSION,
    }

# =========================
# SEARCH (BOOST RANK)
# =========================
@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Inventory ALWAYS shows.
    Controls only BOOST what appears at the top.

    Boost scoring (tweak any time):
      +100: lead type matches (norm or code)
      +60 : state matches (state/state2)
      +40 : zip matches (zip5/zip_code)
      +30 : bucket matches (sold_tiers or lead_age_bucket)
    """
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # ---- BOOST inputs (do not filter inventory) ----
    boost_state = "" if _is_allish(body.state) else _upper(body.state)
    boost_zip = "" if _is_allish(body.zip) else _zip5(body.zip)
    boost_bucket = _bucket_up(body.bucket)
    boost_type_raw = "" if _is_allish(body.lead_type_norm) else _clean_type_norm(body.lead_type_norm)

    # allow type boost via:
    # - lead_type_code exact like "FE"
    # - lead_type_norm case-insensitive exact like "Final Expense"
    # - lead_type_norm underscore style like "final_expense"
    boost_type_code = boost_type_raw.strip().upper()
    boost_type_norm_exact = boost_type_raw.strip()
    boost_type_norm_underscored = boost_type_raw.strip().lower().replace(" ", "_")

    # ---- base match (optional) ----
    match: Dict[str, Any] = {}
    if body.only_available:
        match = {"status": "Available"}

    # ---- total inventory (for UI) ----
    total = await leads_col.count_documents(match)

    # ---- compute boost_score in pipeline ----
    score_parts: List[Dict[str, Any]] = []

    # Lead Type boost (code OR norm exact OR norm underscored)
    if boost_type_raw:
        score_parts.append({
            "$cond": [
                {
                    "$or": [
                        {"$eq": ["$lead_type_code", boost_type_code]},
                        {"$regexMatch": {"input": {"$toString": "$lead_type_norm"}, "regex": f"^{boost_type_norm_exact}$", "options": "i"}},
                        {"$regexMatch": {"input": {"$toString": "$lead_type_norm"}, "regex": f"^{boost_type_norm_underscored}$", "options": "i"}},
                    ]
                },
                100,
                0
            ]
        })

    # State boost (state or state2)
    if boost_state:
        score_parts.append({
            "$cond": [
                {"$or": [{"$eq": ["$state", boost_state]}, {"$eq": ["$state2", boost_state]}]},
                60,
                0
            ]
        })

    # ZIP boost (zip5 or zip_code string/int)
    if boost_zip:
        score_parts.append({
            "$cond": [
                {
                    "$or": [
                        {"$eq": ["$zip5", boost_zip]},
                        {"$eq": [{"$toString": "$zip_code"}, boost_zip]},
                    ]
                },
                40,
                0
            ]
        })

    # Bucket boost (sold_tiers contains bucket OR lead_age_bucket matches any lower form)
    if boost_bucket and boost_bucket != "ALL":
        low_forms = _bucket_low_forms(boost_bucket)
        score_parts.append({
            "$cond": [
                {
                    "$or": [
                        {"$in": [boost_bucket, {"$ifNull": ["$sold_tiers", []]}]},
                        {"$in": ["$lead_age_bucket", low_forms]},
                    ]
                },
                30,
                0
            ]
        })

    if not score_parts:
        score_expr: Any = 0
    else:
        score_expr = {"$add": score_parts}

    pipeline: List[Dict[str, Any]] = [
        {"$match": match},
        {"$addFields": {"boost_score": score_expr}},
        # newest first after boost; createdAt is your real field
        {"$sort": {"boost_score": -1, "createdAt": -1, "_id": -1}},
        {"$skip": skip},
        {"$limit": limit},
    ]

    docs = await leads_col.aggregate(pipeline).to_list(length=limit)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d = _mongo_id_to_str(d)
        items.append(d)

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,            # total inventory
        "items": items,
        "boost": {
            "lead_type": boost_type_raw,
            "state": boost_state,
            "zip": boost_zip,
            "bucket": boost_bucket,
        },
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "version": VERSION,
    }
