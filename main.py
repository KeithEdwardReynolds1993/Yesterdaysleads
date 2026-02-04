from __future__ import annotations

import os
from typing import Any, Dict, List, Optional

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

# ✅ IMPORTANT: your real Atlas collection is LeadsData (case-sensitive)
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-3")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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
def _clean_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _is_all_sentinel(v: Optional[str], *, kind: str) -> bool:
    """
    Treat common UI dropdown placeholders as "no filter".
    """
    if v is None:
        return True
    s = str(v).strip().lower()

    common = {
        "", "all", "any", "none", "null", "undefined",
    }

    by_kind = {
        "state": {"all states", "state", "select state"},
        "age": {"all ages", "age", "select age"},
        "zip": {"enter zip", "zip", "zip code"},
        "type": {"all types", "product type", "no types found", "types: none found", "select type"},
        "bucket": {"all", "all buckets"},
    }

    if s in common:
        return True
    return s in by_kind.get(kind, set())

# =========================
# MODELS
# =========================
class LeadsSearchRequest(BaseModel):
    bucket: str = Field(default="ALL")  # ALL or YESTERDAY_72H, DAYS_4_14, etc.
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=25, ge=1, le=200)

    # optional filters (UI may send "All States" etc)
    state: Optional[str] = None
    zip: Optional[str] = None
    lead_type_norm: Optional[str] = None

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
    Proves Render is pointing at the SAME Mongo DB/collection as Compass.
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

@app.get("/leads")
async def leads():
    """
    Absolute simplest Mongo test.
    """
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "mongo_db": db.name, "mongo_collection": leads_col.name}

    doc["id"] = str(doc.pop("_id"))
    return {"ok": True, "sample": doc, "mongo_db": db.name, "mongo_collection": leads_col.name}

# ---- META for UI dropdowns ----
@app.get("/meta/states")
async def meta_states():
    states = await leads_col.distinct("state")
    # normalize, drop junk, sort
    cleaned = sorted({str(s).strip().upper() for s in states if s is not None and str(s).strip() and str(s).strip().lower() != "unknown"})
    return {"ok": True, "states": cleaned, "count": len(cleaned), "version": VERSION}

@app.get("/meta/lead-types")
async def meta_lead_types():
    # your DB may have lead_type_norm or lead_type_code; return both
    norms = await leads_col.distinct("lead_type_norm")
    codes = await leads_col.distinct("lead_type_code")

    norms_clean = sorted({str(s).strip() for s in norms if s is not None and str(s).strip() and str(s).strip().lower() != "unknown"})
    codes_clean = sorted({str(s).strip().upper() for s in codes if s is not None and str(s).strip() and str(s).strip().lower() != "unknown"})

    return {
        "ok": True,
        "lead_type_norm": norms_clean,
        "lead_type_code": codes_clean,
        "version": VERSION,
    }

@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Matches your HTML viewer, but safely ignores UI placeholder values like "All States".
    """
    q: Dict[str, Any] = {}
    and_clauses: List[Dict[str, Any]] = []

    # ---- bucket ----
    bucket_raw = _clean_str(body.bucket)
    if bucket_raw and not _is_all_sentinel(bucket_raw, kind="bucket"):
        b_up = bucket_raw.strip().upper()
        b_low = bucket_raw.strip().lower()
        and_clauses.append({
            "$or": [
                {"sold_tiers": b_up},
                {"lead_age_bucket": b_low},
                {"lead_age_bucket": b_low.replace("_", "-")},
            ]
        })

    # ---- state ----
    state_raw = _clean_str(body.state)
    if state_raw and not _is_all_sentinel(state_raw, kind="state"):
        st = state_raw.strip().upper()
        and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    # ---- zip ----
    zip_raw = _clean_str(body.zip)
    if zip_raw and not _is_all_sentinel(zip_raw, kind="zip"):
        z = "".join(ch for ch in zip_raw if ch.isdigit())
        if len(z) >= 5:
            z = z[:5]
        if z:
            and_clauses.append({
                "$or": [
                    {"zip5": z},
                    {"zip_code": z},
                    {"zip_code": int(z)} if z.isdigit() else {"zip_code": z},
                ]
            })

    # ---- lead type ----
    lt_raw = _clean_str(body.lead_type_norm)
    if lt_raw and not _is_all_sentinel(lt_raw, kind="type"):
        # don't force lowercase; your DB might store "Veteran" not "veteran"
        # do a case-insensitive exact match
        and_clauses.append({"lead_type_norm": {"$regex": f"^{lt_raw.strip()}$", "$options": "i"}})

    if and_clauses:
        q = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    # Pagination
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # Sort newest first (createdAt preferred; fallback created_at)
    sort = [("createdAt", -1), ("created_at", -1), ("_id", -1)]

    cursor = leads_col.find(q).sort(sort).skip(skip).limit(limit)
    docs = await cursor.to_list(length=limit)
    total = await leads_col.count_documents(q)

    items: List[Dict[str, Any]] = []
    for d in docs:
        d["id"] = str(d.pop("_id"))
        items.append(d)

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "items": items,
        "bucket": (bucket_raw.strip().upper() if bucket_raw else "ALL"),
        "query": q,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "version": VERSION,
    }
