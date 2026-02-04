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

# ✅ IMPORTANT: case-sensitive collection name (your Atlas shows LeadsData)
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-2")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

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

print(f"🧠 Mongo connected → DB='{db.name}', Collection='{leads_col.name}'", flush=True)

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

    # ✅ accept either (UI can send one; API will match both)
    lead_type_code: Optional[str] = None   # FE, LIFE, VET, RET, MED, HOME, HEALTH, AUTO
    lead_type_norm: Optional[str] = None   # final_expense, life, veteran_life, etc. (if present)


# =========================
# HELPERS
# =========================
def _id_to_str(d: Dict[str, Any]) -> Dict[str, Any]:
    if "_id" in d:
        d["id"] = str(d.pop("_id"))
    return d

def _norm_zip(z: str) -> str:
    s = "".join(ch for ch in (z or "").strip() if ch.isdigit())
    return s[:5] if len(s) >= 5 else s

def _norm_state(st: str) -> str:
    return (st or "").strip().upper()

def _norm_code(code: str) -> str:
    return (code or "").strip().upper()

def _norm_norm(v: str) -> str:
    # "Veteran Life" -> "veteran_life"
    return (v or "").strip().lower().replace(" ", "_")

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
        "mongo_db": MONGO_DB,
        "mongo_collection": MONGO_COLLECTION,
    }

@app.get("/__mongo")
async def __mongo():
    return {"ok": True, "mongo_db": db.name, "mongo_collection": leads_col.name}

@app.get("/__counts")
async def __counts():
    """
    🔥 This tells us immediately if Render is pointed at the SAME data you see in Atlas.
    """
    total = await leads_col.count_documents({})
    # quick sanity samples
    has_createdAt = await leads_col.count_documents({"createdAt": {"$exists": True}})
    has_lead_type_code = await leads_col.count_documents({"lead_type_code": {"$exists": True}})
    has_lead_type_norm = await leads_col.count_documents({"lead_type_norm": {"$exists": True}})

    return {
        "ok": True,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "total": total,
        "has_createdAt": has_createdAt,
        "has_lead_type_code": has_lead_type_code,
        "has_lead_type_norm": has_lead_type_norm,
    }

@app.get("/meta/lead-types")
async def meta_lead_types():
    """
    UI should load Product Types from lead_type_code (your data shows AUTO/FE/etc).
    Returns both lists when available.
    """
    codes = await leads_col.distinct("lead_type_code")
    norms = await leads_col.distinct("lead_type_norm")

    codes_clean = sorted([c for c in (str(x).strip().upper() for x in codes) if c])
    norms_clean = sorted([n for n in (str(x).strip().lower() for x in norms) if n])

    return {
        "ok": True,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
        "lead_type_code": codes_clean,
        "lead_type_norm": norms_clean,
    }

@app.get("/leads")
async def leads():
    """
    Absolute simplest Mongo test.
    """
    total = await leads_col.count_documents({})
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "total": total, "mongo_db": db.name, "mongo_collection": leads_col.name}

    doc = _id_to_str(doc)
    return {"ok": True, "count": 1, "total": total, "sample": doc, "mongo_db": db.name, "mongo_collection": leads_col.name}

@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    and_clauses: List[Dict[str, Any]] = []

    # Bucket (optional)
    bucket_raw = (body.bucket or "").strip()
    if bucket_raw and bucket_raw.upper() != "ALL":
        b_up = bucket_raw.upper()
        b_low = bucket_raw.lower()
        and_clauses.append(
            {"$or": [
                {"sold_tiers": b_up},
                {"lead_age_bucket": b_low},
                {"lead_age_bucket": b_low.replace("_", "-")},
            ]}
        )

    # State
    if body.state:
        st = _norm_state(body.state)
        and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    # ZIP
    if body.zip:
        z = _norm_zip(body.zip)
        if z:
            and_clauses.append({"$or": [{"zip5": z}, {"zip_code": z}]})

    # ✅ Lead type: support BOTH fields
    lt_code = _norm_code(body.lead_type_code) if body.lead_type_code else ""
    lt_norm = _norm_norm(body.lead_type_norm) if body.lead_type_norm else ""

    if lt_code and lt_norm:
        and_clauses.append({"$or": [{"lead_type_code": lt_code}, {"lead_type_norm": lt_norm}]})
    elif lt_code:
        and_clauses.append({"lead_type_code": lt_code})
    elif lt_norm:
        and_clauses.append({"lead_type_norm": lt_norm})

    q: Dict[str, Any] = {}
    if and_clauses:
        q = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    # Pagination
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # Sort newest first
    sort = [("submittedAt", -1), ("createdAt", -1)]

    docs = await leads_col.find(q).sort(sort).skip(skip).limit(limit).to_list(length=limit)
    total = await leads_col.count_documents(q)

    items: List[Dict[str, Any]] = []
    for d in docs:
        items.append(_id_to_str(d))

    return {
        "ok": True,
        "page": page,
        "limit": limit,
        "total": total,
        "items": items,
        "bucket": bucket_raw.upper() if bucket_raw else "ALL",
        "query": q,
        "mongo_db": db.name,
        "mongo_collection": leads_col.name,
    }
