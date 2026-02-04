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

# ✅ IMPORTANT: your real Atlas collection is LeadsData (per your screenshots)
# You can still override with env var, but the default must be correct.
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "LeadsData")

SERVICE_NAME = os.environ.get("SERVICE_NAME", "yesterdaysleads")
VERSION = os.environ.get("VERSION", "v2026-02-04-1")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

# CORS (safe for your simple viewer; tighten later)
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
if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# IMPORTANT:
# Mongo collection names ARE case-sensitive.
# Your real collection is: LeadsData
client = AsyncIOMotorClient(MONGO_URI)

db = client[os.environ.get("MONGO_DB", "leads")]

leads_col = db[os.environ.get("MONGO_COLLECTION", "LeadsData")]

print(
    f"🧠 Mongo connected → DB='{db.name}', Collection='{leads_col.name}'",
    flush=True
)


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


@app.get("/leads")
async def leads():
    """
    Absolute simplest Mongo test.
    If this returns a sample doc, your Render deploy + Mongo collection are correct.
    """
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0, "mongo_db": MONGO_DB, "mongo_collection": MONGO_COLLECTION}

    doc["id"] = str(doc.pop("_id"))
    return {"ok": True, "sample": doc, "mongo_db": MONGO_DB, "mongo_collection": MONGO_COLLECTION}


@app.post("/leads/search")
async def leads_search(body: LeadsSearchRequest):
    """
    Search endpoint that matches your HTML viewer.

    Expected doc fields (based on your sample):
      - lead_age_bucket: "days_4_14" / "yesterday_72h" / etc (lowercase)
      - sold_tiers: ["YESTERDAY_72H", ...] (uppercase)
      - state or state2
      - zip_code / zip5
      - lead_type_norm
      - status (optional)
    """
    q: Dict[str, Any] = {}
    and_clauses: List[Dict[str, Any]] = []

    # Bucket handling (support BOTH sold_tiers and lead_age_bucket)
    if body.bucket and body.bucket.strip().upper() != "ALL":
        b_up = body.bucket.strip().upper()
        b_low = body.bucket.strip().lower()
        and_clauses.append({
            "$or": [
                {"sold_tiers": b_up},
                {"lead_age_bucket": b_low},
                {"lead_age_bucket": b_low.replace("_", "-")},
            ]
        })

    if body.state:
        st = body.state.strip().upper()
        and_clauses.append({"$or": [{"state": st}, {"state2": st}]})

    if body.zip:
        z = "".join(ch for ch in body.zip.strip() if ch.isdigit())
        if len(z) >= 5:
            z = z[:5]
        if z:
            and_clauses.append({"$or": [{"zip5": z}, {"zip_code": z}]})

    if body.lead_type_norm:
        lt = body.lead_type_norm.strip().lower()
        and_clauses.append({"lead_type_norm": lt})

    if and_clauses:
        q = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    # Pagination
    page = int(body.page)
    limit = int(body.limit)
    skip = (page - 1) * limit

    # Sort newest first (use submittedAt/createdAt if present)
    sort = [("submittedAt", -1), ("createdAt", -1)]

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
        "bucket": body.bucket.strip().upper() if body.bucket else "ALL",
        "query": q,
    }
