# main.py — FULL REPLACEMENT (Render-safe, single app)

from __future__ import annotations

print("🚀 LOADED main.py — v2026-02-03-2", flush=True)

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "leads")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

app = FastAPI(title="Yesterday's Leads API")

# =========================
# BASIC ROUTES (VERIFY DEPLOY)
# =========================
@app.get("/__whoami")
def whoami():
    return {
        "ok": True,
        "file": "main.py",
        "service": "yesterdaysleads",
        "version": "v2026-02-03-2",
    }

@app.get("/health")
def health():
    return {"ok": True}

# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
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
# HELPERS
# =========================
def serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(doc)
    if "_id" in d:
        d["id"] = str(d["_id"])
        del d["_id"]

    for k in ("createdAt", "submittedAt", "submitted_at"):
        if k in d and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()

    return d

def bucket_bounds(bucket: str):
    now = datetime.now(timezone.utc)
    if bucket == "DAYS_4_14":
        return now - timedelta(days=14), now - timedelta(days=4)
    return None, None

# =========================
# LEADS
# =========================
@app.get("/leads")
async def browse_leads(
    bucket: str = Query("DAYS_4_14"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
):
    skip = (page - 1) * limit

    filt: Dict[str, Any] = {}

    if bucket != "ALL":
        start, end = bucket_bounds(bucket)
        if start and end:
            filt["createdAt"] = {"$gte": start, "$lt": end}

    total = await leads_col.count_documents(filt)
    docs = (
        await leads_col.find(filt)
        .sort("createdAt", -1)
        .skip(skip)
        .limit(limit)
        .to_list(length=limit)
    )

    items = [serialize(d) for d in docs]

    return {
        "bucket": bucket,
        "page": page,
        "limit": limit,
        "total": total,
        "items": items,
    }
