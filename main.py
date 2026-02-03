from __future__ import annotations

import os
from typing import Any, Dict

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

print("🚀 main.py loaded — sanity build", flush=True)

# =========================
# CONFIG
# =========================
MONGO_URI = os.environ.get("MONGO_URI")
MONGO_DB = os.environ.get("MONGO_DB", "leads")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "leads")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI")

# =========================
# APP
# =========================
app = FastAPI(title="Yesterday's Leads API")

# =========================
# DB
# =========================
client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]
leads_col = db[MONGO_COLLECTION]

# =========================
# ROUTES
# =========================
@app.get("/health")
async def health():
    return {"ok": True}

@app.get("/__whoami")
async def whoami():
    return {
        "ok": True,
        "file": "main.py",
        "service": "yesterdaysleads",
        "version": "v2026-02-03-2",
    }

@app.get("/leads")
async def leads():
    """
    Absolute simplest Mongo test.
    If this works, Mongo + deploy are 100% correct.
    """
    doc = await leads_col.find_one({})
    if not doc:
        return {"ok": True, "count": 0}

    doc["id"] = str(doc.pop("_id"))
    return {"ok": True, "sample": doc}
