from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import FastAPI, Query, Response
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pydantic import BaseModel, Field

# =========================
# CONFIG (ENV VARS)
# =========================
MONGO_URI = os.environ.get("MONGO_URI")  # mongodb+srv://...
MONGO_DB = os.environ.get("MONGO_DB", "leads")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "leads")

if not MONGO_URI:
    raise RuntimeError("Missing env var MONGO_URI (mongodb+srv://...)")

app = FastAPI(title="Yesterday's Leads API")

# =========================
# CORS (FIXES WP/Elementor FETCH)
# IMPORTANT: Origins MUST match exactly what the browser sends.
# =========================
ALLOWED_ORIGINS = [
    "https://castudios.tv",
    "https://code.flywheelsites.com",
    "https://www.code.flywheelsites.com",
    "https://first-wrist.flywheelsites.com",
    "http://localhost:3000",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=86400,
)

client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]
leads_col = db[MONGO_COLLECTION]

# =========================
# CONSTANTS
# =========================
PRICE_BY_BUCKET = {
    "YESTERDAY_72H": 4.50,
    "DAYS_4_14": 3.75,
    "DAYS_15_30": 3.00,
    "DAYS_31_90": 2.25,
    "DAYS_91_PLUS": 1.50,
}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def age_filter(bucket: str) -> Dict[str, Any]:
    """
    "Never show 0 days" rule: 1–3 days => YESTERDAY_72H
    """
    now = utcnow()
    if bucket == "YESTERDAY_72H":
        return {"submitted_at": {"$gte": now - timedelta(days=3), "$lte": now}}
    if bucket == "DAYS_4_14":
        return {"submitted_at": {"$gte": now - timedelta(days=14), "$lt": now - timedelta(days=3)}}
    if bucket == "DAYS_15_30":
        return {"submitted_at": {"$gte": now - timedelta(days=30), "$lt": now - timedelta(days=14)}}
    if bucket == "DAYS_31_90":
        return {"submitted_at": {"$gte": now - timedelta(days=90), "$lt": now - timedelta(days=30)}}
    if bucket == "DAYS_91_PLUS":
        return {"submitted_at": {"$lt": now - timedelta(days=90)}}
    return {}  # ALL or unknown


def serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(doc)
    if "_id" in d:
        d["id"] = str(d["_id"])
        del d["_id"]
    for k in ("submitted_at", "createdAt", "updatedAt"):
        if k in d and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d


# =========================
# MODELS
# =========================
class LeadOut(BaseModel):
    id: str
    name: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    type_of_coverage: Optional[str] = None
    submitted_at: Optional[str] = None

    # computed only in ALL mode:
    age_days: Optional[int] = None
    bucket: Optional[str] = None
    price: Optional[float] = None


class LeadsResponse(BaseModel):
    bucket: str
    page: int
    limit: int
    total: int
    items: List[LeadOut] = Field(default_factory=list)


class CheckoutRequest(BaseModel):
    bucket: str
    leadIds: List[str]


class CheckoutResponse(BaseModel):
    bucket: str
    requested: int
    sold: int
    failed: List[str] = Field(default_factory=list)


# =========================
# HEALTH (optional but helpful)
# =========================
@app.get("/health")
async def health():
    try:
        await db.command("ping")
        return {"ok": True, "mongo_configured": True}
    except Exception:
        return {"ok": True, "mongo_configured": False}


# =========================
# (Optional) preflight helper for legacy clients hitting /leads/search
# If your HTML (or old docs) still uses POST /leads/search, this prevents
# OPTIONS from returning 400 in some edge setups.
# =========================
@app.options("/leads/search")
async def leads_search_preflight():
    return Response(status_code=204)


# =========================
# ENDPOINT 1 — BROWSE
# =========================
@app.get("/leads", response_model=LeadsResponse)
async def browse_leads(
    bucket: str = Query("YESTERDAY_72H"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
    type_of_coverage: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None),
):
    skip = (page - 1) * limit

    # ---- MODE A: tier-specific (fast find) ----
    if bucket != "ALL":
        filt: Dict[str, Any] = {}
        filt.update(age_filter(bucket))
        filt["sold_tiers"] = {"$ne": bucket}

        if type_of_coverage:
            filt["type_of_coverage"] = type_of_coverage
        if state:
            filt["state"] = state
        if zip_code:
            filt["zip_code"] = zip_code

        total = await leads_col.count_documents(filt)
        docs = await (
            leads_col.find(filt)
            .sort("submitted_at", -1)
            .skip(skip)
            .limit(limit)
            .to_list(length=limit)
        )

        items = [serialize(d) for d in docs]
        return {"bucket": bucket, "page": page, "limit": limit, "total": total, "items": items}

    # ---- MODE B: ALL ages sorted by price (aggregation) ----
    ms_per_day = 24 * 60 * 60 * 1000
    pipeline: List[Dict[str, Any]] = []

    if type_of_coverage:
        pipeline.append({"$match": {"type_of_coverage": type_of_coverage}})
    if state:
        pipeline.append({"$match": {"state": state}})
    if zip_code:
        pipeline.append({"$match": {"zip_code": zip_code}})

    pipeline.append(
        {
            "$addFields": {
                "age_days": {
                    "$add": [
                        1,
                        {
                            "$floor": {
                                "$divide": [{"$subtract": ["$$NOW", "$submitted_at"]}, ms_per_day]
                            }
                        },
                    ]
                }
            }
        }
    )

    pipeline.append(
        {
            "$addFields": {
                "bucket": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": ["$age_days", 3]}, "then": "YESTERDAY_72H"},
                            {"case": {"$and": [{"$gte": ["$age_days", 4]}, {"$lte": ["$age_days", 14]}]}, "then": "DAYS_4_14"},
                            {"case": {"$and": [{"$gte": ["$age_days", 15]}, {"$lte": ["$age_days", 30]}]}, "then": "DAYS_15_30"},
                            {"case": {"$and": [{"$gte": ["$age_days", 31]}, {"$lte": ["$age_days", 90]}]}, "then": "DAYS_31_90"},
                        ],
                        "default": "DAYS_91_PLUS",
                    }
                }
            }
        }
    )

    pipeline.append({"$match": {"$expr": {"$not": {"$in": ["$bucket", "$sold_tiers"]}}}})

    pipeline.append(
        {
            "$addFields": {
                "price": {
                    "$switch": {
                        "branches": [
                            {"case": {"$eq": ["$bucket", "YESTERDAY_72H"]}, "then": 4.50},
                            {"case": {"$eq": ["$bucket", "DAYS_4_14"]}, "then": 3.75},
                            {"case": {"$eq": ["$bucket", "DAYS_15_30"]}, "then": 3.00},
                            {"case": {"$eq": ["$bucket", "DAYS_31_90"]}, "then": 2.25},
                        ],
                        "default": 1.50,
                    }
                }
            }
        }
    )

    pipeline.append({"$sort": {"price": 1, "submitted_at": -1}})

    pipeline.append(
        {
            "$facet": {
                "items": [
                    {"$skip": skip},
                    {"$limit": limit},
                    {
                        "$project": {
                            "name": 1,
                            "state": 1,
                            "zip_code": 1,
                            "type_of_coverage": 1,
                            "submitted_at": 1,
                            "age_days": 1,
                            "bucket": 1,
                            "price": 1,
                        }
                    },
                ],
                "meta": [{"$count": "total"}],
            }
        }
    )

    res = await leads_col.aggregate(pipeline).to_list(length=1)
    facet = res[0] if res else {"items": [], "meta": []}
    total = (facet.get("meta") or [{}])[0].get("total", 0)
    docs = facet.get("items") or []

    items = [serialize(d) for d in docs]
    return {"bucket": "ALL", "page": page, "limit": limit, "total": total, "items": items}


# =========================
# ENDPOINT 2 — CHECKOUT
# =========================
@app.post("/checkout", response_model=CheckoutResponse)
async def checkout(body: CheckoutRequest):
    bucket = body.bucket
    lead_ids = body.leadIds

    obj_ids = [ObjectId(x) for x in lead_ids]

    res = await leads_col.update_many(
        {"_id": {"$in": obj_ids}, "sold_tiers": {"$ne": bucket}},
        {"$addToSet": {"sold_tiers": bucket}},
    )

    requested = len(lead_ids)

    sold_now = await leads_col.find(
        {"_id": {"$in": obj_ids}, "sold_tiers": bucket},
        {"_id": 1},
    ).to_list(length=requested)

    sold_set = {str(d["_id"]) for d in sold_now}
    failed = [x for x in lead_ids if x not in sold_set]

    return {"bucket": bucket, "requested": requested, "sold": len(sold_set), "failed": failed}
