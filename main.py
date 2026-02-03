from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from fastapi import FastAPI, Query, HTTPException
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
# CORS
# =========================
ALLOWED_ORIGINS = [
    "https://code.flywheelsites.com",
    "https://first-wrist.flywheelsites.com",
    "https://castudios.tv",
    "https://www.castudios.tv",
    "http://localhost:3000",
    "http://localhost:5173",
]

extra = os.environ.get("CORS_ORIGINS", "").strip()
if extra:
    ALLOWED_ORIGINS.extend([o.strip() for o in extra.split(",") if o.strip()])

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
    max_age=86400,
)

# =========================
# DB
# =========================
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


def serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(doc)
    if "_id" in d:
        d["id"] = str(d["_id"])
        del d["_id"]

    # normalize common date fields to iso strings
    for k in ("submitted_at", "submittedAt", "createdAt", "updatedAt", "ts"):
        if k in d and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()
    return d


def bucket_bounds(bucket: str) -> Dict[str, Any]:
    """
    Bucket logic based on a timestamp:
    - Uses 72 hours for newest bucket
    - uses days ranges for others
    """
    now = utcnow()

    if bucket == "YESTERDAY_72H":
        return {"$gte": now - timedelta(hours=72), "$lte": now}
    if bucket == "DAYS_4_14":
        return {"$gte": now - timedelta(days=14), "$lt": now - timedelta(days=4)}
    if bucket == "DAYS_15_30":
        return {"$gte": now - timedelta(days=30), "$lt": now - timedelta(days=15)}
    if bucket == "DAYS_31_90":
        return {"$gte": now - timedelta(days=90), "$lt": now - timedelta(days=31)}
    if bucket == "DAYS_91_PLUS":
        return {"$lt": now - timedelta(days=91)}
    return {}


# =========================
# MODELS
# =========================
class LeadOut(BaseModel):
    id: str
    name: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    type_of_coverage: Optional[str] = None
    createdAt: Optional[str] = None
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


class LeadsSearchBody(BaseModel):
    bucket: str = "DAYS_4_14"
    page: int = 1
    limit: int = 25
    type_of_coverage: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None


class CheckoutRequest(BaseModel):
    bucket: str
    leadIds: List[str]


class CheckoutResponse(BaseModel):
    bucket: str
    requested: int
    sold: int
    failed: List[str] = Field(default_factory=list)


# =========================
# HEALTH
# =========================
@app.get("/health")
async def health():
    try:
        await db.command("ping")
        return {"ok": True, "mongo_configured": True}
    except Exception:
        return {"ok": True, "mongo_configured": False}


# =========================
# ENDPOINT — BROWSE (GET)
# =========================
@app.get("/leads", response_model=LeadsResponse)
async def browse_leads(
    bucket: str = Query("DAYS_4_14"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
    type_of_coverage: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None),
):
    skip = (page - 1) * limit

    # =========================
    # MODE A: NON-ALL (simple find)
    # =========================
    if bucket != "ALL":
        # We filter by a safe timestamp field:
        # ts = createdAt (preferred) else submittedAt else submitted_at
        bounds = bucket_bounds(bucket)

        # Build match
        filt: Dict[str, Any] = {}
        # Sold logic: if sold_tiers missing, still matches (good)
        filt["sold_tiers"] = {"$ne": bucket}

        if type_of_coverage:
            filt["type_of_coverage"] = type_of_coverage
        if state:
            filt["state"] = state
        if zip_code:
            filt["zip_code"] = zip_code

        # We'll do timestamp bounds with $expr so we can use fallback fields
        if bounds:
            # Convert bucket bounds to an $expr against the chosen ts
            # (We compute ts inside the expression using $ifNull chain)
            ts_expr = {
                "$ifNull": [
                    "$createdAt",
                    {"$ifNull": ["$submittedAt", "$submitted_at"]},
                ]
            }

            # Apply bounds (gte/lt/lte) via $expr
            expr_parts = []
            if "$gte" in bounds:
                expr_parts.append({"$gte": [ts_expr, bounds["$gte"]]})
            if "$lt" in bounds:
                expr_parts.append({"$lt": [ts_expr, bounds["$lt"]]})
            if "$lte" in bounds:
                expr_parts.append({"$lte": [ts_expr, bounds["$lte"]]})

            # also ensure ts is a date
            expr_parts.insert(0, {"$eq": [{"$type": ts_expr}, "date"]})

            filt["$expr"] = {"$and": expr_parts}

        total = await leads_col.count_documents(filt)
        docs = await (
            leads_col.find(filt)
            .sort("createdAt", -1)  # best effort; docs lacking createdAt still work via filt
            .skip(skip)
            .limit(limit)
            .to_list(length=limit)
        )

        items = [serialize(d) for d in docs]
        return {"bucket": bucket, "page": page, "limit": limit, "total": total, "items": items}

    # =========================
    # MODE B: ALL (aggregation) ✅ FIXED
    # =========================
    ms_per_day = 24 * 60 * 60 * 1000
    pipeline: List[Dict[str, Any]] = []

    # Optional filters
    if type_of_coverage:
        pipeline.append({"$match": {"type_of_coverage": type_of_coverage}})
    if state:
        pipeline.append({"$match": {"state": state}})
    if zip_code:
        pipeline.append({"$match": {"zip_code": zip_code}})

    # ✅ Pick a timestamp that exists (createdAt preferred)
    pipeline.append(
        {
            "$addFields": {
                "ts": {
                    "$ifNull": [
                        "$createdAt",
                        {"$ifNull": ["$submittedAt", "$submitted_at"]},
                    ]
                }
            }
        }
    )

    # ✅ Drop docs with no timestamp or wrong type
    pipeline.append({"$match": {"ts": {"$type": "date"}}})

    # ✅ Age days from ts (safe)
    pipeline.append(
        {
            "$addFields": {
                "age_days": {
                    "$add": [
                        1,
                        {
                            "$floor": {
                                "$divide": [{"$subtract": ["$$NOW", "$ts"]}, ms_per_day]
                            }
                        },
                    ]
                }
            }
        }
    )

    # Bucket mapping
    pipeline.append(
        {
            "$addFields": {
                "bucket": {
                    "$switch": {
                        "branches": [
                            {"case": {"$lte": ["$age_days", 3]}, "then": "YESTERDAY_72H"},
                            {
                                "case": {"$and": [{"$gte": ["$age_days", 4]}, {"$lte": ["$age_days", 14]}]},
                                "then": "DAYS_4_14",
                            },
                            {
                                "case": {"$and": [{"$gte": ["$age_days", 15]}, {"$lte": ["$age_days", 30]}]},
                                "then": "DAYS_15_30",
                            },
                            {
                                "case": {"$and": [{"$gte": ["$age_days", 31]}, {"$lte": ["$age_days", 90]}]},
                                "then": "DAYS_31_90",
                            },
                        ],
                        "default": "DAYS_91_PLUS",
                    }
                }
            }
        }
    )

    # ✅ sold_tiers may be missing. Treat missing as []
    pipeline.append({"$addFields": {"sold_tiers_safe": {"$ifNull": ["$sold_tiers", []]}}})

    # Exclude already-sold tiers
    pipeline.append({"$match": {"$expr": {"$not": {"$in": ["$bucket", "$sold_tiers_safe"]}}}})

    # Price per bucket
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

    # Sort: cheapest first, then newest
    pipeline.append({"$sort": {"price": 1, "ts": -1}})

    # Pagination + total via facet
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
                            "createdAt": "$ts",
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

    try:
        res = await leads_col.aggregate(pipeline).to_list(length=1)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"agg_failed: {type(e).__name__}: {str(e)}")

    facet = res[0] if res else {"items": [], "meta": []}
    total = (facet.get("meta") or [{}])[0].get("total", 0)
    docs = facet.get("items") or []

    items = [serialize(d) for d in docs]
    return {"bucket": "ALL", "page": page, "limit": limit, "total": total, "items": items}


# =========================
# ENDPOINT — SEARCH (POST)
# =========================
@app.post("/leads/search")
async def leads_search(body: LeadsSearchBody):
    resp = await browse_leads(
        bucket=body.bucket,
        page=body.page,
        limit=body.limit,
        type_of_coverage=body.type_of_coverage,
        state=body.state,
        zip_code=body.zip_code,
    )
    return {
        "bucket": resp["bucket"],
        "page": resp["page"],
        "limit": resp["limit"],
        "total": resp["total"],
        "count": resp["total"],
        "items": resp["items"],
    }


# =========================
# ENDPOINT — CHECKOUT
# =========================
@app.post("/checkout", response_model=CheckoutResponse)
async def checkout(body: CheckoutRequest):
    bucket = body.bucket
    lead_ids = body.leadIds

    try:
        obj_ids = [ObjectId(x) for x in lead_ids]
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid leadIds (must be Mongo ObjectId strings)")

    await leads_col.update_many(
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
