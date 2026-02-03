print("🚀 LOADED main.py — v2026-02-03-1", flush=True)

from fastapi import FastAPI

app = FastAPI(title="Yesterday's Leads API")

@app.get("/__whoami")
def whoami():
    return {"ok": True, "file": "main.py", "version": "v2026-02-03-1"}


# main.py — FULL REPLACEMENT (fix ResponseValidationError + add debug)
from __future__ import annotations

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
    raise RuntimeError("Missing MONGO_URI")

app = FastAPI(title="Yesterday's Leads API")

@app.get("/__whoami")
async def __whoami():
    return {"ok": True, "file": "root main.py", "status": "live"}


ALLOWED_ORIGINS = [
    "https://code.flywheelsites.com",
    "https://first-wrist.flywheelsites.com",
    "https://castudios.tv",
    "https://www.castudios.tv",
    "http://localhost:3000",
    "http://localhost:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]
leads_col = db[MONGO_COLLECTION]

PRICE_BY_BUCKET = {
    "YESTERDAY_72H": 4.50,
    "DAYS_4_14": 3.75,
    "DAYS_15_30": 3.00,
    "DAYS_31_90": 2.25,
    "DAYS_91_PLUS": 1.50,
}

PROJECTION = {
    "_id": 1,
    "external_id": 1,
    "name": 1,
    "first_name": 1,
    "firstName": 1,
    "last_name": 1,
    "lastName": 1,
    "state": 1,
    "state2": 1,
    "zip_code": 1,
    "zip5": 1,
    "createdAt": 1,
    "submittedAt": 1,
    "submitted_at": 1,
    "sold_tiers": 1,
    "lead_type_public": 1,
    "lead_type_norm": 1,
    "lead_type_code": 1,
    "lead_type": 1,
    "type_of_coverage": 1,
    "leadType": 1,
    "product": 1,
    "coverage": 1,
}

def _as_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def _zip_to_str(v: Any) -> Optional[str]:
    s = _as_str(v)
    if not s:
        return None
    if s.isdigit() and len(s) < 5:
        s = s.zfill(5)
    return s

def serialize(doc: Dict[str, Any]) -> Dict[str, Any]:
    d = dict(doc)

    if "_id" in d:
        d["id"] = str(d["_id"])
        del d["_id"]

    # Normalize primitive types to avoid ResponseValidationError
    if "state" in d:
        d["state"] = _as_str(d.get("state")) or "Unknown"
    if "state2" in d:
        d["state2"] = _as_str(d.get("state2"))

    if "zip_code" in d:
        d["zip_code"] = _zip_to_str(d.get("zip_code"))
    if "zip5" in d:
        d["zip5"] = _zip_to_str(d.get("zip5"))

    for k in ("submitted_at", "submittedAt", "createdAt", "updatedAt", "ts"):
        if k in d and hasattr(d[k], "isoformat"):
            d[k] = d[k].isoformat()

    return d

def bucket_bounds(bucket: str) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
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

def normalized_type(d: Dict[str, Any]) -> str:
    val = (
        d.get("lead_type_public")
        or d.get("lead_type_norm")
        or d.get("lead_type_code")
        or d.get("lead_type")
        or d.get("type_of_coverage")
        or d.get("leadType")
        or d.get("product")
        or d.get("coverage")
    )
    s = _as_str(val)
    return s if s else "Unknown"

@app.get("/__debug_sample")
async def __debug_sample():
    doc = await leads_col.find_one({}, PROJECTION)
    if not doc:
        return {"ok": False, "error": "no_docs"}
    # show key->type for first doc so we can see if zip_code is int, etc
    types = {k: type(v).__name__ for k, v in doc.items()}
    return {"ok": True, "keys": sorted(list(doc.keys())), "types": types, "doc_preview": serialize(doc)}

@app.get("/leads")
async def browse_leads(
    bucket: str = Query("DAYS_4_14"),
    page: int = Query(1, ge=1),
    limit: int = Query(25, ge=1, le=200),
    lead_type: Optional[str] = Query(None),
    state: Optional[str] = Query(None),
    zip_code: Optional[str] = Query(None),
):
    try:
        skip = (page - 1) * limit

        filt: Dict[str, Any] = {}
        if lead_type:
            filt["lead_type_norm"] = lead_type
        if state:
            filt["state"] = state
        if zip_code:
            z = zip_code.strip()
            ors = [{"zip_code": z}, {"zip5": z}]
            if z.isdigit():
                ors += [{"zip_code": int(z)}, {"zip5": int(z)}]
            filt["$or"] = ors

        expr_and: List[Dict[str, Any]] = []
        expr_and.append({"$not": {"$in": [bucket, {"$ifNull": ["$sold_tiers", []]}]}})

        if bucket != "ALL":
            bounds = bucket_bounds(bucket)
            if bounds:
                ts_expr = {"$ifNull": ["$createdAt", {"$ifNull": ["$submittedAt", "$submitted_at"]}]}
                expr_and.append({"$eq": [{"$type": ts_expr}, "date"]})
                if "$gte" in bounds:
                    expr_and.append({"$gte": [ts_expr, bounds["$gte"]]})
                if "$lt" in bounds:
                    expr_and.append({"$lt": [ts_expr, bounds["$lt"]]})
                if "$lte" in bounds:
                    expr_and.append({"$lte": [ts_expr, bounds["$lte"]]})

            filt["$expr"] = {"$and": expr_and}

            total = await leads_col.count_documents(filt)
            docs = (
                await leads_col.find(filt, PROJECTION)
                .sort([("createdAt", -1), ("submittedAt", -1), ("submitted_at", -1)])
                .skip(skip)
                .limit(limit)
                .to_list(length=limit)
            )

            items = []
            for d in docs:
                item = serialize(d)
                lt = normalized_type(d)
                item["lead_type_public"] = lt
                item["lead_type_norm"] = lt
                item["lead_type_code"] = _as_str(d.get("lead_type_code"))

                item["price"] = float(PRICE_BY_BUCKET.get(bucket, 1.50))
                item["bucket"] = bucket
                items.append(item)

            return {"bucket": bucket, "page": page, "limit": limit, "total": total, "count": total, "items": items}

        total = await leads_col.count_documents(filt if filt else {})
        docs = (
            await leads_col.find(filt if filt else {}, PROJECTION)
            .sort([("createdAt", -1), ("submittedAt", -1), ("submitted_at", -1)])
            .skip(skip)
            .limit(limit)
            .to_list(length=limit)
        )

        items = []
        for d in docs:
            item = serialize(d)
            lt = normalized_type(d)
            item["lead_type_public"] = lt
            item["lead_type_norm"] = lt
            item["lead_type_code"] = _as_str(d.get("lead_type_code"))
            item["price"] = float(item.get("price") or 1.50)
            item["bucket"] = "ALL"
            items.append(item)

        return {"bucket": "ALL", "page": page, "limit": limit, "total": total, "count": total, "items": items}

    except Exception as e:
        # temporary: surface the error as JSON so you're not blind
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")

@app.post("/leads/search")
async def leads_search(body: Dict[str, Any]):
    return await browse_leads(
        bucket=body.get("bucket", "DAYS_4_14"),
        page=int(body.get("page", 1) or 1),
        limit=int(body.get("limit", 25) or 25),
        lead_type=(body.get("lead_type") or body.get("lead_type_norm") or None),
        state=body.get("state"),
        zip_code=(body.get("zip") or body.get("zip_code") or None),
    )

@app.get("/health")
async def health():
    return {"ok": True}
