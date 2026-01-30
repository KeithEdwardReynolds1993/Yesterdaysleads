import os
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

def get_db():
    mongo_uri = os.environ.get("MONGO_URI", "")
    mongo_db  = os.environ.get("MONGO_DB", "leads")

    if not mongo_uri:
        return None

    client = AsyncIOMotorClient(mongo_uri)
    return client[mongo_db]

@app.get("/health")
async def health():
    db = get_db()
    if db is not None:
        await db.command("ping")
    return {"ok": True, "mongo_configured": db is not None}

@app.get("/debug/env")
async def debug_env():
    # do NOT leak secrets; just confirm presence + length
    uri = os.environ.get("MONGO_URI", "")
    return {
        "has_MONGO_URI": bool(uri),
        "MONGO_URI_len": len(uri),
        "MONGO_DB": os.environ.get("MONGO_DB", ""),
    }

@app.post("/leads/search")
async def leads_search(filters: dict):
    db = get_db()
    if db is None:
        return {"count": 0, "items": [], "note": "Set MONGO_URI env var to enable database"}
    cursor = db.leads.find({}, {"_id": 0}).limit(25)
    items = await cursor.to_list(length=25)
    return {"count": len(items), "items": items}
