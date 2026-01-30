import os
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB  = os.environ.get("MONGO_DB", "leads")

client = AsyncIOMotorClient(MONGO_URI) if MONGO_URI else None
db = client[MONGO_DB] if client else None

@app.get("/health")
async def health():
    if db is not None:
        await db.command("ping")
    return {"ok": True, "mongo_configured": db is not None}

@app.post("/leads/search")
async def leads_search(filters: dict):
    if db is None:
        return {"count": 0, "items": [], "note": "Set MONGO_URI env var to enable database"}
    cursor = db.leads.find({}, {"_id": 0}).limit(25)
    items = await cursor.to_list(length=25)
    return {"count": len(items), "items": items}
