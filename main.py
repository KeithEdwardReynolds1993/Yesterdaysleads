import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

# =========================
# CORS (ALLOW YOUR WP SITE)
# =========================
ALLOWED_ORIGINS = [
    "https://castudios.tv",
    "https://www.castudios.tv",
    "https://first-wrist.flywheelsites.com",
    "https://www.first-wrist.flywheelsites.com",
    "http://localhost:3000",
    "http://localhost:5173",
]

# Optional: allow additional origins via env var (comma separated)
extra = os.environ.get("CORS_ORIGINS", "").strip()
if extra:
    ALLOWED_ORIGINS.extend([o.strip() for o in extra.split(",") if o.strip()])

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

MONGO_URI = os.environ.get("MONGO_URI", "")
MONGO_DB = os.environ.get("MONGO_DB", "leads")


def get_db():
    mongo_uri = os.environ.get("MONGO_URI", "")
    mongo_db = os.environ.get("MONGO_DB", "leads")
    if not mongo_uri:
        return None
    client = AsyncIOMotorClient(mongo_uri)
    return client[mongo_db]


@app.get("/health")
async def health():
    db = get_db()
    mongo_ok = False
    if db is not None:
        try:
            await db.command("ping")
            mongo_ok = True
        except Exception:
            mongo_ok = False
    return {"ok": True, "mongo_configured": mongo_ok}


@app.get("/debug/env")
async def debug_env():
    uri = os.environ.get("MONGO_URI", "")
    return {
        "has_MONGO_URI": bool(uri),
        "MONGO_URI_len": len(uri),
        "MONGO_DB": os.environ.get("MONGO_DB", ""),
        "cors_origins": ALLOWED_ORIGINS,
    }


@app.post("/leads/search")
async def leads_search(filters: dict):
    db = get_db()
    if db is None:
        return {"count": 0, "items": [], "note": "Set MONGO_URI env var to enable database"}

    # For now: return first 25 like your current version
    cursor = db.leads.find({}, {"_id": 0}).limit(25)
    items = await cursor.to_list(length=25)
    return {"count": len(items), "items": items}
