import os
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any

from database import db, create_document
from etl import run_etl
from adapters import COUNTRIES

app = FastAPI(title="Global City Intelligence Platform - API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_seed():
    # Ensure base collections exist and countries are seeded
    if db:
        try:
            if db["country"].count_documents({}) == 0:
                for c in COUNTRIES:
                    try:
                        create_document("country", {"code": c["code"], "name": c["name"]})
                    except Exception:
                        pass
        except Exception:
            pass

@app.get("/")
def root():
    return {"name": "GCIP API", "status": "ok"}

@app.get("/api/health")
def health():
    return {"backend": "ok", "database": bool(db)}

@app.post("/api/admin/run-etl")
def trigger_etl():
    if not db:
        raise HTTPException(status_code=500, detail="Database not configured")
    rid = run_etl()
    return {"run_id": rid}

@app.get("/api/countries")
def list_countries():
    countries = db["country"].find({}, {"_id": 0}) if db else []
    return list(countries)

@app.get("/api/top-cities")
def top_cities(limit: int = 20):
    if not db:
        return []
    cursor = db["score"].find({}, {"_id": 0}).sort("score", -1).limit(limit)
    return list(cursor)

@app.get("/api/country/{code}/cities")
def cities_by_country(code: str):
    if not db:
        return []
    # Join scores and normalized metrics
    scores = list(db["score"].find({"country_code": code}, {"_id": 0}))
    nm = { (d["city"]): d for d in db["normalizedmetric"].find({"country_code": code}, {"_id": 0}) }
    for s in scores:
        s["normalized"] = nm.get(s["city"], {}).get("normalized", {})
    return scores

@app.get("/api/city/compare")
def compare_cities(a: str, b: str):
    if not db:
        return []
    def get(city: str):
        s = db["score"].find_one({"city": city}, {"_id": 0})
        n = db["normalizedmetric"].find_one({"city": city}, {"_id": 0})
        return {"score": s, "normalized": n}
    return {"a": get(a), "b": get(b)}

@app.get("/api/admin/etl-logs")
def etl_logs(limit: int = 50):
    if not db:
        return []
    cur = db["etllog"].find({}, {"_id": 0}).sort("started_at", -1).limit(limit)
    return list(cur)

@app.get("/test")
def test_database():
    response = {
        "backend": "✅ Running",
        "database": "❌ Not Available",
        "database_url": None,
        "database_name": None,
        "connection_status": "Not Connected",
        "collections": []
    }
    try:
        if db is not None:
            response["database"] = "✅ Connected & Working"
            response["database_url"] = "✅ Set" if os.getenv("DATABASE_URL") else "❌ Not Set"
            response["database_name"] = "✅ Set" if os.getenv("DATABASE_NAME") else "❌ Not Set"
            try:
                collections = db.list_collection_names()
                response["collections"] = collections[:10]
            except Exception as e:
                response["database"] = f"⚠️  Connected but Error: {str(e)[:50]}"
        else:
            response["database"] = "⚠️  Available but not initialized"
    except Exception as e:
        response["database"] = f"❌ Error: {str(e)[:50]}"
    return response

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
