from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from analyzer import analyze_double_buying
import uvicorn
import asyncio
import time

app = FastAPI(title="Double-Buying Analyzer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Cache to avoid hammering KRX
cache = {}

@app.get("/")
def root():
    return {"status": "ok", "message": "Double-Buying Analyzer API is running"}

@app.get("/api/health")
def health():
    return {"status": "healthy", "timestamp": time.time()}

@app.get("/api/stocks/debug")
def get_stocks_debug():
    # Return dummy data immediately to test connection/CORS
    return {
        "new": [{"ticker": "000000", "name": "TEST_STOCK", "market": "KOSPI", "foreign": 100, "inst": 200}],
        "continuous": [],
        "ended": []
    }

@app.get("/api/stocks")
async def get_stocks(market: str = "KOSPI"):
    # very simple caching: 1 hour
    if market in cache:
        res, timestamp = cache[market]
        if time.time() - timestamp < 3600:
            return res
            
    try:
        data = await analyze_double_buying(market)
        cache[market] = (data, time.time())
        return data
    except Exception as e:
        return {"error": str(e), "new": [], "continuous": [], "ended": []}

import os

if __name__ == "__main__":
    # Get port from environment variable (default to 8000 for local dev)
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)
