from fastapi import FastAPI

from src.api.endpoints import router

app = FastAPI(
    title="VoiceBot Post-Call Processing",
    version="1.0.0",
)

# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "ok"}

app.include_router(router, prefix="/api/v1")
