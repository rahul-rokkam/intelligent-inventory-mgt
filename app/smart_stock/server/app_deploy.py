"""FastAPI app entrypoint for Databricks Apps deployment.

Goal: be resilient during startup.
- Serve the built frontend if present.
- Load API routers defensively so missing optional config doesn't crash the whole app.
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles


def _load_env_file(filepath: str) -> None:
    p = Path(filepath)
    if not p.exists():
        return
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        if key and value and key not in os.environ:
            os.environ[key] = value


# In Apps, env vars are set by the platform; this is mainly for local/dev.
_load_env_file(".env")
_load_env_file(".env.local")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


app = FastAPI(
    title="SmartStock API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    # Best-effort DB check (works for mock too)
    try:
        from server.db_selector import db

        db.execute_query("SELECT 1 as test", None)
        db_type = "postgres" if "LakebasePostgres" in str(type(db)) else "mock"
        return {"status": "healthy", "db_type": db_type}
    except Exception as e:
        return {"status": "healthy", "db_type": "unknown", "db_error": str(e)}


# Load API routes defensively
try:
    from server.routers import router as api_router

    app.include_router(api_router, prefix="/api", tags=["api"])
    print("✅ Core API routes loaded")
except Exception as e:
    print(f"⚠️  Core API routes failed to load: {e}")

# Optional: agent + genie routes (never crash startup)
try:
    from server.routers import agent as agent_router

    app.include_router(agent_router.router)
    print("✅ Agent routes loaded")
except Exception as e:
    print(f"⚠️  Agent routes not available: {e}")

try:
    from server.routers import genie as genie_router

    app.include_router(genie_router.router)
    print("✅ Genie routes loaded")
except Exception as e:
    print(f"⚠️  Genie routes not available: {e}")


# Serve frontend (prefer build/ used by this repo)
if Path("build").exists():
    app.mount("/", StaticFiles(directory="build", html=True), name="static")
elif Path("client/build").exists():
    app.mount("/", StaticFiles(directory="client/build", html=True), name="static")

