"""Database selector.

Databricks Apps deployments sometimes run without Lakebase credentials (e.g. demo/mock mode).
In those cases we fall back to an in-memory mock database so the app can start and serve UI/API.
"""

import os


def _is_truthy(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


# Prefer explicit mock mode when requested.
use_mock_db = _is_truthy(os.getenv("SMARTSTOCK_MOCK_DATA")) or _is_truthy(os.getenv("MOCK_DATA"))

# Check if we have real database credentials
use_real_db = all([
    os.getenv("DB_HOST"),
    os.getenv("DB_USER"),
    os.getenv("DB_PASSWORD"),
])

# Allow deployments to require DB strictly if desired.
require_db = _is_truthy(os.getenv("SMARTSTOCK_REQUIRE_DB"))

if use_real_db and not use_mock_db:
    from .postgres_database import db

    print("Using Lakebase PostgreSQL database")
elif require_db:
    raise ValueError(
        "PostgreSQL database credentials are required. "
        "Please set DB_HOST, DB_USER, and DB_PASSWORD environment variables."
    )
else:
    from .mock_database import db

    print("Using mock database (no DB_* credentials provided)")

__all__ = ["db"]