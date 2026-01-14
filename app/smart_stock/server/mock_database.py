"""In-memory mock database for SmartStock.

This is used when Lakebase/PostgreSQL credentials are not provided. It implements the
minimal interface used by the API routers: `execute_query` and `execute_update`.

It's intentionally lightweight: it supports a small subset of query patterns by
matching common table names and COUNT queries. This keeps the app bootable for demos.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _lower(s: str) -> str:
    return s.lower()


def _is_count_query(q: str) -> bool:
    ql = _lower(q)
    return "count(*)" in ql and " as total" in ql


def _table_in_query(q: str, table: str) -> bool:
    ql = _lower(q)
    # match both " public.table" and " table" and "schema.table"
    return f".{table}" in ql or f" {table}" in ql or f"{table} " in ql


@dataclass
class _State:
    lock: Lock = field(default_factory=Lock)
    products: list[dict[str, Any]] = field(default_factory=list)
    warehouses: list[dict[str, Any]] = field(default_factory=list)
    inventory_forecast: list[dict[str, Any]] = field(default_factory=list)
    inventory_transactions: list[dict[str, Any]] = field(default_factory=list)
    inventory_historical: list[dict[str, Any]] = field(default_factory=list)
    _transaction_id: int = 1


class MockDatabase:
    """A tiny, query-pattern-based in-memory database."""

    def __init__(self) -> None:
        self._s = _State()
        self._seed()

    def _seed(self) -> None:
        with self._s.lock:
            if self._s.products:
                return

            now = _now()
            self._s.warehouses = [
                {
                    "warehouse_id": 1,
                    "name": "Main Warehouse",
                    "location": "Portland, OR",
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "warehouse_id": 2,
                    "name": "East DC",
                    "location": "Seattle, WA",
                    "created_at": now,
                    "updated_at": now,
                },
            ]

            self._s.products = [
                {
                    "product_id": 1,
                    "name": "Premium Coffee Beans",
                    "description": "Arabica beans",
                    "sku": "COF-001",
                    "price": 24.99,
                    "unit": "lb",
                    "category": "Beverages",
                    "reorder_level": 10,
                    "created_at": now,
                    "updated_at": now,
                },
                {
                    "product_id": 2,
                    "name": "Organic Green Tea",
                    "description": "Matcha blend",
                    "sku": "TEA-001",
                    "price": 18.50,
                    "unit": "box",
                    "category": "Beverages",
                    "reorder_level": 8,
                    "created_at": now,
                    "updated_at": now,
                },
            ]

            self._s.inventory_forecast = [
                {
                    "forecast_id": 1,
                    "product_id": 1,
                    "warehouse_id": 1,
                    "current_stock": 12,
                    "forecast_30_days": 30,
                    "reorder_point": 10,
                    "reorder_quantity": 40,
                    "status": "active",
                    "last_updated": now,
                },
                {
                    "forecast_id": 2,
                    "product_id": 2,
                    "warehouse_id": 2,
                    "current_stock": 2,
                    "forecast_30_days": 20,
                    "reorder_point": 8,
                    "reorder_quantity": 30,
                    "status": "active",
                    "last_updated": now,
                },
            ]

    def execute_query(self, query: str, params: Any | None = None) -> list[dict[str, Any]]:
        ql = _lower(query)

        # health checks
        if "select 1" in ql:
            return [{"test": 1}]

        with self._s.lock:
            # COUNT queries
            if _is_count_query(query):
                if _table_in_query(query, "products"):
                    return [{"total": len(self._s.products)}]
                if _table_in_query(query, "warehouses"):
                    return [{"total": len(self._s.warehouses)}]
                if _table_in_query(query, "inventory_forecast"):
                    return [{"total": len(self._s.inventory_forecast)}]
                if _table_in_query(query, "inventory_transactions"):
                    return [{"total": len(self._s.inventory_transactions)}]
                return [{"total": 0}]

            # products
            if _table_in_query(query, "products"):
                # Simple by-id fetch
                if "where product_id = %s" in ql and params:
                    pid = int(params[0])
                    return [p for p in self._s.products if int(p["product_id"]) == pid]
                # List
                return list(self._s.products)

            # warehouses
            if _table_in_query(query, "warehouses"):
                if "where warehouse_id = %s" in ql and params:
                    wid = int(params[0])
                    return [w for w in self._s.warehouses if int(w["warehouse_id"]) == wid]
                return list(self._s.warehouses)

            # inventory_forecast join query in /inventory/forecast
            if _table_in_query(query, "inventory_forecast"):
                # The API expects fields like item_id, item_name, warehouse_name, etc.
                rows: list[dict[str, Any]] = []
                for f in self._s.inventory_forecast:
                    p = next((x for x in self._s.products if x["product_id"] == f["product_id"]), None)
                    w = next(
                        (x for x in self._s.warehouses if x["warehouse_id"] == f["warehouse_id"]), None
                    )
                    if not p or not w:
                        continue
                    rows.append(
                        {
                            "forecast_id": f["forecast_id"],
                            "item_id": p["sku"],
                            "item_name": p["name"],
                            "stock": f["current_stock"],
                            "forecast_30_days": int(f["forecast_30_days"]),
                            "warehouse_id": w["warehouse_id"],
                            "warehouse_name": w["name"],
                            "warehouse_location": w["location"],
                            "status": "in_stock",
                            "action": "No Action",
                            "severity_rank": 3,
                            "last_updated": f["last_updated"],
                        }
                    )
                return rows

            # inventory_transactions
            if _table_in_query(query, "inventory_transactions"):
                # list endpoint expects already-joined rows; return minimal fields
                return list(self._s.inventory_transactions)

            # default empty
            return []

    def execute_update(self, query: str, params: Any | None = None) -> int:
        ql = _lower(query)
        with self._s.lock:
            # Basic insert into inventory_transactions from transactions router
            if "insert into" in ql and "inventory_transactions" in ql:
                now = _now()
                tx_id = self._s._transaction_id
                self._s._transaction_id += 1
                # Best-effort mapping for the router's insert shape.
                if isinstance(params, (list, tuple)) and len(params) >= 8:
                    self._s.inventory_transactions.append(
                        {
                            "transaction_id": params[0] if params[0] is not None else tx_id,
                            "transaction_number": params[1],
                            "product_id": params[2],
                            "warehouse_id": params[3],
                            "quantity_change": params[4],
                            "transaction_type": params[5],
                            "status": params[6],
                            "notes": params[7],
                            "transaction_timestamp": now,
                        }
                    )
                return 1

            # Updates: pretend they worked
            if ql.strip().startswith("update ") or ql.strip().startswith("delete "):
                return 1

            # DDL: ignore in mock mode
            if ql.strip().startswith("create ") or ql.strip().startswith("drop "):
                return 0

            return 0


# Exported instance used by routers.
db = MockDatabase()

