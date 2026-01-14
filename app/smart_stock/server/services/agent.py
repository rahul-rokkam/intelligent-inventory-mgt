"""SmartStock AI Agent - runs directly in FastAPI.

This agent uses:
1. Databricks Foundation Model APIs for LLM (OpenAI-compatible)
2. Genie API as a tool for natural language → SQL queries
3. Direct database queries for real-time inventory data from Lakebase
"""

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable, Optional

import httpx
from openai import OpenAI

logger = logging.getLogger(__name__)


# =============================================================================
# SYSTEM PROMPT
# =============================================================================

SYSTEM_PROMPT = """
You are SmartStock AI, an intelligent inventory management assistant for supply chain professionals.

## CRITICAL RULE: Answer Only What Is Asked
- Only answer the specific question the user asked
- Do NOT proactively suggest next steps unless the user asks "what should I do?" or "what are my options?"
- Do NOT propose restocking options unless explicitly asked for options
- Do NOT create purchase orders unless the user explicitly confirms they want to proceed
- Keep responses focused and concise

## Your Tools
- `get_critical_inventory_alerts`: Get items at risk of stockout with urgency levels. Returns forecast_id for each alert.
- `estimate_stockout_impact`: Calculate financial impact (lost revenue, expedite costs) for at-risk items
- `query_genie`: Ask analytical questions about inventory data using natural language
- `resolve_inventory_alert`: Create a purchase order to resolve an alert. Requires forecast_id, quantity, and supplier_name. This actually creates the order in the database.

## Response Guidelines

**When asked about inventory status/alerts:**
1. Call `get_critical_inventory_alerts`
2. Present the data: number of items at risk, urgency breakdown
3. Show a table of the critical items
4. STOP. Do not suggest next steps unless asked.

**When asked about financial impact/stockout cost:**
1. Call `estimate_stockout_impact`
2. Present: total exposure, breakdown by urgency
3. Show top items by financial risk
4. STOP. Do not propose solutions unless asked.

**When asked "what are my options?" or "how do I fix this?":**
Present restocking options with SPECIFIC quantities and costs for ALL alert items (URGENT, HIGH, and MEDIUM urgency levels - do not skip any).

Use this supplier data:
| Supplier | Location | Reliability | Lead Time | Expedited | Premium |
|----------|----------|-------------|-----------|-----------|---------|
| VeloTech Components | Germany | 98% | 5 days | 2 days | +35% |
| CycleCore Industries | Poland | 94% | 4 days | 2 days | +25% |
| EuroBike Parts | Netherlands | 96% | 3 days | 1 day | +40% |

For each option, show a table with:
- Product name
- Quantity to order (use `recommended_order_quantity` from alerts)
- Unit price
- Line total (quantity × unit_price × (1 + premium%))
- Forecast ID (for reference)

Structure as:
- **Option A (Fastest):** EuroBike Parts - 1-day expedited (+40% premium)
  [Table showing each product, quantity, and cost]
  **Total: €X,XXX**

- **Option B (Balanced):** VeloTech Components - 5-day delivery (+35% premium)
  [Table showing each product, quantity, and cost]
  **Total: €X,XXX**

- **Option C (Economical):** CycleCore Industries - 4-day delivery (+25% premium)
  [Table showing each product, quantity, and cost]
  **Total: €X,XXX**

This way the user sees exactly what they're ordering before confirming.

**When user says "proceed", "do it", "execute", "create the order", "go with option A/B/C", or confirms any option:**
⚠️ MANDATORY: You MUST call the `resolve_inventory_alert` tool for EVERY alert item - including URGENT, HIGH, and MEDIUM urgency levels. Do NOT skip any items. Do NOT simulate orders.

IMPORTANT: Resolve ALL alerts, not just the most urgent ones. Every item in the alerts list (regardless of urgency level) should have an order created.

Use the quantities you showed in the options presentation:
- `forecast_id`: From the alerts data (process ALL forecast_ids, not just urgent ones)
- `quantity`: The `recommended_order_quantity` you showed in the option table
- `supplier_name`: Based on the option chosen:
  - Option A → "EuroBike Parts"
  - Option B → "VeloTech Components"  
  - Option C → "CycleCore Industries"
  - If no option specified, default to "VeloTech Components"
- `notes`: Add "Expedited shipping" for Option A

Call the tool once for EACH product in the alerts list - do not skip MEDIUM urgency items.

After the tool returns successfully, present the confirmation:

1. **Order Creation:**
   - Show the PO number from the tool response (e.g., "PO-2025-XXXX")
   - List: product name, quantity, unit price, order total from tool response

2. **Supplier Notification:**
   - "📧 Sending order to [supplier_name] via EDI..."
   - "Email confirmation sent to procurement@[supplier-domain].com"
   - "Expected supplier acknowledgment: within 2 hours"

3. **Internal Updates:**
   - "Updating inventory forecast with incoming stock..."
   - "Forecast status changed to: resolved"
   - "Notifying warehouse team for receiving preparation"

4. **Confirmation:**
   - "✅ Order [PO number] submitted successfully"
   - Show expected_delivery from tool response

If the tool returns an error, inform the user of the issue.

## Formatting
- Use tables for lists of items
- Bold key numbers: **€12,450**, **URGENT**
- Currency is always Euros (€)
- Be concise - no unnecessary elaboration

## Context
The user is viewing the SmartStock inventory dashboard. Current date: today. All monetary values in Euros (€).
"""


# =============================================================================
# TOOL DEFINITIONS
# =============================================================================


@dataclass
class Tool:
    """Represents a tool the agent can use."""

    name: str
    description: str
    parameters: dict
    function: Callable


class InventoryTools:
    """Tools for querying inventory data directly from Lakebase PostgreSQL database."""

    def __init__(self, db_connection):
        """Initialize with database connection."""
        self.db = db_connection
        self.schema = os.getenv("DB_SCHEMA", "smart_stock")

    def get_critical_inventory_alerts(self) -> str:
        """Get items that are at risk of stockout or need immediate attention."""
        try:
            logger.info(f"Querying inventory alerts from schema: {self.schema}")
            
            # Query the inventory_forecast table which has urgency levels and predictions
            query = f"""
                SELECT 
                    f.forecast_id,
                    p.name as product_name,
                    p.sku,
                    p.product_id,
                    p.category,
                    p.price,
                    w.name as warehouse_name,
                    f.current_stock,
                    f.forecast_30_days,
                    f.reorder_point,
                    f.status,
                    CASE
                        WHEN f.current_stock = 0 THEN 'URGENT'
                        WHEN f.current_stock < (f.forecast_30_days * 0.5) THEN 'HIGH'
                        WHEN f.current_stock < f.forecast_30_days THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as urgency,
                    CASE
                        WHEN f.forecast_30_days > 0 THEN 
                            CAST((f.current_stock * 30.0 / f.forecast_30_days) AS INT)
                        ELSE 999
                    END as days_until_stockout
                FROM {self.schema}.inventory_forecast f
                JOIN {self.schema}.products p ON f.product_id = p.product_id
                JOIN {self.schema}.warehouses w ON f.warehouse_id = w.warehouse_id
                WHERE f.status = 'active'
                AND f.current_stock < f.forecast_30_days
                ORDER BY urgency, days_until_stockout ASC
            """
            
            logger.info(f"Executing query: {query[:200]}...")
            results = self.db.execute_query(query)
            logger.info(f"Query returned {len(results) if results else 0} rows")

            if not results:
                return json.dumps(
                    {
                        "status": "healthy",
                        "message": "No critical inventory alerts. All items are at healthy stock levels.",
                        "alerts": [],
                    }
                )

            alerts = []
            for row in results:
                forecast_30 = int(row.get("forecast_30_days") or 0)
                current = int(row.get("current_stock") or 0)
                # Recommended quantity = forecast demand minus current stock (at least 10 units)
                recommended_qty = max(10, forecast_30 - current)
                
                alerts.append(
                    {
                        "forecast_id": row.get("forecast_id"),
                        "product_id": row.get("product_id"),
                        "product_name": row.get("product_name"),
                        "sku": row.get("sku"),
                        "category": row.get("category"),
                        "warehouse": row.get("warehouse_name"),
                        "current_stock": current,
                        "forecast_30_days": forecast_30,
                        "reorder_point": int(row.get("reorder_point") or 0),
                        "recommended_order_quantity": recommended_qty,
                        "days_until_stockout": int(row.get("days_until_stockout") or 0),
                        "urgency": row.get("urgency"),
                        "unit_price": round(float(row.get("price", 0)), 2) if row.get("price") else None,
                    }
                )

            # Summarize by urgency
            urgent = len([a for a in alerts if a["urgency"] == "URGENT"])
            high = len([a for a in alerts if a["urgency"] == "HIGH"])
            medium = len([a for a in alerts if a["urgency"] == "MEDIUM"])

            return json.dumps(
                {
                    "status": "alerts_found",
                    "summary": {"total_alerts": len(alerts), "urgent": urgent, "high": high, "medium": medium},
                    "alerts": alerts,
                }
            )

        except Exception as e:
            logger.error(f"Error getting inventory alerts: {e}")
            return json.dumps({"error": str(e)})

    def estimate_stockout_impact(self, product_ids: Optional[list] = None) -> str:
        """Estimate financial impact of potential stockouts."""
        try:
            # Build query for stockout impact
            where_clause = ""
            params = []
            if product_ids:
                placeholders = ",".join(["%s"] * len(product_ids))
                where_clause = f"AND p.product_id IN ({placeholders})"
                params = product_ids

            query = f"""
                WITH critical_items AS (
                    SELECT 
                        f.forecast_id,
                        f.product_id,
                        f.warehouse_id,
                        p.name as product_name,
                        p.sku,
                        p.price,
                        p.category,
                        w.name as warehouse_name,
                        f.current_stock,
                        f.forecast_30_days,
                        CASE
                            WHEN f.current_stock = 0 THEN 'URGENT'
                            WHEN f.current_stock < (f.forecast_30_days * 0.5) THEN 'HIGH'
                            ELSE 'MEDIUM'
                        END as urgency,
                        CASE
                            WHEN f.forecast_30_days > 0 THEN 
                                CAST((f.current_stock * 30.0 / f.forecast_30_days) AS INT)
                            ELSE 999
                        END as days_until_stockout,
                        -- Units we'll be short over next 30 days
                        GREATEST(0, f.forecast_30_days - f.current_stock) as units_at_risk
                    FROM {self.schema}.inventory_forecast f
                    JOIN {self.schema}.products p ON f.product_id = p.product_id
                    JOIN {self.schema}.warehouses w ON f.warehouse_id = w.warehouse_id
                    WHERE f.status = 'active'
                    AND f.current_stock < f.forecast_30_days
                )
                SELECT 
                    product_name,
                    sku,
                    warehouse_name,
                    urgency,
                    days_until_stockout,
                    units_at_risk,
                    -- Lost revenue = units at risk × price × margin assumption (e.g., 100% since it's lost sale)
                    ROUND(cast(units_at_risk * price as numeric), 2) as estimated_lost_revenue,
                    -- Expedite cost assumption: 15-25% of product value for rush shipping
                    ROUND(cast(units_at_risk * price * 
                        CASE urgency
                            WHEN 'URGENT' THEN 0.25  -- 25% expedite premium for urgent
                            WHEN 'HIGH' THEN 0.15    -- 15% for high priority
                            ELSE 0.10
                        end as numeric), 2) as estimated_expedite_cost,
                    -- Total impact
                    ROUND(cast(units_at_risk * price * 
                        CASE urgency
                            WHEN 'URGENT' THEN 1.25
                            WHEN 'HIGH' THEN 1.15
                            ELSE 1.10
                        end as numeric), 2) as total_impact
                FROM critical_items
                ORDER BY total_impact DESC
            """

            results = self.db.execute_query(query, tuple(params) if params else None)

            if not results:
                return json.dumps(
                    {
                        "status": "low_risk",
                        "message": "No significant stockout risk detected in the next 30 days.",
                        "total_exposure_eur": 0,
                        "items": [],
                    }
                )

            items = []
            total_lost_revenue = 0
            total_expedite_cost = 0
            total_impact = 0

            for row in results:
                lost_revenue = float(row.get("estimated_lost_revenue", 0)) if row.get("estimated_lost_revenue") else 0
                expedite_cost = float(row.get("estimated_expedite_cost", 0)) if row.get("estimated_expedite_cost") else 0
                impact = float(row.get("total_impact", 0)) if row.get("total_impact") else 0
                
                total_lost_revenue += lost_revenue
                total_expedite_cost += expedite_cost
                total_impact += impact

                items.append(
                    {
                        "product_name": row.get("product_name"),
                        "sku": row.get("sku"),
                        "warehouse": row.get("warehouse_name"),
                        "urgency": row.get("urgency"),
                        "days_until_stockout": row.get("days_until_stockout"),
                        "units_at_risk": row.get("units_at_risk"),
                        "estimated_lost_revenue": round(lost_revenue, 2),
                        "estimated_expedite_cost": round(expedite_cost, 2),
                        "total_impact": round(impact, 2),
                    }
                )

            return json.dumps(
                {
                    "status": "risk_identified",
                    "summary": {
                        "total_lost_revenue_eur": round(total_lost_revenue, 2),
                        "total_expedite_cost_eur": round(total_expedite_cost, 2),
                        "total_impact_eur": round(total_impact, 2),
                    },
                    "items_at_risk": len(items),
                    "analysis_window_days": 30,
                    "items": items,
                }
            )

        except Exception as e:
            logger.error(f"Error estimating stockout impact: {e}")
            return json.dumps({"error": str(e)})

    def resolve_inventory_alert(
        self,
        forecast_id: int,
        quantity: int,
        supplier_name: str,
        requested_by: str = "SmartStock AI Agent",
        notes: Optional[str] = None,
    ) -> str:
        """
        Resolve an inventory alert by creating a purchase order and inbound transaction.
        
        This creates:
        1. An inbound transaction in 'processing' status
        2. Updates the forecast status to 'resolved'
        
        Args:
            forecast_id: The forecast_id of the alert to resolve
            quantity: The quantity to order
            supplier_name: The supplier to order from
            requested_by: Who requested the order (default: SmartStock AI Agent)
            notes: Optional notes for the order
        """
        import random
        import string
        from datetime import datetime, timedelta
        
        # Ensure quantity is an integer (LLM might pass float)
        quantity = int(quantity)
        forecast_id = int(forecast_id)
        
        try:
            logger.info(f"Resolving inventory alert for forecast_id={forecast_id}, quantity={quantity}")
            
            # First, get the forecast details to find product_id and warehouse_id
            forecast_query = f"""
                SELECT 
                    f.forecast_id,
                    f.product_id,
                    f.warehouse_id,
                    f.current_stock,
                    f.forecast_30_days,
                    f.reorder_point,
                    f.status,
                    p.name as product_name,
                    p.sku,
                    p.price,
                    w.name as warehouse_name
                FROM {self.schema}.inventory_forecast f
                JOIN {self.schema}.products p ON f.product_id = p.product_id
                JOIN {self.schema}.warehouses w ON f.warehouse_id = w.warehouse_id
                WHERE f.forecast_id = %s
            """
            
            forecast_result = self.db.execute_query(forecast_query, (forecast_id,))
            
            if not forecast_result:
                return json.dumps({
                    "success": False,
                    "error": f"Forecast with ID {forecast_id} not found"
                })
            
            forecast = forecast_result[0]
            product_id = forecast["product_id"]
            warehouse_id = forecast["warehouse_id"]
            product_name = forecast["product_name"]
            sku = forecast["sku"]
            warehouse_name = forecast["warehouse_name"]
            unit_price = float(forecast["price"]) if forecast["price"] else 0
            
            # Check if already resolved
            if forecast["status"] == "resolved":
                return json.dumps({
                    "success": False,
                    "error": f"Forecast {forecast_id} is already resolved"
                })
            
            # Generate PO number
            po_number = f"PO-{datetime.now().strftime('%Y')}-{random.randint(1000, 9999)}"
            
            # Generate unique transaction number (format: INB-YYMMDD-XXXXX)
            date_str = datetime.now().strftime("%y%m%d")
            chars = string.ascii_uppercase + string.digits
            suffix = "".join(random.choices(chars, k=5))
            transaction_number = f"INB-{date_str}-{suffix}"
            
            # Build transaction notes
            order_notes = f"PO {po_number} from {supplier_name}"
            if notes:
                order_notes += f" - {notes}"
            
            # Calculate expected delivery (3-7 days based on urgency)
            if forecast["current_stock"] == 0:
                delivery_days = 3  # Urgent
            elif forecast["current_stock"] < forecast["reorder_point"]:
                delivery_days = 5  # Normal
            else:
                delivery_days = 7  # Low priority
            
            expected_delivery = datetime.now() + timedelta(days=delivery_days)
            
            # Get next transaction ID (workaround for sequence issues)
            max_id_query = f"""
                SELECT COALESCE(MAX(transaction_id), 0) + 1 as next_id 
                FROM {self.schema}.inventory_transactions
            """
            id_result = self.db.execute_query(max_id_query)
            next_transaction_id = id_result[0]["next_id"] if id_result else 1
            
            # Create the inbound transaction
            insert_transaction_query = f"""
                INSERT INTO {self.schema}.inventory_transactions
                (transaction_id, transaction_number, product_id, warehouse_id,
                 quantity_change, transaction_type, status, notes,
                 transaction_timestamp, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """
            
            self.db.execute_update(
                insert_transaction_query,
                (
                    next_transaction_id,
                    transaction_number,
                    product_id,
                    warehouse_id,
                    quantity,  # Positive for inbound
                    "inbound",
                    "processing",
                    order_notes,
                ),
            )
            
            logger.info(f"Created transaction {transaction_number} (ID: {next_transaction_id})")
            
            # Update forecast status to resolved
            update_forecast_query = f"""
                UPDATE {self.schema}.inventory_forecast
                SET status = 'resolved', last_updated = CURRENT_TIMESTAMP
                WHERE forecast_id = %s
            """
            self.db.execute_update(update_forecast_query, (forecast_id,))
            
            logger.info(f"Marked forecast {forecast_id} as resolved")
            
            # Calculate order total
            order_total = quantity * unit_price
            
            return json.dumps({
                "success": True,
                "order": {
                    "po_number": po_number,
                    "transaction_number": transaction_number,
                    "transaction_id": next_transaction_id,
                    "product_name": product_name,
                    "sku": sku,
                    "quantity": quantity,
                    "unit_price_eur": round(unit_price, 2),
                    "order_total_eur": round(order_total, 2),
                    "supplier": supplier_name,
                    "warehouse": warehouse_name,
                    "status": "processing",
                    "expected_delivery": expected_delivery.strftime("%Y-%m-%d"),
                    "delivery_days": delivery_days,
                    "requested_by": requested_by,
                },
                "forecast_update": {
                    "forecast_id": forecast_id,
                    "previous_status": forecast["status"],
                    "new_status": "resolved",
                },
                "message": f"Order {po_number} created successfully. {quantity} units of {product_name} ordered from {supplier_name}. Expected delivery: {expected_delivery.strftime('%Y-%m-%d')} ({delivery_days} days)."
            })
            
        except Exception as e:
            logger.error(f"Error resolving inventory alert: {e}", exc_info=True)
            return json.dumps({
                "success": False,
                "error": str(e)
            })


class GenieTool:
    """Tool for querying Genie (natural language to SQL)."""

    def __init__(self):
        """Initialize Genie tool."""
        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.space_id = os.getenv("GENIE_SPACE_ID")

    async def query_genie(self, question: str) -> str:
        """Query Genie with a natural language question about inventory data."""
        if not all([self.host, self.token, self.space_id]):
            return json.dumps(
                {
                    "error": "Genie not configured",
                    "message": "Please set DATABRICKS_HOST, DATABRICKS_TOKEN, and GENIE_SPACE_ID",
                }
            )

        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}

        try:
            import asyncio

            async with httpx.AsyncClient(timeout=90.0, verify=False) as client:
                # Start conversation
                start_url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/start-conversation"
                response = await client.post(start_url, headers=headers, json={"content": question})

                if response.status_code != 200:
                    return json.dumps({"error": f"Failed to start Genie conversation: {response.text}"})

                data = response.json()
                conversation_id = data.get("conversation_id")
                message_id = data.get("message_id")

                # Poll for completion
                for attempt in range(20):
                    await asyncio.sleep(2)

                    status_url = f"{self.host}/api/2.0/genie/spaces/{self.space_id}/conversations/{conversation_id}/messages/{message_id}"
                    status_response = await client.get(status_url, headers=headers)

                    if status_response.status_code != 200:
                        continue

                    status_data = status_response.json()
                    message_status = status_data.get("status", "")

                    if message_status == "COMPLETED":
                        # Extract response
                        attachments = status_data.get("attachments", [])
                        result = {"status": "success", "question": question}

                        for attachment in attachments:
                            if "text" in attachment:
                                result["explanation"] = attachment["text"].get("content", "")

                            if "query" in attachment:
                                query_info = attachment["query"]
                                result["sql_query"] = query_info.get("query", "")
                                result["description"] = query_info.get("description", "")

                                # Fetch query results
                                attachment_id = attachment.get("attachment_id")
                                if attachment_id:
                                    result_url = f"{status_url}/query-result/{attachment_id}"
                                    result_response = await client.get(result_url, headers=headers)

                                    if result_response.status_code == 200:
                                        result_data = result_response.json()
                                        statement_response = result_data.get("statement_response", {})
                                        result_section = statement_response.get("result", {})
                                        data_array = result_section.get("data_array", [])

                                        manifest = statement_response.get("manifest", {})
                                        schema = manifest.get("schema", {})
                                        columns = [col.get("name") for col in schema.get("columns", [])]

                                        result["columns"] = columns
                                        result["data"] = data_array[:50]  # Limit rows
                                        result["row_count"] = len(data_array)

                        return json.dumps(result)

                    elif message_status == "FAILED":
                        return json.dumps(
                            {"error": "Genie query failed", "message": status_data.get("error", "Unknown error")}
                        )

                return json.dumps({"error": "Genie query timed out"})

        except Exception as e:
            logger.error(f"Error querying Genie: {e}")
            return json.dumps({"error": str(e)})


# =============================================================================
# AGENT CLASS
# =============================================================================


class SmartStockAgent:
    """Tool-calling agent that runs locally in FastAPI."""

    def __init__(self, db_connection=None):
        """Initialize the agent."""
        self.host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
        self.token = os.getenv("DATABRICKS_TOKEN")
        self.llm_endpoint = os.getenv("LLM_ENDPOINT", "databricks-gpt-oss-120b")

        # Initialize OpenAI client pointing to Databricks
        if self.host and self.token:
            self.client = OpenAI(api_key=self.token, base_url=f"{self.host}/serving-endpoints")
        else:
            self.client = None
            logger.warning("Databricks credentials not configured for agent")

        # Initialize tools
        self.inventory_tools = InventoryTools(db_connection) if db_connection else None
        self.genie_tool = GenieTool()

        # Define available tools
        self.tools = self._build_tools()

    def _build_tools(self) -> list[dict]:
        """Build the tool definitions for the LLM."""
        tools = []

        # Inventory alerts tool
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "get_critical_inventory_alerts",
                    "description": "Get items that are at risk of stockout or need immediate attention. Returns a list of products with low stock, their current levels, reorder points, days until stockout, and urgency levels (critical, high, medium).",
                    "parameters": {"type": "object", "properties": {}, "required": []},
                },
            }
        )

        # Stockout impact tool
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "estimate_stockout_impact",
                    "description": "Estimate the financial impact of potential stockouts. Calculates potential lost revenue for items at risk of running out of stock in the next 7 days.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "product_ids": {
                                "type": "array",
                                "items": {"type": "integer"},
                                "description": "Optional list of specific product IDs to analyze. If not provided, analyzes all at-risk items.",
                            }
                        },
                        "required": [],
                    },
                },
            }
        )

        # Genie query tool
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "query_genie",
                    "description": "Ask analytical questions about inventory data using natural language. Genie will generate and execute SQL queries to answer questions about trends, KPIs, historical patterns, product performance, warehouse comparisons, sales history, and more.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "A natural language question about the inventory data. Examples: 'What are the top 5 products by revenue?', 'Show monthly sales trend for the last 6 months', 'Which warehouse has the highest turnover rate?'",
                            }
                        },
                        "required": ["question"],
                    },
                },
            }
        )

        # Resolve inventory alert tool - creates purchase order and marks forecast as resolved
        tools.append(
            {
                "type": "function",
                "function": {
                    "name": "resolve_inventory_alert",
                    "description": "Resolve an inventory alert by creating a purchase order. This creates an inbound transaction in 'processing' status and marks the forecast as 'resolved'. Use this when the user confirms they want to proceed with an order.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "forecast_id": {
                                "type": "integer",
                                "description": "The forecast_id of the inventory alert to resolve (from get_critical_inventory_alerts results)",
                            },
                            "quantity": {
                                "type": "integer",
                                "description": "The quantity of units to order",
                            },
                            "supplier_name": {
                                "type": "string",
                                "description": "The name of the supplier to order from (e.g., 'VeloTech Components', 'CycleCore Industries', 'EuroBike Parts')",
                            },
                            "notes": {
                                "type": "string",
                                "description": "Optional notes for the order (e.g., 'Expedited shipping requested')",
                            },
                        },
                        "required": ["forecast_id", "quantity", "supplier_name"],
                    },
                },
            }
        )

        return tools

    async def execute_tool(self, tool_name: str, arguments: dict) -> str:
        """Execute a tool and return the result."""
        logger.info(f"Executing tool: {tool_name} with args: {arguments}")

        if tool_name == "get_critical_inventory_alerts":
            if self.inventory_tools:
                return self.inventory_tools.get_critical_inventory_alerts()
            return json.dumps({"error": "Database not configured"})

        elif tool_name == "estimate_stockout_impact":
            if self.inventory_tools:
                product_ids = arguments.get("product_ids")
                return self.inventory_tools.estimate_stockout_impact(product_ids)
            return json.dumps({"error": "Database not configured"})

        elif tool_name == "query_genie":
            question = arguments.get("question", "")
            return await self.genie_tool.query_genie(question)

        elif tool_name == "resolve_inventory_alert":
            if self.inventory_tools:
                logger.info(f"resolve_inventory_alert called with arguments: {arguments}")
                
                forecast_id = arguments.get("forecast_id")
                quantity = arguments.get("quantity")
                supplier_name = arguments.get("supplier_name")
                notes = arguments.get("notes")
                
                # Log individual parameters for debugging
                logger.info(f"Parsed params - forecast_id: {forecast_id}, quantity: {quantity}, supplier_name: {supplier_name}")
                
                # Check for missing parameters with detailed error
                missing = []
                if forecast_id is None:
                    missing.append("forecast_id")
                if quantity is None:
                    missing.append("quantity")
                if supplier_name is None or supplier_name == "":
                    missing.append("supplier_name")
                
                if missing:
                    error_msg = f"Missing required parameters: {', '.join(missing)}. Received arguments: {arguments}"
                    logger.error(error_msg)
                    return json.dumps({"error": error_msg, "received_arguments": arguments})
                
                try:
                    # Ensure integer types for IDs and quantities
                    return self.inventory_tools.resolve_inventory_alert(
                        forecast_id=int(forecast_id),
                        quantity=int(quantity),
                        supplier_name=str(supplier_name),
                        notes=str(notes) if notes else None,
                    )
                except (ValueError, TypeError) as e:
                    error_msg = f"Invalid parameter types: {e}. forecast_id={forecast_id}, quantity={quantity}"
                    logger.error(error_msg)
                    return json.dumps({"error": error_msg})
            return json.dumps({"error": "Database not configured"})

        else:
            return json.dumps({"error": f"Unknown tool: {tool_name}"})

    def _extract_text_content(self, content) -> str:
        """Extract text content from various response formats.
        
        Some models return content as a list of blocks (e.g., reasoning + text),
        while others return a plain string. This normalizes the response.
        """
        if content is None:
            return ""
        
        if isinstance(content, str):
            return content
        
        if isinstance(content, list):
            # Handle list of content blocks (e.g., from extended thinking models)
            text_parts = []
            for block in content:
                if isinstance(block, dict):
                    # Look for text content in various formats
                    if block.get("type") == "text":
                        text_parts.append(block.get("text", ""))
                    elif block.get("type") == "reasoning":
                        # Skip reasoning blocks, we just want the final text
                        continue
                    elif "text" in block:
                        text_parts.append(block["text"])
                    elif "content" in block:
                        text_parts.append(str(block["content"]))
                elif isinstance(block, str):
                    text_parts.append(block)
            return "\n".join(text_parts)
        
        # Fallback: convert to string
        return str(content)

    def _build_tool_summary(self, tool_calls_made: list) -> str:
        """Build a summary from tool call results when LLM fails to summarize."""
        orders_created = []
        orders_failed = []
        other_actions = []
        
        for tc in tool_calls_made:
            tool_name = tc.get("name", "")
            result_preview = tc.get("result_preview", "")
            
            if tool_name == "resolve_inventory_alert":
                # Try to parse the result to extract order details
                try:
                    result_data = json.loads(result_preview)
                    if result_data.get("success"):
                        order = result_data.get("order", {})
                        orders_created.append(order)
                    else:
                        orders_failed.append(result_data.get("error", "Unknown error"))
                except:
                    pass
            
            elif tool_name == "get_critical_inventory_alerts":
                other_actions.append("📊 Retrieved inventory alerts")
            
            elif tool_name == "estimate_stockout_impact":
                other_actions.append("💰 Calculated stockout financial impact")
        
        # Build the summary
        summary_parts = []
        
        # Add other actions first
        for action in other_actions:
            summary_parts.append(action)
        
        # Summarize orders
        if orders_created:
            total_value = sum(o.get("order_total_eur", 0) for o in orders_created)
            supplier = orders_created[0].get("supplier", "Unknown") if orders_created else "Unknown"
            
            summary_parts.append(f"## ✅ Orders Created Successfully\n")
            summary_parts.append(f"**{len(orders_created)} purchase orders** submitted to **{supplier}**\n")
            
            # Create a table of orders
            summary_parts.append("| PO Number | Product | Qty | Total |")
            summary_parts.append("|-----------|---------|-----|-------|")
            
            for order in orders_created:
                po = order.get("po_number", "N/A")
                product = order.get("product_name", "N/A")[:30]  # Truncate long names
                qty = order.get("quantity", "N/A")
                total = order.get("order_total_eur", 0)
                summary_parts.append(f"| {po} | {product} | {qty} | €{total:.2f} |")
            
            summary_parts.append(f"\n**Grand Total: €{total_value:.2f}**")
            
            # Add delivery info
            if orders_created:
                delivery = orders_created[0].get("expected_delivery", "N/A")
                summary_parts.append(f"\n📦 **Expected Delivery:** {delivery}")
            
            # Add supplier notification
            summary_parts.append(f"\n---\n")
            summary_parts.append(f"📧 Sending orders to {supplier} via EDI...")
            summary_parts.append(f"Email confirmation sent to procurement@{supplier.lower().replace(' ', '')}.com")
            summary_parts.append(f"Expected supplier acknowledgment: within 2 hours")
            summary_parts.append(f"\n✅ All forecasts marked as **resolved**")
        
        if orders_failed:
            summary_parts.append(f"\n## ❌ Failed Orders\n")
            for error in orders_failed:
                summary_parts.append(f"- {error}")
        
        if summary_parts:
            return "\n".join(summary_parts)
        
        return "Actions completed. Please check the results."

    async def chat(self, messages: list[dict], max_iterations: int = 20) -> dict:
        """Process a chat request with tool calling support.

        Args:
            messages: List of message dicts with 'role' and 'content'
            max_iterations: Maximum number of LLM calls (each call may execute multiple tools)

        Returns:
            Dict with 'content' (response text) and 'tool_calls' (list of tools used)
        """
        if not self.client:
            return {
                "content": "I'm sorry, but I'm not properly configured. Please check the Databricks credentials.",
                "tool_calls": [],
                "error": "Client not configured",
            }

        # Prepare messages with system prompt
        full_messages = [{"role": "system", "content": SYSTEM_PROMPT}]
        full_messages.extend(messages)

        tool_calls_made = []

        for iteration in range(max_iterations):
            try:
                logger.info(f"Agent iteration {iteration + 1}/{max_iterations}")
                
                # After first tool call, don't force more tool calls
                # This helps the model know it can respond with text
                current_tool_choice = "auto"
                if iteration > 0 and tool_calls_made:
                    # After we've made tool calls, let the model decide freely
                    # Some models need "none" to stop calling tools, others work with "auto"
                    current_tool_choice = "auto"
                
                # Call the LLM
                response = self.client.chat.completions.create(
                    model=self.llm_endpoint,
                    messages=full_messages,
                    tools=self.tools if self.tools else None,
                    tool_choice=current_tool_choice,
                )

                assistant_message = response.choices[0].message
                finish_reason = response.choices[0].finish_reason
                
                # Extract text content (handles both string and list formats)
                message_content = self._extract_text_content(assistant_message.content)
                
                logger.info(f"LLM response - finish_reason: {finish_reason}, has_tool_calls: {bool(assistant_message.tool_calls)}, has_content: {bool(message_content)}")

                # Check if the model wants to call tools
                if assistant_message.tool_calls:
                    # Add assistant message to history
                    full_messages.append(
                        {
                            "role": "assistant",
                            "content": message_content,
                            "tool_calls": [
                                {
                                    "id": tc.id,
                                    "type": "function",
                                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                                }
                                for tc in assistant_message.tool_calls
                            ],
                        }
                    )

                    # Execute each tool call
                    for tool_call in assistant_message.tool_calls:
                        tool_name = tool_call.function.name
                        raw_arguments = tool_call.function.arguments
                        
                        logger.info(f"Tool call: {tool_name}, raw arguments: {raw_arguments}")
                        
                        try:
                            arguments = json.loads(raw_arguments) if raw_arguments else {}
                        except json.JSONDecodeError as e:
                            logger.error(f"Failed to parse tool arguments: {e}. Raw: {raw_arguments}")
                            arguments = {}

                        logger.info(f"Executing tool: {tool_name} with parsed arguments: {arguments}")
                        
                        # Execute the tool
                        result = await self.execute_tool(tool_name, arguments)
                        
                        # Truncate very large results to avoid context overflow
                        if len(result) > 8000:
                            result = result[:8000] + "\n... (result truncated for brevity)"

                        tool_calls_made.append(
                            {
                                "name": tool_name,
                                "arguments": arguments,
                                "result_preview": result[:500] if len(result) > 500 else result,
                            }
                        )

                        # Add tool result to messages
                        full_messages.append({"role": "tool", "tool_call_id": tool_call.id, "content": result})

                    # Check if we've made many resolve_inventory_alert calls - if so, we should wrap up
                    resolve_calls = [tc for tc in tool_calls_made if tc["name"] == "resolve_inventory_alert"]
                    if len(resolve_calls) >= 15 and iteration >= 8:
                        # We've created enough orders, force a summary
                        logger.info(f"Created {len(resolve_calls)} orders, forcing summary to avoid timeout")
                        content = self._build_tool_summary(tool_calls_made)
                        return {"content": content, "tool_calls": tool_calls_made}

                    # Continue the loop to let the model process tool results
                    continue

                else:
                    # No tool calls, check if we have content
                    content = message_content  # Already extracted above
                    
                    # If we have tool results but no content, ask the LLM to summarize
                    if not content and tool_calls_made:
                        logger.info("LLM returned no content after tool calls, requesting summary...")
                        
                        # Add a prompt to get the LLM to summarize the tool results
                        full_messages.append({
                            "role": "user",
                            "content": "Please summarize what you just did based on the tool results above. Include any PO numbers, order details, and confirmation messages."
                        })
                        
                        # Make one more call to get a proper summary
                        try:
                            summary_response = self.client.chat.completions.create(
                                model=self.llm_endpoint,
                                messages=full_messages,
                                tools=None,  # No tools, just get a text response
                            )
                            # Extract text content from summary response too
                            content = self._extract_text_content(summary_response.choices[0].message.content)
                            logger.info(f"Got summary response: {content[:200] if content else 'empty'}...")
                        except Exception as e:
                            logger.error(f"Failed to get summary: {e}")
                            # Fall back to building our own summary from tool results
                            content = self._build_tool_summary(tool_calls_made)
                    
                    if not content:
                        content = "I couldn't generate a response. Please try rephrasing your question."
                    
                    logger.info(f"Agent completed with {len(tool_calls_made)} tool calls")
                    return {"content": content, "tool_calls": tool_calls_made}

            except Exception as e:
                logger.error(f"Error in agent chat iteration {iteration}: {e}", exc_info=True)
                
                # If we have some tool results, try to return a useful response
                if tool_calls_made:
                    return {
                        "content": f"I encountered an issue but gathered some data. Error: {str(e)}",
                        "tool_calls": tool_calls_made,
                        "error": str(e),
                    }
                
                return {
                    "content": f"I encountered an error while processing your request: {str(e)}",
                    "tool_calls": tool_calls_made,
                    "error": str(e),
                }

        # Max iterations reached - try to provide a useful response with the data we have
        logger.warning(f"Agent reached max iterations ({max_iterations}) with {len(tool_calls_made)} tool calls")
        
        # Build a summary of what we found using our helper function
        if tool_calls_made:
            # Use the built-in summary function to create a proper response
            content = self._build_tool_summary(tool_calls_made)
            return {
                "content": content,
                "tool_calls": tool_calls_made,
            }
        
        return {
            "content": "I've reached the maximum number of attempts. Please try a simpler question.",
            "tool_calls": tool_calls_made,
        }


# =============================================================================
# SINGLETON INSTANCE
# =============================================================================

_agent_instance: Optional[SmartStockAgent] = None


def get_agent() -> SmartStockAgent:
    """Get or create the agent singleton."""
    global _agent_instance

    if _agent_instance is None:
        # Try to get database connection
        db_connection = None
        try:
            from server.db_selector import db

            db_connection = db
        except Exception as e:
            logger.warning(f"Could not get database connection for agent: {e}")

        _agent_instance = SmartStockAgent(db_connection=db_connection)

    return _agent_instance

