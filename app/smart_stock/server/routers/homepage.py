from fastapi import APIRouter, HTTPException
from datetime import datetime, timedelta
from typing import List, Dict, Any
from server.postgres_database import db
from server.models import (
    HomepageData, TrendingProduct, SupplierMetrics,
    WarehouseDetail, DailySummary
)
import random

router = APIRouter()

@router.get("/homepage/critical-counts")
async def get_critical_counts() -> Dict[str, int]:
    """Get critical inventory counts quickly for immediate display."""
    counts = get_critical_inventory_counts()
    return {
        "criticalCount": counts.get('critical', 0),
        "warningCount": counts.get('warning', 0)
    }

@router.get("/homepage/data", response_model=HomepageData)
async def get_homepage_data() -> HomepageData:
    """Get all data needed for the homepage dashboard."""

    # Generate daily summary
    daily_summary = generate_daily_summary()

    # Get trending products
    trending_products = get_trending_products()

    # Get supplier metrics
    supplier_metrics = get_supplier_metrics()

    # Get warehouse details
    warehouse_details = get_warehouse_details()

    # Get critical inventory counts
    critical_counts = get_critical_inventory_counts()

    return HomepageData(
        dailySummary=daily_summary,
        trendingProducts=trending_products,
        supplierMetrics=supplier_metrics,
        warehouseDetails=warehouse_details,
        criticalCount=critical_counts.get('critical', 0),
        warningCount=critical_counts.get('warning', 0)
    )

def generate_daily_summary() -> str:
    """Generate a personalized daily summary for Elena."""

    try:
        # Get yesterday's metrics
        yesterday = datetime.now() - timedelta(days=1)

        # Query transaction counts
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        COUNT(*) as transaction_count,
                        COALESCE(SUM(quantity * unit_price), 0) as total_value
                    FROM inventory_transactions
                    WHERE DATE(transaction_timestamp) = %s
                """, (yesterday.date(),))

                result = cursor.fetchone()
                transaction_count = result[0] if result else 0
                total_value = result[1] if result else 0

                # Get urgent reorders
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM inventory_forecast
                    WHERE urgency_level = 'urgent'
                """)
                urgent_reorders = cursor.fetchone()[0] or 0

                # Get warehouse with highest activity
                cursor.execute("""
                    SELECT location, COUNT(*) as activity
                    FROM inventory_transactions
                    WHERE DATE(transaction_timestamp) = %s
                    GROUP BY location
                    ORDER BY activity DESC
                    LIMIT 1
                """, (yesterday.date(),))

                top_warehouse = cursor.fetchone()
                warehouse_info = ""
                if top_warehouse:
                    warehouse_info = f" The {top_warehouse[0]} warehouse had the highest activity."

                # Get pending orders count
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM inventory_transactions
                    WHERE transaction_status = 'pending'
                """)
                pending_orders = cursor.fetchone()[0] or 0

    except Exception as e:
        # Fallback to default summary if database query fails
        transaction_count = 127
        total_value = 48350
        urgent_reorders = 3
        pending_orders = 5
        warehouse_info = " The Newport News Shipyard had the highest activity."

    today = datetime.now()
    summary = f"""Good morning! 👋 Today is {today.strftime('%A, %B %d, %Y')}.
    Yesterday, we processed {transaction_count} transactions with a total value of ${total_value:,.0f}.{warehouse_info}
    {urgent_reorders} urgent reorders need your attention today.
    You have {pending_orders} pending orders to review.
    Today's focus: Optimize inventory levels and review critical stock alerts for frigate construction."""

    return summary

def get_trending_products() -> List[TrendingProduct]:
    """Get the top trending products based on recent sales."""

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Query for trending products (last 7 days vs previous 7 days)
                cursor.execute("""
                    WITH recent_sales AS (
                        SELECT
                            p.sku,
                            p.name,
                            SUM(CASE
                                WHEN it.transaction_timestamp >= CURRENT_DATE - INTERVAL '7 days'
                                THEN it.quantity
                                ELSE 0
                            END) as current_week,
                            SUM(CASE
                                WHEN it.transaction_timestamp >= CURRENT_DATE - INTERVAL '14 days'
                                AND it.transaction_timestamp < CURRENT_DATE - INTERVAL '7 days'
                                THEN it.quantity
                                ELSE 0
                            END) as previous_week
                        FROM products p
                        LEFT JOIN inventory_transactions it ON p.sku = it.sku
                        WHERE it.transaction_type = 'sale'
                        AND it.transaction_timestamp >= CURRENT_DATE - INTERVAL '14 days'
                        GROUP BY p.sku, p.name
                    )
                    SELECT
                        sku,
                        name,
                        current_week as sales,
                        CASE
                            WHEN previous_week = 0 THEN 100
                            ELSE ((current_week - previous_week) / previous_week::float * 100)::int
                        END as trend
                    FROM recent_sales
                    WHERE current_week > 0
                    ORDER BY current_week DESC
                    LIMIT 5
                """)

                results = cursor.fetchall()
                if results:
                    trending = []
                    for row in results:
                        trending.append(TrendingProduct(
                            sku=row[0],
                            name=row[1],
                            trend=row[3],
                            sales=row[2]
                        ))
                    return trending

    except Exception as e:
        pass  # Fall through to default data

    # Default data if query fails - Shipbuilding materials
    return [
        TrendingProduct(sku='HII-HUL-00001', name='Steel Plate HY-80 1in', trend=32, sales=245),
        TrendingProduct(sku='HII-PIP-00001', name='Seawater Pipe 6in CuNi', trend=28, sales=189),
        TrendingProduct(sku='HII-ELE-00001', name='Power Cable 4/0 AWG', trend=24, sales=156),
        TrendingProduct(sku='HII-FST-00001', name='Hex Bolt SS 1/2-13x2', trend=-12, sales=98),
        TrendingProduct(sku='HII-MEC-00001', name='Seawater Pump 500GPM', trend=18, sales=134)
    ]

def get_supplier_metrics() -> List[SupplierMetrics]:
    """Get supplier performance metrics."""

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Query for supplier metrics - HII shipbuilding suppliers
                cursor.execute("""
                    SELECT
                        supplier,
                        AVG(EXTRACT(DAY FROM (delivery_date - order_date))) as avg_days,
                        COUNT(CASE WHEN delivery_date <= expected_date THEN 1 END) * 100.0 / COUNT(*) as on_time
                    FROM (
                        SELECT
                            CASE
                                WHEN location = 'Newport News' THEN
                                    CASE WHEN RANDOM() < 0.5 THEN 'US Steel' ELSE 'Flowserve' END
                                WHEN location = 'Pascagoula' THEN
                                    CASE WHEN RANDOM() < 0.5 THEN 'Rolls-Royce' ELSE 'Parker Hannifin' END
                                ELSE
                                    CASE WHEN RANDOM() < 0.5 THEN 'BAE Systems' ELSE 'L3Harris' END
                            END as supplier,
                            transaction_timestamp as order_date,
                            transaction_timestamp + INTERVAL '3 days' + (RANDOM() * INTERVAL '2 days') as delivery_date,
                            transaction_timestamp + INTERVAL '4 days' as expected_date,
                            location
                        FROM inventory_transactions
                        WHERE transaction_type = 'inbound'
                        AND transaction_timestamp >= CURRENT_DATE - INTERVAL '30 days'
                    ) supplier_data
                    GROUP BY supplier
                """)

                results = cursor.fetchall()
                if results:
                    metrics = []
                    for row in results:
                        metrics.append(SupplierMetrics(
                            supplier=row[0],
                            avgDays=round(row[1], 1) if row[1] else 3.5,
                            onTime=round(row[2], 0) if row[2] else 90
                        ))
                    return metrics

    except Exception as e:
        pass  # Fall through to default data

    # Default supplier metrics - HII shipbuilding suppliers
    return [
        SupplierMetrics(supplier='US Steel', avgDays=3.2, onTime=95),
        SupplierMetrics(supplier='Flowserve', avgDays=4.1, onTime=88),
        SupplierMetrics(supplier='Rolls-Royce', avgDays=2.8, onTime=97),
        SupplierMetrics(supplier='Parker Hannifin', avgDays=3.5, onTime=92),
        SupplierMetrics(supplier='BAE Systems', avgDays=4.5, onTime=85),
        SupplierMetrics(supplier='L3Harris', avgDays=3.8, onTime=90)
    ]

def get_critical_inventory_counts() -> Dict[str, int]:
    """Get counts of critical and warning inventory items."""

    import os

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                # Get the schema
                schema = os.getenv('DB_SCHEMA', 'public')

                # Get counts based on status (matching the frontend logic)
                cursor.execute(f"""
                    SELECT
                        SUM(CASE
                            WHEN (
                                CASE
                                    WHEN f.status = 'resolved' THEN 'resolved'
                                    WHEN f.current_stock = 0 THEN 'out_of_stock'
                                    WHEN f.current_stock < (f.forecast_30_days * 0.5) THEN 'reorder_needed'
                                    WHEN f.current_stock < f.forecast_30_days THEN 'low_stock'
                                    ELSE 'in_stock'
                                END
                            ) IN ('out_of_stock', 'reorder_needed') THEN 1
                            ELSE 0
                        END) as critical_count,
                        SUM(CASE
                            WHEN (
                                CASE
                                    WHEN f.status = 'resolved' THEN 'resolved'
                                    WHEN f.current_stock = 0 THEN 'out_of_stock'
                                    WHEN f.current_stock < (f.forecast_30_days * 0.5) THEN 'reorder_needed'
                                    WHEN f.current_stock < f.forecast_30_days THEN 'low_stock'
                                    ELSE 'in_stock'
                                END
                            ) = 'low_stock' THEN 1
                            ELSE 0
                        END) as warning_count
                    FROM {schema}.inventory_forecast f
                    WHERE f.status != 'resolved'
                """)

                result = cursor.fetchone()
                if result:
                    return {
                        'critical': result[0] or 0,
                        'warning': result[1] or 0
                    }
    except Exception as e:
        # No fallback - return actual zero counts
        return {'critical': 0, 'warning': 0}

    return {'critical': 0, 'warning': 0}

def get_warehouse_details() -> List[WarehouseDetail]:
    """Get detailed information for each warehouse."""

    warehouses = []

    # Define warehouse base data - HII Shipbuilding Facilities
    warehouse_data = [
        {
            'id': 'newport-news',
            'name': 'Newport News Shipyard',
            'location': 'Newport News, VA',
            'lat': 36.9788,
            'lng': -76.4280,
            'capacity': 125000,
            'manager': 'James Morrison',
            'phone': '+1 757 380-XXXX'
        },
        {
            'id': 'pascagoula',
            'name': 'Ingalls Shipbuilding',
            'location': 'Pascagoula, MS',
            'lat': 30.3658,
            'lng': -88.5561,
            'capacity': 98000,
            'manager': 'Sarah Williams',
            'phone': '+1 228 935-XXXX'
        },
        {
            'id': 'hampton',
            'name': 'Hampton Roads Supply Depot',
            'location': 'Hampton, VA',
            'lat': 37.0299,
            'lng': -76.3452,
            'capacity': 65000,
            'manager': 'Robert Chen',
            'phone': '+1 757 722-XXXX'
        }
    ]

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                for wh in warehouse_data:
                    # Get current stock level for this warehouse
                    cursor.execute("""
                        SELECT COALESCE(SUM(quantity_on_hand), 0) as total_stock
                        FROM inventory
                        WHERE location = %s
                    """, (wh['location'].split(',')[0],))

                    stock_level = cursor.fetchone()[0] or 0

                    # Determine status based on various factors
                    if wh['id'] == 'hampton':
                        status = 'maintenance'  # Hampton is under maintenance
                    elif stock_level / wh['capacity'] > 0.95:
                        status = 'critical'  # Near capacity
                    else:
                        status = 'operational'

                    # Get recent incidents (simplified for demo)
                    recent_incidents = []
                    if status == 'maintenance':
                        recent_incidents = ['Scheduled maintenance - HVAC system upgrade']
                    elif status == 'critical':
                        recent_incidents = ['Near capacity - consider redistribution']

                    warehouses.append(WarehouseDetail(
                        id=wh['id'],
                        name=wh['name'],
                        location=wh['location'],
                        lat=wh['lat'],
                        lng=wh['lng'],
                        capacity=wh['capacity'],
                        currentStock=int(stock_level),
                        status=status,
                        manager=wh['manager'],
                        phone=wh['phone'],
                        recentIncidents=recent_incidents,
                        lastAudit=datetime.now().isoformat()
                    ))

    except Exception as e:
        # Default data if query fails
        for wh in warehouse_data:
            status = 'maintenance' if wh['id'] == 'hampton' else 'operational'
            stock_level = wh['capacity'] * 0.8  # Default to 80% capacity

            warehouses.append(WarehouseDetail(
                id=wh['id'],
                name=wh['name'],
                location=wh['location'],
                lat=wh['lat'],
                lng=wh['lng'],
                capacity=wh['capacity'],
                currentStock=int(stock_level),
                status=status,
                manager=wh['manager'],
                phone=wh['phone'],
                recentIncidents=[],
                lastAudit=datetime.now().isoformat()
            ))

    return warehouses