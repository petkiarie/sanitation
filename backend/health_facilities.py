# health_facilities.py
from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

health_facilities_bp = Blueprint("health_facilities", __name__)

# ============================================================
# SUMMARY (KPI METRICS)
# ============================================================
@health_facilities_bp.route("/api/health-facilities/summary", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def health_facilities_summary():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if ward:
        cur.execute(
            """
            SELECT *
            FROM mv_health_facilities_ward_summary
            WHERE ward = %s
            """,
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute(
            """
            SELECT
                SUM(total_health_facilities) AS total_health_facilities,
                SUM(total_estimated_users) AS total_estimated_users,
                ROUND(AVG(facilities_with_sanitation_pct), 1) AS facilities_with_sanitation_pct,
                ROUND(AVG(handwashing_available_pct), 1) AS handwashing_available_pct,
                ROUND(AVG(continuous_water_supply_pct), 1) AS continuous_water_supply_pct,
                ROUND(AVG(proper_waste_management_pct), 1) AS proper_waste_management_pct,
                ROUND(AVG(pwd_accessible_pct), 1) AS pwd_accessible_pct
            FROM mv_health_facilities_ward_summary
            """
        )
        row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify(row)


# ============================================================
# CHART DATA
# ============================================================
@health_facilities_bp.route("/api/health-facilities/charts", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def health_facilities_charts():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_health_institutions_charts
        WHERE (%s IS NULL OR ward = %s)
        GROUP BY chart_type, category
        ORDER BY chart_type, value DESC
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, (ward, ward))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    charts = {
        "containmentTypes": [],
        "handwashingStatus": [],
        "floodRisk": [],
    }

    chart_type_map = {
        "containment_type": "containmentTypes",
        "handwashing_status": "handwashingStatus",
        "flood_risk": "floodRisk",
    }

    for row in rows:
        key = chart_type_map.get(row["chart_type"])
        if key:
            charts[key].append(
                {
                    "label": row["label"],
                    "value": row["value"],
                }
            )

    return jsonify(charts)
