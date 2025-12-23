from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

overview_bp = Blueprint("overview", __name__)

# ============================================================
# SUMMARY (UNCHANGED)
# ============================================================
@overview_bp.route("/api/overview/summary", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def overview_summary():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    conn = get_conn()
    cur = conn.cursor()

    if ward:
        cur.execute(
            "SELECT * FROM mv_overview_ward_summary WHERE ward = %s",
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute("""
            SELECT
                SUM(plots_surveyed) AS plots_surveyed,
                SUM(total_households) AS total_households,
                SUM(total_population) AS total_population,
                ROUND(AVG(avg_household_size), 1) AS avg_household_size,
                ROUND(AVG(water_access_pct), 1) AS water_access_pct,
                ROUND(AVG(sanitation_facilities_pct), 1) AS sanitation_facilities_pct,
                ROUND(AVG(shared_facilities_pct), 1) AS shared_facilities_pct,
                ROUND(AVG(handwashing_available_pct), 1) AS handwashing_available_pct,
                ROUND(AVG(self_financed_pct), 1) AS self_financed_pct,
                ROUND(AVG(never_emptied_pct), 1) AS never_emptied_pct
            FROM mv_overview_ward_summary
        """)
        row = cur.fetchone()

    cur.close()
    conn.close()
    return jsonify(row)


# ============================================================
# WATER SOURCE GROUPING (OVERVIEW ONLY)
# ============================================================
def group_water_source(label: str) -> str:
    l = label.lower()

    if "naivawasco" in l:
        return "Piped Water (NAIVAWASCO)"

    if "piped water" in l or "community/private" in l:
        return "Piped Water (Other Providers)"

    if "borehole" in l:
        return "Borehole"

    if "rain" in l:
        return "Rain Water"

    if "water kiosk" in l or "kiosk" in l:
        return "Water Kiosk"

    return "Other Sources"


# ============================================================
# CHART DATA (SIMPLE & DIRECT)
# ============================================================
@overview_bp.route("/api/overview/charts", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def overview_charts():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_overview_charts
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
        "toiletTypes": [],
        "waterSources": [],
        "sharingPatterns": [],
        "containmentTypes": [],
        "emptyingFrequency": [],
        "handwashingFacilities": [],
    }

    chart_type_map = {
        "sanitation_type": "toiletTypes",
        "water_source": "waterSources",
        "toilet_sharing": "sharingPatterns",
        "containment_type": "containmentTypes",
        "emptying_frequency": "emptyingFrequency",
        "handwashing_status": "handwashingFacilities",
    }

    for row in rows:
        key = chart_type_map.get(row["chart_type"])
        if not key:
            continue

        # Group ONLY water sources
        if key == "waterSources":
            grouped_label = group_water_source(row["label"])

            existing = next(
                (x for x in charts[key] if x["label"] == grouped_label),
                None
            )

            if existing:
                existing["value"] += row["value"]
            else:
                charts[key].append({
                    "label": grouped_label,
                    "value": row["value"],
                })
        else:
            charts[key].append({
                "label": row["label"],
                "value": row["value"],
            })

    return jsonify(charts)


# ============================================================
# WARDS (UNCHANGED)
# ============================================================
@overview_bp.route("/api/wards", methods=["GET"])
@cache.cached(timeout=600)
def wards():
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        SELECT DISTINCT ward
        FROM mv_overview_ward_summary
        WHERE ward IS NOT NULL
        ORDER BY ward
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify([r["ward"] for r in rows])
