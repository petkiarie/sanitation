from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

households_bp = Blueprint("households", __name__)

# ============================================================
# HELPERS (CLEAN & EXPLICIT)
# ============================================================

def normalize_ward():
    ward = request.args.get("ward")
    return None if not ward or ward.upper() == "ALL" else ward.lower()


def normalize_label(label: str) -> str:
    return label.lower().strip()


def classify_sanitation_type(label: str) -> str:
    l = normalize_label(label)

    if "sewer" in l or "septic" in l:
        return "Flush to Sewer / Septic"

    if "pit latrine with slab" in l:
        return "Pit Latrine (Improved)"

    if "pit latrine without slab" in l or "open pit" in l:
        return "Pit Latrine (Unimproved)"

    if "vip" in l:
        return "VIP Latrine"

    return "Other / None"


def classify_water_source(label: str) -> str:
    l = normalize_label(label)

    # Priority-based classification (best source wins)
    if "naivawasco" in l:
        return "Utility (NAIVAWASCO)"

    if "community" in l or "private operators" in l:
        return "Small Service Providers (SSP)"

    if "borehole" in l or "shallow well" in l:
        return "Borehole / Well"

    if "rain" in l:
        return "Rainwater"

    if "kiosk" in l:
        return "Water Kiosk"

    return "Other Sources"


def dict_to_list(d: dict):
    return [{"label": k, "value": v} for k, v in d.items()]


# ============================================================
# SUMMARY (KPI METRICS)
# ============================================================
@households_bp.route("/api/households/summary", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def households_summary():
    ward = normalize_ward()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if ward:
        cur.execute(
            "SELECT * FROM mv_household_sanitation_ward_summary WHERE ward = %s",
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute("""
            SELECT
                SUM(total_households) AS total_households,
                ROUND(AVG(households_with_sanitation_pct), 1) AS households_with_sanitation_pct,
                ROUND(AVG(households_without_sanitation_pct), 1) AS households_without_sanitation_pct,
                ROUND(AVG(shared_facilities_pct), 1) AS shared_facilities_pct,
                ROUND(AVG(water_access_pct), 1) AS water_access_pct,
                ROUND(AVG(handwashing_available_pct), 1) AS handwashing_available_pct,
                ROUND(AVG(pwd_accessible_pct), 1) AS pwd_accessible_pct,
                ROUND(AVG(provides_privacy_pct), 1) AS provides_privacy_pct,
                ROUND(AVG(safe_for_women_pct), 1) AS safe_for_women_pct,
                ROUND(AVG(adequate_lighting_pct), 1) AS adequate_lighting_pct
            FROM mv_household_sanitation_ward_summary
        """)
        row = cur.fetchone()

    cur.close()
    conn.close()
    return jsonify(row)


# ============================================================
# CHART DATA (AGGREGATED & CLEAN)
# ============================================================
@households_bp.route("/api/households/charts", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def households_charts():
    ward = normalize_ward()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_household_sanitation_charts
        WHERE (%s IS NULL OR ward = %s)
        GROUP BY chart_type, category
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, (ward, ward))
    rows = cur.fetchall()
    cur.close()
    conn.close()

    sanitation_types = {}
    water_sources = {}
    sharing_patterns = {}

    for row in rows:
        chart_type = row["chart_type"]
        label = row["label"]
        value = row["value"]

        if chart_type == "sanitation_type":
            key = classify_sanitation_type(label)
            sanitation_types[key] = sanitation_types.get(key, 0) + value

        elif chart_type == "water_source":
            key = classify_water_source(label)
            water_sources[key] = water_sources.get(key, 0) + value

        elif chart_type == "toilet_sharing":
            sharing_patterns[label] = sharing_patterns.get(label, 0) + value

    return jsonify({
        "sanitationTypes": dict_to_list(sanitation_types),
        "waterSources": dict_to_list(water_sources),
        "sharingPatterns": dict_to_list(sharing_patterns),
    })
