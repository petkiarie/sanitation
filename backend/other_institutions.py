from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

other_institutions_bp = Blueprint("other_institutions", __name__)

# ============================================================
# SUMMARY (KPI METRICS)
# ============================================================
@other_institutions_bp.route("/api/other-institutions/summary", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def other_institutions_summary():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if ward:
        cur.execute(
            """
            SELECT *
            FROM mv_other_institutions_ward_summary
            WHERE ward = %s
            """,
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute(
            """
            SELECT
                SUM(total_other_institutions) AS total_other_institutions,
                SUM(total_estimated_users) AS total_estimated_users,
                ROUND(AVG(institutions_with_sanitation_pct), 1) AS institutions_with_sanitation_pct,
                ROUND(AVG(water_access_pct), 1) AS water_access_pct,
                ROUND(AVG(handwashing_available_pct), 1) AS handwashing_available_pct,
                ROUND(AVG(regularly_cleaned_pct), 1) AS regularly_cleaned_pct,
                ROUND(AVG(pwd_accessible_pct), 1) AS pwd_accessible_pct,
                ROUND(AVG(toilets_per_user_ratio), 3) AS toilets_per_user_ratio
            FROM mv_other_institutions_ward_summary
            """
        )
        row = cur.fetchone()

    cur.close()
    conn.close()
    return jsonify(row)


# ============================================================
# CHART DATA
# ============================================================
@other_institutions_bp.route("/api/other-institutions/charts", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def other_institutions_charts():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_other_institutions_charts
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
        "pwdAccessibility": [],
        "genderSegregation": [],
    }

    chart_type_map = {
        "containment_type": "containmentTypes",
        "pwd_accessibility": "pwdAccessibility",
        "gender_segregation": "genderSegregation",
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
