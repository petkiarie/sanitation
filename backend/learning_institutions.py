from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

learning_institutions_bp = Blueprint(
    "learning_institutions",
    __name__
)

# ============================================================
# SUMMARY (KPI METRICS)
# ============================================================
@learning_institutions_bp.route(
    "/api/learning-institutions/summary",
    methods=["GET"]
)
@cache.cached(timeout=300, query_string=True)
def learning_institutions_summary():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if ward:
        cur.execute(
            """
            SELECT *
            FROM mv_learning_institutions_ward_summary
            WHERE ward = %s
            """,
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute(
            """
            SELECT
                SUM(total_learning_institutions) AS total_learning_institutions,
                SUM(total_students) AS total_students,
                ROUND(AVG(institutions_with_sanitation_pct), 1)
                    AS institutions_with_sanitation_pct,
                ROUND(AVG(handwashing_available_pct), 1)
                    AS handwashing_available_pct,
                ROUND(AVG(gender_segregated_pct), 1)
                    AS gender_segregated_pct,
                ROUND(AVG(continuous_water_supply_pct), 1)
                    AS continuous_water_supply_pct,
                ROUND(AVG(mhm_facilities_pct), 1)
                    AS mhm_facilities_pct,
                ROUND(AVG(pwd_accessible_pct), 1)
                    AS pwd_accessible_pct,
                ROUND(AVG(toilets_per_student_ratio), 3)
                    AS toilets_per_student_ratio
            FROM mv_learning_institutions_ward_summary
            """
        )
        row = cur.fetchone()

    cur.close()
    conn.close()

    return jsonify(row)


# ============================================================
# CHART DATA
# ============================================================
@learning_institutions_bp.route(
    "/api/learning-institutions/charts",
    methods=["GET"]
)
@cache.cached(timeout=300, query_string=True)
def learning_institutions_charts():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_learning_institutions_charts
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
        "mhmFacilities": [],
    }

    chart_type_map = {
        "containment_type": "containmentTypes",
        "handwashing_status": "handwashingStatus",
        "mhm_facilities": "mhmFacilities",
    }

    for row in rows:
        key = chart_type_map.get(row["chart_type"])
        if not key:
            continue

        charts[key].append(
            {
                "label": row["label"],
                "value": row["value"],
            }
        )

    return jsonify(charts)
