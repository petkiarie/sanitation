from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

institutions_diagnostics_bp = Blueprint(
    "institutions_diagnostics", __name__
)

# ============================================================
# CHART AGGREGATES (GENERIC, FAST)
# ============================================================
@institutions_diagnostics_bp.route(
    "/api/institutions/diagnostics/charts", methods=["GET"]
)
@cache.cached(timeout=300, query_string=True)
def institutions_diagnostics_charts():
    """
    Generic chart endpoint backed by mv_institutions_chart_aggregates

    Query params (all optional):
    - ward
    - institution_category
    - institution_subcategory
    - metric
    """

    ward = request.args.get("ward")
    category = request.args.get("institution_category")
    subcategory = request.args.get("institution_subcategory")
    metric = request.args.get("metric")

    # Normalize filters
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()
    category = None if not category or category.upper() == "ALL" else category
    subcategory = None if not subcategory or subcategory.upper() == "ALL" else subcategory
    metric = None if not metric or metric.upper() == "ALL" else metric

    sql = """
        SELECT
            metric,
            category AS label,
            SUM(value)::int AS value
        FROM mv_institutions_chart_aggregates
        WHERE (%s IS NULL OR ward = %s)
          AND (%s IS NULL OR institution_category = %s)
          AND (%s IS NULL OR institution_subcategory = %s)
          AND (%s IS NULL OR metric = %s)
        GROUP BY metric, category
        ORDER BY metric, value DESC
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        sql,
        (
            ward, ward,
            category, category,
            subcategory, subcategory,
            metric, metric
        )
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Shape response by metric (frontend-friendly)
    charts = {}
    for row in rows:
        m = row["metric"]
        if m not in charts:
            charts[m] = []
        charts[m].append(
            {
                "label": row["label"],
                "value": row["value"]
            }
        )

    return jsonify(charts)

# ============================================================
# OPTION SUMMARY (TABLES, RISK PANELS)
# ============================================================
@institutions_diagnostics_bp.route(
    "/api/institutions/diagnostics/options", methods=["GET"]
)
@cache.cached(timeout=300, query_string=True)
def institutions_diagnostics_options():
    """
    Option-heavy diagnostics endpoint backed by mv_institutions_option_summary

    Query params (all optional):
    - ward
    - institution_category
    - institution_subcategory
    """

    ward = request.args.get("ward")
    category = request.args.get("institution_category")
    subcategory = request.args.get("institution_subcategory")

    # Normalize filters
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()
    category = None if not category or category.upper() == "ALL" else category
    subcategory = None if not subcategory or subcategory.upper() == "ALL" else subcategory

    sql = """
        SELECT
            ward,
            institution_category,
            institution_subcategory,

            total_institutions,

            ever_emptied_yes,
            ever_emptied_no,

            safe_sludge_yes,
            safe_sludge_no,

            solid_waste_open_dump,
            solid_waste_burning,
            solid_waste_collected,

            water_access_yes,
            water_access_no,
            water_continuous,

            handwashing_yes,
            handwashing_no,

            soap_available_yes,
            soap_available_no,

            maintenance_plan_yes,
            maintenance_plan_no,

            flood_affected_yes,
            flood_affected_no

        FROM mv_institutions_option_summary
        WHERE (%s IS NULL OR ward = %s)
          AND (%s IS NULL OR institution_category = %s)
          AND (%s IS NULL OR institution_subcategory = %s)
        ORDER BY ward, institution_subcategory
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        sql,
        (
            ward, ward,
            category, category,
            subcategory, subcategory
        )
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify(rows)


# ============================================================
# NARRATIVE DIAGNOSTICS (QUALITATIVE INSIGHTS)
# ============================================================

@institutions_diagnostics_bp.route(
    "/api/institutions/diagnostics/narrative", methods=["GET"]
)
@cache.cached(timeout=300, query_string=True)
def institutions_diagnostics_narrative():
    """
    Qualitative diagnostics endpoint backed by mv_institutions_diagnostics

    Query params (all optional):
    - ward
    - institution_category
    - institution_subcategory
    - metric
    """

    ward = request.args.get("ward")
    category = request.args.get("institution_category")
    subcategory = request.args.get("institution_subcategory")
    metric = request.args.get("metric")

    # Normalize filters
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()
    category = None if not category or category.upper() == "ALL" else category
    subcategory = None if not subcategory or subcategory.upper() == "ALL" else subcategory
    metric = None if not metric or metric.upper() == "ALL" else metric

    sql = """
        SELECT
            metric,
            category AS insight,
            SUM(value)::int AS count
        FROM mv_institutions_diagnostics
        WHERE (%s IS NULL OR ward = %s)
          AND (%s IS NULL OR institution_category = %s)
          AND (%s IS NULL OR institution_subcategory = %s)
          AND (%s IS NULL OR metric = %s)
        GROUP BY metric, category
        ORDER BY metric, count DESC
    """

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute(
        sql,
        (
            ward, ward,
            category, category,
            subcategory, subcategory,
            metric, metric
        )
    )

    rows = cur.fetchall()
    cur.close()
    conn.close()

    # Group results by metric for frontend consumption
    narrative = {}
    for row in rows:
        m = row["metric"]
        if m not in narrative:
            narrative[m] = []
        narrative[m].append(
            {
                "insight": row["insight"],
                "count": row["count"]
            }
        )

    return jsonify(narrative)