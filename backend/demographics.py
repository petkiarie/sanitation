from flask import Blueprint, jsonify, request
from db import get_conn
from extensions import cache
from psycopg2.extras import RealDictCursor

demographics_bp = Blueprint("demographics", __name__)

# ============================================================
# SUMMARY
# ============================================================
@demographics_bp.route("/api/demographics/summary", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def demographics_summary():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    if ward:
        cur.execute(
            "SELECT * FROM mv_demographics_ward_summary WHERE ward = %s",
            (ward,)
        )
        row = cur.fetchone()
    else:
        cur.execute("""
            SELECT
                SUM(plots_surveyed) AS plots_surveyed,
                SUM(total_households) AS total_households,
                SUM(total_population) AS total_population,
                ROUND(AVG(avg_household_size), 2) AS avg_household_size,
                ROUND(AVG(avg_households_per_plot), 2) AS avg_households_per_plot,
                SUM(children_under_5_count) AS children_under_5_count,
                ROUND(AVG(children_under_5_pct), 1) AS children_under_5_pct,
                SUM(pwd_households_count) AS pwd_households_count,
                ROUND(AVG(pwd_households_pct), 1) AS pwd_households_pct,
                ROUND(AVG(male_population_pct), 1) AS male_population_pct,
                ROUND(AVG(female_population_pct), 1) AS female_population_pct,
                ROUND(AVG(male_owned_plots_pct), 1) AS male_owned_plots_pct,
                ROUND(AVG(female_owned_plots_pct), 1) AS female_owned_plots_pct
            FROM mv_demographics_ward_summary
        """)
        row = cur.fetchone()

    cur.close()
    conn.close()
    return jsonify(row)

# ============================================================
# CHARTS
# ============================================================
@demographics_bp.route("/api/demographics/charts", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def demographics_charts():
    ward = request.args.get("ward")
    ward = None if not ward or ward.upper() == "ALL" else ward.lower()

    sql = """
        SELECT
            chart_type,
            category AS label,
            SUM(value)::int AS value
        FROM mv_demographics_charts
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
        "populationAgeGroup": [],
        "genderDistribution": [],
        "disabilityTypes": [],
        "plotOwnershipGender": [],
    }

    chart_map = {
        "population_age_group": "populationAgeGroup",
        "gender_distribution": "genderDistribution",
        "disability_type": "disabilityTypes",
        "plot_ownership_gender": "plotOwnershipGender",
    }

    for r in rows:
        key = chart_map.get(r["chart_type"])
        if key:
            charts[key].append({
                "label": r["label"],
                "value": r["value"]
            })

    return jsonify(charts)
