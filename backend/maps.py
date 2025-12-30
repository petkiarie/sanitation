# maps.py - ADD THESE IMPORTS AT THE TOP
import os
import json
from flask import Blueprint, jsonify, request, current_app
import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_CONFIG
from extensions import cache

# Blueprint
maps_bp = Blueprint("maps", __name__, url_prefix="/api/maps")

# Database helper
def get_db_conn():
    return psycopg2.connect(**DB_CONFIG)

# Utility helpers
def normalize_ward(ward: str | None) -> str | None:
    if not ward:
        return None
    ward = ward.strip().upper()
    if ward == "ALL":
        return None
    return ward

def row_to_feature(row: dict) -> dict:
    return {
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [row["lon"], row["lat"]],
        },
        "properties": {
            "plot_id": row["plot_id"],
            "ward": row["ward"],
            "settlement": row["settlement"],
            "sub_county": row["sub_county"],
            "sanitation_class": row["sanitation_class"],
            "sanitation_type": row["sanitation_type"],
            "is_shared": row["is_shared"],
            "households_sharing": row["households_sharing"],
            "has_handwashing": row["has_handwashing"],
            "solid_waste_mgmt": row["solid_waste_mgmt"],
            "total_persons": row["total_persons"],
            "children_under_5": row["children_under_5"],
            "financed_by": row["financed_by"],
            "photo": row["photo"],
        },
    }

# ============================================================
# WARD BOUNDARIES FROM GEOJSON FILE
# ============================================================

def get_geojson_file_path():
    """Get the path to the GeoJSON file."""
    # Try different possible locations
    possible_paths = [
        os.path.join(os.path.dirname(__file__), 'data', 'geojson', 'naivasha_wards.geojson'),
        os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'geojson', 'naivasha_wards.geojson'),
        os.path.join(current_app.root_path, 'data', 'geojson', 'naivasha_wards.geojson'),
        'data/geojson/naivasha_wards.geojson',
        'naivasha_wards.geojson'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            return path
    
    return None

def load_ward_boundaries():
    """Load ward boundaries from GeoJSON file."""
    file_path = get_geojson_file_path()
    
    if not file_path:
        current_app.logger.warning(f"GeoJSON file not found. Tried: {get_geojson_file_path()}")
        return {"type": "FeatureCollection", "features": []}
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Validate structure
        if not isinstance(data, dict) or data.get("type") != "FeatureCollection":
            current_app.logger.error("Invalid GeoJSON structure")
            return {"type": "FeatureCollection", "features": []}
        
        current_app.logger.info(f"Loaded {len(data.get('features', []))} ward boundaries from {file_path}")
        return data
        
    except json.JSONDecodeError as e:
        current_app.logger.error(f"Failed to parse GeoJSON: {e}")
        return {"type": "FeatureCollection", "features": []}
    except Exception as e:
        current_app.logger.error(f"Failed to load GeoJSON: {e}")
        return {"type": "FeatureCollection", "features": []}

# Cache the loaded data to avoid reading file on every request
WARD_BOUNDARIES_CACHE = None

def get_cached_ward_boundaries():
    """Get cached ward boundaries, loading if necessary."""
    global WARD_BOUNDARIES_CACHE
    if WARD_BOUNDARIES_CACHE is None:
        WARD_BOUNDARIES_CACHE = load_ward_boundaries()
    return WARD_BOUNDARIES_CACHE

@maps_bp.route("/ward-boundaries", methods=["GET"])
@cache.cached(timeout=3600, query_string=True)  # Cache for 1 hour
def ward_boundaries():
    """
    Returns ward boundaries as GeoJSON polygons from file.
    Optionally includes statistics for coloring.
    """
    ward = request.args.get("ward")
    include_stats = request.args.get("include_stats", "false").lower() == "true"
    
    # Get all ward boundaries
    data = get_cached_ward_boundaries()
    features = data.get("features", [])
    
    # If we need statistics, fetch them from the database
    ward_stats = {}
    if include_stats:
        try:
            ward_stats = fetch_ward_statistics()
        except Exception as e:
            current_app.logger.warning(f"Could not fetch ward statistics: {e}")
    
    # Filter by ward if specified
    if ward and ward.upper() != "ALL":
        filtered_features = []
        for feature in features:
            props = feature.get("properties", {})
            shape_name = props.get("shapeName", "")
            ward_name = props.get("ward", "")
            name = props.get("name", "")
            
            # Check if any of these match the requested ward
            feature_ward = shape_name or ward_name or name
            if feature_ward.lower() == ward.lower():
                # Add statistics if available
                if include_stats and ward_stats:
                    stats = ward_stats.get(feature_ward.upper(), {})
                    props.update(stats)
                filtered_features.append(feature)
        
        features = filtered_features
    else:
        # Add statistics to all features if requested
        if include_stats and ward_stats:
            for feature in features:
                props = feature.get("properties", {})
                shape_name = props.get("shapeName", "")
                ward_name = props.get("ward", "")
                name = props.get("name", "")
                feature_ward = shape_name or ward_name or name
                
                if feature_ward:
                    stats = ward_stats.get(feature_ward.upper(), {})
                    props.update(stats)
    
    return jsonify({
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "count": len(features),
            "ward": ward or "ALL",
            "source": "geojson_file",
            "include_stats": include_stats
        }
    })

def fetch_ward_statistics():
    """
    Fetch statistics for each ward from the database for coloring.
    """
    sql = """
        SELECT 
            UPPER(ward) as ward,
            COALESCE(total_households, 0) as total_households,
            COALESCE(households_with_sanitation_pct, 0) as sanitation_pct,
            COALESCE(water_access_pct, 0) as water_pct,
            COALESCE(safe_sanitation_pct, 0) as safety_pct,
            COALESCE(households_without_sanitation_pct, 0) as no_sanitation_pct
        FROM mv_household_sanitation_ward_summary
        WHERE ward IS NOT NULL
    """
    
    ward_stats = {}
    
    try:
        with get_db_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                
                for row in rows:
                    ward_stats[row["ward"]] = {
                        "total_households": row["total_households"],
                        "sanitation_pct": row["sanitation_pct"],
                        "water_pct": row["water_pct"],
                        "safety_pct": row["safety_pct"],
                        "no_sanitation_pct": row["no_sanitation_pct"]
                    }
    except Exception as e:
        current_app.logger.error(f"Error fetching ward statistics: {e}")
    
    return ward_stats

@maps_bp.route("/ward-boundaries/debug", methods=["GET"])
def ward_boundaries_debug():
    """
    Debug endpoint to see what's in the GeoJSON file.
    """
    data = get_cached_ward_boundaries()
    features = data.get("features", [])
    
    # Extract unique ward names
    ward_names = set()
    for feature in features[:10]:  # First 10 features
        props = feature.get("properties", {})
        shape_name = props.get("shapeName", "No shapeName")
        ward_name = props.get("ward", "No ward")
        name = props.get("name", "No name")
        ward_names.add(f"{shape_name} (shapeName) | {ward_name} (ward) | {name} (name)")
    
    return jsonify({
        "total_features": len(features),
        "sample_ward_names": list(ward_names),
        "file_path": get_geojson_file_path(),
        "file_exists": os.path.exists(get_geojson_file_path() or "")
    })

# ============================================================
# EXISTING ENDPOINTS (KEEP THESE AS IS)
# ============================================================

@maps_bp.route("/households", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def map_households():
    ward = normalize_ward(request.args.get("ward"))
    sql = """
        select
            plot_id,
            ward,
            settlement,
            sub_county,
            lat,
            lon,
            sanitation_class,
            sanitation_type,
            is_shared,
            households_sharing,
            has_handwashing,
            solid_waste_mgmt,
            total_persons,
            children_under_5,
            financed_by,
            photo
        from public.mv_map_households
    """
    params = []
    if ward:
        sql += " where ward = %s"
        params.append(ward)
    features: list[dict] = []
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            for row in rows:
                if row["lat"] is None or row["lon"] is None:
                    continue
                features.append(row_to_feature(row))
    return jsonify(
        {
            "type": "FeatureCollection",
            "features": features,
            "meta": {
                "category": "households",
                "ward": ward or "ALL",
                "count": len(features),
            },
        }
    )

@maps_bp.route("/wards", methods=["GET"])
@cache.cached(timeout=3600)
def map_wards():
    sql = """
        select distinct ward
        from public.mv_map_households
        where ward is not null
        order by ward
    """
    wards: list[str] = []
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            wards = [r[0] for r in cur.fetchall()]
    return jsonify(wards)

@maps_bp.route("/institutions", methods=["GET"])
@cache.cached(timeout=300, query_string=True)
def map_institutions():
    ward = request.args.get("ward")
    category = request.args.get("category")
    sql = """
        select
            institution_id,
            institution_name,
            institution_category,
            ward,
            location,
            lat,
            lon,
            has_sanitation,
            handwashing_status,
            estimated_users,
            institution_photo
        from public.mv_map_institutions
        where 1=1
    """
    params = []
    if ward and ward.upper() != "ALL":
        sql += " and ward = %s"
        params.append(ward.lower())
    if category:
        sql += " and institution_category = %s"
        params.append(category)
    features = []
    with get_db_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            for r in rows:
                if r["lat"] is None or r["lon"] is None:
                    continue
                features.append({
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [r["lon"], r["lat"]],
                    },
                    "properties": {
                        "institution_id": r["institution_id"],
                        "name": r["institution_name"],
                        "category": r["institution_category"],
                        "ward": r["ward"],
                        "location": r["location"],
                        "has_sanitation": r["has_sanitation"],
                        "handwashing_status": r["handwashing_status"],
                        "estimated_users": r["estimated_users"],
                        "photo": r["institution_photo"],
                    },
                })
    return jsonify({
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "ward": ward or "ALL",
            "category": category or "ALL",
            "count": len(features),
        },
    })

@maps_bp.route("/health", methods=["GET"])
def maps_health():
    return jsonify(
        {
            "status": "ok",
            "module": "maps",
        }
    )