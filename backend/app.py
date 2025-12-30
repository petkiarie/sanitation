# app.py
from flask import Flask
from extensions import cache
from overview import overview_bp
from demographics import demographics_bp
from households import households_bp
from learning_institutions import learning_institutions_bp
from health_facilities import health_facilities_bp
from other_institutions import other_institutions_bp
from maps import maps_bp
from flask_cors import CORS

app = Flask(__name__)

app.config["CACHE_TYPE"] = "SimpleCache"
app.config["CACHE_DEFAULT_TIMEOUT"] = 300

cache.init_app(app)

# CORRECT CORS CONFIG
CORS(app, resources={r"/api/*": {"origins": "http://localhost:8080"}})

app.register_blueprint(overview_bp)
app.register_blueprint(demographics_bp)
app.register_blueprint(households_bp)
app.register_blueprint(learning_institutions_bp)
app.register_blueprint(health_facilities_bp)
app.register_blueprint(other_institutions_bp)
app.register_blueprint(maps_bp)

@app.route("/api/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
