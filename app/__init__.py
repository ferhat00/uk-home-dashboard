"""Flask application factory for UK Home Dashboard."""
from flask import Flask

import config


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = config.SECRET_KEY
    app.config["DEBUG"] = config.DEBUG
    app.config["DATABASE_PATH"] = str(config.DATABASE_PATH)

    from app.routes import bp
    app.register_blueprint(bp)

    @app.context_processor
    def inject_config():
        return {"config": config}

    @app.template_filter("score_color")
    def score_color_filter(score):
        try:
            score = float(score)
        except (TypeError, ValueError):
            return "#6c757d"
        if score >= 70:
            return "#198754"
        if score >= 55:
            return "#0d6efd"
        if score >= 40:
            return "#ffc107"
        return "#dc3545"

    # Register as a global function for use in templates
    app.jinja_env.globals["score_color"] = score_color_filter

    return app
