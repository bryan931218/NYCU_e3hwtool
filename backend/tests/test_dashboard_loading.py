"""Template compilation is cached, not account content or request context."""
import unittest
from unittest.mock import patch
from flask import Flask
from e3_tracker.assignments.routes.dashboard import register_dashboard_routes


class DashboardLoadingTests(unittest.TestCase):
    def test_templates_compile_once_but_each_request_gets_its_own_data(self):
        app = Flask(__name__)
        user = {"username": "one"}
        stats = {"online": 1}
        register_dashboard_routes(app=app, current_user=lambda: user or None,
            HOME_TEMPLATE="home {{ stats.online }}", WEB_TEMPLATE="{{ name }} {{ app_home_url }}",
            _build_dashboard_context=lambda current: {"name": current["username"]},
            usage_stats=lambda: dict(stats), current_stats_version=lambda: 0, app_home_url="<unsafe>", support_email="")
        client = app.test_client()
        with patch.object(app.jinja_env, "from_string", wraps=app.jinja_env.from_string) as compile_template:
            self.assertEqual(client.get("/").text, "one &lt;unsafe&gt;")
            user["username"] = "two"
            self.assertEqual(client.get("/").text, "two &lt;unsafe&gt;")
            user.clear()
            self.assertEqual(client.get("/").text, "home 1")
            stats["online"] = 2
            self.assertEqual(client.get("/").text, "home 2")
            self.assertEqual(compile_template.call_count, 2)
