"""Guard site ownership while retaining the single existing deployment."""

import ast
import inspect
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

from e3_tracker.bootstrap import create_app
from e3_tracker.platform.paths import ROOT_DIR, FRONTEND_ROOT, FRONTEND_OWNERS
from e3_tracker.platform.persistence.metadata import metadata


class SiteBoundaryTests(unittest.TestCase):
    def test_sites_do_not_import_each_other_or_the_deployment(self):
        package = ROOT_DIR / "backend/e3_tracker"
        for owner, opposite in (("assignments", "study"), ("study", "assignments")):
            for path in (package / owner).rglob("*.py"):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    modules = []
                    if isinstance(node, ast.Import):
                        modules = [alias.name for alias in node.names]
                    elif isinstance(node, ast.ImportFrom):
                        modules = [node.module or ""]
                    for module in modules:
                        with self.subTest(
                            path=str(path.relative_to(package)), module=module
                        ):
                            self.assertFalse(
                                module.startswith(f"e3_tracker.{opposite}")
                            )
                            self.assertNotIn(
                                module,
                                (
                                    "e3_tracker.bootstrap",
                                    "e3_tracker.platform.application",
                                    "e3_tracker.platform.storage",
                                    "e3_tracker.application_storage",
                                    "e3_tracker.platform.persistence.schema",
                                ),
                            )

    def test_product_tables_have_their_own_schema(self):
        from e3_tracker.assignments.persistence import schema as assignments
        from e3_tracker.study.persistence import schema as study
        from e3_tracker.platform.persistence import core_schema as platform

        self.assertIs(assignments.metadata, metadata)
        self.assertIs(study.metadata, metadata)
        self.assertIs(platform.metadata, metadata)
        self.assertIs(metadata.tables["assignments"], assignments.assignments_table)
        self.assertIs(
            metadata.tables["study_plan_videos"], study.study_plan_videos_table
        )
        self.assertIs(metadata.tables["users"], platform.users_table)
        self.assertFalse(hasattr(study, "assignments_table"))
        self.assertFalse(hasattr(assignments, "study_plan_videos_table"))
        self.assertFalse(hasattr(platform, "study_plan_videos_table"))

    def test_templates_use_owner_namespaces(self):
        for owner in FRONTEND_OWNERS:
            for path in (FRONTEND_ROOT / owner / "templates").rglob("*"):
                if not path.is_file():
                    continue
                source = path.read_text(encoding="utf-8")
                references = re.findall(
                    r"\{%\s*(?:include|import|extends)\s+['\"]([^'\"]+)", source
                )
                for reference in references:
                    namespace, relative = reference.split("/", 1)
                    with self.subTest(path=str(path), reference=reference):
                        self.assertIn(namespace, (owner, "shared"))
                        self.assertTrue(
                            (
                                FRONTEND_ROOT / namespace / "templates" / relative
                            ).is_file()
                        )

    def test_routes_and_assets_respect_ownership(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            "os.environ",
            {
                "E3_CACHE_DIR": directory,
                "E3_DATABASE_URL": "",
                "DATABASE_URL": "",
                "E3_CANONICAL_HOST": "",
                "E3_SESSION_COOKIE_SECURE": "0",
                "E3_YOUTUBE_AUTO_SYNC_ENABLED": "0",
                "OPENAI_API_KEY": "",
            },
        ):
            app = create_app()
            try:
                endpoints = {
                    "index": "assignments",
                    "api_assignments": "assignments",
                    "admin_study_home": "study",
                    "admin_traffic": "platform",
                    "health_check": "platform",
                    "session_status": "platform",
                }
                for endpoint, owner in endpoints.items():
                    function = inspect.unwrap(app.view_functions[endpoint])
                    self.assertTrue(
                        function.__module__.startswith(f"e3_tracker.{owner}."), endpoint
                    )
                from tests.security_helpers import csrf_client
                client = csrf_client(app)
                for asset in (
                    "/assets/assignments/css/workbench.css",
                    "/assets/study/css/study-center.css",
                    "/assets/shared/css/tokens.css",
                    "/assets/study/discord/e3-study-cover.png",
                    "/static/discord/e3-study-cover.png",
                ):
                    with client.get(asset) as response:
                        self.assertEqual(response.status_code, 200, asset)
                for asset in (
                    "/assets/study/css/workbench.css",
                    "/assets/assignments/css/study-center.css",
                    "/assets/shared/../study/templates/admin_study_home.html",
                    "/assets/unknown/css/tokens.css",
                    "/assets/study/../templates/study_recall.html",
                ):
                    with client.get(asset) as response:
                        self.assertEqual(response.status_code, 404, asset)
                self.assertEqual(client.get("/healthz").status_code, 200)
            finally:
                app.extensions["e3_storage"]._engine.dispose()


if __name__ == "__main__":
    unittest.main()
