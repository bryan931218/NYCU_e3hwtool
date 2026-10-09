"""Read-only, bounded GA4 reports; credentials and raw reports never reach the UI."""

import json
import os
import re
import threading
import time
from datetime import datetime, timedelta

import requests

from e3_tracker.platform.constants import TAIPEI_TZ


REPORTS = {
    "summary": ([], ["activeUsers", "newUsers", "sessions", "screenPageViews", "engagementRate", "userEngagementDuration"]),
    "sources": (["sessionSourceMedium", "sessionCampaignName"], ["sessions", "activeUsers", "engagementRate"]),
    "pages": (["pagePath"], ["screenPageViews", "activeUsers", "userEngagementDuration"]),
    "devices": (["deviceCategory"], ["activeUsers", "sessions"]),
    "returning": (["newVsReturning"], ["activeUsers", "sessions"]),
    "events": (["eventName"], ["totalUsers", "eventCount"]),
}

FUNNELS = {
    "login_funnel": [("首頁", "landing_view"), ("登入頁", "login_view"), ("登入成功", "login")],
    "line_funnel": [("登入成功", "login"), ("綁定 LINE", "line_link_success"), ("啟用 LINE 通知", "line_notification_enable")],
}


def analytics_config():
    measurement = os.getenv("E3_GA4_MEASUREMENT_ID", "").strip()
    property_id = os.getenv("E3_GA4_PROPERTY_ID", "").strip()
    return {
        "measurement_id": measurement if re.fullmatch(r"G-[A-Z0-9]{4,20}", measurement) else "",
        "property_id": property_id if re.fullmatch(r"[0-9]{1,20}", property_id) else "",
        "campaigns": [value for value in os.getenv("E3_GA4_CAMPAIGNS", "").split(",")
                      if re.fullmatch(r"[a-z0-9-]{1,48}", value)],
    }


class GoogleAnalyticsReports:
    def __init__(self, config):
        self.config = config
        self._cache = {}
        self._busy = set()
        self._lock = threading.Lock()

    def status(self):
        return {
            **self.config,
            "collection_ready": bool(self.config["measurement_id"]),
            "reports_ready": bool(self.config["property_id"] and os.getenv("E3_GA4_SERVICE_ACCOUNT_JSON", "").strip()),
        }

    def get(self, days=30):
        days = days if days in {7, 30, 90} else 30
        if not self.status()["reports_ready"]:
            return {"status": "not_configured"}
        with self._lock:
            cached = self._cache.get(days)
            if cached and time.time() - cached["checked_at"] < 900:
                return cached
            if days not in self._busy:
                self._busy.add(days)
                threading.Thread(target=self._refresh, args=(days,), daemon=True).start()
            return {**(cached or {}), "status": "loading"}

    def _refresh(self, days):
        try:
            reports = self._fetch(days)
            result = {"status": "ready", "reports": reports,
                      "updated_at": datetime.now(TAIPEI_TZ).strftime("%Y-%m-%d %H:%M")}
        except Exception:
            # Neither Google error bodies nor credential parse errors are public diagnostics.
            with self._lock:
                previous = self._cache.get(days, {})
            result = {**previous, "status": "stale" if previous.get("reports") else "unavailable"}
        with self._lock:
            self._cache[days] = {**result, "checked_at": time.time()}
            self._busy.discard(days)

    def _fetch(self, days):
        from google.auth.transport.requests import Request
        from google.oauth2.service_account import Credentials

        info = json.loads(os.environ["E3_GA4_SERVICE_ACCOUNT_JSON"])
        if info.get("type") != "service_account" or info.get("token_uri") != "https://oauth2.googleapis.com/token":
            raise ValueError("Unsupported credentials")
        credentials = Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/analytics.readonly"],
        )
        with requests.Session() as client:
            transport = Request(session=client)
            credentials.refresh(lambda **kwargs: transport(**{**kwargs, "timeout": 10}))
            today = datetime.now(TAIPEI_TZ).date()
            date_range = {"startDate": (today - timedelta(days=days - 1)).isoformat(), "endDate": today.isoformat()}
            reports = [{
                "dateRanges": [date_range],
                "dimensions": [{"name": name} for name in dimensions],
                "metrics": [{"name": name} for name in metrics], "limit": "100" if key == "events" else "30",
                "orderBys": [{"metric": {"metricName": metrics[0]}, "desc": True}],
            } for key, (dimensions, metrics) in REPORTS.items()]
            payload = []
            for offset in range(0, len(reports), 5):
                response = client.post(
                    f"https://analyticsdata.googleapis.com/v1beta/properties/{self.config['property_id']}:batchRunReports",
                    headers={"Authorization": f"Bearer {credentials.token}"},
                    json={"requests": reports[offset:offset + 5]}, timeout=(5, 20),
                )
                response.raise_for_status()
                payload.extend(response.json().get("reports", []))
            funnels = {}
            for key, steps in FUNNELS.items():
                try:
                    response = client.post(
                        f"https://analyticsdata.googleapis.com/v1alpha/properties/{self.config['property_id']}:runFunnelReport",
                        headers={"Authorization": f"Bearer {credentials.token}"},
                        json={"dateRanges": [date_range], "limit": "10", "funnel": {
                            "isOpenFunnel": False,
                            "steps": [{"name": name, "filterExpression": {"funnelEventFilter": {"eventName": event}},
                                       **({"withinDurationFromPriorStep": "604800s"} if index else {})}
                                      for index, (name, event) in enumerate(steps)],
                        }}, timeout=(5, 15),
                    )
                    response.raise_for_status()
                    report = response.json()["funnelTable"]
                    dimensions = [header["name"] for header in report["dimensionHeaders"]]
                    metrics = [header["name"] for header in report["metricHeaders"]]
                    funnels[key] = [{
                        **{name: str(value["value"])[:80] for name, value in zip(dimensions, row["dimensionValues"])},
                        **{name: float(value["value"]) for name, value in zip(metrics, row["metricValues"])},
                    } for row in report.get("rows", [])[:10]]
                except Exception:
                    # The alpha funnel API must not make standard reports unavailable.
                    funnels[key] = None
        if len(payload) != len(REPORTS):
            raise ValueError("Incomplete reports")
        result = {}
        for (key, (dimensions, metrics)), report in zip(REPORTS.items(), payload):
            rows = []
            for row in report.get("rows", [])[:100 if key == "events" else 30]:
                values = {name: str(value["value"])[:200] for name, value in zip(dimensions, row.get("dimensionValues", []))}
                values.update({name: float(value["value"]) for name, value in zip(metrics, row["metricValues"])})
                rows.append(values)
            result[key] = rows
        result.update(funnels)
        return result
