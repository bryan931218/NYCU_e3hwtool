"""Production storage behavior composed through inheritance, not monkey patches."""
from e3_tracker.platform.deployment_runtime import DeploymentSafeStorage
from e3_tracker.study.domain.study_activity_progress_runtime import credit_only_new_video_progress


class ApplicationStorage(DeploymentSafeStorage):
    def list_study_plan_activity_events(self, *, day=None, start_day=None, end_day=None):
        requested_start = str(day or start_day or "").strip() or None
        requested_end = str(day or end_day or "").strip() or None
        if requested_start is None and requested_end is None:
            return super().list_study_plan_activity_events(day=day, start_day=start_day, end_day=end_day)
        events = super().list_study_plan_activity_events(
            start_day="1970-01-01", end_day=requested_end or requested_start,
        )
        return [event for event in credit_only_new_video_progress(events)
                if (not requested_start or str(event.get("day") or "") >= requested_start)
                and (not requested_end or str(event.get("day") or "") <= requested_end)]
