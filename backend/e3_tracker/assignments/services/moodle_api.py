"""Read the authenticated user's enrolled courses and assignments over Moodle REST."""
from datetime import datetime
from html import unescape

import requests
from bs4 import BeautifulSoup

from e3_tracker.platform.constants import TAIPEI_TZ
from e3_tracker.assignments.domain.parsing import assignment_description
from e3_tracker.assignments.services.http import E3LoginError, configure_tls
from e3_tracker.assignments.services.mobile_auth import api_call, verified_site_info
from e3_tracker.assignments.services.submission_counts import class_submission_counts


def _text(value):
    return BeautifulSoup(str(value or ""), "html.parser").get_text(" ", strip=True)


def _positive(value):
    return type(value) is int and value > 0


def _date(value):
    return datetime.fromtimestamp(value, TAIPEI_TZ).strftime("%Y-%m-%d %H:%M") if value else None


def collect_api_assignments(options):
    from e3_tracker.assignments.services.collector import (
        _course_semester, _course_sort_key, _global_sort_key, _is_non_assignment_course,
        _semester_catalog, current_semester_key, normalize_semester_keys,
    )
    base = options.base_url.rstrip("/")
    token, user_id = options.moodle_token, options.moodle_user_id
    with requests.Session() as sess:
        configure_tls(sess, cafile=options.cafile, insecure=options.insecure)
        info = verified_site_info(sess, base, token, user_id=user_id, timeout=options.timeout)
        user_id = info["userid"]
        raw_courses = api_call(sess, base, token, "core_enrol_get_users_courses", userid=user_id,
                               timeout=options.timeout, step="courses")
        if not isinstance(raw_courses, list):
            raise E3LoginError("upstream_response_invalid", auth_step="courses")
        courses = []
        for raw in raw_courses:
            if not isinstance(raw, dict) or not _positive(raw.get("id")):
                raise E3LoginError("upstream_response_invalid", auth_step="courses")
            title = unescape(str(raw.get("fullname") or raw.get("shortname") or ""))
            if _is_non_assignment_course(raw["id"], title):
                continue
            semester = _course_semester({"title": title})
            if semester["key"] == "other":
                semester = _course_semester({"title": raw.get("shortname")})
            courses.append({"id": raw["id"], "title": title, "url": f"{base}/course/view.php?id={raw['id']}",
                            "semester_key": semester["key"], "semester_label": semester["label"]})
        catalog = _semester_catalog(courses)
        selected = normalize_semester_keys(options.semester_keys)
        if options.course_id:
            courses = [course for course in courses if course["id"] == options.course_id]
        elif selected:
            courses = [course for course in courses if course["semester_key"] in selected]
        elif not options.all_courses_all_terms:
            current = [course for course in courses if course["semester_key"] == current_semester_key()]
            if current:
                courses = current
            else:
                latest = next((entry["key"] for entry in catalog if entry["key"] != "other"), None)
                if latest:
                    courses = [course for course in courses if course["semester_key"] == latest]
        allowed = {course["id"]: course for course in courses}
        grouped = {}
        if courses:
            payload = {f"courseids[{index}]": course["id"] for index, course in enumerate(courses)}
            listing = api_call(sess, base, token, "mod_assign_get_assignments", timeout=options.timeout,
                               step="assignments", **payload)
            if not isinstance(listing, dict) or not isinstance(listing.get("courses"), list):
                raise E3LoginError("upstream_response_invalid", auth_step="assignments")
            for group in listing["courses"]:
                if not isinstance(group, dict) or group.get("id") not in allowed or not isinstance(group.get("assignments"), list):
                    raise E3LoginError("upstream_response_invalid", auth_step="assignments")
                grouped[group["id"]] = group["assignments"]
        totals = class_submission_counts(options, [course for course in courses if grouped.get(course["id"])])
        all_items = []
        for course in courses:
            items = []
            assignments = grouped.get(course["id"], [])
            for assignment in assignments:
                if not isinstance(assignment, dict) or not _positive(assignment.get("id")) or not _positive(assignment.get("cmid")):
                    raise E3LoginError("upstream_response_invalid", auth_step="assignments")
                if assignment.get("course", course["id"]) != course["id"]:
                    raise E3LoginError("identity_mismatch", auth_step="assignments")
                status = api_call(sess, base, token, "mod_assign_get_submission_status",
                                  assignid=assignment["id"], userid=user_id,
                                  timeout=options.timeout, step="submission")
                if not isinstance(status, dict):
                    raise E3LoginError("upstream_response_invalid", auth_step="submission")
                attempt, feedback = status.get("lastattempt") or {}, status.get("feedback") or {}
                submission = attempt.get("submission") or attempt.get("teamsubmission") or {}
                grade = _text(feedback.get("gradefordisplay"))
                grade = grade if grade and grade.casefold() not in {"-", "—", "未評分", "尚未評分", "not graded", "not yet graded"} else None
                graded = attempt.get("gradingstatus") == "graded" or bool(grade)
                submitted = submission.get("status") == "submitted"
                complete = submitted or graded
                if complete and not options.include_completed:
                    continue
                due = attempt.get("extensionduedate") or assignment.get("duedate") or None
                due = int(due) if isinstance(due, (int, float)) and due > 0 else None
                submitted_at = submission.get("timemodified") if submitted else None
                submitted_at = int(submitted_at) if isinstance(submitted_at, (int, float)) and submitted_at > 0 else None
                url = f"{base}/mod/assign/view.php?id={assignment['cmid']}&scope=assignment"
                intro = assignment_description('<div id="intro">' + str(assignment.get("intro") or "") + '</div>', url)
                comments = []
                for plugin in feedback.get("plugins") or []:
                    for field in plugin.get("editorfields") or []:
                        if field.get("text"):
                            comments.append(_text(field["text"]))
                summary = status.get("gradingsummary") or {}
                counts = totals.get((course["id"], assignment["cmid"]))
                item = {
                    "course_id": course["id"], "course_title": course["title"],
                    "semester_key": course["semester_key"], "semester_label": course["semester_label"],
                    "title": _text(assignment.get("name")), "url": url,
                    "due_at": _date(due) or "", "due_ts": due,
                    "submitted_at": _date(submitted_at), "submitted_ts": submitted_at,
                    "remaining_text": None, "overdue": bool(due and due < datetime.now(TAIPEI_TZ).timestamp()),
                    "completed": complete,
                    "raw_status_text": "已評分" if graded else "已提交" if submitted else "草稿" if submission.get("status") == "draft" else "尚未提交",
                    "grade_text": grade, "feedback_text": "\n".join(comments) or None, "description": intro,
                    "submitted_count": counts[0] if counts else summary.get("submissionssubmittedcount"),
                    "participant_count": counts[1] if counts else summary.get("participantcount"),
                }
                items.append(item)
            items.sort(key=_course_sort_key)
            course.update(assignments=items, detected_assign_links=len(assignments))
            all_items.extend(items)
        all_items.sort(key=_global_sort_key)
        return {"courses": courses, "all_assignments": all_items, "debug_files": [], "errors": [],
                "login_method": "api", "available_semesters": catalog,
                "selected_semesters": selected or [entry["key"] for entry in catalog]}
