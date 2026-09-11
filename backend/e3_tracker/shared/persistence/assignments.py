"""Persistence operations for assignments."""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional
from sqlalchemy import delete, insert, func, select

from .schema import users_table, user_preferences_table, courses_table, assignments_table, assignment_views_table, user_fetch_state_table, fetch_errors_table, google_tokens_table, web_sessions_table


class AssignmentsStorage:
    def load_user_preferences(self, username: str) -> Dict[str, Any]:
        if not username:
            return {}
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(
                    user_preferences_table.c.view_mode,
                    user_preferences_table.c.status_filter,
                    user_preferences_table.c.semester_filter,
                    user_preferences_table.c.include_ignored_overdue,
                    user_preferences_table.c.show_overdue,
                    user_preferences_table.c.show_completed,
                    user_preferences_table.c.show_graded,
                    user_preferences_table.c.ignored_overdue_uids,
                )
                .select_from(user_preferences_table.join(users_table, user_preferences_table.c.user_id == users_table.c.id))
                .where(users_table.c.username == username)
            ).fetchone()
        if not row:
            return {}
        ignored_overdue_uids: List[str] = []
        if row.ignored_overdue_uids:
            try:
                parsed = json.loads(row.ignored_overdue_uids)
                if isinstance(parsed, list):
                    ignored_overdue_uids = [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                ignored_overdue_uids = []
        semester_filter: List[str] = []
        if row.semester_filter:
            try:
                parsed = json.loads(row.semester_filter)
                if isinstance(parsed, list):
                    semester_filter = [str(item).strip() for item in parsed if str(item).strip()]
            except Exception:
                semester_filter = []
        return {
            "view_mode": row.view_mode,
            "status_filter": row.status_filter,
            "semester_filter": semester_filter,
            "include_ignored_overdue": bool(row.include_ignored_overdue),
            "show_overdue": bool(row.show_overdue),
            "show_completed": bool(row.show_completed),
            "show_graded": bool(row.show_graded),
            "ignored_overdue_uids": ignored_overdue_uids,
        }

    def save_user_preferences(self, username: str, prefs: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(prefs, dict):
            return
        view_mode = prefs.get("view_mode")
        status_filter = prefs.get("status_filter")
        if isinstance(status_filter, list):
            status_filter = json.dumps(
                [str(item).strip() for item in status_filter if str(item).strip()],
                ensure_ascii=False,
            )
        semester_filter = prefs.get("semester_filter")
        if not isinstance(semester_filter, list):
            semester_filter = []
        semester_filter = [str(item).strip() for item in semester_filter if str(item).strip()]
        include_ignored_overdue = self._coerce_bool_int(prefs.get("include_ignored_overdue"))
        show_overdue = self._coerce_bool_int(prefs.get("show_overdue"))
        show_completed = self._coerce_bool_int(prefs.get("show_completed"))
        show_graded = self._coerce_bool_int(prefs.get("show_graded"))
        ignored_overdue_uids = prefs.get("ignored_overdue_uids")
        if not isinstance(ignored_overdue_uids, list):
            ignored_overdue_uids = []
        ignored_overdue_uids = [str(item).strip() for item in ignored_overdue_uids if str(item).strip()]
        now = self._now_iso()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(
                insert(user_preferences_table).values(
                    user_id=user_id,
                    view_mode=view_mode,
                    status_filter=status_filter,
                    semester_filter=json.dumps(semester_filter, ensure_ascii=False),
                    include_ignored_overdue=include_ignored_overdue,
                    show_overdue=show_overdue,
                    show_completed=show_completed,
                    show_graded=show_graded,
                    ignored_overdue_uids=json.dumps(ignored_overdue_uids, ensure_ascii=False),
                    updated_at=now,
                )
            )

    def save_user_cache(self, username: str, payload: Dict[str, Any]) -> None:
        if not username:
            return
        if not isinstance(payload, dict):
            return
        prefs = payload.get("preferences")
        if isinstance(prefs, dict):
            self.save_user_preferences(username, prefs)
        result = payload.get("result")
        if not isinstance(result, dict):
            return
        courses = result.get("courses") or []
        errors = result.get("errors") or []
        semester_catalog = result.get("available_semesters") or []
        selected_semesters = result.get("selected_semesters") or []
        excel_data = payload.get("excel_data")
        ts_raw = payload.get("ts")
        try:
            fetched_ts = int(ts_raw)
        except (TypeError, ValueError):
            fetched_ts = int(datetime.utcnow().timestamp())
        fetched_at = datetime.utcfromtimestamp(fetched_ts).isoformat()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            conn.execute(delete(user_fetch_state_table).where(user_fetch_state_table.c.user_id == user_id))
            conn.execute(
                insert(user_fetch_state_table).values(
                    user_id=user_id,
                    fetched_at=fetched_at,
                    fetched_ts=fetched_ts,
                    excel_data=excel_data,
                    error_count=len(errors),
                    semester_catalog=json.dumps(semester_catalog, ensure_ascii=False),
                    selected_semesters=json.dumps(selected_semesters, ensure_ascii=False),
                )
            )
            conn.execute(delete(fetch_errors_table).where(fetch_errors_table.c.user_id == user_id))

            course_ids = conn.execute(
                select(courses_table.c.id).where(courses_table.c.user_id == user_id)
            ).scalars().all()
            if course_ids:
                conn.execute(delete(assignments_table).where(assignments_table.c.course_id.in_(course_ids)))
            conn.execute(delete(courses_table).where(courses_table.c.user_id == user_id))

            now = self._now_iso()
            assignment_rows: List[Dict[str, Any]] = []
            for course in courses:
                try:
                    course_code = int(course.get("id"))
                except (TypeError, ValueError):
                    continue
                title = str(course.get("title") or "").strip()
                if not title:
                    title = f"Course {course_code}"
                url = course.get("url")
                insert_result = conn.execute(
                    insert(courses_table).values(
                        user_id=user_id,
                        course_code=course_code,
                        title=title,
                        url=url,
                        semester_key=course.get("semester_key"),
                        semester_label=course.get("semester_label"),
                        created_at=now,
                        updated_at=now,
                    )
                )
                course_pk = int(insert_result.inserted_primary_key[0])
                for item in course.get("assignments") or []:
                    title_val = str(item.get("title") or "").strip()
                    if not title_val:
                        continue
                    due_ts = item.get("due_ts")
                    if due_ts in ("", None):
                        due_ts_val = None
                    else:
                        try:
                            due_ts_val = int(due_ts)
                        except (TypeError, ValueError):
                            due_ts_val = None
                    submitted_ts = item.get("submitted_ts")
                    if submitted_ts in ("", None):
                        submitted_ts_val = None
                    else:
                        try:
                            submitted_ts_val = int(submitted_ts)
                        except (TypeError, ValueError):
                            submitted_ts_val = None
                    assignment_rows.append(
                        {
                            "course_id": course_pk,
                            "uid": self._assignment_uid(course_code, title_val, item.get("url")),
                            "title": title_val,
                            "url": item.get("url"),
                            "due_at": item.get("due_at"),
                            "due_ts": due_ts_val,
                            "overdue": self._coerce_bool_int(item.get("overdue")),
                            "completed": self._coerce_bool_int(item.get("completed")),
                            "raw_status_text": item.get("raw_status_text"),
                            "grade_text": item.get("grade_text"),
                            "submitted_at": item.get("submitted_at"),
                            "submitted_ts": submitted_ts_val,
                            "remaining_text": item.get("remaining_text"),
                            "submitted_count": item.get("submitted_count"),
                            "participant_count": item.get("participant_count"),
                            "updated_at": now,
                        }
                    )
            if assignment_rows:
                conn.execute(insert(assignments_table), assignment_rows)

            error_rows: List[Dict[str, Any]] = []
            for err in errors:
                if not isinstance(err, dict):
                    continue
                course_code = err.get("course_id")
                try:
                    course_code_val = int(course_code) if course_code is not None else None
                except (TypeError, ValueError):
                    course_code_val = None
                error_rows.append(
                    {
                        "user_id": user_id,
                        "course_code": course_code_val,
                        "course_title": err.get("course_title"),
                        "assignment_title": err.get("assignment_title"),
                        "message": str(err.get("message") or ""),
                    }
                )
            if error_rows:
                conn.execute(insert(fetch_errors_table), error_rows)

    def mark_assignment_views(
        self,
        username: str,
        assignment_uids: List[str],
        *,
        seen_ts: Optional[int] = None,
    ) -> Dict[str, int]:
        if not username:
            return {}
        normalized_uids = [str(item or "").strip() for item in assignment_uids if str(item or "").strip()]
        if not normalized_uids:
            return {}
        unique_uids = list(dict.fromkeys(normalized_uids))
        if seen_ts is None:
            seen_ts = int(datetime.utcnow().timestamp())
        seen_at = datetime.utcfromtimestamp(int(seen_ts)).isoformat()
        with self._lock, self._engine.begin() as conn:
            user_id = self._ensure_user(conn, username)
            existing_rows = conn.execute(
                select(
                    assignment_views_table.c.assignment_uid,
                    assignment_views_table.c.first_seen_ts,
                ).where(
                    assignment_views_table.c.user_id == user_id,
                    assignment_views_table.c.assignment_uid.in_(unique_uids),
                )
            ).fetchall()
            first_seen_map = {str(row.assignment_uid): int(row.first_seen_ts) for row in existing_rows}
            missing_uids = [uid for uid in unique_uids if uid not in first_seen_map]
            if missing_uids:
                conn.execute(
                    insert(assignment_views_table),
                    [
                        {
                            "user_id": user_id,
                            "assignment_uid": uid,
                            "first_seen_at": seen_at,
                            "first_seen_ts": int(seen_ts),
                        }
                        for uid in missing_uids
                    ],
                )
                for uid in missing_uids:
                    first_seen_map[uid] = int(seen_ts)
        return first_seen_map

    def load_assignment_view_map(self, username: str, assignment_uids: List[str]) -> Dict[str, int]:
        if not username:
            return {}
        normalized_uids = [str(item or "").strip() for item in assignment_uids if str(item or "").strip()]
        if not normalized_uids:
            return {}
        unique_uids = list(dict.fromkeys(normalized_uids))
        with self._lock, self._engine.connect() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return {}
            rows = conn.execute(
                select(
                    assignment_views_table.c.assignment_uid,
                    assignment_views_table.c.first_seen_ts,
                ).where(
                    assignment_views_table.c.user_id == int(user_row.id),
                    assignment_views_table.c.assignment_uid.in_(unique_uids),
                )
            ).fetchall()
        return {str(row.assignment_uid): int(row.first_seen_ts) for row in rows}

    def load_user_cache(self, username: str) -> Optional[Dict[str, Any]]:
        if not username:
            return None
        with self._lock, self._engine.connect() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return None
            state_row = conn.execute(
                select(
                    user_fetch_state_table.c.fetched_ts,
                    user_fetch_state_table.c.excel_data,
                    user_fetch_state_table.c.semester_catalog,
                    user_fetch_state_table.c.selected_semesters,
                )
                .where(user_fetch_state_table.c.user_id == user_row.id)
                .limit(1)
            ).fetchone()
            if not state_row:
                return None

            course_rows = conn.execute(
                select(
                    courses_table.c.id,
                    courses_table.c.course_code,
                    courses_table.c.title,
                    courses_table.c.url,
                    courses_table.c.semester_key,
                    courses_table.c.semester_label,
                )
                .where(courses_table.c.user_id == user_row.id)
            ).fetchall()

            courses: List[Dict[str, Any]] = []
            course_map: Dict[int, Dict[str, Any]] = {}
            for row in course_rows:
                entry = {
                    "id": row.course_code,
                    "title": row.title,
                    "url": row.url,
                    "semester_key": row.semester_key,
                    "semester_label": row.semester_label,
                    "assignments": [],
                    "detected_assign_links": 0,
                }
                courses.append(entry)
                course_map[int(row.id)] = entry

            course_ids = [row.id for row in course_rows]
            all_assignments: List[Dict[str, Any]] = []
            if course_ids:
                assignment_rows = conn.execute(
                    select(
                        assignments_table.c.course_id,
                        assignments_table.c.title,
                        assignments_table.c.url,
                        assignments_table.c.due_at,
                        assignments_table.c.due_ts,
                        assignments_table.c.overdue,
                        assignments_table.c.completed,
                        assignments_table.c.raw_status_text,
                        assignments_table.c.grade_text,
                        assignments_table.c.submitted_at,
                        assignments_table.c.submitted_ts,
                        assignments_table.c.remaining_text,
                        assignments_table.c.submitted_count,
                        assignments_table.c.participant_count,
                    )
                    .where(assignments_table.c.course_id.in_(course_ids))
                ).fetchall()
                for row in assignment_rows:
                    course_entry = course_map.get(int(row.course_id))
                    if not course_entry:
                        continue
                    course_title = course_entry["title"]
                    item = {
                        "course_id": course_entry["id"],
                        "course_title": course_title,
                        "title": row.title,
                        "url": row.url,
                        "due_at": row.due_at,
                        "due_ts": row.due_ts,
                        "overdue": bool(row.overdue),
                        "completed": bool(row.completed),
                        "raw_status_text": row.raw_status_text,
                        "grade_text": row.grade_text,
                        "submitted_at": row.submitted_at,
                        "submitted_ts": row.submitted_ts,
                        "remaining_text": row.remaining_text,
                        "submitted_count": row.submitted_count,
                        "participant_count": row.participant_count,
                    }
                    course_entry["assignments"].append(item)
                    course_entry["detected_assign_links"] += 1
                    all_assignments.append(item)

            for course_entry in courses:
                course_entry["assignments"].sort(key=self._course_sort_key)
            all_assignments.sort(key=self._global_sort_key)

            error_rows = conn.execute(
                select(
                    fetch_errors_table.c.course_code,
                    fetch_errors_table.c.course_title,
                    fetch_errors_table.c.assignment_title,
                    fetch_errors_table.c.message,
                ).where(fetch_errors_table.c.user_id == user_row.id)
            ).fetchall()
            errors = [
                {
                    "course_id": row.course_code,
                    "course_title": row.course_title,
                    "assignment_title": row.assignment_title,
                    "message": row.message,
                }
                for row in error_rows
            ]

            available_semesters: List[Dict[str, Any]] = []
            selected_semesters: List[str] = []
            try:
                parsed_catalog = json.loads(state_row.semester_catalog or "[]")
                if isinstance(parsed_catalog, list):
                    available_semesters = [item for item in parsed_catalog if isinstance(item, dict)]
            except Exception:
                available_semesters = []
            try:
                parsed_selected = json.loads(state_row.selected_semesters or "[]")
                if isinstance(parsed_selected, list):
                    selected_semesters = [str(item).strip() for item in parsed_selected if str(item).strip()]
            except Exception:
                selected_semesters = []

        cache: Dict[str, Any] = {
            "result": {
                "courses": courses,
                "all_assignments": all_assignments,
                "errors": errors,
                "available_semesters": available_semesters,
                "selected_semesters": selected_semesters,
            },
            "excel_data": state_row.excel_data,
            "ts": state_row.fetched_ts,
        }
        prefs = self.load_user_preferences(username)
        if prefs:
            cache["preferences"] = prefs
        return cache

    def list_cached_users(self, limit: int = 500) -> List[Dict[str, Any]]:
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(
                select(
                    users_table.c.username,
                    users_table.c.is_admin,
                    users_table.c.is_guest,
                    users_table.c.last_seen,
                    user_fetch_state_table.c.fetched_ts,
                    func.count(func.distinct(courses_table.c.id)).label("course_count"),
                    func.count(func.distinct(assignments_table.c.id)).label("assignment_count"),
                )
                .select_from(
                    users_table.join(user_fetch_state_table, user_fetch_state_table.c.user_id == users_table.c.id)
                    .outerjoin(courses_table, courses_table.c.user_id == users_table.c.id)
                    .outerjoin(assignments_table, assignments_table.c.course_id == courses_table.c.id)
                )
                .where(users_table.c.is_guest == 0)
                .group_by(
                    users_table.c.id,
                    users_table.c.username,
                    users_table.c.is_admin,
                    users_table.c.is_guest,
                    users_table.c.last_seen,
                    user_fetch_state_table.c.fetched_ts,
                )
                .order_by(
                    func.coalesce(user_fetch_state_table.c.fetched_ts, 0).desc(),
                    func.coalesce(users_table.c.last_seen, "").desc(),
                    users_table.c.username.asc(),
                )
                .limit(limit)
            ).fetchall()
        return [dict(row._mapping) for row in rows]

    def delete_user_cache(self, username: str) -> None:
        if not username:
            return
        with self._lock, self._engine.begin() as conn:
            user_row = conn.execute(
                select(users_table.c.id).where(users_table.c.username == username)
            ).fetchone()
            if not user_row:
                return
            user_id = int(user_row.id)
            course_ids = conn.execute(
                select(courses_table.c.id).where(courses_table.c.user_id == user_id)
            ).scalars().all()
            if course_ids:
                conn.execute(delete(assignments_table).where(assignments_table.c.course_id.in_(course_ids)))
            conn.execute(delete(courses_table).where(courses_table.c.user_id == user_id))
            conn.execute(delete(assignment_views_table).where(assignment_views_table.c.user_id == user_id))
            conn.execute(delete(user_fetch_state_table).where(user_fetch_state_table.c.user_id == user_id))
            conn.execute(delete(fetch_errors_table).where(fetch_errors_table.c.user_id == user_id))
            conn.execute(delete(user_preferences_table).where(user_preferences_table.c.user_id == user_id))
            conn.execute(delete(google_tokens_table).where(google_tokens_table.c.user_id == user_id))
            conn.execute(delete(web_sessions_table).where(web_sessions_table.c.username == username))
