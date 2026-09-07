"""Persistence operations for assistant."""
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import insert, select, update
from sqlalchemy.exc import IntegrityError

from .schema import study_assistant_actions_table


class AssistantStorage:
    def create_study_assistant_action(
        self,
        *,
        action_id: str,
        username: str,
        action_type: str,
        action_payload: Dict[str, Any],
        before_state: Dict[str, Any],
        summary: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_id = str(action_id or "").strip()[:48]
        normalized_user = str(username or "").strip()[:191]
        normalized_type = str(action_type or "").strip()[:48]
        normalized_summary = " ".join(str(summary or "").split()).strip()[:280]
        if not normalized_id or not normalized_user or not normalized_type or not normalized_summary:
            return None
        now = self._now_iso()
        try:
            with self._lock, self._engine.begin() as conn:
                conn.execute(
                    insert(study_assistant_actions_table).values(
                        action_id=normalized_id,
                        username=normalized_user,
                        status="pending",
                        action_type=normalized_type,
                        action_payload=json.dumps(action_payload, ensure_ascii=False, sort_keys=True),
                        before_state=json.dumps(before_state, ensure_ascii=False, sort_keys=True),
                        after_state=None,
                        summary=normalized_summary,
                        created_at=now,
                        updated_at=now,
                    )
                )
        except IntegrityError:
            return None
        return self.get_study_assistant_action(normalized_id, username=normalized_user)

    def get_study_assistant_action(
        self,
        action_id: str,
        *,
        username: str,
    ) -> Optional[Dict[str, Any]]:
        normalized_id = str(action_id or "").strip()[:48]
        normalized_user = str(username or "").strip()[:191]
        if not normalized_id or not normalized_user:
            return None
        with self._lock, self._engine.connect() as conn:
            row = conn.execute(
                select(study_assistant_actions_table)
                .where(study_assistant_actions_table.c.action_id == normalized_id)
                .where(study_assistant_actions_table.c.username == normalized_user)
            ).fetchone()
        if not row:
            return None

        def decode(value: Any) -> Dict[str, Any]:
            try:
                parsed = json.loads(str(value or "{}"))
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}

        return {
            "action_id": str(row.action_id),
            "username": str(row.username),
            "status": str(row.status),
            "action_type": str(row.action_type),
            "action_payload": decode(row.action_payload),
            "before_state": decode(row.before_state),
            "after_state": decode(row.after_state),
            "summary": str(row.summary or ""),
            "created_at": str(row.created_at or ""),
            "updated_at": str(row.updated_at or ""),
        }

    def transition_study_assistant_action(
        self,
        action_id: str,
        *,
        username: str,
        expected_status: str,
        next_status: str,
        after_state: Optional[Dict[str, Any]] = None,
    ) -> bool:
        normalized_id = str(action_id or "").strip()[:48]
        normalized_user = str(username or "").strip()[:191]
        if not normalized_id or not normalized_user:
            return False
        values: Dict[str, Any] = {
            "status": str(next_status or "")[:16],
            "updated_at": self._now_iso(),
        }
        if after_state is not None:
            values["after_state"] = json.dumps(after_state, ensure_ascii=False, sort_keys=True)
        with self._lock, self._engine.begin() as conn:
            result = conn.execute(
                update(study_assistant_actions_table)
                .where(study_assistant_actions_table.c.action_id == normalized_id)
                .where(study_assistant_actions_table.c.username == normalized_user)
                .where(study_assistant_actions_table.c.status == str(expected_status or ""))
                .values(**values)
            )
        return bool(result.rowcount)

    def list_study_assistant_actions(
        self,
        *,
        username: str,
        action_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 25,
    ) -> List[Dict[str, Any]]:
        """Return recent audited actions for one user.

        This is intentionally scoped by username so recovery code can never
        inspect or repair another user's confirmed changes.
        """
        normalized_user = str(username or "").strip()[:191]
        if not normalized_user:
            return []
        stmt = select(study_assistant_actions_table).where(
            study_assistant_actions_table.c.username == normalized_user
        )
        if action_type:
            stmt = stmt.where(
                study_assistant_actions_table.c.action_type == str(action_type or "")[:48]
            )
        if status:
            stmt = stmt.where(
                study_assistant_actions_table.c.status == str(status or "")[:16]
            )
        stmt = stmt.order_by(
            study_assistant_actions_table.c.updated_at.desc(),
            study_assistant_actions_table.c.created_at.desc(),
        ).limit(max(1, min(int(limit or 25), 100)))
        with self._lock, self._engine.connect() as conn:
            rows = conn.execute(stmt).fetchall()

        def decode(value: Any) -> Dict[str, Any]:
            try:
                parsed = json.loads(str(value or "{}"))
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}

        return [{
            "action_id": str(row.action_id),
            "username": str(row.username),
            "status": str(row.status),
            "action_type": str(row.action_type),
            "action_payload": decode(row.action_payload),
            "before_state": decode(row.before_state),
            "after_state": decode(row.after_state),
            "summary": str(row.summary or ""),
            "created_at": str(row.created_at or ""),
            "updated_at": str(row.updated_at or ""),
        } for row in rows]
