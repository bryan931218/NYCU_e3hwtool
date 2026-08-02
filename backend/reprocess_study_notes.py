import argparse
import cProfile
import io
import json
import mimetypes
import os
import pstats
import secrets
import shutil
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from e3_tracker.api.web import create_app
from e3_tracker.shared.config import load_env_defaults
from e3_tracker.shared.storage import PersistentStorage


def configured_paths() -> Tuple[PersistentStorage, Path]:
    config = load_env_defaults()
    cache_root = Path(config.get("cache_dir") or ".localdata").expanduser()
    database = config.get("database_url") or str((cache_root / "e3_tracker.sqlite3").resolve())
    upload_root = Path(
        config.get("study_upload_dir") or cache_root / "study_note_images"
    ).expanduser()
    return PersistentStorage(database), upload_root


def session_inventory(storage: PersistentStorage, upload_root: Path) -> List[Dict[str, Any]]:
    inventory: List[Dict[str, Any]] = []
    for row in storage.list_study_recall_sessions(limit=None):
        session_id = int(row["id"])
        session = storage.get_study_recall_session(session_id) or row
        filenames = [str(name) for name in session.get("image_filenames") or []]
        image_dir = upload_root / str(session_id)
        inventory.append(
            {
                "id": session_id,
                "study_date": str(session.get("study_date") or ""),
                "subject": str(session.get("subject") or ""),
                "title": str(session.get("title") or ""),
                "images": len(filenames),
                "cards": len(session.get("key_concepts") or []),
                "missing_images": [name for name in filenames if not (image_dir / name).is_file()],
            }
        )
    return inventory


def remove_session(storage: PersistentStorage, upload_root: Path, session_id: int) -> None:
    storage.delete_study_recall_session(session_id)
    image_dir = (upload_root / str(session_id)).resolve()
    resolved_root = upload_root.resolve()
    if image_dir.parent == resolved_root and image_dir.is_dir():
        shutil.rmtree(image_dir)


def prune_repeated_relation_associations(storage: PersistentStorage) -> Dict[str, int]:
    """Remove stale generic or duplicated relation copy without touching study cards."""
    sessions = storage.list_study_recall_sessions(limit=None)
    concepts_by_session: Dict[int, List[Dict[str, Any]]] = {}
    title_by_card: Dict[Tuple[int, int], str] = {}
    for session in sessions:
        session_id = int(session["id"])
        concepts = [
            concept if isinstance(concept, dict) else {}
            for concept in (session.get("key_concepts") or [])
        ]
        concepts_by_session[session_id] = concepts
        for concept_index, concept in enumerate(concepts):
            title_by_card[(session_id, concept_index)] = " ".join(
                str(concept.get("concept") or "").split()
            )

    generic_markers = (
        "這兩張卡屬於同一份筆記中的直接相關觀念",
        "這兩張卡適合一起複習",
        "兩張卡適合一起複習",
        "兩者相關可一起複習",
    )
    seen_signatures: Dict[str, Tuple[Tuple[int, int], Tuple[int, int]]] = {}
    rejected_pairs = set()
    checked_pairs = set()
    for session_id, concepts in concepts_by_session.items():
        for concept_index, concept in enumerate(concepts):
            source = (session_id, concept_index)
            for relation in concept.get("relations") or []:
                if not isinstance(relation, dict):
                    continue
                try:
                    target = (int(relation.get("session_id") or 0), int(relation.get("concept_index")))
                except (TypeError, ValueError):
                    continue
                if target not in title_by_card or target == source:
                    continue
                pair = tuple(sorted((source, target)))
                if pair in checked_pairs:
                    continue
                checked_pairs.add(pair)
                association = " ".join(str(relation.get("association") or "").split())
                source_title = title_by_card[source].casefold()
                target_title = title_by_card[target].casefold()
                signature = association.casefold()
                for title in (source_title, target_title):
                    if title:
                        signature = signature.replace(title, "{card}")
                signature = "".join(char for char in signature if char.isalnum() or char in "\\{}")
                if not signature or any(marker in association for marker in generic_markers):
                    rejected_pairs.add(pair)
                    continue
                previous_pair = seen_signatures.get(signature)
                if previous_pair is not None and previous_pair != pair:
                    rejected_pairs.add(pair)
                    continue
                seen_signatures[signature] = pair

    removed = 0
    for session_id, concepts in concepts_by_session.items():
        for concept_index, concept in enumerate(concepts):
            source = (session_id, concept_index)
            relations = concept.get("relations")
            if not isinstance(relations, list):
                continue
            retained = []
            for relation in relations:
                if not isinstance(relation, dict):
                    continue
                try:
                    target = (int(relation.get("session_id") or 0), int(relation.get("concept_index")))
                except (TypeError, ValueError):
                    removed += 1
                    continue
                if tuple(sorted((source, target))) in rejected_pairs:
                    removed += 1
                    continue
                retained.append(relation)
            concept["relations"] = retained
    storage.replace_study_recall_concepts_bulk(concepts_by_session)
    return {"sessions": len(concepts_by_session), "pairs_removed": len(rejected_pairs), "relations_removed": removed}


def audit_relation_associations(storage: PersistentStorage) -> Dict[str, Any]:
    """Return an aggregate-only audit of relation copy for production diagnostics."""
    edges: Dict[Tuple[Tuple[int, int], Tuple[int, int]], str] = {}
    for session in storage.list_study_recall_sessions(limit=None):
        session_id = int(session["id"])
        for concept_index, concept in enumerate(session.get("key_concepts") or []):
            if not isinstance(concept, dict):
                continue
            source = (session_id, concept_index)
            for relation in concept.get("relations") or []:
                if not isinstance(relation, dict):
                    continue
                try:
                    target = (int(relation.get("session_id") or 0), int(relation.get("concept_index")))
                except (TypeError, ValueError):
                    continue
                if target == source:
                    continue
                edges.setdefault(tuple(sorted((source, target))), " ".join(str(relation.get("association") or "").split()))
    counts = Counter(edges.values())
    return {
        "pairs": len(edges),
        "unique_associations": len(counts),
        "duplicate_groups": [
            {"count": count, "association": association}
            for association, count in counts.most_common()
            if count > 1
        ][:20],
    }


def _profile_app_request(path: str, *, authenticated: bool = False) -> Dict[str, Any]:
    """Profile an app view against the configured production data."""
    app = create_app()
    client = app.test_client()
    config = load_env_defaults()
    canonical_host = str(config.get("canonical_host") or "").strip()
    base_url = f"https://{canonical_host}" if canonical_host else "http://localhost"
    if authenticated:
        username = str(config.get("admin_user_id") or "112550103")
        token = f"study-profile-{secrets.token_urlsafe(18)}"
        storage, _upload_root = configured_paths()
        storage.save_web_session(token, username)
        with client.session_transaction(base_url=base_url) as flask_session:
            flask_session.update(
                username=username,
                session_token=token,
                is_admin=True,
                is_guest=False,
            )
    profiler = cProfile.Profile()
    started = time.perf_counter()
    response = profiler.runcall(client.get, path, base_url=base_url)
    elapsed_ms = round((time.perf_counter() - started) * 1000)
    stats = pstats.Stats(profiler).sort_stats("cumulative")
    rows = []
    for function, values in sorted(stats.stats.items(), key=lambda item: item[1][3], reverse=True)[:20]:
        filename, line, name = function
        calls, primitive_calls, total_time, cumulative_time, _callers = values
        rows.append(
            {
                "function": f"{Path(filename).name}:{line}({name})",
                "calls": calls,
                "self_ms": round(total_time * 1000, 1),
                "cumulative_ms": round(cumulative_time * 1000, 1),
            }
        )
    return {"path": path, "status": response.status_code, "elapsed_ms": elapsed_ms, "top": rows}


def profile_public_study_progress() -> Dict[str, Any]:
    return _profile_app_request("/study-progress")


def profile_study_recall() -> Dict[str, Any]:
    return _profile_app_request("/admin/study-recall", authenticated=True)


def validate_replacement(
    storage: PersistentStorage,
    session_id: int,
    expected_images: int,
) -> Dict[str, Any]:
    session = storage.get_study_recall_session(session_id)
    if session is None:
        raise RuntimeError("replacement session was not saved")
    pages = [page for page in session.get("source_transcription") or [] if isinstance(page, dict)]
    cards = [card for card in session.get("key_concepts") or [] if isinstance(card, dict)]
    if len(session.get("image_filenames") or []) != expected_images:
        raise RuntimeError("replacement image count does not match")
    if len(pages) != expected_images or any(not str(page.get("transcription") or "").strip() for page in pages):
        raise RuntimeError("replacement transcription is incomplete")
    if any(str(page.get("transcription_mode") or "") != "isolated_v1" for page in pages):
        raise RuntimeError("replacement did not use isolated page transcription")
    if not cards:
        raise RuntimeError("replacement has no study cards")

    source_groups = 0
    located_groups = 0
    for card in cards:
        refs_by_page: Dict[int, List[Dict[str, Any]]] = {}
        for ref in card.get("source_refs") or []:
            if not isinstance(ref, dict):
                continue
            try:
                image_index = int(ref.get("image_index") or 0)
            except (TypeError, ValueError):
                continue
            if image_index > 0:
                refs_by_page.setdefault(image_index, []).append(ref)
        for refs in refs_by_page.values():
            source_groups += 1
            if any(isinstance(ref.get("bbox"), dict) for ref in refs):
                located_groups += 1
    if source_groups == 0:
        raise RuntimeError("replacement cards have no source references")
    location_ratio = located_groups / source_groups
    if location_ratio < 0.80:
        raise RuntimeError(
            f"replacement source localization is too low ({located_groups}/{source_groups})"
        )
    return {
        "cards": len(cards),
        "source_groups": source_groups,
        "located_groups": located_groups,
        "location_ratio": round(location_ratio, 4),
    }


def stage_images(client: Any, image_paths: List[Path], base_url: str) -> str:
    upload_id = ""
    for image_index, image_path in enumerate(image_paths, start=1):
        response = client.post(
            "/admin/study-recall/upload-staging",
            data={
                "upload_id": upload_id,
                "image_index": str(image_index),
                "total_images": str(len(image_paths)),
                "note_image": (
                    io.BytesIO(image_path.read_bytes()),
                    image_path.name,
                    mimetypes.guess_type(image_path.name)[0] or "image/jpeg",
                ),
            },
            headers={"X-E3-Study-Upload": "1", "X-E3-Study-Reprocess": "1"},
            content_type="multipart/form-data",
            base_url=base_url,
        )
        payload = response.get_json(silent=True) or {}
        if response.status_code != 200 or not payload.get("ok"):
            detail = response.get_data(as_text=True)[:240].strip()
            raise RuntimeError(
                str(
                    payload.get("error")
                    or f"failed to stage source image (HTTP {response.status_code}: {detail})"
                )
            )
        upload_id = str(payload.get("upload_id") or "")
    if not upload_id:
        raise RuntimeError("staging did not return an upload id")
    return upload_id


def wait_for_job(
    client: Any,
    job_id: str,
    timeout_seconds: int,
    base_url: str,
) -> Dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    last_progress = -1
    while time.monotonic() < deadline:
        response = client.get(
            f"/admin/study-recall/upload-jobs/{job_id}",
            base_url=base_url,
        )
        payload = response.get_json(silent=True) or {}
        if response.status_code != 200 or not payload.get("ok"):
            raise RuntimeError(str(payload.get("error") or "upload job disappeared"))
        progress = int(payload.get("progress") or 0)
        if progress != last_progress:
            print(
                json.dumps(
                    {
                        "event": "progress",
                        "job_id": job_id,
                        "progress": progress,
                        "message": payload.get("message") or "",
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            last_progress = progress
        if payload.get("status") != "running":
            return payload
        time.sleep(2)
    client.post(
        f"/admin/study-recall/upload-jobs/{job_id}/cancel",
        base_url=base_url,
    )
    raise RuntimeError(f"upload job exceeded {timeout_seconds} seconds")


def reprocess_all(
    timeout_seconds: int,
    expected_notes: int,
    *,
    latest_only: bool = False,
) -> int:
    storage, upload_root = configured_paths()
    inventory = session_inventory(storage, upload_root)
    if not inventory:
        print(json.dumps({"event": "complete", "notes": 0}, ensure_ascii=False))
        return 0
    if latest_only:
        inventory = inventory[:1]
    if expected_notes > 0 and len(inventory) != expected_notes:
        raise RuntimeError(
            f"expected exactly {expected_notes} notes, found {len(inventory)}; no changes were made"
        )
    missing = [item for item in inventory if item["missing_images"]]
    if missing:
        raise RuntimeError(f"source images are missing: {json.dumps(missing, ensure_ascii=False)}")

    backup_dir = upload_root / "_reprocess_backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    manifest_path = backup_dir / f"{run_id}.json"
    manifest_path.write_text(
        json.dumps({"status": "running", "inventory": inventory}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    app = create_app()
    client = app.test_client()
    config = load_env_defaults()
    username = str(config.get("admin_user_id") or "112550103")
    canonical_host = str(config.get("canonical_host") or "").strip()
    base_url = f"https://{canonical_host}" if canonical_host else "http://localhost"
    token = f"study-reprocess-{secrets.token_urlsafe(18)}"
    storage.save_web_session(token, username)
    with client.session_transaction(base_url=base_url) as flask_session:
        flask_session.update(
            username=username,
            session_token=token,
            is_admin=True,
            is_guest=False,
        )

    replacements: List[Dict[str, Any]] = []
    created_new_ids: List[int] = []
    try:
        for note_index, item in enumerate(inventory, start=1):
            old_session = storage.get_study_recall_session(int(item["id"]))
            if old_session is None:
                raise RuntimeError(f"old session {item['id']} disappeared")
            image_dir = upload_root / str(item["id"])
            image_paths = [image_dir / name for name in old_session.get("image_filenames") or []]
            print(
                json.dumps(
                    {
                        "event": "start",
                        "index": note_index,
                        "total": len(inventory),
                        "old_session_id": item["id"],
                        "title": item["title"],
                        "images": len(image_paths),
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
            upload_id = stage_images(client, image_paths, base_url)
            headers = {"X-E3-Study-Upload": "1", "X-E3-Study-Reprocess": "1"}
            if note_index < len(inventory):
                headers["X-E3-Skip-Relation-Rebuild"] = "1"
            response = client.post(
                "/admin/study-recall/upload",
                data={
                    "upload_id": upload_id,
                    "study_date": old_session.get("study_date") or "",
                    "subject": old_session.get("subject") or "",
                    "title": old_session.get("title") or "",
                    "allow_corrections": "1",
                },
                headers=headers,
                base_url=base_url,
            )
            payload = response.get_json(silent=True) or {}
            if response.status_code != 202 or not payload.get("job_id"):
                raise RuntimeError(str(payload.get("error") or "failed to start reprocessing"))
            job = wait_for_job(
                client,
                str(payload["job_id"]),
                timeout_seconds,
                base_url,
            )
            if job.get("status") != "success" or not job.get("session_id"):
                raise RuntimeError(str(job.get("message") or "note reprocessing failed"))
            new_session_id = int(job["session_id"])
            created_new_ids.append(new_session_id)
            validation = validate_replacement(storage, new_session_id, len(image_paths))
            replacement = {
                "old_session_id": int(item["id"]),
                "new_session_id": new_session_id,
                "title": item["title"],
                **validation,
            }
            replacements.append(replacement)
            print(json.dumps({"event": "validated", **replacement}, ensure_ascii=False), flush=True)

        for replacement in replacements:
            remove_session(storage, upload_root, int(replacement["old_session_id"]))
        manifest_path.write_text(
            json.dumps(
                {"status": "complete", "inventory": inventory, "replacements": replacements},
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(
            json.dumps(
                {"event": "complete", "notes": len(replacements), "replacements": replacements},
                ensure_ascii=False,
            ),
            flush=True,
        )
        return 0
    except Exception as exc:
        for new_session_id in reversed(created_new_ids):
            remove_session(storage, upload_root, new_session_id)
        manifest_path.write_text(
            json.dumps(
                {
                    "status": "failed",
                    "error": str(exc),
                    "inventory": inventory,
                    "cleaned_replacements": replacements,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        raise


def latest_background_paths(upload_root: Path) -> Tuple[Path, Path]:
    state_root = upload_root / "_reprocess_backups"
    state_root.mkdir(parents=True, exist_ok=True)
    return state_root / "latest-background.log", state_root / "latest-background.pid"


def start_latest_background(timeout_seconds: int) -> int:
    _storage, upload_root = configured_paths()
    log_path, pid_path = latest_background_paths(upload_root)
    if pid_path.is_file():
        try:
            existing_pid = int(pid_path.read_text(encoding="utf-8").strip())
            os.kill(existing_pid, 0)
        except (OSError, ValueError):
            pid_path.unlink(missing_ok=True)
        else:
            raise RuntimeError(f"latest-note reprocess is already running (pid={existing_pid})")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "reprocess",
        "--latest",
        "--expected-notes",
        "1",
        "--timeout",
        str(max(300, timeout_seconds)),
    ]
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            close_fds=True,
            start_new_session=True,
        )
    pid_path.write_text(str(process.pid), encoding="utf-8")
    print(
        json.dumps(
            {
                "event": "background_started",
                "pid": process.pid,
                "log_path": str(log_path),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 0


def latest_background_status() -> int:
    _storage, upload_root = configured_paths()
    log_path, pid_path = latest_background_paths(upload_root)
    pid = 0
    running = False
    if pid_path.is_file():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip())
            os.kill(pid, 0)
            running = True
        except (OSError, ValueError):
            pid_path.unlink(missing_ok=True)
    lines = []
    if log_path.is_file():
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-40:]
    print(
        json.dumps(
            {
                "event": "background_status",
                "pid": pid,
                "running": running,
                "log": lines,
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Inventory or safely rebuild stored study notes.")
    parser.add_argument(
        "command",
        choices=(
            "inventory",
            "reprocess",
            "start-latest",
            "latest-status",
            "remove",
            "prune-relations",
            "audit-relations",
            "profile-public",
            "profile-recall",
        ),
    )
    parser.add_argument("--timeout", type=int, default=2400)
    parser.add_argument("--expected-notes", type=int, default=0)
    parser.add_argument("--session-id", type=int, default=0)
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Reprocess only the most recently uploaded note.",
    )
    args = parser.parse_args()
    if args.command == "inventory":
        storage, upload_root = configured_paths()
        inventory = session_inventory(storage, upload_root)
        print(json.dumps({"notes": len(inventory), "inventory": inventory}, ensure_ascii=False, indent=2))
        return 0
    if args.command == "remove":
        if args.session_id <= 0:
            raise RuntimeError("remove requires --session-id")
        storage, upload_root = configured_paths()
        session = storage.get_study_recall_session(args.session_id)
        if session is None:
            raise RuntimeError(f"session {args.session_id} does not exist")
        remove_session(storage, upload_root, args.session_id)
        print(
            json.dumps(
                {"event": "removed", "session_id": args.session_id, "title": session.get("title") or ""},
                ensure_ascii=False,
            )
        )
        return 0
    if args.command == "prune-relations":
        storage, _upload_root = configured_paths()
        result = prune_repeated_relation_associations(storage)
        print(json.dumps({"event": "relations_pruned", **result}, ensure_ascii=False))
        return 0
    if args.command == "audit-relations":
        storage, _upload_root = configured_paths()
        print(json.dumps({"event": "relations_audited", **audit_relation_associations(storage)}, ensure_ascii=False))
        return 0
    if args.command == "profile-public":
        print(json.dumps({"event": "public_page_profile", **profile_public_study_progress()}, ensure_ascii=False))
        return 0
    if args.command == "profile-recall":
        print(json.dumps({"event": "study_recall_profile", **profile_study_recall()}, ensure_ascii=False))
        return 0
    if args.command == "start-latest":
        return start_latest_background(max(300, args.timeout))
    if args.command == "latest-status":
        return latest_background_status()
    return reprocess_all(
        max(300, args.timeout),
        max(0, args.expected_notes),
        latest_only=bool(args.latest),
    )


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"event": "error", "error": str(error)}, ensure_ascii=False), flush=True)
        sys.exit(1)
