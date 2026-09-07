"""Video routes and their feature helpers."""

import base64
import math
import re
from typing import Any, Dict, List, Optional, Tuple
import requests
from flask import request
from ...services.youtube_frames import YoutubeAudioError, YoutubeFrameError, fetch_youtube_audio_clip, fetch_youtube_cached_frame
from ...shared.config import normalize_openai_reasoning_effort


def register_video_routes(*,
    STUDY_PLAN_SUBJECTS,
    _extract_openai_text,
    _invalidate_study_progress_context,
    _is_openai_quota_error,
    _openai_error_details,
    _persist_youtube_storyboard_metadata,
    _repair_study_decoded_text,
    _request_openai_response,
    _study_plan_business_date,
    _study_plan_progress_week,
    _study_plan_today_task_videos,
    _study_plan_video_completion,
    _study_plan_week_rows,
    admin_required,
    app,
    openai_api_key,
    openai_model,
    openai_transcription_model,
    record_ui_event,
    storage,
):
    def _transcribe_study_plan_marker_audio(
        audio_clip: Dict[str, Any],
        *,
        subject: str,
        video_title: str,
        marker_name: str,
    ) -> str:
        audio_bytes = audio_clip.get("bytes")
        if not isinstance(audio_bytes, bytes) or not audio_bytes:
            raise ValueError("empty audio clip")
        context_prompt = (
            "研究所考試課程影片，請保留中文、英文專有名詞、公式念法與程式術語。"
            f"科目：{subject[:48]}；影片：{video_title[:120]}；"
            f"關鍵點：{marker_name[:160]}。"
        )
        response = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {openai_api_key}"},
            files={
                "file": (
                    str(audio_clip.get("filename") or "marker-context.wav"),
                    audio_bytes,
                    str(audio_clip.get("mime_type") or "audio/wav"),
                )
            },
            data={
                "model": openai_transcription_model,
                "language": "zh",
                "response_format": "json",
                "prompt": context_prompt,
            },
            timeout=90,
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("invalid transcription response")
        return " ".join(str(payload.get("text") or "").split()).strip()[:6000]

    def _collect_study_plan_video_context(
        video: Dict[str, Any],
        playback_seconds: float,
        *,
        radius_seconds: float,
        context_name: str,
    ) -> Dict[str, Any]:
        """Collect the same audio-and-frame context for markers and questions."""
        youtube_video_id = str(video.get("youtube_video_id") or "").strip()
        duration_seconds = max(0.0, float(video.get("duration_seconds") or 0))
        radius_seconds = max(1.0, min(30.0, float(radius_seconds)))
        window_start = max(0.0, playback_seconds - radius_seconds)
        window_end = playback_seconds + radius_seconds
        if duration_seconds > 0:
            window_end = min(duration_seconds, window_end)

        audio_transcript = ""
        audio_error = ""
        audio_clip: Optional[Dict[str, Any]] = None
        try:
            audio_clip = fetch_youtube_audio_clip(
                youtube_video_id,
                playback_seconds,
                radius_seconds=radius_seconds,
            )
            audio_transcript = _transcribe_study_plan_marker_audio(
                audio_clip,
                subject=str(video.get("subject") or ""),
                video_title=str(video.get("title") or ""),
                marker_name=context_name,
            )
            window_start = float(audio_clip.get("start_seconds") or window_start)
            window_end = float(audio_clip.get("end_seconds") or window_end)
        except (YoutubeAudioError, requests.RequestException, ValueError, TypeError) as exc:
            audio_error = str(exc)
            app.logger.warning(
                "Video audio context unavailable for %s: %s",
                youtube_video_id,
                type(exc).__name__,
            )

        sample_points: List[float] = []
        for offset in (
            -radius_seconds,
            -radius_seconds / 2,
            0.0,
            radius_seconds / 2,
            radius_seconds,
        ):
            point = max(0.0, playback_seconds + offset)
            if duration_seconds > 0:
                point = min(point, max(0.0, duration_seconds - 0.05))
            if not any(abs(existing - point) < 0.5 for existing in sample_points):
                sample_points.append(point)

        storyboard_metadata = storage.get_youtube_storyboard_metadata(youtube_video_id)
        sampled_frames: List[Dict[str, Any]] = []
        frame_errors: List[str] = []
        for sample_seconds in sample_points:
            try:
                frame = fetch_youtube_cached_frame(
                    youtube_video_id,
                    sample_seconds,
                    storyboard_metadata=storyboard_metadata,
                )
            except YoutubeFrameError as exc:
                frame_errors.append(str(exc))
                continue
            refreshed_metadata = frame.get("storyboard_metadata")
            _persist_youtube_storyboard_metadata(storage, refreshed_metadata, app.logger)
            if isinstance(refreshed_metadata, dict):
                storyboard_metadata = refreshed_metadata
            sampled_frames.append(frame)

        return {
            "window_start": window_start,
            "window_end": window_end,
            "audio_transcript": audio_transcript,
            "audio_error": audio_error,
            "audio_clip": audio_clip,
            "frames": sampled_frames,
            "frame_errors": frame_errors,
        }

    def _generate_study_plan_marker_summary(
        marker: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], Optional[str]]:
        marker_id = int(marker.get("id") or 0)
        note = " ".join(str(marker.get("note") or "").split()).strip()
        if not openai_api_key:
            saved = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="unavailable",
            )
            return saved or marker, "尚未設定 OpenAI API 金鑰。"
        if not note or note == "關鍵片段":
            saved = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="needs_name",
            )
            return saved or marker, "請先替關鍵點命名。"

        processing = storage.update_study_plan_video_marker_summary(
            marker_id,
            summary="",
            status="processing",
        ) or marker
        video = next(
            (
                item
                for item in storage.list_study_plan_videos_with_records()
                if int(item.get("id") or 0) == int(marker.get("video_id") or 0)
            ),
            None,
        )
        if not video or not str(video.get("youtube_video_id") or "").strip():
            failed = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="failed",
            )
            return failed or processing, "這部影片尚未對應 YouTube。"

        youtube_video_id = str(video.get("youtube_video_id") or "").strip()
        duration_seconds = max(0.0, float(video.get("duration_seconds") or 0))
        playback_seconds = max(0.0, float(marker.get("playback_seconds") or 0))
        if duration_seconds > 0:
            playback_seconds = min(playback_seconds, duration_seconds)
        context = _collect_study_plan_video_context(
            video,
            playback_seconds,
            radius_seconds=15.0,
            context_name=note,
        )
        window_start = float(context["window_start"])
        window_end = float(context["window_end"])
        audio_transcript = str(context["audio_transcript"] or "")
        audio_error = str(context["audio_error"] or "")
        audio_clip = context.get("audio_clip")
        sampled_frames = list(context["frames"] or [])
        frame_errors = list(context["frame_errors"] or [])

        if not sampled_frames and not audio_transcript:
            failed = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="failed",
            )
            error = (
                audio_error
                or (frame_errors[0] if frame_errors else "無法取得關鍵點附近的聲音與畫面。")
            )
            return failed or processing, error

        prompt = (
            "你是研究所考試課程的影片筆記助教。請根據關鍵點前後各 15 秒的語音逐字稿"
            "與依時間排列的影片畫面，"
            "替學生已命名的關鍵點寫一段可快速複習的摘要。關鍵點名稱代表學生想記住的主題，"
            "但不能凌駕於影音證據；請用畫面校正逐字稿中的同音字、符號與公式，並整合老師口頭講解、"
            "定義、推導步驟、程式流程或解題技巧。只整理這 30 秒內能可靠判斷的內容，"
            "不要補充課程外知識。若影音不足以支持完整結論，直接指出目前能確認的部分。"
            "回答使用好理解的繁體中文，全文盡量控制在 120 個中文字內，並固定使用容易掃讀的兩行格式："
            "第一行以『重點｜』開頭，只寫一個與關鍵點名稱直接相關的核心結論；"
            "第二行僅在必要時以『關鍵｜』開頭，補一個必要條件、步驟或公式，沒有就省略。"
            "每行都要是完整短句，不要在同一行塞入多個不同觀念。"
            "刪除重複解釋、背景鋪陳與『老師提到』『這段影片說明』等轉述語，"
            "除了『重點｜』『關鍵｜』之外，不要輸出其他標題、項目符號或開場白。"
            "公式使用 LaTeX，行內公式用 \\( ... \\)，獨立公式用 \\[ ... \\]。\n\n"
            f"科目：{str(video.get('subject') or '')[:48]}\n"
            f"影片：第 {int(video.get('sequence') or 0):03d} 支・{str(video.get('title') or '')[:180]}\n"
            f"關鍵點時間：{playback_seconds:.1f} 秒\n"
            f"分析範圍：{window_start:.1f} 至 {window_end:.1f} 秒\n"
            f"關鍵點名稱：{note[:280]}\n"
            "語音逐字稿（可能含辨識錯字，需以畫面與上下文校正）：\n"
            f"{audio_transcript or '音訊取得失敗，本次只能依畫面整理。'}"
        )
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for index, frame in enumerate(sampled_frames, start=1):
            content.append(
                {
                    "type": "input_text",
                    "text": (
                        f"畫面 {index}，影片時間約 "
                        f"{float(frame.get('frame_seconds') or 0):.1f} 秒："
                    ),
                }
            )
            content.append(
                {
                    "type": "input_image",
                    "image_url": (
                        f"data:{frame['mime_type']};base64,"
                        + base64.b64encode(frame["bytes"]).decode("ascii")
                    ),
                    "detail": "high",
                }
            )
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [{"role": "user", "content": content}],
            "reasoning": {
                "effort": normalize_openai_reasoning_effort(openai_model, "low")
            },
            "max_output_tokens": 320,
        }
        try:
            response_payload = _request_openai_response(
                name="video marker summary",
                request_body=request_body,
                timeout=90,
            )
            summary = _repair_study_decoded_text(
                _extract_openai_text(response_payload)
            ).strip()[:600]
            if summary:
                summary = re.sub(r"^\s*重點\s*[：:|｜]\s*", "重點｜", summary)
                summary = re.sub(r"\s*關鍵\s*[：:|｜]\s*", "\n關鍵｜", summary)
                lines = [line.strip() for line in summary.splitlines() if line.strip()]
                if lines and not lines[0].startswith("重點｜"):
                    lines[0] = "重點｜" + lines[0]
                if len(lines) > 1 and not lines[1].startswith("關鍵｜"):
                    lines[1] = "關鍵｜" + lines[1]
                summary = "\n".join(lines[:2])
        except requests.HTTPError as exc:
            error_code, error_type, error_message = _openai_error_details(exc.response)
            if _is_openai_quota_error(error_code, error_type, error_message):
                error = "OpenAI API 額度不足。"
            else:
                error = "AI 暫時無法整理這個關鍵點。"
            failed = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="failed",
            )
            return failed or processing, error
        except (requests.RequestException, ValueError, TypeError):
            failed = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="failed",
            )
            return failed or processing, "AI 暫時無法整理這個關鍵點。"
        if not summary:
            failed = storage.update_study_plan_video_marker_summary(
                marker_id,
                summary="",
                status="failed",
            )
            return failed or processing, "AI 沒有產生有效摘要。"
        ready = storage.update_study_plan_video_marker_summary(
            marker_id,
            summary=summary,
            status="ready",
        )
        record_ui_event(
            "study_plan_video_marker_summarized",
            meta={
                "marker_id": marker_id,
                "frame_count": len(sampled_frames),
                "audio_transcribed": bool(audio_transcript),
                "audio_seconds": round(
                    float((audio_clip or {}).get("duration_seconds") or 0),
                    1,
                ),
                "window_start": round(window_start, 1),
                "window_end": round(window_end, 1),
            },
        )
        return ready or processing, None

    @app.post("/admin/study-plan/video-markers")
    @admin_required
    def admin_study_plan_video_markers():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            playback_seconds = float(payload.get("playback_seconds") or 0)
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        if video_id <= 0 or not math.isfinite(playback_seconds):
            return {"ok": False, "error": "invalid_payload"}, 400
        marker = storage.create_study_plan_video_marker(
            video_id=video_id,
            playback_seconds=playback_seconds,
            note=str(payload.get("note") or ""),
        )
        if not marker:
            return {"ok": False, "error": "video_not_found"}, 404
        record_ui_event(
            "study_plan_video_marker_created",
            meta={"video_id": video_id, "playback_seconds": round(marker["playback_seconds"], 1)},
        )
        requested_note = " ".join(str(payload.get("note") or "").split()).strip()
        summary_error = None
        if requested_note and bool(payload.get("auto_summary", True)):
            marker, summary_error = _generate_study_plan_marker_summary(marker)
        return {"ok": True, "marker": marker, "summary_error": summary_error}

    @app.post("/admin/study-plan/video-question")
    @admin_required
    def admin_study_plan_video_question():
        if not openai_api_key:
            return {"ok": False, "error": "尚未設定 OpenAI API 金鑰。"}, 503
        payload = request.get_json(silent=True) or {}
        question = " ".join(str(payload.get("question") or "").split()).strip()
        try:
            video_id = int(payload.get("video_id") or 0)
            playback_seconds = float(payload.get("playback_seconds") or 0)
        except (TypeError, ValueError):
            return {"ok": False, "error": "提問資料格式不正確。"}, 400
        if video_id <= 0 or not math.isfinite(playback_seconds) or playback_seconds < 0:
            return {"ok": False, "error": "目前的影片或播放時間無效。"}, 400
        if not question:
            return {"ok": False, "error": "請先輸入想問的問題。"}, 400
        if len(question) > 800:
            return {"ok": False, "error": "問題請控制在 800 字內。"}, 400

        video = next(
            (
                item
                for item in storage.list_study_plan_videos_with_records()
                if int(item.get("id") or 0) == video_id
            ),
            None,
        )
        if not video:
            return {"ok": False, "error": "找不到這部影片。"}, 404
        youtube_video_id = str(video.get("youtube_video_id") or "").strip()
        if not youtube_video_id:
            return {"ok": False, "error": "這部影片尚未對應 YouTube。"}, 400
        duration_seconds = max(0.0, float(video.get("duration_seconds") or 0))
        if duration_seconds > 0:
            playback_seconds = min(playback_seconds, duration_seconds)

        context = _collect_study_plan_video_context(
            video,
            playback_seconds,
            radius_seconds=10.0,
            context_name=question,
        )
        sampled_frames = list(context.get("frames") or [])
        audio_transcript = str(context.get("audio_transcript") or "")
        if not sampled_frames and not audio_transcript:
            context_error = str(context.get("audio_error") or "")
            frame_errors = list(context.get("frame_errors") or [])
            if not context_error and frame_errors:
                context_error = str(frame_errors[0])
            record_ui_event(
                "study_plan_video_frame_capture",
                "error",
                {
                    "stage": "question",
                    "video_id": video_id,
                    "youtube_video_id": youtube_video_id,
                    "playback_seconds": round(playback_seconds, 1),
                    "reason": context_error[:160],
                },
            )
            return {
                "ok": False,
                "error": context_error or "無法取得這一幕前後 10 秒的影音內容。",
            }, 502

        primary_frame = min(
            sampled_frames,
            key=lambda item: abs(float(item.get("frame_seconds") or 0) - playback_seconds),
        ) if sampled_frames else None
        image_data_url = (
            f"data:{primary_frame['mime_type']};base64,"
            + base64.b64encode(primary_frame["bytes"]).decode("ascii")
            if primary_frame else ""
        )
        frame_source = str((primary_frame or {}).get("source") or "storyboard")
        window_start = float(context.get("window_start") or max(0.0, playback_seconds - 10.0))
        window_end = float(context.get("window_end") or playback_seconds + 10.0)
        prompt = (
            "你是研究所考試課程的即時影音助教。請根據學生按下提問前後各 10 秒的語音逐字稿、"
            "依時間排列的多張畫面，以及學生的問題直接作答。先理解老師在這 20 秒中的完整論述，"
            "再用畫面校正逐字稿裡的同音字、符號、公式與程式術語；學生提問是回答重點，"
            "不要只是摘要片段。科目與影片標題只能作背景，影音中能確認的內容才是主要依據。"
            "若部分畫面或聲音取得失敗，仍須善用成功取得的前後文回答；只有現有影音確實不足以判斷時，"
            "才精確指出缺少的條件，不可猜測。回答使用精簡、好理解的繁體中文，控制在 2 至 8 個短段落。"
            "數學表達式一律使用 LaTeX：行內公式用 \\( ... \\)，獨立公式用 \\[ ... \\]。"
            "不要輸出 Markdown 標題、粗體標記或程式碼區塊。\n\n"
            f"科目：{str(video.get('subject') or '')[:48]}\n"
            f"影片：第 {int(video.get('sequence') or 0):03d} 支・{str(video.get('title') or '')[:180]}\n"
            f"學生按下提問時間：{playback_seconds:.1f} 秒\n"
            f"分析範圍：{window_start:.1f} 至 {window_end:.1f} 秒\n"
            f"學生的問題：{question}\n"
            "語音逐字稿（可能含辨識錯字，需以畫面與上下文校正）：\n"
            f"{audio_transcript or '音訊取得失敗，本次依前後畫面回答。'}"
        )
        content: List[Dict[str, Any]] = [{"type": "input_text", "text": prompt}]
        for index, frame in enumerate(sampled_frames, start=1):
            content.append({
                "type": "input_text",
                "text": f"畫面 {index}，影片時間約 {float(frame.get('frame_seconds') or 0):.1f} 秒：",
            })
            content.append({
                "type": "input_image",
                "image_url": (
                    f"data:{frame['mime_type']};base64,"
                    + base64.b64encode(frame["bytes"]).decode("ascii")
                ),
                "detail": "high",
            })
        request_body = {
            "model": openai_model,
            "store": False,
            "input": [
                {
                    "role": "user",
                    "content": content,
                }
            ],
            "reasoning": {
                "effort": normalize_openai_reasoning_effort(openai_model, "low")
            },
            "max_output_tokens": 1800,
        }
        try:
            response_payload = _request_openai_response(
                name="video frame question",
                request_body=request_body,
                timeout=90,
            )
            answer = _extract_openai_text(response_payload).strip()[:8000]
        except requests.HTTPError as exc:
            error_code, error_type, error_message = _openai_error_details(exc.response)
            if _is_openai_quota_error(error_code, error_type, error_message):
                return {"ok": False, "error": "OpenAI API 額度不足，請補充額度後再試。"}, 503
            return {"ok": False, "error": "AI 暫時無法回答，請稍後再試。"}, 502
        except (requests.RequestException, ValueError, TypeError):
            return {"ok": False, "error": "AI 暫時無法回答，請稍後再試。"}, 502
        if not answer:
            return {"ok": False, "error": "AI 沒有產生有效回答，請換個方式提問。"}, 502
        record_ui_event(
            "study_plan_video_frame_question",
            meta={
                "video_id": video_id,
                "requested_seconds": round(playback_seconds, 1),
                "frame_seconds": round(float((primary_frame or {}).get("frame_seconds") or playback_seconds), 1),
                "frame_source": frame_source,
                "frame_count": len(sampled_frames),
                "audio_transcribed": bool(audio_transcript),
                "window_start": round(window_start, 1),
                "window_end": round(window_end, 1),
            },
        )
        return {
            "ok": True,
            "answer": answer,
            "frame_image": image_data_url,
            "requested_seconds": round(playback_seconds, 2),
            "frame_seconds": round(float((primary_frame or {}).get("frame_seconds") or playback_seconds), 2),
            "frame_source": frame_source,
            "context_start_seconds": round(window_start, 2),
            "context_end_seconds": round(window_end, 2),
            "context_frame_count": len(sampled_frames),
            "context_audio": bool(audio_transcript),
        }

    @app.post("/admin/study-plan/video-frame")
    @admin_required
    def admin_study_plan_video_frame():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            playback_seconds = float(payload.get("playback_seconds") or 0)
        except (TypeError, ValueError):
            return {"ok": False, "error": "取樣資料格式不正確。"}, 400
        if video_id <= 0 or not math.isfinite(playback_seconds) or playback_seconds < 0:
            return {"ok": False, "error": "目前的影片或播放時間無效。"}, 400
        video = next(
            (
                item
                for item in storage.list_study_plan_videos_with_records()
                if int(item.get("id") or 0) == video_id
            ),
            None,
        )
        if not video:
            return {"ok": False, "error": "找不到這部影片。"}, 404
        youtube_video_id = str(video.get("youtube_video_id") or "").strip()
        if not youtube_video_id:
            return {"ok": False, "error": "這部影片尚未對應 YouTube。"}, 400
        duration_seconds = max(0.0, float(video.get("duration_seconds") or 0))
        if duration_seconds > 0:
            playback_seconds = min(playback_seconds, duration_seconds)
        storyboard_metadata = storage.get_youtube_storyboard_metadata(youtube_video_id)
        try:
            frame = fetch_youtube_cached_frame(
                youtube_video_id,
                playback_seconds,
                storyboard_metadata=storyboard_metadata,
            )
        except YoutubeFrameError as exc:
            record_ui_event(
                "study_plan_video_frame_capture",
                "error",
                {
                    "stage": "prefetch",
                    "video_id": video_id,
                    "youtube_video_id": youtube_video_id,
                    "playback_seconds": round(playback_seconds, 1),
                    "reason": str(exc)[:160],
                },
            )
            return {"ok": False, "error": str(exc)}, 502
        _persist_youtube_storyboard_metadata(
            storage,
            frame.get("storyboard_metadata"),
            app.logger,
        )
        return {
            "ok": True,
            "frame_image": (
                f"data:{frame['mime_type']};base64,"
                + base64.b64encode(frame["bytes"]).decode("ascii")
            ),
            "requested_seconds": round(playback_seconds, 2),
            "frame_seconds": round(float(frame["frame_seconds"]), 2),
            "frame_source": str(frame.get("source") or "storyboard"),
        }

    @app.patch("/admin/study-plan/video-markers/<int:marker_id>")
    @admin_required
    def admin_study_plan_video_marker_update(marker_id: int):
        payload = request.get_json(silent=True) or {}
        marker = storage.update_study_plan_video_marker(marker_id, note=str(payload.get("note") or ""))
        if not marker:
            return {"ok": False, "error": "marker_not_found"}, 404
        record_ui_event("study_plan_video_marker_updated", meta={"marker_id": marker_id})
        summary_error = None
        if bool(payload.get("auto_summary", True)):
            marker, summary_error = _generate_study_plan_marker_summary(marker)
        return {"ok": True, "marker": marker, "summary_error": summary_error}

    @app.post("/admin/study-plan/video-markers/<int:marker_id>/summary")
    @admin_required
    def admin_study_plan_video_marker_summary(marker_id: int):
        marker = storage.get_study_plan_video_marker(marker_id)
        if not marker:
            return {"ok": False, "error": "找不到這個關鍵點。"}, 404
        marker, summary_error = _generate_study_plan_marker_summary(marker)
        return {
            "ok": summary_error is None,
            "marker": marker,
            "error": summary_error,
        }, 200 if summary_error is None else 502

    @app.delete("/admin/study-plan/video-markers/<int:marker_id>")
    @admin_required
    def admin_study_plan_video_marker_delete(marker_id: int):
        if not storage.delete_study_plan_video_marker(marker_id):
            return {"ok": False, "error": "marker_not_found"}, 404
        record_ui_event("study_plan_video_marker_deleted", meta={"marker_id": marker_id})
        return {"ok": True}

    @app.post("/admin/study-plan/video-progress")
    @admin_required
    def admin_study_plan_video_progress():
        payload = request.get_json(silent=True) or {}
        try:
            video_id = int(payload.get("video_id") or 0)
            watched_seconds = float(payload.get("watched_seconds") or 0)
            expected_version = int(payload.get("expected_version"))
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        if video_id <= 0 or expected_version < 0:
            return {"ok": False, "error": "missing_video"}, 400
        if not math.isfinite(watched_seconds):
            return {"ok": False, "error": "invalid_progress"}, 400
        result = storage.update_study_plan_video_progress(
            video_id=video_id,
            watched_seconds=watched_seconds,
            expected_version=expected_version,
        )
        if not result:
            return {"ok": False, "error": "video_not_found"}, 404
        result["completion"] = _study_plan_video_completion(
            result.get("duration_seconds"),
            result.get("watched_seconds"),
        )
        if not result.get("stale"):
            _invalidate_study_progress_context()
        videos = storage.list_study_plan_videos_with_records()
        week_rows, _calendar_week, summary = _study_plan_week_rows(videos)
        current_week = _study_plan_progress_week(week_rows)
        videos_by_subject: Dict[str, List[Dict[str, Any]]] = {
            subject: [] for subject in STUDY_PLAN_SUBJECTS
        }
        for video in videos:
            videos_by_subject.setdefault(str(video.get("subject") or ""), []).append(video)
        today_task_videos = _study_plan_today_task_videos(
            videos_by_subject,
            week_rows,
            current_week,
        )
        if not result.get("stale"):
            record_ui_event(
                "study_plan_youtube_progress_saved",
                meta={"video_id": video_id, "watched_seconds": round(float(result["watched_seconds"]), 1)},
            )
        return {
            "ok": True,
            **result,
            "summary": {
                "total_watched_hours": round(float(summary["total_watched"]) / 60, 1),
                "completion": round(float(summary["completion"]), 1),
                "completed_videos": int(summary["completed_videos"]),
                "total_videos": int(summary["total_videos"]),
                "video_completion": round(float(summary["video_completion"]), 1),
            },
            "today_task_videos": today_task_videos,
            "current_week": {
                "start": current_week["start"],
                "watched_minutes": round(float(current_week["watched_minutes"]), 1),
                "watched_hours": round(float(current_week["watched_seconds"]) / 3600, 2),
                "remaining_hours": round(float(current_week["remaining_hours"]), 1),
                "completion": round(float(current_week["completion"]), 1),
                "state": current_week["state"],
                "state_label": current_week["state_label"],
                "daily_recommendations": current_week["daily_recommendations"],
            },
        }

    @app.post("/admin/study-plan/study-time")
    @admin_required
    def admin_study_plan_study_time():
        payload = request.get_json(silent=True) or {}
        try:
            elapsed_seconds = float(payload.get("elapsed_seconds") or 0)
            video_id = int(payload.get("video_id") or 0) or None
        except (TypeError, ValueError):
            return {"ok": False, "error": "invalid_payload"}, 400
        session_key = str(payload.get("session_id") or "").strip()
        kind = str(payload.get("kind") or "").strip().lower()
        if (
            not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", session_key)
            or kind not in {"video", "practice"}
            or not math.isfinite(elapsed_seconds)
            or elapsed_seconds < 0
        ):
            return {"ok": False, "error": "invalid_payload"}, 400
        summary = storage.record_study_time_session(
            session_key=session_key,
            kind=kind,
            elapsed_seconds=elapsed_seconds,
            video_id=video_id,
            label=str(payload.get("label") or ""),
            completed=bool(payload.get("completed")),
        )
        if summary is None:
            return {"ok": False, "error": "session_not_saved"}, 400
        _invalidate_study_progress_context()
        return {
            "ok": True,
            "session_id": session_key,
            "elapsed_seconds": round(elapsed_seconds, 1),
            "today": {
                "total_seconds": round(float(summary["total_seconds"]), 1),
                "video_seconds": round(float(summary["video_seconds"]), 1),
                "practice_seconds": round(float(summary["practice_seconds"]), 1),
                "sessions": storage.list_study_time_sessions(day=str(summary["day"])),
            },
        }

    @app.delete("/admin/study-plan/study-time/<session_key>")
    @admin_required
    def admin_study_plan_study_time_delete(session_key: str):
        if not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", str(session_key or "")):
            return {"ok": False, "error": "invalid_session"}, 400
        if not storage.delete_study_time_session(session_key):
            return {"ok": False, "error": "session_not_found"}, 404
        day = _study_plan_business_date().isoformat()
        summary = storage.get_study_time_summary(day=day)
        _invalidate_study_progress_context()
        return {
            "ok": True,
            "today": {
                "total_seconds": round(float(summary["total_seconds"]), 1),
                "video_seconds": round(float(summary["video_seconds"]), 1),
                "practice_seconds": round(float(summary["practice_seconds"]), 1),
                "sessions": storage.list_study_time_sessions(day=day),
            },
        }

    return (
        _transcribe_study_plan_marker_audio,
        _collect_study_plan_video_context,
        _generate_study_plan_marker_summary,
        admin_study_plan_video_markers,
        admin_study_plan_video_question,
        admin_study_plan_video_frame,
        admin_study_plan_video_marker_update,
        admin_study_plan_video_marker_summary,
        admin_study_plan_video_marker_delete,
        admin_study_plan_video_progress,
        admin_study_plan_study_time,
        admin_study_plan_study_time_delete,
    )
