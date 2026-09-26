"""Study-owned inventory diagnostics for the deployment health endpoint."""


def youtube_inventory_health(storage):
    videos = [video for video in storage.list_study_plan_videos_with_records()
              if video.get("subject") == "資料結構"]
    missing = [int(video["sequence"]) for video in videos
               if not str(video.get("youtube_video_id") or "").strip()]
    return {
        "data_structure_total": len(videos),
        "data_structure_linked": len(videos) - len(missing),
        "data_structure_missing_sequences": missing,
    }
