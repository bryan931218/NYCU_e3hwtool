"""The verified Nov 19 plan, before the Nov 20 scheduling reset."""

PLAN_START = "2026-11-20"
PLAN_END = "2026-11-30"
# The owner confirmed the schedule was complete through Nov 18. From Nov 19
# onward, only recorded gains count toward the schedule's time balance.
FIRST_TRACKED_DAY = "2026-11-19"

# Reconstructed from the Nov 19 progress snapshot and the matching lengths
# in the video inventory. Algorithm 13 had 202 seconds recorded at that point.
BASELINE_BY_SUBJECT = {
    "線性代數": 246466.1,
    "離散數學": 286226.0,
    "資料結構": 395397.7,
    "作業系統": 254195.7,
    "計算機組織": 418888.9,
    "演算法": 71446.5,
}
SUBJECT_TARGETS = {"作業系統": 74504.7, "演算法": 25184.8}


def uses_november_checkpoint(settings):
    settings = settings or {}
    return (
        settings.get("start_date") == PLAN_START
        and settings.get("end_date") == PLAN_END
        and settings.get("baseline_by_subject") == BASELINE_BY_SUBJECT
        and settings.get("subject_targets") == SUBJECT_TARGETS
    )
