import re
from typing import Dict, Set

import pytz

TAIPEI_TZ = pytz.timezone("Asia/Taipei")

ASSIGN_LINK_RE = re.compile(r"/mod/assign/view\.php\?id=\d+")
COURSE_LINK_RE = re.compile(r"/course/view\.php\?id=(\d+)")

COMPLETED_KEYWORDS: Set[str] = {
    "已繳交",
    "已提交",
    "已送交",
    "已評分",
    "已提交評分",
    "submitted",
    "submitted for grading",
    "assignment was submitted",
    "was submitted",
    "submitted on",
    "graded",
}
INCOMPLETE_KEYWORDS: Set[str] = {
    "未繳交",
    "未提交",
    "尚未交",
    "草稿",
    "需要重交",
    "no submissions have been made yet",
    "no submission",
    "draft",
    "requires resubmission",
    "not submitted",
    "not yet submitted",
    "not submitted for grading",
}
DUE_LABELS: Set[str] = {
    "截止時間",
    "到期日",
    "截止日期",
    "Due date",
    "Due",
    "截止時間（Due）",
    "Cut-off date",
    "結束時間",
    "截止",
    "End date",
    "End Date",
    "Close date",
}

HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
