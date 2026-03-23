import re
from datetime import datetime
from typing import List, Optional, Sequence, Set, Tuple

from bs4 import BeautifulSoup
from dateutil import parser as dtparser

from .constants import (
    ASSIGN_LINK_RE,
    COMPLETED_KEYWORDS,
    DUE_LABELS,
    INCOMPLETE_KEYWORDS,
    TAIPEI_TZ,
)


def extract_text(el) -> str:
    return re.sub(r"\s+", " ", el.get_text(strip=True)) if el else ""


def _is_placeholder_title(title: str) -> bool:
    if not title:
        return True
    normalized = title.strip().lower()
    if len(normalized) <= 2:
        return True
    compact = re.sub(r"[\s\[\]]+", "", normalized)
    return compact in {"view", "檢視", "查看", "assignment", "作業"}


def find_due_and_status_from_assign_page(html: str) -> Tuple[bool, Optional[bool], Optional[datetime], str]:
    soup = BeautifulSoup(html, "html.parser")
    status_cell_text = ""
    due_str = None

    for tr in soup.find_all("tr"):
        th = tr.find(["th", "td"])
        tds = tr.find_all("td")
        label = extract_text(th).lower() if th else ""
        if any(lbl in label for lbl in ["submission status", "繳交狀態", "提交狀態"]):
            status_cell_text = extract_text(tds[-1]) if tds else extract_text(tr)
        if any(lbl.lower() in label for lbl in [s.lower() for s in DUE_LABELS]):
            if tds:
                due_str = extract_text(tds[-1])

    if not status_cell_text:
        for dt in soup.find_all("dt"):
            label = extract_text(dt).lower()
            if any(lbl in label for lbl in ["submission status", "繳交狀態", "提交狀態"]):
                status_cell_text = extract_text(dt.find_next_sibling("dd"))
            if any(lbl.lower() in label for lbl in [s.lower() for s in DUE_LABELS]):
                due_str = extract_text(dt.find_next_sibling("dd"))

    if not due_str:
        block_text = extract_text(soup)
        for label in DUE_LABELS:
            if label.lower() in block_text.lower():
                match = re.search(r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}(\s+\d{1,2}:\d{2}(:\d{2})?)?)", block_text)
                if match:
                    due_str = match.group(1)
                    break

    low = status_cell_text.lower()
    status_is_complete = any(kw in low for kw in [s.lower() for s in COMPLETED_KEYWORDS])
    status_is_incomplete = any(kw in low for kw in [s.lower() for s in INCOMPLETE_KEYWORDS])
    if status_is_complete and status_is_incomplete:
        status_is_complete = False

    due_dt = None
    if due_str:
        try:
            due_dt = dtparser.parse(due_str, dayfirst=False, fuzzy=True)
            if due_dt.tzinfo is None:
                due_dt = TAIPEI_TZ.localize(due_dt)
            else:
                due_dt = due_dt.astimezone(TAIPEI_TZ)
        except Exception:
            due_dt = None

    return status_is_complete, status_is_incomplete, due_dt, status_cell_text.strip()


def parse_due_text_to_dt(due_text: Optional[str]):
    if not due_text:
        return None
    try:
        value = dtparser.parse(due_text, dayfirst=False, fuzzy=True)
        if value.tzinfo is None:
            return TAIPEI_TZ.localize(value)
        return value.astimezone(TAIPEI_TZ)
    except Exception:
        return None


def gather_assign_links_from_list_page(
    html: str, base_url: str
) -> List[Tuple[str, str, Optional[str], Optional[int], Optional[int]]]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[Tuple[str, str, Optional[str], Optional[int], Optional[int]]] = []
    date_pattern = re.compile(r"\d{4}[/-]\d{1,2}[/-]\d{1,2}(?:\s+\d{1,2}:\d{2}(?::\d{2})?)?")

    for table in soup.find_all("table"):
        headers: Sequence[str] = []
        header_row = table.find("tr")
        if header_row:
            headers = [extract_text(th).strip() for th in header_row.find_all(["th", "td"])]
        
        due_col_idx = -1
        status_col_idx = -1
        for idx, header in enumerate(headers):
            if any(lbl in header for lbl in DUE_LABELS) or any(lbl.lower() in header.lower() for lbl in DUE_LABELS):
                due_col_idx = idx
            if "繳交狀態" in header or "submission status" in header.lower():
                status_col_idx = idx

        for tr in table.find_all("tr"):
            cells = tr.find_all(["td", "th"])
            candidates = tr.find_all("a", href=True)
            target = None
            for a_tag in candidates:
                if ASSIGN_LINK_RE.search(a_tag["href"]):
                    target = a_tag
                    break
            if not target:
                continue
            href = target["href"]
            url = href if href.startswith("http") else base_url.rstrip("/") + "/" + href.lstrip("/")
            title = extract_text(target)
            if _is_placeholder_title(title):
                alt_title = target.get("data-activityname") or target.get("aria-label") or target.get("title")
                if alt_title:
                    title = extract_text(BeautifulSoup(str(alt_title), "html.parser"))
            
            if _is_placeholder_title(title) and cells:
                guessed = extract_text(cells[0])
                if guessed:
                    title = guessed
            
            due_text = None
            if due_col_idx != -1 and len(cells) > due_col_idx:
                due_text = extract_text(cells[due_col_idx])
            if not due_text:
                row_text = extract_text(tr)
                match = date_pattern.search(row_text)
                if match:
                    due_text = match.group(0)

            submitted_count, participant_count = None, None
            if status_col_idx != -1 and len(cells) > status_col_idx:
                status_text = extract_text(cells[status_col_idx])
                s_match = re.search(r"(\d+)\s*(?:個)?已繳", status_text)
                u_match = re.search(r"(\d+)\s*(?:個)?未繳", status_text)
                submitted = int(s_match.group(1)) if s_match else 0
                unsubmitted = int(u_match.group(1)) if u_match else 0
                
                if s_match or u_match:
                    submitted_count = submitted
                    participant_count = submitted + unsubmitted

            links.append((title, url, due_text, submitted_count, participant_count))

    if not links:
        # Fallback for when there's no table structure
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if ASSIGN_LINK_RE.search(href):
                url = href if href.startswith("http") else base_url.rstrip("/") + "/" + href.lstrip("/")
                title = extract_text(a_tag)
                if _is_placeholder_title(title):
                    alt_title = a_tag.get("data-activityname") or a_tag.get("aria-label") or a_tag.get("title")
                    if alt_title:
                        title = extract_text(BeautifulSoup(str(alt_title), "html.parser"))
                if _is_placeholder_title(title):
                    tr_parent = a_tag.find_parent("tr")
                    if tr_parent:
                        tds = tr_parent.find_all(["td", "th"])
                        if tds:
                            guessed = extract_text(tds[0])
                            if guessed:
                                title = guessed
                links.append((title, url, None, None, None))

    uniq = []
    seen: Set[str] = set()
    for title, url, due_text, s_count, p_count in links:
        if url not in seen:
            uniq.append((title, url, due_text, s_count, p_count))
            seen.add(url)
    return uniq
