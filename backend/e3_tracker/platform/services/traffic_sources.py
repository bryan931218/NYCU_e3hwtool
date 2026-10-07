"""Coarse arrival labels for current and previously stored page visits."""
import re
from urllib.parse import urlsplit

DIRECT_SOURCE = "直接／來源不明"
INTERNAL_SOURCE = "站內導覽"
OTHER_SOURCE = "其他來源"
SOURCE_DOMAINS = {
    "Dcard": ("dcard.tw",),
    "Google": ("google.com", "google.com.tw", "google.co.jp", "google.co.uk", "google.co.kr",
               "google.com.hk", "google.com.sg", "google.de", "google.fr", "google.ca", "google.com.au",
               "google.co.in", "google.co.id", "google.co.th"),
    "Facebook": ("facebook.com", "fb.com", "fb.me"),
    "Instagram": ("instagram.com",), "LINE": ("line.me", "lin.ee"),
    "YouTube": ("youtube.com", "youtu.be"), "Threads": ("threads.net", "threads.com"),
    "Bing": ("bing.com",), "DuckDuckGo": ("duckduckgo.com",),
    "PTT": ("ptt.cc",), "Reddit": ("reddit.com", "redd.it"),
    "Discord": ("discord.com", "discord.gg"), "X": ("x.com", "twitter.com", "t.co"),
    "Yahoo": ("yahoo.com", "yahoo.com.tw"), "ChatGPT": ("chatgpt.com", "chat.openai.com"),
    "E3": ("e3p.nycu.edu.tw",),
}

APP_PACKAGES = {"jp.naver.line.android": "LINE", "com.sparkslab.dcardreader": "Dcard",
                "com.google.android.googlequicksearchbox": "Google", "com.facebook.katana": "Facebook",
                "com.instagram.android": "Instagram"}


def clean_tag(value):
    return re.sub(r"[^\w.-]", "", str(value or "").strip().lower())[:48]


def hostname(value):
    try:
        parsed = urlsplit(value or "")
        if parsed.scheme not in {"http", "https"}:
            return ""
        host = (parsed.hostname or "").lower().rstrip(".")
        host = host.encode("idna").decode("ascii")
        return host if len(host) <= 253 and re.fullmatch(r"[a-z0-9.-]+", host) else ""
    except (ValueError, UnicodeError):
        return ""


def arrival_source(referrer, source_tag="", *, site_hosts=()):
    tag = str(source_tag or "").strip().lower()[:80]
    if tag:
        aliases = {"fb": "facebook", "ig": "instagram", "liff": "line", "google_search": "google"}
        tag = aliases.get(tag, tag)
        if tag in {"bookmark", "bookmarks", "書籤"}:
            return "書籤（來源標記）"
        if tag == "direct":
            return DIRECT_SOURCE
        return next((label for label, domains in SOURCE_DOMAINS.items()
                     if tag == label.lower() or tag in domains), "來源標記：" + clean_tag(tag) if clean_tag(tag) else OTHER_SOURCE)
    host = hostname(referrer)
    if not host:
        return DIRECT_SOURCE
    if host in set(site_hosts):
        return INTERNAL_SOURCE
    return next((label for label, domains in SOURCE_DOMAINS.items()
                 if any(host == domain or host.endswith("." + domain) for domain in domains)), host)


def detect_arrival(referrer, *, query, site_hosts, user_agent="", fetch_site=""):
    host = hostname(referrer)
    # A tagged link clicked inside this site is still internal navigation.
    if (host and host in site_hosts) or (not host and fetch_site in {"same-origin", "same-site"}):
        return INTERNAL_SOURCE
    tag = clean_tag(query.get("utm_source"))
    if tag:
        return "tag:" + tag
    for key in ("source", "from", "ref"):
        value = clean_tag(query.get(key))
        label = arrival_source("", value) if value else DIRECT_SOURCE
        if label in SOURCE_DOMAINS:
            return "tag:" + value
    if host:
        return "ref:" + host[:251]
    try:
        parsed = urlsplit(referrer or "")
        if parsed.scheme == "android-app" and parsed.netloc in APP_PACKAGES:
            return "android:" + parsed.netloc
    except ValueError:
        pass
    if any(query.get(key) for key in ("gclid", "gbraid", "wbraid")):
        return "click:Google"
    if query.get("fbclid"):
        return "click:Meta"
    ua = user_agent or ""
    for label, pattern in (("Instagram", r"\bInstagram\b"), ("Facebook", r"\bFB(?:AN|AV)/"),
                           ("LINE", r"\bLine/\d"), ("Dcard", r"\bDcard[/ ]"), ("Google", r"\bGSA/")):
        if re.search(pattern, ua, re.IGNORECASE):
            return "app:" + label
    return DIRECT_SOURCE


def source_info(value):
    value = str(value or "")
    prefix, _, detail = value.partition(":")
    if prefix == "ref":
        return {"label": arrival_source("https://" + detail), "method": "來源網域", "evidence": detail, "inferred": False}
    if prefix == "tag":
        return {"label": arrival_source("", detail), "method": "連結標記", "evidence": detail, "inferred": False}
    if prefix == "android" and detail in APP_PACKAGES:
        return {"label": APP_PACKAGES[detail], "method": "App 來源", "evidence": APP_PACKAGES[detail], "inferred": False}
    if prefix == "app" and detail in SOURCE_DOMAINS:
        return {"label": detail + "（推測）", "method": "App 瀏覽器線索", "evidence": detail, "inferred": True}
    if prefix == "click" and detail in {"Google", "Meta"}:
        return {"label": detail, "method": "點擊標記", "evidence": detail, "inferred": False}
    if value.startswith("nav:"):
        return {"label": INTERNAL_SOURCE, "method": "重新整理／上一頁", "evidence": "", "inferred": False}
    if prefix in {"ref", "tag", "android", "app", "click", "nav"}:
        return {"label": OTHER_SOURCE, "method": "未提供線索", "evidence": "", "inferred": False}
    label = stored_source_label(value)
    return {"label": label, "method": "未提供線索" if label in {DIRECT_SOURCE, OTHER_SOURCE} else "既有紀錄",
            "evidence": "" if label in {DIRECT_SOURCE, OTHER_SOURCE} else value, "inferred": False}


def stored_source_label(value):
    """Group legacy hostname rows without modifying existing historical records."""
    if value in {DIRECT_SOURCE, "直接開啟 / 書籤", "direct"}:
        return DIRECT_SOURCE
    if value in {INTERNAL_SOURCE, OTHER_SOURCE} or value in SOURCE_DOMAINS:
        return value
    if str(value or "").startswith(("ref:", "tag:", "android:", "app:", "click:", "nav:")):
        return source_info(value)["label"]
    return arrival_source(value if "://" in str(value or "") else "https://" + str(value or "")) if value else DIRECT_SOURCE
