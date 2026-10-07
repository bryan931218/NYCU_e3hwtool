"""Coarse arrival labels for current and previously stored page visits."""
from urllib.parse import urlsplit

DIRECT_SOURCE = "直接／來源不明"
INTERNAL_SOURCE = "站內導覽"
OTHER_SOURCE = "其他來源"
SOURCE_DOMAINS = {
    "Dcard": ("dcard.tw",),
    "Google": ("google.com", "google.com.tw", "google.co.jp", "google.co.uk",
               "google.com.hk", "google.com.sg", "google.de", "google.fr", "google.ca", "google.com.au"),
    "Facebook": ("facebook.com", "fb.com", "fb.me"),
    "Instagram": ("instagram.com",), "LINE": ("line.me", "lin.ee"),
    "YouTube": ("youtube.com", "youtu.be"), "Threads": ("threads.net", "threads.com"),
    "Bing": ("bing.com",), "DuckDuckGo": ("duckduckgo.com",),
}


def hostname(value):
    try:
        parsed = urlsplit(value or "")
        if parsed.scheme not in {"http", "https"}:
            return ""
        host = (parsed.hostname or "").lower().rstrip(".")
        return host if len(host) <= 253 else ""
    except ValueError:
        return ""


def arrival_source(referrer, source_tag="", *, site_hosts=()):
    tag = str(source_tag or "").strip().lower()[:80]
    if tag:
        aliases = {"fb": "facebook", "ig": "instagram"}
        tag = aliases.get(tag, tag)
        if tag in {"bookmark", "bookmarks", "書籤", "direct"}:
            return DIRECT_SOURCE
        return next((label for label, domains in SOURCE_DOMAINS.items()
                     if tag == label.lower() or tag in domains), OTHER_SOURCE)
    host = hostname(referrer)
    if not host:
        return DIRECT_SOURCE
    if host in set(site_hosts):
        return INTERNAL_SOURCE
    return next((label for label, domains in SOURCE_DOMAINS.items()
                 if any(host == domain or host.endswith("." + domain) for domain in domains)), OTHER_SOURCE)


def stored_source_label(value):
    """Group legacy hostname rows without modifying existing historical records."""
    if value in {DIRECT_SOURCE, "直接開啟 / 書籤", "direct"}:
        return DIRECT_SOURCE
    if value in {INTERNAL_SOURCE, OTHER_SOURCE} or value in SOURCE_DOMAINS:
        return value
    return arrival_source("https://" + str(value or "")) if value else DIRECT_SOURCE
