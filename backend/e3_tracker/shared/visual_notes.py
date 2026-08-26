"""Deterministic helpers for visual regions found in uploaded study notes."""

from __future__ import annotations

import html
import math
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


VISUAL_REGION_TYPES = {
    "tree",
    "flowchart",
    "graph",
    "chart",
    "table",
    "diagram",
    "architecture",
    "circuit",
    "geometry",
    "image",
    "other",
}
REDRAWABLE_REGION_TYPES = {"tree", "flowchart", "graph", "architecture"}


def _number(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def normalize_visual_bbox(value: Any) -> Optional[Dict[str, int]]:
    if not isinstance(value, Mapping):
        return None
    left = round(max(0.0, min(1000.0, _number(value.get("left")))))
    top = round(max(0.0, min(1000.0, _number(value.get("top")))))
    right = round(max(0.0, min(1000.0, _number(value.get("right")))))
    bottom = round(max(0.0, min(1000.0, _number(value.get("bottom")))))
    if right - left < 20 or bottom - top < 20:
        return None
    return {"left": left, "top": top, "right": right, "bottom": bottom}


def _clean(value: Any, limit: int) -> str:
    return " ".join(str(value or "").split()).strip()[:limit]


def normalize_visual_regions(
    regions: Any,
    *,
    image_index: int,
    max_regions: int = 20,
) -> List[Dict[str, Any]]:
    if not isinstance(regions, list) or image_index <= 0:
        return []
    prepared: List[Dict[str, Any]] = []
    for raw in regions[:max_regions]:
        if not isinstance(raw, Mapping):
            continue
        bbox = normalize_visual_bbox(raw.get("bbox"))
        region_type = str(raw.get("region_type") or "other").strip().lower()
        if region_type not in VISUAL_REGION_TYPES:
            region_type = "other"
        title = _clean(raw.get("title"), 100)
        description = _clean(raw.get("description"), 700)
        visible_text = _clean(raw.get("visible_text"), 800)
        if bbox is None or not (title or description or visible_text):
            continue

        nodes: List[Dict[str, Any]] = []
        seen_node_ids = set()
        for node_index, node in enumerate(raw.get("nodes") or []):
            if not isinstance(node, Mapping):
                continue
            node_id = _clean(node.get("id"), 32) or f"n{node_index + 1}"
            if node_id in seen_node_ids:
                continue
            label = _clean(node.get("label"), 100)
            if not label:
                continue
            seen_node_ids.add(node_id)
            nodes.append(
                {
                    "id": node_id,
                    "label": label,
                    "x": round(max(40.0, min(960.0, _number(node.get("x"), 500.0)))),
                    "y": round(max(40.0, min(960.0, _number(node.get("y"), 500.0)))),
                }
            )
            if len(nodes) >= 36:
                break

        valid_node_ids = {node["id"] for node in nodes}
        edges: List[Dict[str, str]] = []
        seen_edges = set()
        for edge in raw.get("edges") or []:
            if not isinstance(edge, Mapping):
                continue
            source = _clean(edge.get("from"), 32)
            target = _clean(edge.get("to"), 32)
            label = _clean(edge.get("label"), 80)
            edge_key = (source, target, label)
            if (
                source not in valid_node_ids
                or target not in valid_node_ids
                or source == target
                or edge_key in seen_edges
            ):
                continue
            seen_edges.add(edge_key)
            edges.append({"from": source, "to": target, "label": label})
            if len(edges) >= 60:
                break

        confidence = str(raw.get("confidence") or "medium").strip().lower()
        if confidence not in {"high", "medium", "low"}:
            confidence = "medium"
        render_mode = (
            "svg"
            if confidence in {"high", "medium"}
            and region_type in REDRAWABLE_REGION_TYPES
            and len(nodes) >= 2
            and edges
            else "crop"
        )
        prepared.append(
            {
                "region_id": f"p{image_index}v{len(prepared) + 1}",
                "image_index": image_index,
                "region_type": region_type,
                "title": title or description[:100] or visible_text[:100],
                "description": description,
                "visible_text": visible_text,
                "bbox": bbox,
                "nodes": nodes,
                "edges": edges,
                "confidence": confidence,
                "render_mode": render_mode,
            }
        )
    return prepared


def _intersection_over_union(first: Mapping[str, int], second: Mapping[str, int]) -> float:
    left = max(first["left"], second["left"])
    top = max(first["top"], second["top"])
    right = min(first["right"], second["right"])
    bottom = min(first["bottom"], second["bottom"])
    intersection = max(0, right - left) * max(0, bottom - top)
    first_area = (first["right"] - first["left"]) * (first["bottom"] - first["top"])
    second_area = (second["right"] - second["left"]) * (second["bottom"] - second["top"])
    return intersection / max(1, first_area + second_area - intersection)


def merge_visual_regions(
    region_sets: Iterable[Any],
    *,
    image_index: int,
) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for regions in region_sets:
        for candidate in normalize_visual_regions(regions, image_index=image_index):
            duplicate_index = next(
                (
                    index
                    for index, existing in enumerate(merged)
                    if existing["region_type"] == candidate["region_type"]
                    and _intersection_over_union(existing["bbox"], candidate["bbox"]) >= 0.45
                ),
                None,
            )
            if duplicate_index is None:
                merged.append(candidate)
                continue
            existing = merged[duplicate_index]
            existing_score = (
                len(existing["nodes"]) * 8
                + len(existing["edges"]) * 5
                + len(existing["description"])
                + len(existing["visible_text"])
            )
            candidate_score = (
                len(candidate["nodes"]) * 8
                + len(candidate["edges"]) * 5
                + len(candidate["description"])
                + len(candidate["visible_text"])
            )
            if candidate_score > existing_score:
                merged[duplicate_index] = candidate
    for index, region in enumerate(merged, start=1):
        region["region_id"] = f"p{image_index}v{index}"
    return merged


def visual_region_crop_box(
    region: Mapping[str, Any],
    *,
    image_width: int,
    image_height: int,
    padding_ratio: float = 0.005,
) -> Optional[Tuple[int, int, int, int]]:
    """Crop the detected vertical band while retaining the full source width."""

    bbox = normalize_visual_bbox(region.get("bbox"))
    if bbox is None or image_width <= 0 or image_height <= 0:
        return None
    pad_y = round(image_height * max(0.0, min(0.08, padding_ratio)))
    top = max(0, math.floor(image_height * bbox["top"] / 1000) - pad_y)
    bottom = min(image_height, math.ceil(image_height * bbox["bottom"] / 1000) + pad_y)
    return (0, top, image_width, bottom) if bottom > top else None


def render_visual_region_svg(region: Mapping[str, Any]) -> Optional[str]:
    if region.get("render_mode") != "svg":
        return None
    nodes = [node for node in region.get("nodes") or [] if isinstance(node, Mapping)]
    edges = [edge for edge in region.get("edges") or [] if isinstance(edge, Mapping)]
    node_lookup = {str(node.get("id") or ""): node for node in nodes}
    if len(node_lookup) < 2 or not edges:
        return None

    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1000 1000" role="img">',
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" '
        'markerWidth="7" markerHeight="7" orient="auto-start-reverse">'
        '<path d="M 0 0 L 10 5 L 0 10 z" fill="#526b86"/></marker></defs>',
        '<rect width="1000" height="1000" fill="#fbfdff"/>',
    ]
    for edge in edges:
        source = node_lookup.get(str(edge.get("from") or ""))
        target = node_lookup.get(str(edge.get("to") or ""))
        if source is None or target is None:
            continue
        x1, y1 = int(source["x"]), int(source["y"])
        x2, y2 = int(target["x"]), int(target["y"])
        parts.append(
            f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" '
            'stroke="#526b86" stroke-width="5" marker-end="url(#arrow)"/>'
        )
        label = _clean(edge.get("label"), 50)
        if label:
            parts.append(
                f'<text x="{(x1 + x2) // 2}" y="{(y1 + y2) // 2 - 12}" '
                'text-anchor="middle" font-family="sans-serif" font-size="26" '
                f'fill="#40536c">{html.escape(label)}</text>'
            )
    for node in nodes:
        x, y = int(node["x"]), int(node["y"])
        label = _clean(node.get("label"), 32)
        width = max(150, min(300, 72 + len(label) * 24))
        parts.append(
            f'<rect x="{x - width // 2}" y="{y - 42}" width="{width}" height="84" '
            'rx="8" fill="#ffffff" stroke="#1769d8" stroke-width="5"/>'
        )
        parts.append(
            f'<text x="{x}" y="{y + 9}" text-anchor="middle" '
            'font-family="sans-serif" font-size="30" font-weight="700" '
            f'fill="#172033">{html.escape(label)}</text>'
        )
    title = _clean(region.get("title"), 80)
    if title:
        parts.append(
            '<text x="32" y="48" font-family="sans-serif" font-size="26" '
            f'font-weight="700" fill="#087f80">{html.escape(title)}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)
