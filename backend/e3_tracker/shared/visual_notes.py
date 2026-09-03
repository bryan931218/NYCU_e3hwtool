"""Deterministic helpers for visual regions found in uploaded study notes."""

from __future__ import annotations

import html
import math
import textwrap
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


def _svg_label_lines(value: Any, *, width: int = 14, max_lines: int = 3) -> List[str]:
    label = _clean(value, 100)
    if not label:
        return [""]
    lines = textwrap.wrap(
        label,
        width=max(6, width),
        break_long_words=True,
        break_on_hyphens=False,
    ) or [label]
    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = (lines[-1][:-1] + "…") if lines[-1] else "…"
    return lines


def _hierarchical_positions(
    nodes: List[Mapping[str, Any]],
    edges: List[Mapping[str, Any]],
) -> Optional[Dict[str, Tuple[float, float]]]:
    node_ids = [str(node.get("id") or "") for node in nodes]
    indegree = {node_id: 0 for node_id in node_ids}
    outgoing = {node_id: [] for node_id in node_ids}
    for edge in edges:
        source = str(edge.get("from") or "")
        target = str(edge.get("to") or "")
        if source in outgoing and target in indegree:
            outgoing[source].append(target)
            indegree[target] += 1
    queue = [node_id for node_id in node_ids if indegree[node_id] == 0]
    if not queue:
        return None
    level = {node_id: 0 for node_id in queue}
    cursor = 0
    while cursor < len(queue):
        source = queue[cursor]
        cursor += 1
        for target in outgoing[source]:
            level[target] = max(level.get(target, 0), level[source] + 1)
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
    if len(level) != len(node_ids):
        return None

    grouped: Dict[int, List[Mapping[str, Any]]] = {}
    for node in nodes:
        grouped.setdefault(level[str(node.get("id") or "")], []).append(node)
    max_level = max(grouped, default=0)
    positions: Dict[str, Tuple[float, float]] = {}
    for level_index, level_nodes in grouped.items():
        level_nodes.sort(key=lambda item: (_number(item.get("x"), 500), str(item.get("id") or "")))
        y = 172.0 if max_level == 0 else 172.0 + (level_index / max_level) * 452.0
        count = len(level_nodes)
        for index, node in enumerate(level_nodes):
            x = 600.0 if count == 1 else 116.0 + (index / (count - 1)) * 968.0
            positions[str(node.get("id") or "")] = (x, y)
    return positions


def _spatial_positions(nodes: List[Mapping[str, Any]]) -> Dict[str, Tuple[float, float]]:
    xs = [_number(node.get("x"), 500) for node in nodes]
    ys = [_number(node.get("y"), 500) for node in nodes]
    x_span = max(xs) - min(xs) if xs else 0
    y_span = max(ys) - min(ys) if ys else 0
    positions: Dict[str, Tuple[float, float]] = {}
    if x_span < 60 and y_span < 60 and len(nodes) > 1:
        radius = min(245.0, 112.0 + len(nodes) * 18.0)
        for index, node in enumerate(nodes):
            angle = -math.pi / 2 + (2 * math.pi * index / len(nodes))
            positions[str(node.get("id") or "")] = (
                600.0 + math.cos(angle) * radius,
                402.0 + math.sin(angle) * radius,
            )
        return positions
    for node in nodes:
        raw_x = _number(node.get("x"), 500)
        raw_y = _number(node.get("y"), 500)
        x = 132.0 + ((raw_x - min(xs)) / max(1.0, x_span)) * 936.0
        y = 168.0 + ((raw_y - min(ys)) / max(1.0, y_span)) * 456.0
        positions[str(node.get("id") or "")] = (x, y)
    return positions


def _edge_endpoints(
    source: Mapping[str, float],
    target: Mapping[str, float],
) -> Tuple[float, float, float, float]:
    dx = target["x"] - source["x"]
    dy = target["y"] - source["y"]
    if abs(dx) + abs(dy) < 0.001:
        return source["x"], source["y"], target["x"], target["y"]
    source_scale = 1.0 / max(
        abs(dx) / max(1.0, source["width"] / 2),
        abs(dy) / max(1.0, source["height"] / 2),
    )
    target_scale = 1.0 / max(
        abs(dx) / max(1.0, target["width"] / 2),
        abs(dy) / max(1.0, target["height"] / 2),
    )
    return (
        source["x"] + dx * source_scale,
        source["y"] + dy * source_scale,
        target["x"] - dx * target_scale,
        target["y"] - dy * target_scale,
    )


def render_visual_region_svg(region: Mapping[str, Any]) -> Optional[str]:
    if region.get("render_mode") != "svg":
        return None
    nodes = [node for node in region.get("nodes") or [] if isinstance(node, Mapping)]
    edges = [edge for edge in region.get("edges") or [] if isinstance(edge, Mapping)]
    node_lookup = {str(node.get("id") or ""): node for node in nodes}
    if len(node_lookup) < 2 or not edges:
        return None

    region_type = str(region.get("region_type") or "diagram").strip().lower()
    positions = (
        _hierarchical_positions(nodes, edges)
        if region_type in {"tree", "flowchart", "architecture"}
        else None
    ) or _spatial_positions(nodes)
    node_geometry: Dict[str, Dict[str, float]] = {}
    node_lines: Dict[str, List[str]] = {}
    for node in nodes:
        node_id = str(node.get("id") or "")
        lines = _svg_label_lines(node.get("label"), width=14, max_lines=3)
        longest = max((len(line) for line in lines), default=1)
        node_geometry[node_id] = {
            "x": positions[node_id][0],
            "y": positions[node_id][1],
            "width": max(132.0, min(246.0, 54.0 + longest * 14.0)),
            "height": 54.0 + max(0, len(lines) - 1) * 25.0,
        }
        node_lines[node_id] = lines

    type_labels = {
        "tree": "樹狀結構",
        "flowchart": "流程圖",
        "graph": "關係圖",
        "architecture": "架構圖",
    }
    title = _clean(region.get("title"), 80)
    parts = [
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1200 720" '
        'preserveAspectRatio="xMidYMid meet" role="img">',
        '<defs>'
        '<linearGradient id="canvas" x1="0" y1="0" x2="1" y2="1">'
        '<stop offset="0" stop-color="#f8fbff"/><stop offset="1" stop-color="#eef7f6"/>'
        '</linearGradient>'
        '<linearGradient id="nodeFill" x1="0" y1="0" x2="0" y2="1">'
        '<stop offset="0" stop-color="#ffffff"/><stop offset="1" stop-color="#f7fbff"/>'
        '</linearGradient>'
        '<filter id="shadow" x="-20%" y="-30%" width="140%" height="170%">'
        '<feDropShadow dx="0" dy="7" stdDeviation="9" flood-color="#173b5e" flood-opacity=".14"/>'
        '</filter>'
        '<marker id="arrow" viewBox="0 0 12 12" refX="10" refY="6" '
        'markerWidth="9" markerHeight="9" orient="auto">'
        '<path d="M 1 1 L 11 6 L 1 11 z" fill="#58738d"/></marker>'
        '</defs>',
        '<rect width="1200" height="720" rx="28" fill="url(#canvas)"/>',
        '<path d="M0 96H1200" stroke="#d9e7f0" stroke-width="2"/>',
        '<circle cx="48" cy="48" r="17" fill="#118d8f" opacity=".16"/>',
        '<path d="M40 49l6 6 11-14" fill="none" stroke="#087f80" stroke-width="4" '
        'stroke-linecap="round" stroke-linejoin="round"/>',
        f'<text x="80" y="57" font-family="Inter, Noto Sans TC, sans-serif" font-size="27" '
        f'font-weight="750" fill="#18334d">{html.escape(title or "結構示意")}</text>',
        '<rect x="1012" y="29" width="142" height="38" rx="19" fill="#e0f3f1"/>',
        f'<text x="1083" y="54" text-anchor="middle" font-family="Inter, Noto Sans TC, sans-serif" '
        f'font-size="17" font-weight="700" fill="#087f80">{html.escape(type_labels.get(region_type, "視覺圖解"))}</text>',
    ]
    directed = region_type != "graph"
    edge_counts: Dict[Tuple[str, str], int] = {}
    for edge_index, edge in enumerate(edges):
        source = node_lookup.get(str(edge.get("from") or ""))
        target = node_lookup.get(str(edge.get("to") or ""))
        if source is None or target is None:
            continue
        source_id = str(source.get("id") or "")
        target_id = str(target.get("id") or "")
        source_box = node_geometry[source_id]
        target_box = node_geometry[target_id]
        x1, y1, x2, y2 = _edge_endpoints(source_box, target_box)
        pair = tuple(sorted((source_id, target_id)))
        pair_index = edge_counts.get(pair, 0)
        edge_counts[pair] = pair_index + 1
        mid_y = (y1 + y2) / 2
        bend = (pair_index % 3 - 1) * 22 if pair_index else 0
        control_x1 = x1 + (x2 - x1) * 0.28 + bend
        control_x2 = x1 + (x2 - x1) * 0.72 + bend
        control_y1 = mid_y if abs(y2 - y1) > 70 else y1 - 38 - bend
        control_y2 = mid_y if abs(y2 - y1) > 70 else y2 - 38 - bend
        marker = ' marker-end="url(#arrow)"' if directed else ""
        parts.append(
            f'<path d="M{x1:.1f},{y1:.1f} C{control_x1:.1f},{control_y1:.1f} '
            f'{control_x2:.1f},{control_y2:.1f} {x2:.1f},{y2:.1f}" fill="none" '
            f'stroke="#58738d" stroke-width="3.5" stroke-linecap="round"{marker}/>'
        )
        label = _clean(edge.get("label"), 50)
        if label:
            label_x = (x1 + x2) / 2 + bend
            label_y = (y1 + y2) / 2 - 9
            label_width = max(54, min(190, 26 + len(label) * 13))
            parts.append(
                f'<rect x="{label_x - label_width / 2:.1f}" y="{label_y - 18:.1f}" '
                f'width="{label_width}" height="30" rx="15" fill="#f8fbff" stroke="#d4e3ed"/>'
                f'<text x="{label_x:.1f}" y="{label_y + 3:.1f}" text-anchor="middle" '
                'font-family="Inter, Noto Sans TC, sans-serif" font-size="16" font-weight="650" '
                f'fill="#3f5e78">{html.escape(label)}</text>'
            )
    incoming_ids = {str(edge.get("to") or "") for edge in edges}
    for node_index, node in enumerate(nodes):
        node_id = str(node.get("id") or "")
        geometry = node_geometry[node_id]
        x, y = geometry["x"], geometry["y"]
        width, height = geometry["width"], geometry["height"]
        is_root = node_id not in incoming_ids and region_type in {"tree", "flowchart", "architecture"}
        accent = "#087f80" if is_root else ("#1769d8" if node_index % 2 == 0 else "#2f7fb4")
        parts.append(
            f'<g filter="url(#shadow)"><rect x="{x - width / 2:.1f}" y="{y - height / 2:.1f}" '
            f'width="{width:.1f}" height="{height:.1f}" rx="18" fill="url(#nodeFill)" '
            f'stroke="{accent}" stroke-width="2.5"/>'
            f'<rect x="{x - width / 2:.1f}" y="{y - height / 2:.1f}" width="8" '
            f'height="{height:.1f}" rx="4" fill="{accent}"/></g>'
        )
        lines = node_lines[node_id]
        first_y = y - (len(lines) - 1) * 12.5 + 6
        escaped_lines = []
        for line_index, line in enumerate(lines):
            escaped_lines.append(
                f'<tspan x="{x + 4:.1f}" y="{first_y + line_index * 25:.1f}">{html.escape(line)}</tspan>'
            )
        parts.append(
            f'<text x="{x + 4:.1f}" y="{first_y:.1f}" text-anchor="middle" '
            'font-family="Inter, Noto Sans TC, sans-serif" font-size="19" font-weight="700" '
            f'fill="#18334d">{"".join(escaped_lines)}</text>'
        )
    parts.append("</svg>")
    return "".join(parts)
