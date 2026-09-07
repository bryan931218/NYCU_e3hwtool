"""Note geometry; dependencies are bound per application."""

import base64
import io
import math
from typing import Any, Dict, List, Tuple
from PIL import Image, ImageChops, ImageDraw, ImageFilter, ImageFont, ImageOps
from ..shared.source_localization import canonicalize_source_text, literal_source_evidence, match_source_evidence_to_lines


def build_note_geometry(*,
    _validated_study_source_bbox,
):
    def _study_coordinate_guide_data_url(source: Image.Image) -> str:
        coordinate_guide = source.convert("RGBA")
        max_side = max(coordinate_guide.size)
        if max_side > 1800 or max_side < 500:
            scale = (1800 if max_side > 1800 else 500) / max_side
            coordinate_guide = coordinate_guide.resize(
                (
                    max(1, round(coordinate_guide.width * scale)),
                    max(1, round(coordinate_guide.height * scale)),
                ),
                Image.Resampling.LANCZOS,
            )
        pixel_width, pixel_height = coordinate_guide.size
        overlay = Image.new("RGBA", coordinate_guide.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        line_width = max(2, round(min(pixel_width, pixel_height) / 550))
        font_size = max(18, round(min(pixel_width, pixel_height) / 42))
        try:
            guide_font = ImageFont.truetype("DejaVuSans.ttf", font_size)
        except OSError:
            guide_font = ImageFont.load_default()
        for coordinate in range(0, 1001, 100):
            x = min(pixel_width - 1, round(pixel_width * coordinate / 1000))
            y = min(pixel_height - 1, round(pixel_height * coordinate / 1000))
            draw.line((x, 0, x, pixel_height), fill=(220, 30, 55, 115), width=line_width)
            draw.line((0, y, pixel_width, y), fill=(220, 30, 55, 115), width=line_width)
            if coordinate < 1000:
                if x + 2 < pixel_width:
                    draw.rectangle(
                        (x + 2, 0, min(pixel_width, x + font_size * 2.8), min(pixel_height, font_size * 1.25)),
                        fill=(255, 255, 255, 220),
                    )
                    draw.text((x + 4, 1), f"X{coordinate}", fill=(180, 0, 25, 255), font=guide_font)
                if y + 2 < pixel_height:
                    draw.rectangle(
                        (0, y + 2, min(pixel_width, font_size * 2.8), min(pixel_height, y + font_size * 1.3)),
                        fill=(255, 255, 255, 220),
                    )
                    draw.text((2, y + 3), f"Y{coordinate}", fill=(180, 0, 25, 255), font=guide_font)
        coordinate_guide = Image.alpha_composite(coordinate_guide, overlay).convert("RGB")
        guide_buffer = io.BytesIO()
        coordinate_guide.save(guide_buffer, format="JPEG", quality=88, optimize=True)
        return f"data:image/jpeg;base64,{base64.b64encode(guide_buffer.getvalue()).decode('ascii')}"

    def _study_image_data_url(source: Image.Image, *, max_side: int = 1800) -> str:
        encoded_image = source.convert("RGB")
        current_max_side = max(encoded_image.size)
        if current_max_side > max_side:
            scale = max_side / current_max_side
            encoded_image = encoded_image.resize(
                (
                    max(1, round(encoded_image.width * scale)),
                    max(1, round(encoded_image.height * scale)),
                ),
                Image.Resampling.LANCZOS,
            )
        image_buffer = io.BytesIO()
        encoded_image.save(image_buffer, format="JPEG", quality=90, optimize=True)
        return f"data:image/jpeg;base64,{base64.b64encode(image_buffer.getvalue()).decode('ascii')}"

    def _canonical_study_source_match_text(value: Any) -> str:
        return canonicalize_source_text(value)

    def _literal_study_source_evidence(value: Any) -> str:
        return literal_source_evidence(value)

    def _match_study_source_evidence_to_lines(
        evidence: str,
        lines: List[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, float]]:
        return match_source_evidence_to_lines(evidence, lines)

    def _build_study_source_ink_mask(source: Image.Image) -> Image.Image:
        working = source.convert("RGB")
        max_side = max(working.size)
        if max_side > 1800:
            scale = 1800 / max_side
            working = working.resize(
                (max(1, round(working.width * scale)), max(1, round(working.height * scale))),
                Image.Resampling.LANCZOS,
            )
        gray = ImageOps.grayscale(working)
        background = gray.filter(ImageFilter.GaussianBlur(radius=max(4.0, min(working.size) / 52)))
        local_contrast = ImageChops.difference(background, gray)
        mask = local_contrast.point(lambda value: 255 if value >= 14 else 0)
        return mask.filter(ImageFilter.MaxFilter(3))

    def _study_source_visual_line_bands(source: Image.Image) -> List[Tuple[int, int]]:
        """Segment physical ink rows; coordinates are normalized to the crop."""
        mask = _build_study_source_ink_mask(source)
        width, height = mask.size
        if width < 40 or height < 40:
            return []
        projection = list(mask.resize((1, height), Image.Resampling.BOX).getdata())
        smoothed = [
            sum(projection[max(0, index - 2) : min(height, index + 3)])
            / max(1, min(height, index + 3) - max(0, index - 2))
            for index in range(height)
        ]
        positive = sorted(value for value in smoothed if value > 0)
        background = positive[min(len(positive) - 1, round(len(positive) * 0.22))] if positive else 0
        threshold = max(3.0, min(12.0, background * 1.6))
        active_rows = [index for index, value in enumerate(smoothed) if value >= threshold]
        if not active_rows:
            return []
        maximum_gap = max(2, round(height * 0.004))
        raw_bands: List[Tuple[int, int]] = []
        band_start = active_rows[0]
        previous = active_rows[0]
        for current in active_rows[1:]:
            if current - previous > maximum_gap:
                raw_bands.append((band_start, previous + 1))
                band_start = current
            previous = current
        raw_bands.append((band_start, previous + 1))

        bands: List[Tuple[int, int]] = []
        for band_top, band_bottom in raw_bands:
            band_height = band_bottom - band_top
            if band_height < max(2, round(height * 0.004)):
                continue
            strip = mask.crop((0, band_top, width, band_bottom))
            column_projection = list(strip.resize((width, 1), Image.Resampling.BOX).getdata())
            active_columns = [index for index, value in enumerate(column_projection) if value >= 3]
            if not active_columns:
                continue
            ink_width_ratio = (active_columns[-1] - active_columns[0] + 1) / width
            normalized_height = band_height * 1000 / height
            if normalized_height <= 13 and ink_width_ratio >= 0.42:
                continue
            if normalized_height >= 115 and ink_width_ratio >= 0.62:
                continue
            bands.append(
                (
                    max(0, round((band_top - max(1, height * 0.002)) * 1000 / height)),
                    min(1000, round((band_bottom + max(1, height * 0.002)) * 1000 / height)),
                )
            )
        return bands

    def _study_source_band_sheet_data_urls(
        source: Image.Image,
        bands: List[Tuple[int, int]],
    ) -> List[str]:
        """Render physical ink rows with fixed IDs so OCR cannot swap their y positions."""
        if not bands:
            return []
        working = source.convert("RGB")
        content_width = min(1500, working.width)
        scale = content_width / max(1, working.width)
        label_width = 92
        try:
            label_font = ImageFont.truetype("DejaVuSans-Bold.ttf", 25)
        except OSError:
            label_font = ImageFont.load_default()
        sheet_urls: List[str] = []
        for group_start in range(0, len(bands), 8):
            rendered_rows: List[Image.Image] = []
            for band_index in range(group_start, min(len(bands), group_start + 8)):
                band_top, band_bottom = bands[band_index]
                pixel_top = max(0, math.floor(working.height * band_top / 1000))
                pixel_bottom = min(
                    working.height,
                    max(pixel_top + 1, math.ceil(working.height * band_bottom / 1000)),
                )
                vertical_padding = max(3, round((pixel_bottom - pixel_top) * 0.22))
                strip = working.crop(
                    (
                        0,
                        max(0, pixel_top - vertical_padding),
                        working.width,
                        min(working.height, pixel_bottom + vertical_padding),
                    )
                )
                if strip.width != content_width:
                    strip = strip.resize(
                        (content_width, max(1, round(strip.height * scale))),
                        Image.Resampling.LANCZOS,
                    )
                row_height = max(54, strip.height + 12)
                row = Image.new("RGB", (label_width + content_width, row_height), "white")
                row.paste(strip, (label_width, max(0, round((row_height - strip.height) / 2))))
                row_draw = ImageDraw.Draw(row)
                row_draw.rectangle(
                    (0, 0, row.width - 1, row.height - 1),
                    outline=(62, 92, 124),
                    width=2,
                )
                row_draw.text(
                    (12, max(4, round((row_height - 30) / 2))),
                    f"B{band_index + 1:02d}",
                    fill=(12, 72, 130),
                    font=label_font,
                )
                rendered_rows.append(row)
            sheet = Image.new(
                "RGB",
                (label_width + content_width, sum(row.height for row in rendered_rows)),
                "white",
            )
            offset_y = 0
            for row in rendered_rows:
                sheet.paste(row, (0, offset_y))
                offset_y += row.height
            sheet_urls.append(_study_image_data_url(sheet, max_side=2400))
        return sheet_urls

    def _align_study_source_lines_to_visual_bands(
        lines: List[Dict[str, Any]],
        bands: List[Tuple[int, int]],
    ) -> Dict[int, Tuple[int, int]]:
        """Monotonically align OCR line order to image-derived ink bands."""
        line_count = len(lines)
        band_count = len(bands)
        if not line_count or band_count < line_count or band_count > line_count * 3 + 8:
            return {}
        line_centers = [(line["top"] + line["bottom"]) / 2 for line in lines]
        band_centers = [(top + bottom) / 2 for top, bottom in bands]
        line_span = max(1.0, line_centers[-1] - line_centers[0])
        band_span = max(1.0, band_centers[-1] - band_centers[0])

        def assignment_cost(line_index: int, band_index: int) -> float:
            observed_cost = abs(line_centers[line_index] - band_centers[band_index])
            line_position = (
                (line_centers[line_index] - line_centers[0]) / line_span
                if line_count > 1
                else 0.5
            )
            band_position = (
                (band_centers[band_index] - band_centers[0]) / band_span
                if band_count > 1
                else 0.5
            )
            order_cost = abs(line_position - band_position) * 1000
            return observed_cost * 0.58 + order_cost * 0.42

        previous_costs = [float("inf")] * band_count
        backtrack: List[List[int]] = [[-1] * band_count for _ in range(line_count)]
        for band_index in range(0, band_count - line_count + 1):
            previous_costs[band_index] = assignment_cost(0, band_index)
        for line_index in range(1, line_count):
            current_costs = [float("inf")] * band_count
            best_previous_cost = float("inf")
            best_previous_index = -1
            minimum_band = line_index
            maximum_band = band_count - (line_count - line_index)
            for band_index in range(minimum_band, maximum_band + 1):
                candidate_previous_index = band_index - 1
                candidate_previous_cost = previous_costs[candidate_previous_index]
                if candidate_previous_cost < best_previous_cost:
                    best_previous_cost = candidate_previous_cost
                    best_previous_index = candidate_previous_index
                if best_previous_index >= 0:
                    current_costs[band_index] = best_previous_cost + assignment_cost(
                        line_index,
                        band_index,
                    )
                    backtrack[line_index][band_index] = best_previous_index
            previous_costs = current_costs
        final_band = min(range(band_count), key=lambda index: previous_costs[index])
        if not math.isfinite(previous_costs[final_band]):
            return {}
        assignments = [final_band]
        for line_index in range(line_count - 1, 0, -1):
            final_band = backtrack[line_index][final_band]
            if final_band < 0:
                return {}
            assignments.append(final_band)
        assignments.reverse()
        return {
            id(line): bands[band_index]
            for line, band_index in zip(lines, assignments)
        }

    def _study_source_page_content_top(source: Image.Image) -> int:
        """Find the first note row below a detected blue note-app toolbar."""
        working = source.convert("RGB")
        if working.width > 360:
            scale = 360 / working.width
            working = working.resize(
                (360, max(1, round(working.height * scale))),
                Image.Resampling.LANCZOS,
            )
        pixels = working.load()
        scan_bottom = max(1, round(working.height * 0.28))
        blue_rows: List[int] = []
        for y in range(scan_bottom):
            blue_pixels = 0
            for x in range(working.width):
                red, green, blue = pixels[x, y]
                if blue >= red + 24 and blue >= green + 12 and blue >= 70:
                    blue_pixels += 1
            if blue_pixels / working.width >= 0.18:
                blue_rows.append(y)
        if not blue_rows:
            return 0
        toolbar_bottom = max(blue_rows)
        toolbar_bottom_normalized = round(toolbar_bottom * 1000 / working.height)
        mask = _build_study_source_ink_mask(working)
        for band_top, band_bottom in _study_source_visual_line_bands(working):
            if band_top <= toolbar_bottom_normalized + 5:
                continue
            pixel_top = max(0, round(band_top * mask.height / 1000))
            pixel_bottom = min(mask.height, max(pixel_top + 1, round(band_bottom * mask.height / 1000)))
            strip = mask.crop((0, pixel_top, mask.width, pixel_bottom))
            column_projection = list(strip.resize((mask.width, 1), Image.Resampling.BOX).getdata())
            active_columns = [index for index, value in enumerate(column_projection) if value >= 3]
            if not active_columns:
                continue
            ink_width_ratio = (active_columns[-1] - active_columns[0] + 1) / mask.width
            if ink_width_ratio >= 0.22 and band_bottom - band_top >= 12:
                return max(toolbar_bottom_normalized, band_top - 8)
        return min(1000, toolbar_bottom_normalized + 35)

    def _refine_study_source_bbox_with_text_lines(
        mask: Image.Image,
        bbox: Dict[str, int],
        *,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        evidence_length: int,
    ) -> Tuple[Dict[str, int], bool]:
        width, height = mask.size
        if width < 40 or height < 40:
            return bbox, False

        def normalized_x(pixel: int) -> int:
            return max(0, min(1000, round(pixel * 1000 / width)))

        def normalized_y(pixel: int) -> int:
            return max(0, min(1000, round(pixel * 1000 / height)))

        def pixel_x(value: int) -> int:
            return max(0, min(width, round(width * value / 1000)))

        def pixel_y(value: int) -> int:
            return max(0, min(height, round(height * value / 1000)))

        def merged_ranges(indices: List[int], max_gap: int) -> List[Tuple[int, int]]:
            if not indices:
                return []
            ranges: List[Tuple[int, int]] = []
            range_start = indices[0]
            previous = indices[0]
            for current in indices[1:]:
                if current - previous > max_gap:
                    ranges.append((range_start, previous + 1))
                    range_start = current
                previous = current
            ranges.append((range_start, previous + 1))
            return ranges

        context_left = max(0, min(bbox["left"], start_x, end_x) - 70)
        context_right = min(1000, max(bbox["right"], start_x, end_x) + 70)
        context_top = max(0, min(bbox["top"], start_y, end_y) - 70)
        context_bottom = min(1000, max(bbox["bottom"], start_y, end_y) + 70)
        crop_left = pixel_x(context_left)
        crop_right = pixel_x(context_right)
        crop_top = pixel_y(context_top)
        crop_bottom = pixel_y(context_bottom)
        if crop_right - crop_left < 20 or crop_bottom - crop_top < 20:
            return bbox, False

        region = mask.crop((crop_left, crop_top, crop_right, crop_bottom))
        row_projection = list(region.resize((1, region.height), Image.Resampling.BOX).getdata())
        sorted_projection = sorted(row_projection)
        background_level = sorted_projection[min(len(sorted_projection) - 1, round(len(sorted_projection) * 0.55))]
        row_threshold = max(4, min(16, round(background_level * 1.45)))
        smoothed_rows = [
            sum(row_projection[max(0, index - 1) : min(len(row_projection), index + 2)])
            / max(1, min(len(row_projection), index + 2) - max(0, index - 1))
            for index in range(len(row_projection))
        ]
        active_rows = [index for index, value in enumerate(smoothed_rows) if value >= row_threshold]
        raw_bands = merged_ranges(active_rows, max(2, round(height * 0.0015)))
        maximum_rule_height = max(5, round(height * 0.0045))
        text_bands: List[Tuple[int, int]] = []
        rule_bands: List[Tuple[int, int]] = []
        for band_top, band_bottom in raw_bands:
            band_values = row_projection[band_top:band_bottom]
            band_height = band_bottom - band_top
            is_horizontal_rule = (
                band_height <= maximum_rule_height
                and band_values
                and max(band_values) >= 42
                and sum(band_values) / len(band_values) >= 18
            )
            if is_horizontal_rule:
                rule_bands.append((band_top, band_bottom))
            elif band_height >= 3:
                text_bands.append((band_top, band_bottom))
        if not text_bands:
            return bbox, False

        local_start_y = pixel_y(start_y) - crop_top
        local_end_y = pixel_y(end_y) - crop_top

        def band_distance(band: Tuple[int, int], anchor: int) -> int:
            if band[0] <= anchor <= band[1]:
                return 0
            return min(abs(anchor - band[0]), abs(anchor - band[1]))

        start_index = min(range(len(text_bands)), key=lambda index: band_distance(text_bands[index], local_start_y))
        end_index = min(range(len(text_bands)), key=lambda index: band_distance(text_bands[index], local_end_y))
        anchor_limit = max(24, round(height * 0.045))
        if (
            band_distance(text_bands[start_index], local_start_y) > anchor_limit
            or band_distance(text_bands[end_index], local_end_y) > anchor_limit
        ):
            return bbox, False
        if start_index > end_index:
            start_index, end_index = end_index, start_index

        selected_top = text_bands[start_index][0]
        selected_bottom = text_bands[end_index][1]
        selected_height = selected_bottom - selected_top
        if selected_height < 4:
            return bbox, False

        selected = region.crop((0, selected_top, region.width, selected_bottom))
        selected_draw = ImageDraw.Draw(selected)
        for rule_top, rule_bottom in rule_bands:
            if rule_bottom <= selected_top or rule_top >= selected_bottom:
                continue
            selected_draw.rectangle(
                (0, max(0, rule_top - selected_top), selected.width, min(selected.height, rule_bottom - selected_top)),
                fill=0,
            )
        column_projection = list(selected.resize((selected.width, 1), Image.Resampling.BOX).getdata())
        active_columns = [index for index, value in enumerate(column_projection) if value >= 2]
        if not active_columns:
            return bbox, False

        total_column_ink = sum(column_projection[index] for index in active_columns)

        def weighted_column(quantile: float) -> int:
            target = total_column_ink * quantile
            running = 0
            for index in active_columns:
                running += column_projection[index]
                if running >= target:
                    return index
            return active_columns[-1]

        ink_left = weighted_column(0.003)
        ink_right = weighted_column(0.997) + 1
        pad_x = max(4, round(width * 0.006))
        pad_y = max(4, round(height * 0.004))
        refined = {
            **bbox,
            "left": normalized_x(max(0, crop_left + ink_left - pad_x)),
            "top": normalized_y(max(0, crop_top + selected_top - pad_y)),
            "right": normalized_x(min(width, crop_left + ink_right + pad_x)),
            "bottom": normalized_y(min(height, crop_top + selected_bottom + pad_y)),
        }
        refined["left"] = max(0, min(refined["left"], start_x - 8, end_x - 8))
        refined["top"] = max(0, min(refined["top"], start_y - 8, end_y - 8))
        refined["right"] = min(1000, max(refined["right"], start_x + 8, end_x + 8))
        refined["bottom"] = min(1000, max(refined["bottom"], start_y + 8, end_y + 8))
        refined_width = refined["right"] - refined["left"]
        refined_height = refined["bottom"] - refined["top"]
        if evidence_length > 120 and refined_width < min(180, (bbox["right"] - bbox["left"]) * 0.35):
            return bbox, False
        if not (12 <= refined_width <= 960 and 8 <= refined_height <= 850):
            return bbox, False
        return refined, True

    def _snap_study_source_bbox_to_ink(
        image_bytes: bytes,
        bbox: Dict[str, int],
    ) -> Dict[str, int]:
        try:
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                source = ImageOps.exif_transpose(source_image).convert("RGB")
            image_width, image_height = source.size
            margin_x = 24
            margin_y = 28
            crop_left = max(0, round(image_width * (bbox["left"] - margin_x) / 1000))
            crop_top = max(0, round(image_height * (bbox["top"] - margin_y) / 1000))
            crop_right = min(image_width, round(image_width * (bbox["right"] + margin_x) / 1000))
            crop_bottom = min(image_height, round(image_height * (bbox["bottom"] + margin_y) / 1000))
            if crop_right - crop_left < 24 or crop_bottom - crop_top < 16:
                return bbox
            crop = source.crop((crop_left, crop_top, crop_right, crop_bottom))
            max_side = max(crop.size)
            if max_side > 1200:
                scale = 1200 / max_side
                crop = crop.resize(
                    (max(1, round(crop.width * scale)), max(1, round(crop.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            gray = ImageOps.grayscale(crop)
            blur_radius = max(3.0, min(crop.size) / 55)
            local_background = gray.filter(ImageFilter.GaussianBlur(radius=blur_radius))
            contrast = ImageChops.difference(local_background, gray)
            mask = contrast.point(lambda value: 255 if value >= 16 else 0)
            mask = mask.filter(ImageFilter.MaxFilter(3))

            local_left = round((bbox["left"] / 1000 * image_width - crop_left) * crop.width / max(1, crop_right - crop_left))
            local_top = round((bbox["top"] / 1000 * image_height - crop_top) * crop.height / max(1, crop_bottom - crop_top))
            local_right = round((bbox["right"] / 1000 * image_width - crop_left) * crop.width / max(1, crop_right - crop_left))
            local_bottom = round((bbox["bottom"] / 1000 * image_height - crop_top) * crop.height / max(1, crop_bottom - crop_top))
            row_projection = list(mask.resize((1, mask.height), Image.Resampling.BOX).getdata())
            row_threshold = 3
            row_padding = max(3, round(mask.height * 0.012))
            row_candidates = [
                index
                for index, value in enumerate(row_projection)
                if value >= row_threshold and local_top - row_padding <= index <= local_bottom + row_padding
            ]
            if not row_candidates:
                return bbox
            ink_top = max(0, min(row_candidates) - 3)
            ink_bottom = min(mask.height, max(row_candidates) + 4)
            line_mask = mask.crop((0, ink_top, mask.width, max(ink_top + 1, ink_bottom)))
            column_projection = list(line_mask.resize((line_mask.width, 1), Image.Resampling.BOX).getdata())
            column_padding = max(4, round(mask.width * 0.012))
            column_candidates = [
                index
                for index, value in enumerate(column_projection)
                if value >= 3 and local_left - column_padding <= index <= local_right + column_padding
            ]
            if not column_candidates:
                return bbox
            ink_left = max(0, min(column_candidates) - 4)
            ink_right = min(mask.width, max(column_candidates) + 5)

            def x_to_normalized(value: int) -> int:
                pixel = crop_left + value / max(1, crop.width) * (crop_right - crop_left)
                return round(pixel * 1000 / image_width)

            def y_to_normalized(value: int) -> int:
                pixel = crop_top + value / max(1, crop.height) * (crop_bottom - crop_top)
                return round(pixel * 1000 / image_height)

            snapped = {
                **bbox,
                "left": max(bbox["left"] - 20, min(bbox["left"] + 20, x_to_normalized(ink_left))),
                "top": max(bbox["top"] - 20, min(bbox["top"] + 20, y_to_normalized(ink_top))),
                "right": max(bbox["right"] - 20, min(bbox["right"] + 20, x_to_normalized(ink_right))),
                "bottom": max(bbox["bottom"] - 20, min(bbox["bottom"] + 20, y_to_normalized(ink_bottom))),
            }
            if snapped["right"] - snapped["left"] < 12 or snapped["bottom"] - snapped["top"] < 8:
                return bbox
            return snapped
        except (OSError, ValueError, TypeError):
            return bbox

    def _expand_study_source_bbox_through_edge_ink(
        image_bytes: bytes,
        bbox: Dict[str, int],
        *,
        end_x: int,
        end_y: int,
        evidence_length: int,
    ) -> Dict[str, int]:
        try:
            with Image.open(io.BytesIO(image_bytes)) as source_image:
                source = ImageOps.exif_transpose(source_image).convert("RGB")
            max_side = max(source.size)
            if max_side > 1400:
                scale = 1400 / max_side
                source = source.resize(
                    (max(1, round(source.width * scale)), max(1, round(source.height * scale))),
                    Image.Resampling.LANCZOS,
                )
            gray = ImageOps.grayscale(source)
            background = gray.filter(ImageFilter.GaussianBlur(radius=max(3.0, min(source.size) / 55)))
            contrast = ImageChops.difference(background, gray)
            mask = contrast.point(lambda value: 255 if value >= 16 else 0).filter(ImageFilter.MaxFilter(3))

            left = round(mask.width * bbox["left"] / 1000)
            top = round(mask.height * bbox["top"] / 1000)
            right = round(mask.width * bbox["right"] / 1000)
            bottom = round(mask.height * bbox["bottom"] / 1000)

            def extend_projection(
                projection: List[int],
                edge: int,
                limit: int,
                trigger_gap: int,
                stop_gap: int,
            ) -> int:
                active = [
                    index
                    for index, value in enumerate(projection)
                    if value >= 3 and edge - trigger_gap <= index <= limit
                ]
                if not active:
                    return edge
                first_after_edge = next((index for index in active if index >= edge), None)
                if first_after_edge is None or first_after_edge - edge > trigger_gap:
                    return edge
                extended = first_after_edge
                previous = first_after_edge
                for index in active:
                    if index < first_after_edge:
                        continue
                    if index - previous > stop_gap:
                        break
                    extended = index
                    previous = index
                return max(edge, extended + 5)

            expanded = dict(bbox)
            if (evidence_length > 180 or bbox["right"] - end_x <= 50) and right < mask.width:
                vertical_top = max(0, top - round(mask.height * 0.01))
                vertical_bottom = min(mask.height, bottom + round(mask.height * 0.01))
                right_projection = list(
                    mask.crop((0, vertical_top, mask.width, max(vertical_top + 1, vertical_bottom)))
                    .resize((mask.width, 1), Image.Resampling.BOX)
                    .getdata()
                )
                right_limit = min(
                    mask.width - 1,
                    right + round(mask.width * (0.35 if evidence_length > 160 else 0.30)),
                )
                extended_right = extend_projection(
                    right_projection,
                    right,
                    right_limit,
                    max(12, round(mask.width * 0.02)),
                    max(16, round(mask.width * (0.045 if evidence_length > 160 else 0.018))),
                )
                expanded["right"] = min(1000, round(extended_right * 1000 / mask.width))
            if bbox["bottom"] - end_y <= 50 and bottom < mask.height:
                horizontal_left = max(0, left - round(mask.width * 0.01))
                horizontal_right = min(mask.width, right + round(mask.width * 0.01))
                bottom_projection = list(
                    mask.crop((horizontal_left, 0, max(horizontal_left + 1, horizontal_right), mask.height))
                    .resize((1, mask.height), Image.Resampling.BOX)
                    .getdata()
                )
                bottom_limit = min(
                    mask.height - 1,
                    bottom + round(mask.height * (0.08 if evidence_length > 160 else 0.12)),
                )
                extended_bottom = extend_projection(
                    bottom_projection,
                    bottom,
                    bottom_limit,
                    max(12, round(mask.height * 0.012)),
                    max(10, round(mask.height * 0.009)),
                )
                expanded["bottom"] = min(1000, round(extended_bottom * 1000 / mask.height))
            if _validated_study_source_bbox(expanded) is None:
                return bbox
            return expanded
        except (OSError, ValueError, TypeError):
            return bbox

    return (
        _study_coordinate_guide_data_url,
        _study_image_data_url,
        _canonical_study_source_match_text,
        _literal_study_source_evidence,
        _match_study_source_evidence_to_lines,
        _build_study_source_ink_mask,
        _study_source_visual_line_bands,
        _study_source_band_sheet_data_urls,
        _align_study_source_lines_to_visual_bands,
        _study_source_page_content_top,
        _refine_study_source_bbox_with_text_lines,
        _snap_study_source_bbox_to_ink,
        _expand_study_source_bbox_through_edge_ink,
    )
