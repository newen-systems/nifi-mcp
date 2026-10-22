"""Deterministic viewport packing of indivisible, already laid-out flow blocks.

Only translations are returned. The caller owns topology, card/route bounds and
port alignment; include all routing clearance in each supplied bounding box.
Numeric ``order`` priorities reserve earlier rows for lower values, with missing
priorities defaulting to zero. IDs break geometric ties, never infer semantics.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from itertools import pairwise

Size = tuple[float, float]
Point = tuple[float, float]
Row = list[str]
_GRID = 8.0


def _snap(value: float) -> float:
    return math.ceil(value / _GRID) * _GRID


def _inputs(
    blocks: Mapping[str, Size], target_ratio: float, gap: float, order: Mapping[str, float], extra_height: float,
) -> dict[str, Size]:
    if not math.isfinite(extra_height) or extra_height < 0:
        raise ValueError("extra_height must be finite and nonnegative")
    if not math.isfinite(target_ratio) or target_ratio <= 0:
        raise ValueError("target_ratio must be finite and positive")
    if not math.isfinite(gap) or gap < 32:
        raise ValueError("gap must be finite and at least 32")
    if order.keys() - blocks.keys():
        raise ValueError("order contains an unknown block ID")
    if any(not math.isfinite(value) for value in order.values()):
        raise ValueError("order priorities must be finite")
    sizes = {}
    for cid, (width, height) in blocks.items():
        if not all(math.isfinite(v) and v > 0 for v in (width, height)):
            raise ValueError(f"block {cid!r} dimensions must be finite and positive")
        sizes[cid] = (_snap(width), _snap(height))
    return sizes


def _rows_by_width(ids: list[str], sizes: dict[str, Size], width: float, gap: float) -> list[Row]:
    rows: list[Row] = []
    used: list[float] = []
    for cid in ids:
        # Best fitting existing shelf reduces empty space beside tall clusters.
        fits = [i for i, value in enumerate(used) if value + gap + sizes[cid][0] <= width]
        if fits:
            index = max(fits, key=lambda i: (used[i], -i))
            rows[index].append(cid)
            used[index] += gap + sizes[cid][0]
        else:
            rows.append([cid])
            used.append(sizes[cid][0])
    return rows


def _place(rows: list[Row], sizes: dict[str, Size], gap: float) -> tuple[dict[str, Point], float, float]:
    placed = {}
    y, width = 0.0, 0.0
    for row in rows:
        x = 0.0
        for cid in row:
            placed[cid] = (x, y)
            x += sizes[cid][0] + gap
        width = max(width, x - gap)
        y += max(sizes[cid][1] for cid in row) + gap
    return placed, width, y - gap


def _skyline(bands: list[list[str]], sizes: dict[str, Size], width: float, gap: float) -> dict[str, Point]:
    """Fill space alongside tall clusters without splitting their rectangles."""
    placed: dict[str, Point] = {}
    floor = 0.0
    for band in bands:
        shelves: list[tuple[float, float, float]] = []
        for cid in band:
            w, h = sizes[cid]
            xs = {0.0, *(right + gap for _, right, _ in shelves)}
            choices = []
            for x in sorted(xs):
                if x + w > width:
                    continue
                y = max((bottom + gap for left, right, bottom in shelves
                         if x < right + gap and x + w + gap > left), default=floor)
                choices.append((y, x))
            y, x = min(choices)
            placed[cid] = (x, y)
            shelves.append((x, x + w, y + h))
        floor = max(bottom for _, _, bottom in shelves) + gap
    return placed


def _measure(placed: dict[str, Point], sizes: dict[str, Size]) -> tuple[float, float, list[Row]]:
    rows: dict[float, Row] = {}
    for cid, (_, y) in placed.items():
        rows.setdefault(y, []).append(cid)
    return (max(x + sizes[cid][0] for cid, (x, _) in placed.items()),
            max(y + sizes[cid][1] for cid, (_, y) in placed.items()),
            [sorted(row) for _, row in sorted(rows.items())])


def _score(width: float, height: float, area: float, rows: list[Row], target_ratio: float) -> tuple:
    # Minimise the viewport height needed to display every block at one scale.
    # Normalisation makes the first value independent of absolute diagram size.
    # Compactness breaks equal-scale ties; no row/column-count preference applies.
    fitted_height = max(width / target_ratio, height)
    scale = fitted_height / math.sqrt(area / target_ratio)
    aspect = abs(math.log((width / height) / target_ratio))
    return (scale, width * height / area, aspect, tuple(-len(row) for row in rows),
            height, width, tuple(tuple(row) for row in rows))


def _swap_bottom(
    placed: dict[str, Point], sizes: dict[str, Size], bottom: set[str],
    priorities: Mapping[str, float], height: float,
) -> dict[str, Point]:
    """Exchange equal footprints to keep full first rows without reflection."""
    swapped = dict(placed)
    for cid in sorted(bottom):
        if swapped[cid][1] + sizes[cid][1] == height:
            continue
        targets = [other for other in swapped if other not in bottom
                   and sizes[other] == sizes[cid]
                   and priorities.get(other, 0) == priorities.get(cid, 0)
                   and swapped[other][1] + sizes[other][1] == height]
        if not targets:
            return placed
        target = min(targets, key=lambda other: (swapped[other][0], other))
        swapped[cid], swapped[target] = swapped[target], swapped[cid]
    return swapped


def _anchored(
    placed: dict[str, Point], sizes: dict[str, Size], bottom: set[str],
    priorities: Mapping[str, float], gap: float,
) -> list[dict[str, Point]]:
    if not bottom:
        return [placed]
    _, height, _ = _measure(placed, sizes)
    swapped = _swap_bottom(placed, sizes, bottom, priorities, height)
    mirrored = {cid: (x, height - y - sizes[cid][1]) for cid, (x, y) in placed.items()}
    # Always have a feasible fallback: a bottom-aligned final shelf. Try compact
    # candidates first; their unrelated blocks may share the boundary shelf.
    fallback = {cid: pos for cid, pos in placed.items() if cid not in bottom}
    floor = max((y + sizes[cid][1] + gap for cid, (_, y) in fallback.items()), default=0.0)
    base = floor + max(sizes[cid][1] for cid in bottom)
    x = 0.0
    for cid in sorted(bottom):
        fallback[cid] = (x, base - sizes[cid][1])
        x += sizes[cid][0] + gap
    return [candidate for candidate in (placed, swapped, mirrored, fallback)
            if _boundary_valid(candidate, sizes, bottom, priorities, gap)]


def _boundary_valid(
    placed: dict[str, Point], sizes: dict[str, Size], bottom: set[str],
    priorities: Mapping[str, float], gap: float,
) -> bool:
    height = max(y + sizes[cid][1] for cid, (_, y) in placed.items())
    if any(placed[cid][1] + sizes[cid][1] != height for cid in bottom):
        return False
    # A mirror may reverse explicit priority bands; those candidates are invalid.
    spans: dict[float, list[float]] = {}
    for cid, (_, y) in placed.items():
        rank = priorities.get(cid, 0)
        low, high = spans.get(rank, [y, y + sizes[cid][1]])
        spans[rank] = [min(low, y), max(high, y + sizes[cid][1])]
    ordered = [span for _, span in sorted(spans.items())]
    return all(a[1] + gap <= b[0] for a, b in pairwise(ordered))


def _bottom_anchors(bottom, sizes, priorities) -> set[str]:
    anchors = set(bottom or ())
    if anchors - sizes.keys():
        raise ValueError("bottom contains an unknown block ID")
    if anchors and any(priorities.get(cid, 0) != max(priorities.get(c, 0) for c in sizes) for cid in anchors):
        raise ValueError("bottom blocks must belong to the last priority band")
    return anchors


def pack_blocks(
    blocks: Mapping[str, Size], *, target_ratio: float = 16 / 9, gap: float = 64,
    order: Mapping[str, float] | None = None, bottom: set[str] | None = None, extra_height: float = 0,
) -> dict[str, Point]:
    """Pack whole blocks into compact shelves, returning 8px-grid offsets.

    Bounds are rounded upward; gap is rounded upward and must be at least 32px.
    Lower numeric ``order`` priorities occupy strictly earlier shelves. Supply
    e.g. -1 for input clusters, 0 for ordinary blocks, +1 for output clusters.
    Priorities are hard bands; aspect is a preference, not a geometry guarantee.
    ``bottom`` instead anchors nominated blocks at the overall bottom without
    reserving a separate band. Use this for output-only clusters alongside
    isolated blocks. Equal-size swaps and vertically mirrored offsets are compared;
    internal block coordinates are never reflected. A final shelf is a fallback.
    Anchors must share the last priority band if ``order`` is also supplied.
    Boundary equality uses rounded bounding heights, preserving 8px offsets.
    ``extra_height`` reserves finite, nonnegative global boundary height in the
    viewport score only. It adds no offset, block, gap or padding to the result;
    the caller places global ports afterward. Score normalisation uses body area.
    Unknown IDs and nonfinite/nonpositive dimensions are rejected.
    Input mapping iteration order and original coordinates have no influence.

    Candidate column counts, width-limited shelves and skyline fills minimise
    required viewport scale, then occupied bounding area and aspect error. Ties
    prefer full earlier rows before stable ID ordering. No
    rotation, block splitting or extra whitespace is used to force the ratio.
    """
    priorities = order or {}
    sizes = _inputs(blocks, target_ratio, gap, priorities, extra_height)
    anchors = _bottom_anchors(bottom, sizes, priorities)
    if not sizes:
        return {}
    gap = _snap(gap)
    bands = [sorted((cid for cid in sizes if priorities.get(cid, 0) == priority),
                    key=lambda cid: (-sizes[cid][1], -sizes[cid][0], cid))
             for priority in sorted({priorities.get(cid, 0) for cid in sizes})]
    area = sum(w * h for w, h in sizes.values())
    candidates: list[list[Row]] = []
    for columns in range(1, max(map(len, bands)) + 1):
        candidates.append([band[i:i + columns] for band in bands for i in range(0, len(band), columns)])
    minimum = max(w for w, _ in sizes.values())
    maximum = sum(w for w, _ in sizes.values()) + gap * (len(sizes) - 1)
    ideal = math.sqrt(area * target_ratio)
    widths = {minimum, maximum, *(_snap(ideal * factor / 8) for factor in range(4, 25))}
    for width in sorted(widths):
        if minimum <= width <= maximum:
            candidates.append([row for band in bands for row in _rows_by_width(band, sizes, width, gap)])
    placements = [_place(rows, sizes, gap)[0] for rows in candidates]
    placements.extend(_skyline(bands, sizes, width, gap) for width in sorted(widths) if minimum <= width <= maximum)
    best, best_score = {}, None
    for placement in placements:
        for placed in _anchored(placement, sizes, anchors, priorities, gap):
            width, height, rows = _measure(placed, sizes)
            score = _score(width, height + extra_height, area, rows, target_ratio)
            if best_score is None or score < best_score:
                best, best_score = placed, score
    return {cid: best[cid] for cid in sorted(best)}
