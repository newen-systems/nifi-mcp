"""Canvas layout: top-down, sideways only for forks, every spacing derived from NiFi's real sizes.

Sizes (cited below): processor 352x128, port 240x48, group 384x176, funnel 48x48, connection
label 240x79 at most, canvas snap 8. M = LABEL_MARGIN = 16. grid(v) rounds v up to a multiple of 8.

Gap G between rows      = grid(79 + 2M) = 112, the same under every card type.
Row pitch of a row      = tallest real card in the row + G; siblings in a row are top-aligned.
                          y(next row) = y(row) + height(row) + G: processor row 240, port-only
                          row 160, group row 288.
Sibling pitch of a card = grid(max(card width + 2M, 2 * (240 + M)))           = 512 for every kind.
    Every card in a row shares the tallest card's gap, so the label of each parent-to-child edge
    sits on the gap's centre line at the edge's midpoint. Two siblings s apart put their labels s/2
    apart, so s/2 >= label width + M; the row pitch drops out because it is the same for both.
Side pitch parent->child = grid(parent width / 2 + 240 + 2S + child width / 2) = 672 for processors,
    S = SIDE_LABEL_MARGIN = 40. A child on its parent's row is joined by a horizontal line with the
    label on it; the 320px gap between the two cards holds the label and room for the arrowhead.
Placement: a chain stays on one axis. At a fork the main child (deepest subtree, then the one
feeding the fork's join, then relationship order) continues one row down on the parent's axis;
the others keep relationship order left to right through it. The nearest child on each side sits
on the parent's row, perpendicular, at least a side pitch out; any further child on that side drops
one row, in its own column past the inner child's family, and is reached by a routed line.
A fork whose children are all leaves has no main child: they sit one row down, one sibling pitch
apart, centred on the parent. Every family reserves a span per row (a card reserves its sibling
pitch), and a side child moves out until its whole family clears what is reserved on every row.
A join (two or more inputs) goes back to the axis of the fork where its branches split, one row
below everything in that family. Each card is centred on its axis by its own real width.
A connection whose straight line or label would still cross a card is routed (route_connections).
Parts of a canvas that no connection joins are laid out one by one as blocks and packed in name order
into the near-square grid, row by row, 32px apart: no label sits between them.
"""

from __future__ import annotations

import itertools
import math
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Any

# Real NiFi 2.x card sizes, from nifi-frontend/src/main/frontend/apps/nifi/src/app/pages/flow-designer/
# service/manager/: process-group-manager.service.ts:53-56 (384x176), processor-manager.service.ts:52-55
# (352x128), port-manager.service.ts:53-56 (240x48).
PG_WIDTH = 384.0
PG_HEIGHT = 176.0
PROCESSOR_SIZE = (352.0, 128.0)
PORT_SIZE = (240.0, 48.0)
# A remote-access port is drawn 240x80 (port-manager.service.ts:57-60); occupancy uses that.
REMOTE_PORT_SIZE = (240.0, 80.0)
# Connection label, centred on the midpoint of the line between the two card edges
# (connection-manager.service.ts:476-484, canvas-utils.service.ts:754-769). 240 wide
# (connection-manager.service.ts:71-74). 19px per row (:1285) for up to four rows, From, To, Name and
# Queued (:1295, :1409, :1523, :1609), plus 3px for back pressure (:86, :1808). So 79 tall at most.
CONNECTION_LABEL_WIDTH = 240.0
CONNECTION_LABEL_HEIGHT = 4 * 19.0 + 3.0
LABEL_MARGIN = 16.0
# Either side of the label on a horizontal fork line, so the arrowhead has room before the card.
SIDE_LABEL_MARGIN = 40.0
# NiFi snaps a dragged card to 8px (nifi-frontend apps/nifi/src/app/ui/common/canvas/canvas.constants.ts:61).
CANVAS_GRID = 8.0


def _grid(value: float) -> float:
    return math.ceil(value / CANVAS_GRID) * CANVAS_GRID


LABEL_GAP = _grid(CONNECTION_LABEL_HEIGHT + 2 * LABEL_MARGIN)  # 112 between any two stacked cards


def row_pitch(card_height: float) -> float:
    """Card, then the one gap that holds the tallest label with LABEL_MARGIN above and below."""
    return card_height + LABEL_GAP


def sibling_pitch(width: float) -> float:
    """Axis spacing of two fork siblings: clear cards, and labels on one gap centre line clear of each other."""
    return _grid(max(width + 2 * LABEL_MARGIN, 2 * (CONNECTION_LABEL_WIDTH + LABEL_MARGIN)))


# Processor cell: occupancy box roomier than the real card, so a card never sits flush with another.
CARD_WIDTH = 420.0
CARD_HEIGHT = 200.0
COL_PITCH = sibling_pitch(PG_WIDTH)  # 512, the same for every kind
COL_GAP = COL_PITCH - CARD_WIDTH  # 92
ROW_PITCH = row_pitch(PROCESSOR_SIZE[1])  # 240; ports share the processor rows
ROW_GAP = ROW_PITCH - CARD_HEIGHT  # 40 below the occupancy box, 112 below a real processor
# Child groups stack top-down, one per row, in flow order, by the same pitch rule.
PG_GAP = 40.0
PG_COL_PITCH = PG_WIDTH + PG_GAP  # 424
PG_ROW_PITCH = row_pitch(PG_HEIGHT)  # 288
PG_ROW_GAP = PG_ROW_PITCH - PG_HEIGHT  # 112
PG_WRAP = 1
# Every card centres on the axis of its column: a real processor card at x=0 is centred on x=176.
AXIS_CENTRE = PROCESSOR_SIZE[0] / 2
PG_AXIS_X = AXIS_CENTRE - PG_WIDTH / 2  # -16
# Cards NiFi draws that this server never moves (nifi-frontend funnel-manager.service.ts,
# remote-process-group-manager.service.ts, label-manager.service.ts INITIAL_WIDTH/HEIGHT).
FUNNEL_SIZE = (48.0, 48.0)
REMOTE_GROUP_SIZE = (384.0, 176.0)
LABEL_DEFAULT_SIZE = (148.0, 148.0)
PROCESSOR_WRAP = 4

_PLACEABLE = frozenset({"processor", "input_port", "output_port"})
Point = tuple[float, float]
Bend = dict[str, float]
Cell = tuple[int, int]
# x, y, width, height. Footprints include the gap on the right and bottom edge, so two
# footprints that do not overlap leave at least the smaller gap between the real cards.
Box = tuple[float, float, float, float]


def boxes_overlap(
    a: Point,
    b: Point,
    *,
    width: float = CARD_WIDTH,
    height: float = CARD_HEIGHT,
) -> bool:
    ax, ay = a
    bx, by = b
    return ax < bx + width and bx < ax + width and ay < by + height and by < ay + height


def rects_overlap(a: Box, b: Box) -> bool:
    ax, ay, aw, ah = a
    bx, by, bw, bh = b
    return ax < bx + bw and bx < ax + aw and ay < by + bh and by < ay + ah


def processor_footprint(point: Point) -> Box:
    """Processor or port card plus its row/column gap."""
    return (point[0], point[1], COL_PITCH, ROW_PITCH)


def group_footprint(point: Point) -> Box:
    """Process group card plus its lattice gap."""
    return (point[0], point[1], PG_COL_PITCH, PG_ROW_PITCH)


def _fixed_box(item: dict[str, Any], size: tuple[float, float]) -> Box:
    pos = item.get("position") or {}
    width = float(item.get("width") or size[0])
    height = float(item.get("height") or size[1])
    return (float(pos.get("x") or 0), float(pos.get("y") or 0), width + COL_GAP, height + ROW_GAP)


def fixed_footprints(flow: dict[str, Any]) -> dict[str, Box]:
    """Footprints of the funnels, remote process groups and labels in a compact outline, keyed by id.

    Placement and relayout never move these, so every other card must clear them.
    """
    boxes: dict[str, Box] = {}
    for key, size in (
        ("funnels", FUNNEL_SIZE),
        ("remote_process_groups", REMOTE_GROUP_SIZE),
        ("labels", LABEL_DEFAULT_SIZE),
    ):
        for index, item in enumerate(flow.get(key) or []):
            if isinstance(item, dict):
                boxes[str(item.get("id") or f"{key}[{index}]")] = _fixed_box(item, size)
    return boxes


def _edge_point(box: Box, toward: Point) -> Point:
    """Where the line from a card's centre to `toward` leaves the card, as NiFi draws a connection end."""
    x, y, width, height = box
    cx, cy = x + width / 2, y + height / 2
    dx, dy = toward[0] - cx, toward[1] - cy
    scales = [abs(width / 2 / dx) if dx else math.inf, abs(height / 2 / dy) if dy else math.inf]
    scale = min(1.0, *scales)
    return cx + dx * scale, cy + dy * scale


STROKE_MARGIN = 8.0


def _label_box(point: Point) -> Box:
    return (
        point[0] - CONNECTION_LABEL_WIDTH / 2,
        point[1] - CONNECTION_LABEL_HEIGHT / 2,
        CONNECTION_LABEL_WIDTH,
        CONNECTION_LABEL_HEIGHT,
    )


def _grow(box: Box, margin: float) -> Box:
    x, y, width, height = box
    return (x - margin, y - margin, width + 2 * margin, height + 2 * margin)


def _segment_hits(p: Point, q: Point, box: Box) -> bool:
    """Whether the segment p-q passes through the box (Liang-Barsky clip)."""
    x, y, width, height = box
    dx, dy = q[0] - p[0], q[1] - p[1]
    t0, t1 = 0.0, 1.0
    for pk, qk in ((-dx, p[0] - x), (dx, x + width - p[0]), (-dy, p[1] - y), (dy, y + height - p[1])):
        if pk == 0:
            if qk < 0:
                return False
            continue
        t = qk / pk
        if pk < 0:
            t0 = max(t0, t)
        else:
            t1 = min(t1, t)
        if t0 >= t1:
            return False
    return True


def _centre(box: Box) -> Point:
    return box[0] + box[2] / 2, box[1] + box[3] / 2


def _path(boxes: dict[str, Box], src: str, dst: str, bends: list[Bend]) -> list[Point]:
    """The polyline NiFi draws: source edge, each bend, destination edge."""
    points = [(bend["x"], bend["y"]) for bend in bends]
    first = points[0] if points else _centre(boxes[dst])
    last = points[-1] if points else _centre(boxes[src])
    return [_edge_point(boxes[src], first), *points, _edge_point(boxes[dst], last)]


def _path_label(path: list[Point], bends: list[Bend], label_index: int | None) -> Box:
    if bends:
        bend = bends[min(label_index or 0, len(bends) - 1)]
        return _label_box((bend["x"], bend["y"]))
    return _label_box(((path[0][0] + path[-1][0]) / 2, (path[0][1] + path[-1][1]) / 2))


def _path_hits_cards(path: list[Point], boxes: dict[str, Box], ends: tuple[str, str]) -> str | None:
    for key, box in boxes.items():
        if key in ends:
            continue
        grown = _grow(box, STROKE_MARGIN)
        if any(_segment_hits(p, q, grown) for p, q in itertools.pairwise(path)):
            return key
    return None


def _collinear_overlap(first: tuple[Point, Point], second: tuple[Point, Point]) -> bool:
    """Two segments on one line that share a stretch of it, in either direction."""
    (px, py), (qx, qy) = first
    dx, dy = qx - px, qy - py
    length = dx * dx + dy * dy
    if not length:
        return False
    ends = []
    for x, y in second:
        if abs(dx * (y - py) - dy * (x - px)) > 1e-6:
            return False
        ends.append(((x - px) * dx + (y - py) * dy) / length)
    lo, hi = sorted(ends)
    return min(1.0, hi) - max(0.0, lo) > 1e-6


LANE_STEP = 24.0
OUTER_LANES = 8


def _lanes(
    obstacles: list[Box], top: float, bottom: float, near: tuple[float, float], right_first: bool
) -> list[float]:
    """Free vertical x values between top and bottom, cheapest first: inside each gap between the
    obstacles' columns (its middle, then LANE_STEP either way), and past the outermost. Cards and
    labels are obstacles, the two end cards included, so a lane never runs through them.
    right_first puts lanes right of both ends first."""
    blocked = sorted(
        (x - LABEL_MARGIN, x + width + LABEL_MARGIN)
        for x, y, width, height in obstacles
        if y < bottom and y + height > top
    )
    if not blocked:
        return [near[0]]
    left, right = blocked[0][0], max(hi for _lo, hi in blocked)
    candidates = [left - LABEL_MARGIN - k * LANE_STEP for k in range(OUTER_LANES)]
    candidates += [right + LABEL_MARGIN + k * LANE_STEP for k in range(OUTER_LANES)]
    reach = blocked[0][1]
    for lo, hi in blocked[1:]:
        if lo > reach:
            middle = (reach + lo) / 2
            steps = int((lo - reach) / 2 // LANE_STEP)
            inside = (middle + k * LANE_STEP for k in range(-steps, steps + 1))
            candidates += [x for x in inside if reach < x < lo]
        reach = max(reach, hi)
    src_x, dst_x = near
    beyond = max(src_x, dst_x)
    return sorted(
        candidates,
        key=lambda c: (right_first and c < beyond, abs(c - src_x) + abs(c - dst_x), abs(c - src_x)),
    )


def _gap_lines(boxes: dict[str, Box], top: float, bottom: float) -> list[float]:
    """Centre lines of the row gaps strictly between top and bottom, where a label clears every row."""
    lines = {y - LABEL_GAP / 2 for _x, y, _w, _h in boxes.values()}
    return sorted(line for line in lines if top < line < bottom)


# Where a routed line may leave or enter a card's side: its centre line first, then steps off it,
# so routed lines into one side never share a line.
SIDE_OFFSETS = (0.0, -24.0, 24.0, -48.0, 48.0)
# A routed line that leaves through a card's top or bottom aims at a point this far off the card's
# centre in the row gap, so its first stretch clears the label of a straight line on the axis
# (half a label, 120, plus 80: the slanted exit is still 8px clear at the label's near edge).
PORT_REACH = CONNECTION_LABEL_WIDTH / 2 + 5 * LABEL_MARGIN
# Extra reach for a top or bottom port, so several routed lines can use one face.
FACE_OFFSETS = (0.0, 48.0, 96.0)
# (how a routed line leaves the source, how it enters the target), most preferred first. "near" is
# the source's face toward the target (bottom going down, top going up), "far" the target's face
# toward the source, "away" the source's other face. A side port's offset moves it along y, a face
# port's moves it further out along x.
ROUTE_ENDS = (
    *((("side", a), ("side", b)) for a, b in itertools.product(SIDE_OFFSETS, SIDE_OFFSETS)),
    *((("near", a), ("side", b)) for a, b in itertools.product(FACE_OFFSETS, SIDE_OFFSETS)),
    *((("side", a), ("far", b)) for a, b in itertools.product(SIDE_OFFSETS, FACE_OFFSETS)),
    *((("near", a), ("far", b)) for a, b in itertools.product(FACE_OFFSETS, FACE_OFFSETS)),
    *((("away", a), ("side", b)) for a, b in itertools.product(FACE_OFFSETS, SIDE_OFFSETS)),
    *((("away", a), ("far", b)) for a, b in itertools.product(FACE_OFFSETS, FACE_OFFSETS)),
)
# NiFi draws a new self-loop with two bends 25px above and below the card's centre line, right of
# the card (connection-manager.service.ts:80-81, canvas-utils.service.ts:2315-2328). NiFi's x,
# 125px out, leaves a label on the bend 5px from the card, so the loop sits half a label plus a
# margin out, with a middle bend that carries the label on its outer vertical stretch.
SELF_LOOP_Y = 25.0
LOOP_REACH = CONNECTION_LABEL_WIDTH / 2 + LABEL_MARGIN


def loop_extent(width: float, count: int, side: int) -> float:
    """How far past a card's centre its self-loops reach on one side (1 right, -1 left), label included."""
    loops = (count + 1) // 2 if side > 0 else count // 2
    if not loops:
        return width / 2
    return width / 2 + LOOP_REACH + (loops - 1) * (CONNECTION_LABEL_WIDTH + LABEL_MARGIN) + LOOP_REACH


def _loop_route(box: Box, index: int) -> tuple[list[Bend], int]:
    """The index-th self-loop of a card: right, then left, then further out on each side."""
    cx, cy = _centre(box)
    side = 1.0 if index % 2 == 0 else -1.0
    x = cx + side * (box[2] / 2 + LOOP_REACH + (index // 2) * (CONNECTION_LABEL_WIDTH + LABEL_MARGIN))
    return [{"x": x, "y": cy - SELF_LOOP_Y}, {"x": x, "y": cy}, {"x": x, "y": cy + SELF_LOOP_Y}], 1


def _port(box: Box, how: tuple[str, float], lane: float, down: bool, leaving: bool) -> tuple[Point, float]:
    """The bend next to a card where a routed line leaves or enters it, and the y of its run to the lane."""
    x, y, width, height = box
    cx, cy = x + width / 2, y + height / 2
    side = math.copysign(1.0, lane - cx)
    kind, offset = how
    if kind == "side":
        return (cx + side * (width / 2 + LABEL_MARGIN), cy + offset), cy + offset
    # near / far face the other end; away faces from it. Going down the source's near face is its bottom.
    facing_down = (kind in ("near", "far")) == (down == leaving)
    line = y + height + LABEL_GAP / 2 if facing_down else y - LABEL_GAP / 2
    return (cx + side * (PORT_REACH + offset), line), line


def _points(*candidates: Point) -> list[Point]:
    points: list[Point] = []
    for point in candidates:
        if not points or points[-1] != point:
            points.append(point)
    return points


def _row_route(
    boxes: dict[str, Box], src: str, dst: str, labels: list[Box], segments: list[tuple[Point, Point]], bent: list
) -> tuple[list[Bend], int | None] | None:
    """Two cards on one row with a card or label between: out of the source's bottom or top, along a
    run past what is between, and into the target the same way, label on the run. Runs tried in
    order: the row gap just below and above, a line just off the row's cards (clear of the labels
    that sit on the gap's centre line), then row gaps further out."""
    source, target = boxes[src], boxes[dst]
    (src_x, _), (dst_x, _) = _centre(source), _centre(target)
    side = math.copysign(1.0, dst_x - src_x)
    bottom = max(source[1] + source[3], target[1] + target[3])
    top = min(source[1], target[1])
    near_lines = [bottom + LABEL_GAP / 2, top - LABEL_GAP / 2]
    hug = STROKE_MARGIN + (LABEL_GAP - CONNECTION_LABEL_HEIGHT) / 4
    around = _gap_lines(boxes, top - 3 * ROW_PITCH, bottom + 3 * ROW_PITCH)
    far_lines = [line for line in around if line not in near_lines]
    runs = [*near_lines, bottom + hug, top - hug, *sorted(far_lines, key=lambda line: abs(line - (top + bottom) / 2))]
    fallback = None
    for run in runs:
        start, finish = (src_x + side * PORT_REACH, run), (dst_x - side * PORT_REACH, run)
        path = _path(boxes, src, dst, [{"x": x, "y": y} for x, y in (start, finish)])
        fallback = fallback or ([{"x": x, "y": y} for x, y in (start, finish)], 0)
        if not _line_is_clear(boxes, (src, dst), path, labels, segments):
            continue
        for k in range(1, int(abs(finish[0] - start[0]) // LANE_STEP)):
            spot = (start[0] + side * k * LANE_STEP, run)
            if _label_is_clear(_label_box(spot), boxes, labels, bent):
                return [{"x": x, "y": y} for x, y in (start, spot, finish)], 1
    return fallback


def _kink_route(
    boxes: dict[str, Box], src: str, dst: str, labels: list[Box], segments: list[tuple[Point, Point]], bent: list
) -> tuple[list[Bend], int | None] | None:
    """Cards on adjacent rows: one bend in the row gap between them, beside the straight line, far
    enough out that its label clears a label on that line (a label width plus a margin), then further
    out a LANE_STEP at a time. None when no such bend is clear."""
    source, target = boxes[src], boxes[dst]
    upper, lower = (source, target) if source[1] < target[1] else (target, source)
    if lower[1] - (upper[1] + upper[3]) > LABEL_GAP:
        return None
    line = lower[1] - LABEL_GAP / 2
    middle = (_centre(source)[0] + _centre(target)[0]) / 2
    reach = CONNECTION_LABEL_WIDTH + LABEL_MARGIN
    for k in range(3 * OUTER_LANES):
        for side in (1.0, -1.0):
            bend = [{"x": middle + side * (reach + k * LANE_STEP), "y": line}]
            path = _path(boxes, src, dst, bend)
            if _line_is_clear(boxes, (src, dst), path, labels, segments) and _label_is_clear(
                _label_box((bend[0]["x"], line)), boxes, labels, bent
            ):
                return bend, 0
    return None


def _label_is_clear(label: Box, boxes: dict[str, Box], labels: list[Box], bent: list[tuple[Point, Point]]) -> bool:
    """A label clears every card and label, and no routed line runs through it."""
    return not any(rects_overlap(label, other) for other in [*boxes.values(), *labels]) and not any(
        _segment_hits(p, q, label) for p, q in bent
    )


def _side_route(
    boxes: dict[str, Box], src: str, dst: str, labels: list[Box], segments: list[tuple[Point, Point]], bent: list
) -> tuple[list[Bend], int | None] | None:
    """Out of the source (its side facing the lane, else its top or bottom), along to a free lane,
    along the lane with the label on a row gap, and into the target (its side, else its top or
    bottom). A line back up to an earlier card takes a lane right of both first. The first choice
    that crosses no card, runs along no other line and crosses no other label wins; failing that, the
    first choice, which the check then reports. None when the two cards share a row."""
    source, target = boxes[src], boxes[dst]
    down = target[1] >= source[1] + source[3] + LABEL_GAP
    if not down and source[1] < target[1] + target[3] + LABEL_GAP:
        return _row_route(boxes, src, dst, labels, segments, bent)
    (src_x, src_y), (dst_x, dst_y) = _centre(source), _centre(target)
    near = (src_x, dst_x)
    top, bottom = min(source[1], target[1]) - LABEL_GAP, max(source[1] + source[3], target[1] + target[3]) + LABEL_GAP
    if kink := _kink_route(boxes, src, dst, labels, segments, bent):
        return kink
    obstacles = [*boxes.values(), *labels]
    lanes = _lanes(obstacles, min(src_y, dst_y), max(src_y, dst_y), near, not down)
    wide_lanes = _lanes(obstacles, top, bottom, near, not down)
    lines = _gap_lines(boxes, top, bottom)
    fallback: tuple[list[Bend], int | None] | None = None
    for ends in ROUTE_ENDS:
        for lane in lanes if ends[0][0] == ends[1][0] == "side" else wide_lanes:
            start, exit_y = _port(source, ends[0], lane, down, True)
            finish, entry_y = _port(target, ends[1], lane, down, False)
            spots = [line for line in lines if min(exit_y, entry_y) < line < max(exit_y, entry_y)]
            if not spots:
                continue
            corners = _points(start, (lane, exit_y), (lane, entry_y), finish)
            path = _path(boxes, src, dst, [{"x": x, "y": y} for x, y in corners])
            if not _line_is_clear(boxes, (src, dst), path, labels, segments):
                fallback = fallback or _with_label(start, lane, exit_y, spots[0], entry_y, finish)
                continue
            ordered = spots if down else spots[::-1]
            for points, spot in _label_spots(start, (lane, exit_y), (lane, entry_y), finish, ordered):
                if _label_is_clear(_label_box(spot), boxes, labels, bent):
                    return [{"x": x, "y": y} for x, y in points], points.index(spot)
            fallback = fallback or _with_label(start, lane, exit_y, spots[0], entry_y, finish)
    return fallback


def _label_spots(start: Point, top: Point, bottom: Point, finish: Point, lines: list[float]):
    """Where a routed line's label may sit, best first: on the lane at a row gap, then along the run
    into the target, then along the run out of the source, every LANE_STEP. Yields the bends with
    the spot added, and the spot."""
    for line in lines:
        yield _points(start, top, (top[0], line), bottom, finish), (top[0], line)
    for a, b, before in ((bottom, finish, True), (start, top, False)):
        steps = int(abs(b[0] - a[0]) // LANE_STEP)
        for k in range(1, steps):
            spot = (a[0] + math.copysign(k * LANE_STEP, b[0] - a[0]), a[1])
            points = _points(start, top, bottom, spot, finish) if before else _points(start, spot, top, bottom, finish)
            yield points, spot


def _with_label(
    start: Point, lane: float, exit_y: float, line: float, entry_y: float, finish: Point
) -> tuple[list[Bend], int]:
    points = _points(start, (lane, exit_y), (lane, line), (lane, entry_y), finish)
    return [{"x": x, "y": y} for x, y in points], points.index((lane, line))


def _line_is_clear(
    boxes: dict[str, Box],
    ends: tuple[str, str],
    path: list[Point],
    labels: list[Box],
    segments: list[tuple[Point, Point]],
) -> bool:
    own = list(itertools.pairwise(path))
    return (
        not _path_hits_cards(path, boxes, ends)
        and not any(_collinear_overlap(seg, other) for seg in own for other in segments)
        and not any(_segment_hits(p, q, other) for p, q in own for other in labels)
    )


def route_connections(
    boxes: dict[str, Box], connections: list[tuple[str, str]]
) -> list[tuple[list[Bend], int | None]]:
    """route_connections in a fixed order: by source y and x, then destination y and x, then keys.
    The result is in the caller's order, so a relayout routes the same whatever order NiFi lists in."""

    def key(i: int) -> tuple:
        src, dst = connections[i]
        far = (math.inf, math.inf)
        return (*(boxes[src][1::-1] if src in boxes else far), *(boxes[dst][1::-1] if dst in boxes else far), src, dst)

    order = sorted(range(len(connections)), key=key)
    routed = _route_in_order(boxes, [connections[i] for i in order])
    result: list[tuple[list[Bend], int | None]] = [([], None)] * len(connections)
    for i, route in zip(order, routed, strict=True):
        result[i] = route
    return result


def _route_in_order(
    boxes: dict[str, Box], connections: list[tuple[str, str]]
) -> list[tuple[list[Bend], int | None]]:
    """Bends and label index for each connection between these real card boxes.

    A self-loop goes out to one side of its card (_loop_route). A connection stays straight when its
    line and label clear every other card and its label clears every label placed so far; so does
    only the first of two connections between the same pair. Every other one is routed by
    _side_route, clear of cards, of other connections' lines and of their labels.
    """
    result: list[tuple[list[Bend], int | None]] = [([], None)] * len(connections)
    labels: list[Box] = []
    segments: list[tuple[Point, Point]] = []
    bent: list[tuple[Point, Point]] = []
    blocked: list[int] = []
    loops: dict[str, int] = defaultdict(int)
    seen: set[tuple[str, str]] = set()

    def claim(i: int, route: tuple[list[Bend], int | None]) -> None:
        src, dst = connections[i]
        result[i] = route
        path = _path(boxes, src, dst, route[0])
        labels.append(_path_label(path, *route))
        segments.extend(itertools.pairwise(path))
        if route[0]:
            bent.extend(itertools.pairwise(path))

    for i, (src, dst) in enumerate(connections):
        if src not in boxes or dst not in boxes:
            continue
        if src == dst:
            claim(i, _loop_route(boxes[src], loops[src]))
            loops[src] += 1
            continue
        path = _path(boxes, src, dst, [])
        label = _path_label(path, [], None)
        if (
            (src, dst) in seen
            or _path_hits_cards(path, boxes, (src, dst))
            or any(rects_overlap(label, other) for other in [*boxes.values(), *labels])
        ):
            blocked.append(i)
        else:
            claim(i, ([], None))
        seen.add((src, dst))
    for i in blocked:
        route = _side_route(boxes, *connections[i], labels, segments, bent)
        if route is not None:
            claim(i, route)
    return result


def connection_label_boxes(
    boxes: dict[str, Box],
    connections: list[tuple[str, str]],
    routes: list[tuple[list[Bend], int | None]] | None = None,
) -> dict[str, Box]:
    """The label box NiFi draws for each connection, routed as given or by route_connections."""
    labels: dict[str, Box] = {}
    routes = route_connections(boxes, connections) if routes is None else routes
    for index, ((src, dst), (bends, label_index)) in enumerate(zip(connections, routes, strict=True)):
        if src in boxes and dst in boxes:
            labels[f"label {src}->{dst} #{index}"] = _path_label(_path(boxes, src, dst, bends), bends, label_index)
    return labels


def assert_no_box_overlap(
    boxes: dict[str, Box],
    connections: list[tuple[str, str]] | None = None,
    routes: list[tuple[list[Bend], int | None]] | None = None,
) -> None:
    """One occupancy check for every card on a canvas, whatever its kind; raises the first problem.

    Pass real card sizes (REAL_CARD_SIZES). With `connections`, each connection as route_connections
    draws it is an obstacle too (see layout_problems). `routes` checks connections as drawn elsewhere,
    such as the straight lines already on a canvas.
    """
    if problems := layout_problems(boxes, connections, routes):
        raise ValueError(problems[0])


def layout_problems(
    boxes: dict[str, Box],
    connections: list[tuple[str, str]] | None = None,
    routes: list[tuple[list[Bend], int | None]] | None = None,
) -> list[str]:
    """Every problem on a canvas: two cards or labels overlapping, a line crossing a card other than
    its own ends (grown by STROKE_MARGIN), two connections along one line, a routed line across
    another connection's label."""
    connections = connections or []
    routes = route_connections(boxes, connections) if routes is None else routes
    everything = {**boxes, **connection_label_boxes(boxes, connections, routes)}
    names = list(everything)
    problems = [
        f"{left} at {everything[left][:2]} overlaps {right} at {everything[right][:2]}"
        for i, left in enumerate(names)
        for right in names[i + 1 :]
        if rects_overlap(everything[left], everything[right])
    ]
    drawn = []
    for (src, dst), (bends, index) in zip(connections, routes, strict=True):
        if src not in boxes or dst not in boxes:
            continue
        path = _path(boxes, src, dst, bends)
        if hit := _path_hits_cards(path, boxes, (src, dst)):
            problems.append(f"connection {src}->{dst} crosses {hit} at {boxes[hit][:2]}")
        drawn.append((f"{src}->{dst}", path, bends, index))
    return problems + _line_problems(drawn)


def _line_problems(drawn: list[tuple[str, list[Point], list[Bend], int | None]]) -> list[str]:
    """No two connections run along one line, and no routed line crosses another connection's label."""
    problems: list[str] = []
    labels = {name: _path_label(path, bends, index) for name, path, bends, index in drawn}
    for i, (name, path, bends, _index) in enumerate(drawn):
        segments = list(itertools.pairwise(path))
        for other, other_path, _bends, _other_index in drawn[i + 1 :]:
            problems += [
                f"connection {name} runs along connection {other} from {seg[0]} to {seg[1]}"
                for seg in segments
                if any(_collinear_overlap(seg, other_seg) for other_seg in itertools.pairwise(other_path))
            ]
        if bends:
            problems += [
                f"routed connection {name} crosses the label of connection {other}"
                for other, label in labels.items()
                if other != name and any(_segment_hits(p, q, label) for p, q in segments)
            ]
    return problems


def _graph(
    nodes: list[str], edges: list[tuple[str, str]]
) -> tuple[dict[str, list[str]], dict[str, int]]:
    outgoing: dict[str, list[str]] = defaultdict(list)
    incoming: dict[str, int] = dict.fromkeys(nodes, 0)
    seen: set[tuple[str, str]] = set()
    for src, dst in edges:
        if src not in incoming or dst not in incoming or src == dst:
            continue
        pair = (src, dst)
        if pair in seen:
            continue
        seen.add(pair)
        outgoing[src].append(dst)
        incoming[dst] += 1
    return outgoing, incoming


def _dag(nodes: list[str], edges: list[tuple[str, str]]) -> tuple[list[str], list[tuple[str, str]]]:
    """Topological order, and the edges that run forward in it.

    A cycle is entered where the flow already placed reaches it: at the card with the most inputs
    already in the order (then list order), so a retry line back up is the edge that is dropped.
    """
    known = set(nodes)
    unique = list(dict.fromkeys((s, d) for s, d in edges if s in known and d in known and s != d))
    waiting = dict.fromkeys(nodes, 0)
    for _src, dst in unique:
        waiting[dst] += 1
    inputs = dict(waiting)
    order: list[str] = []
    done: set[str] = set()
    while len(order) < len(nodes):
        # ponytail: O(n^2) scan, fine for canvas-sized graphs
        node = next((n for n in nodes if n not in done and waiting[n] == 0), None)
        node = node or max((n for n in nodes if n not in done), key=lambda n: inputs[n] - waiting[n])
        order.append(node)
        done.add(node)
        for src, dst in unique:
            if src == node:
                waiting[dst] -= 1
    rank = {name: i for i, name in enumerate(order)}
    return order, [(s, d) for s, d in unique if rank[s] < rank[d]]


def _common_ancestor(preds: list[str], parent: dict[str, str | None]) -> str | None:
    def chain(node: str | None) -> list[str | None]:
        out: list[str | None] = []
        while node is not None:
            out.append(node)
            node = parent[node]
        return [*out, None]

    others = [set(chain(p)) for p in preds[1:]]
    return next(a for a in chain(preds[0]) if all(a in other for other in others))


# Reserved span per row, relative to a family's own axis and row: row -> (left, right).
Shape = dict[int, tuple[float, float]]
Offsets = dict[str, tuple[float, int]]


def side_pitch(parent_width: float, child_width: float) -> float:
    """Axis distance from a parent to a child on its own row: a label fits on the line between them."""
    return _grid(parent_width / 2 + CONNECTION_LABEL_WIDTH + 2 * SIDE_LABEL_MARGIN + child_width / 2)


def _merge(shape: Shape, other: Shape, dx: float, dr: int) -> None:
    for row, (lo, hi) in other.items():
        lo, hi = lo + dx, hi + dx
        if row + dr in shape:
            lo, hi = min(lo, shape[row + dr][0]), max(hi, shape[row + dr][1])
        shape[row + dr] = (lo, hi)


def _clear(shape: Shape, other: Shape, side: int, dr: int = 0) -> float:
    """The smallest shift, right (side 1) or left (side -1), that keeps `other` off `shape` on every row."""
    gaps = [
        shape[row + dr][1] - lo if side > 0 else shape[row + dr][0] - hi
        for row, (lo, hi) in other.items()
        if row + dr in shape
    ]
    if not gaps:
        return 0.0
    return max(gaps) if side > 0 else min(gaps)


class _Tree:
    """Forks and joins of a flow graph: each card hangs under one parent; None is the canvas.

    At a fork the main child continues down the parent's axis and every other child sits on the
    parent's row, perpendicular to it. A fork of leaves has no main child: they sit one row down,
    spread around the axis. A join goes back on its fork's axis below everything in that family.
    """

    def __init__(self, nodes: list[str], edges: list[tuple[str, str]], sizes: dict[str, tuple[float, float]]):
        self.sizes = sizes
        self.loops = Counter(src for src, dst in edges if src == dst)
        self.forks: dict[str | None, list[str]] = defaultdict(list)
        self.joins: dict[str | None, list[str]] = defaultdict(list)
        self.preds: dict[str, list[str]] = defaultdict(list)
        parent: dict[str, str | None] = {}
        order, dag = _dag(nodes, edges)
        for src, dst in dag:
            self.preds[dst].append(src)
        for node in order:
            preds = self.preds[node]
            if len(preds) < 2:
                parent[node] = preds[0] if preds else None
                self.forks[parent[node]].append(node)
            else:
                parent[node] = _common_ancestor(preds, parent)
                self.joins[parent[node]].append(node)
        rank = {pair: i for i, pair in enumerate(dag)}
        for key, kids in self.forks.items():
            if key is not None:
                kids.sort(key=lambda kid, key=key: rank[(key, kid)])

    def _pitch(self, key: str) -> float:
        return sibling_pitch(self.sizes[key][0])

    def _main(self, key: str, kids: list[str], subs: dict[str, tuple[Shape, Offsets]]) -> str | None:
        """The child that continues down the axis: deepest, then the one feeding this fork's join,
        then relationship order. None when every child is a leaf."""
        if len(kids) == 1:
            return kids[0]
        if not any(self.forks[kid] or self.joins[kid] for kid in kids):
            return None
        feeders = {pred for join in self.joins[key] for pred in self.preds[join]}

        def rank(kid: str) -> tuple[int, int, int]:
            shape, offsets = subs[kid]
            return (-max(shape), -int(bool(feeders & set(offsets))), kids.index(kid))

        return min(kids, key=rank)

    def layout(self, key: str | None) -> tuple[Shape, Offsets]:
        """Offsets (axis x, row) of a card's whole family relative to the card, and the span it reserves."""
        shape: Shape = {}
        offsets: Offsets = {}
        if key is not None:
            half, width, count = self._pitch(key) / 2, self.sizes[key][0], self.loops[key]
            # A self-loop's label reaches past the card; reserve it with a margin like any card.
            shape[0] = (
                -max(half, loop_extent(width, count, -1) + LABEL_MARGIN),
                max(half, loop_extent(width, count, 1) + LABEL_MARGIN),
            )
            offsets[key] = (0.0, 0)

        def put(sub: tuple[Shape, Offsets], dx: float, dr: int) -> None:
            _merge(shape, sub[0], dx, dr)
            offsets.update({name: (x + dx, row + dr) for name, (x, row) in sub[1].items()})

        kids = self.forks[key]
        subs = {kid: self.layout(kid) for kid in kids}
        if key is None:
            for kid in kids:
                put(subs[kid], _clear(shape, subs[kid][0], 1), 0)
        else:
            self._place_fork(key, kids, subs, shape, put)
        for join in self.joins[key]:
            sub = self.layout(join)
            put(sub, 0.0, max(shape, default=-1) + 1)
        return shape, offsets

    def _place_fork(self, key: str, kids: list[str], subs: dict, shape: Shape, put: Any) -> None:
        main = self._main(key, kids, subs) if kids else None
        if main is None:
            # Each leaf takes the span it reserves on its row, self-loops included.
            spans = [subs[kid][0][0] for kid in kids]
            left = -sum(hi - lo for lo, hi in spans) / 2
            for kid, (lo, hi) in zip(kids, spans, strict=True):
                put(subs[kid], left - lo, 1)
                left += hi - lo
            return
        put(subs[main], 0.0, 1)
        # Relationship order runs left to right through the main child. The nearest child on each side
        # sits on the fork's row; any further one drops a row, in its own column past the inner one.
        at = kids.index(main)
        for side, group in ((-1, kids[:at][::-1]), (1, kids[at + 1 :])):
            for n, kid in enumerate(group):
                if n == 0:
                    reach = side_pitch(self.sizes[key][0], self.sizes[kid][0])
                    dx = _clear(shape, subs[kid][0], side)
                    put(subs[kid], max(dx, reach) if side > 0 else min(dx, -reach), 0)
                    continue
                own_column = _clear(shape, {0: subs[kid][0][0]}, side)
                below = _clear(shape, subs[kid][0], side, 1)
                put(subs[kid], max(own_column, below) if side > 0 else min(own_column, below), 1)


def _longest_path_layers(
    nodes: list[str], outgoing: dict[str, list[str]], incoming: dict[str, int]
) -> dict[str, int] | None:
    layer_of: dict[str, int] = dict.fromkeys(nodes, 0)
    ready = deque(n for n in nodes if incoming[n] == 0)
    remaining = dict(incoming)
    seen = 0
    while ready:
        node = ready.popleft()
        seen += 1
        for nxt in outgoing[node]:
            layer_of[nxt] = max(layer_of[nxt], layer_of[node] + 1)
            remaining[nxt] -= 1
            if remaining[nxt] == 0:
                ready.append(nxt)
    if seen < len(nodes):
        return None
    return layer_of


def layered_positions(
    node_names: list[str],
    edges: list[tuple[str, str]],
    *,
    origin: Point = (0.0, 0.0),
    kinds: dict[str, str] | None = None,
) -> dict[str, Point]:
    """Top-left x/y of each card: top-down, forks sideways, sizes and pitches as in the module docstring.

    Axis 0 is the centre of a processor at origin x. `kinds` maps a name to its card kind (default processor).
    """
    if not node_names:
        return {}
    nodes = list(dict.fromkeys(node_names))
    sizes = {name: REAL_CARD_SIZES[(kinds or {}).get(name, "processor")] for name in nodes}
    _shape, offsets = _Tree(nodes, edges, sizes).layout(None)
    heights: dict[int, float] = defaultdict(float)
    for name, (_x, row) in offsets.items():
        heights[row] = max(heights[row], sizes[name][1])
    tops = [origin[1]]
    for row in range(max(heights)):
        tops.append(tops[-1] + row_pitch(heights[row]))
    placed = {
        name: (origin[0] + AXIS_CENTRE + offsets[name][0] - sizes[name][0] / 2, tops[offsets[name][1]])
        for name in nodes
    }
    assert_no_box_overlap({name: (*placed[name], *sizes[name]) for name in nodes})
    return placed


def _edge_names(item: dict[str, Any]) -> tuple[str, str] | None:
    src = item.get("source") or item.get("from")
    dst = item.get("target") or item.get("destination") or item.get("to")
    if not src or not dst:
        return None
    return str(src), str(dst)


def origin_below(obstacles: list[Box]) -> Point:
    """Layout origin on the processor row lattice below every obstacle, so a new tree clears the canvas."""
    if not obstacles:
        return 0.0, 0.0
    bottom = max(y + height for _x, y, _width, height in obstacles)
    return 0.0, math.ceil(bottom / ROW_PITCH) * ROW_PITCH


def spec_kind(item: dict[str, Any]) -> str:
    """A spec item's kind: "type", or its alias "kind". The builder and the layout both read it here."""
    return str(item.get("type") or item.get("kind") or "").strip().lower()


def assign_canvas_positions(
    objects: list[dict[str, Any]],
    extra_connections: list[dict[str, Any]] | None = None,
    *,
    origin: Point = (0.0, 0.0),
) -> dict[str, Point]:
    """Mutate processor/port objects with non-overlapping x/y from the connection graph."""
    placeable = [item for item in objects if spec_kind(item) in _PLACEABLE]
    names = [str(item.get("name")) for item in placeable if item.get("name")]
    edges: list[tuple[str, str]] = []
    for item in objects + list(extra_connections or []):
        if spec_kind(item) != "connection":
            continue
        pair = _edge_names(item)
        if pair:
            edges.append(pair)
    kinds = {str(item.get("name")): spec_kind(item) for item in placeable}
    positions = layered_positions(names, edges, origin=origin, kinds=kinds)
    for item in placeable:
        name = str(item.get("name") or "")
        if name not in positions:
            continue
        x, y = positions[name]
        item["x"] = x
        item["y"] = y
        item["position"] = {"x": x, "y": y}
    return positions


def _cell(point: Point, col_pitch: float, row_pitch: float) -> tuple[int, int]:
    return round(point[0] / col_pitch), round(point[1] / row_pitch)


def next_grid_point(
    occupied: list[Point],
    *,
    col_pitch: float = PG_COL_PITCH,
    row_pitch: float = PG_ROW_PITCH,
    wrap: int = PG_WRAP,
    obstacles: list[Box] | None = None,
    origin_x: float = 0.0,
) -> Point:
    """First lattice cell whose footprint clears every real card. Used when placing a process group.

    `occupied` are process-group positions (on or off the lattice); `obstacles` are footprints of
    other cards on the same canvas, such as processor_footprint() boxes.
    """
    boxes = [(x, y, col_pitch, row_pitch) for x, y in occupied] + list(obstacles or [])
    index = 0
    while True:
        cell = (index % wrap, index // wrap)
        candidate = (origin_x + cell[0] * col_pitch, cell[1] * row_pitch, col_pitch, row_pitch)
        if not any(rects_overlap(candidate, box) for box in boxes):
            return candidate[0], candidate[1]
        index += 1


def relationship_order(conn: dict[str, Any]) -> tuple[str, str]:
    """Fork siblings sit left to right in relationship order, as NiFi lists them, then by name."""
    return ",".join(sorted(conn.get("relationships") or [])), str((conn.get("destination") or {}).get("name") or "")


def real_boxes(flow: dict[str, Any], positions: dict[str, Point]) -> dict[str, Box]:
    """Every card in a compact outline as NiFi draws it, at its new position where it has one."""
    boxes: dict[str, Box] = {}
    for key, kind in OUTLINE_KINDS.items():
        for item in flow.get(key) or []:
            cid = str(item.get("id") or "")
            if not cid:
                continue
            pos = item.get("position") or {}
            x, y = positions.get(cid, (float(pos.get("x") or 0), float(pos.get("y") or 0)))
            width, height = REAL_CARD_SIZES[kind]
            if kind == "label":
                width, height = float(item.get("width") or width), float(item.get("height") or height)
            boxes[cid] = (x, y, width, height)
    return boxes


def outline_edges(flow: dict[str, Any]) -> list[tuple[str, str]]:
    """Every connection as (source card, destination card); a port inside a child group is that group."""
    groups = {str(item["id"]) for item in flow.get("process_groups") or [] if item.get("id")}
    return [
        (_end_key(conn.get("source") or {}, groups), _end_key(conn.get("destination") or {}, groups))
        for conn in flow.get("connections") or []
    ]


def _processor_positions(flow: dict[str, Any], fixed: dict[str, Box], kinds: dict[str, str]) -> dict[str, Point]:
    """Family-tree layout of processors and ports, moved below the fixed cards if it would hit one."""
    id_order: list[str] = []
    for key in ("processors", "input_ports", "output_ports"):
        for item in flow.get(key) or []:
            cid = item.get("id")
            if cid:
                id_order.append(str(cid))
    if not id_order:
        return {}
    edges: list[tuple[str, str]] = []
    known = set(id_order)
    for conn in sorted(flow.get("connections") or [], key=relationship_order):
        src = (conn.get("source") or {}).get("id")
        dst = (conn.get("destination") or {}).get("id")
        if src in known and dst in known:
            edges.append((str(src), str(dst)))
    placed = layered_positions(id_order, edges, kinds=kinds)
    if any(rects_overlap(processor_footprint(p), box) for p in placed.values() for box in fixed.values()):
        placed = layered_positions(id_order, edges, origin=origin_below(list(fixed.values())), kinds=kinds)
    return placed


def _end_key(end: dict[str, Any], groups: set[str]) -> str:
    """A connection end as a card on this canvas: a port inside a child group is that group."""
    group_id = str(end.get("group_id") or "")
    return group_id if group_id in groups else str(end.get("id") or "")


def group_edges(flow: dict[str, Any]) -> list[tuple[str, str]]:
    """Connections that start or end at a child process group, as (source card, destination card)."""
    groups = {str(item["id"]) for item in flow.get("process_groups") or [] if item.get("id")}
    return [edge for edge in outline_edges(flow) if groups & set(edge)]


def _flow_order(ids: list[str], edges: list[tuple[str, str]], names: dict[str, str]) -> list[str]:
    """Upstream first: by longest-path layer, then name. A cycle falls back to name order."""
    outgoing, incoming = _graph(ids, edges)
    layers = _longest_path_layers(ids, outgoing, incoming) or {}
    return sorted(ids, key=lambda key: (layers.get(key, 0), names[key], key))


def _group_positions(flow: dict[str, Any], canvas: Canvas) -> dict[str, Point]:
    """Child groups top-down on the processor axis, one per row, below every card placed so far."""
    names = {
        str(item["id"]): str(item.get("name") or item["id"])
        for item in flow.get("process_groups") or []
        if item.get("id")
    }
    placed: dict[str, Point] = {}
    y = 0.0
    for gid in _flow_order(list(names), group_edges(flow), names):
        box = (PG_AXIS_X, y, PG_COL_PITCH, PG_ROW_PITCH)
        while hits := [other for other in canvas.footprints() if rects_overlap(box, other)]:
            box = (PG_AXIS_X, max(top + height for _x, top, _w, height in hits), PG_COL_PITCH, PG_ROW_PITCH)
        placed[gid], _note = canvas.place(gid, "process_group", (box[0], box[1]))
        y = placed[gid][1] + PG_ROW_PITCH
    return placed


# Blocks with no connection between them carry no label between them either.
BLOCK_GAP = 32.0
_MOVABLE = ("processors", "input_ports", "output_ports", "process_groups")


def _components(flow: dict[str, Any]) -> list[list[str]]:
    """Cards this server moves, split into groups that connections join, in outline order."""
    ids = [str(item["id"]) for key in _MOVABLE for item in flow.get(key) or [] if item.get("id")]
    root = {cid: cid for cid in ids}

    def find(cid: str) -> str:
        while root[cid] != cid:
            root[cid] = root[root[cid]]
            cid = root[cid]
        return cid

    for src, dst in outline_edges(flow):
        if src in root and dst in root:
            root[find(src)] = find(dst)
    parts: dict[str, list[str]] = defaultdict(list)
    for cid in ids:
        parts[find(cid)].append(cid)
    return list(parts.values())


def _sub_flow(flow: dict[str, Any], keep: set[str]) -> dict[str, Any]:
    sub: dict[str, Any] = {
        key: [item for item in flow.get(key) or [] if str(item.get("id")) in keep] for key in _MOVABLE
    }
    sub["connections"] = [
        conn
        for conn, (src, _dst) in zip(flow.get("connections") or [], outline_edges(flow), strict=True)
        if src in keep
    ]
    return sub


def _block(flow: dict[str, Any], part: list[str]) -> tuple[str, dict[str, Point], Box]:
    """One connected component laid out on its own: a name to sort by, its positions and the box
    its cards, labels and routed lines fill."""
    sub = _sub_flow(flow, set(part))
    placed = positions_from_flow(sub)
    boxes = real_boxes(sub, placed)
    pairs = [pair for pair in outline_edges(sub) if set(pair) <= set(boxes)]
    routes = route_connections(boxes, pairs)
    kinds = {str(item["id"]): OUTLINE_KINDS[key] for key in _MOVABLE for item in sub[key]}
    # The house occupancy boxes too (a processor holds 420x200), so packed blocks never collide under it.
    occupied = [make_card(cid, kinds[cid], point).rect for cid, point in placed.items()]
    extent = [*boxes.values(), *occupied, *connection_label_boxes(boxes, pairs, routes).values()]
    extent += [(b["x"], b["y"], 0.0, 0.0) for bends, _index in routes for b in bends]
    left, top = min(b[0] for b in extent), min(b[1] for b in extent)
    right, bottom = max(b[0] + b[2] for b in extent), max(b[1] + b[3] for b in extent)
    names = {str(item["id"]): str(item.get("name") or item["id"]) for key in _MOVABLE for item in sub[key]}
    first = min(part, key=lambda cid: (placed[cid][1], placed[cid][0]))
    return names[first], placed, (left, top, right - left, bottom - top)


def _grid_rows(sizes: list[tuple[float, float]], columns: int) -> tuple[float, float]:
    rows = [sizes[i : i + columns] for i in range(0, len(sizes), columns)]
    width = max(sum(w for w, _h in row) + BLOCK_GAP * (len(row) - 1) for row in rows)
    height = sum(max(h for _w, h in row) for row in rows) + BLOCK_GAP * (len(rows) - 1)
    return width, height


def _pack_components(flow: dict[str, Any], parts: list[list[str]]) -> dict[str, Point]:
    """Components with no connection between them, packed into a near-square grid.

    Each component is laid out on its own and becomes a block. Blocks are sorted by name, so name
    families sit together, and filled row by row with BLOCK_GAP between them. The column count is
    the one whose grid is closest to square.
    """
    blocks = sorted((_block(flow, part) for part in parts), key=lambda block: block[0])
    sizes = [(_grid(box[2]), _grid(box[3])) for _name, _placed, box in blocks]

    def squareness(columns: int) -> float:
        width, height = _grid_rows(sizes, columns)
        return abs(math.log(width / height))

    columns = min(range(1, len(blocks) + 1), key=squareness)
    placed: dict[str, Point] = {}
    top = 0.0
    for start in range(0, len(blocks), columns):
        row = blocks[start : start + columns]
        left = 0.0
        row_sizes = sizes[start : start + columns]
        for (_name, positions, (x0, y0, _w, _h)), (width, _height) in zip(row, row_sizes, strict=True):
            dx, dy = _grid(left - x0), _grid(top - y0)
            placed.update({cid: (x + dx, y + dy) for cid, (x, y) in positions.items()})
            left += width + BLOCK_GAP
        top += max(height for _width, height in sizes[start : start + columns]) + BLOCK_GAP
    fixed = list(fixed_footprints(flow).values())
    cards = real_boxes(_sub_flow(flow, set(placed)), placed).values()
    if any(rects_overlap(card, box) for card in cards for box in fixed):
        shift = origin_below(fixed)[1]
        placed = {cid: (x, y + shift) for cid, (x, y) in placed.items()}
    return placed


def positions_from_flow(flow: dict[str, Any]) -> dict[str, Point]:
    """Layout processors by connection graph; stack child process groups top-down below them.

    Every card goes through one Canvas: funnels, remote process groups and labels stay put and
    are placed first, the processor tree starts below them when it would otherwise land on one,
    and groups stack in flow order clear of everything placed before them.
    """
    parts = _components(flow)
    if len(parts) > 1:
        return _pack_components(flow, parts)
    fixed = fixed_footprints(flow)
    canvas = Canvas.from_outline(flow, kinds={"funnel", "remote_process_group", "label"})
    kind_of = {
        str(item.get("id")): kind
        for key, kind in OUTLINE_KINDS.items()
        if kind in _PLACEABLE
        for item in flow.get(key) or []
        if item.get("id")
    }
    placed: dict[str, Point] = {}
    for cid, point in _processor_positions(flow, fixed, kind_of).items():
        placed[cid], _note = canvas.place(cid, kind_of[cid], point)
    placed.update(_group_positions(flow, canvas))
    if clash := canvas.overlaps():
        left, right = clash[0]
        raise ValueError(f"{left.key} at {left.rect[:2]} overlaps {right.key} at {right.rect[:2]}")
    return placed

# Card kinds NiFi draws, keyed like the compact outline, with their size and the gap kept around them.
OUTLINE_KINDS = {
    "processors": "processor",
    "input_ports": "input_port",
    "output_ports": "output_port",
    "process_groups": "process_group",
    "funnels": "funnel",
    "remote_process_groups": "remote_process_group",
    "labels": "label",
}
CARD_SIZES: dict[str, tuple[float, float]] = {
    "processor": (CARD_WIDTH, CARD_HEIGHT),
    "input_port": REMOTE_PORT_SIZE,
    "output_port": REMOTE_PORT_SIZE,
    "process_group": (PG_WIDTH, PG_HEIGHT),
    "funnel": FUNNEL_SIZE,
    "remote_process_group": REMOTE_GROUP_SIZE,
    "label": LABEL_DEFAULT_SIZE,
}
# What NiFi actually draws, for overlap checks. CARD_SIZES keeps the roomier processor cell.
REAL_CARD_SIZES: dict[str, tuple[float, float]] = {
    **CARD_SIZES,
    "processor": PROCESSOR_SIZE,
    "input_port": PORT_SIZE,
    "output_port": PORT_SIZE,
}
# (column pitch, row pitch, wrap) of the lattice a kind snaps to when it takes a free slot.
_PROCESSOR_LATTICE = (COL_PITCH, ROW_PITCH, PROCESSOR_WRAP)
_GROUP_LATTICE = (PG_COL_PITCH, PG_ROW_PITCH, PG_WRAP)


def axis_offset(kind: str) -> float:
    """How far right of its cell's left edge a card sits so its centre is on the column axis."""
    return AXIS_CENTRE - REAL_CARD_SIZES[kind][0] / 2


def _lattice(kind: str) -> tuple[float, float, int]:
    return _GROUP_LATTICE if kind == "process_group" else _PROCESSOR_LATTICE


@dataclass(frozen=True)
class Card:
    key: str
    kind: str
    x: float
    y: float
    width: float
    height: float

    @property
    def rect(self) -> Box:
        return (self.x, self.y, self.width, self.height)

    @property
    def footprint(self) -> Box:
        """The card plus its gap: two footprints that do not overlap leave that gap between the cards."""
        col_pitch, row_pitch, _wrap = _lattice(self.kind)
        if self.kind in _PLACEABLE or self.kind == "process_group":
            return (self.x, self.y, col_pitch, row_pitch)
        return (self.x, self.y, self.width + COL_GAP, self.height + ROW_GAP)


def make_card(key: str, kind: str, point: Point, size: tuple[float, float] | None = None) -> Card:
    width, height = size or CARD_SIZES[kind]
    return Card(key, kind, float(point[0]), float(point[1]), float(width), float(height))


class Canvas:
    """The one occupancy model: every card on one process group canvas, whatever its kind.

    Every placement (spec builds in auto or manual layout, nifi_create_processor,
    nifi_create_process_group, nifi_import_flow, a processor move, relayout) goes through place(),
    which never returns a point whose card overlaps a card already on the canvas.
    """

    def __init__(self) -> None:
        self.cards: dict[str, Card] = {}
        self.placed: set[str] = set()

    @classmethod
    def from_outline(cls, outline: dict[str, Any], *, kinds: set[str] | None = None) -> Canvas:
        """Cards in a compact_flow outline, optionally only those of the given kinds."""
        canvas = cls()
        for key, kind in OUTLINE_KINDS.items():
            if kinds is not None and kind not in kinds:
                continue
            for index, item in enumerate(outline.get(key) or []):
                if not isinstance(item, dict):
                    continue
                pos = item.get("position") or {}
                width, height = CARD_SIZES[kind]
                if kind == "label":
                    width = float(item.get("width") or width)
                    height = float(item.get("height") or height)
                cid = str(item.get("id") or f"{key}[{index}]")
                canvas.add(make_card(cid, kind, (float(pos.get("x") or 0), float(pos.get("y") or 0)), (width, height)))
        return canvas

    def add(self, card: Card) -> Card:
        self.cards[card.key] = card
        return card

    def remove(self, key: str) -> None:
        self.cards.pop(key, None)

    def footprints(self) -> list[Box]:
        return [card.footprint for card in self.cards.values()]

    def collisions(self, card: Card) -> list[Card]:
        return [
            other for other in self.cards.values() if other.key != card.key and rects_overlap(card.rect, other.rect)
        ]

    def free_slot(self, kind: str) -> Point:
        """First lattice cell for this kind whose footprint clears every card's footprint."""
        col_pitch, row_pitch, wrap = _lattice(kind)
        origin_x = axis_offset(kind)
        return next_grid_point(
            [], col_pitch=col_pitch, row_pitch=row_pitch, wrap=wrap, obstacles=self.footprints(), origin_x=origin_x
        )

    def place(
        self,
        key: str,
        kind: str,
        point: Point | None = None,
        size: tuple[float, float] | None = None,
        *,
        label: str | None = None,
    ) -> tuple[Point, str | None]:
        """Claim a spot for a card and return it, with a note when the requested point was taken.

        No point: the first free lattice slot. A point: kept when the card clears every other card,
        else shifted down one row pitch at a time until it does.
        """
        self.placed.add(key)
        if point is None:
            card = make_card(key, kind, self.free_slot(kind), size)
            self.add(card)
            return (card.x, card.y), None
        card = make_card(key, kind, point, size)
        blockers = self.collisions(card)
        if not blockers:
            self.add(card)
            return (card.x, card.y), None
        _col, row_pitch, _wrap = _lattice(kind)
        while self.collisions(card):
            card = make_card(key, kind, (card.x, card.y + row_pitch), size)
        self.add(card)
        # The requested point is not repeated: it is a submitted value.
        note = (
            f"The requested x/y for {label or f'{kind} {key}'} would overlap {blockers[0].kind} "
            f"{blockers[0].key}; placed at ({card.x:g}, {card.y:g}) instead."
        )
        return (card.x, card.y), note

    def overlaps(self) -> list[tuple[Card, Card]]:
        """Overlapping pairs involving a card placed here. Cards that were already there may overlap
        each other; this server did not put them there and does not move them."""
        cards = list(self.cards.values())
        return [
            (left, right)
            for i, left in enumerate(cards)
            for right in cards[i + 1 :]
            if (left.key in self.placed or right.key in self.placed) and rects_overlap(left.rect, right.rect)
        ]
