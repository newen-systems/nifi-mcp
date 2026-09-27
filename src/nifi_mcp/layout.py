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
"""

from __future__ import annotations

import itertools
import math
from collections import defaultdict, deque
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
BEND_SPREAD = 56.0
ARROW_INSET = 20.0

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


def _lanes(boxes: dict[str, Box], top: float, bottom: float, near: tuple[float, float]) -> list[float]:
    """Free vertical x values between top and bottom, cheapest first: the middle of each gap between
    card columns, and just past the outermost. The two end cards count, so a lane never runs through them."""
    blocked = sorted(
        (x - LABEL_MARGIN, x + width + LABEL_MARGIN)
        for x, y, width, height in boxes.values()
        if y < bottom and y + height > top
    )
    if not blocked:
        return [near[0]]
    candidates = [blocked[0][0] - LABEL_MARGIN, max(hi for _lo, hi in blocked) + LABEL_MARGIN]
    reach = blocked[0][1]
    for lo, hi in blocked[1:]:
        if lo > reach:
            candidates.append((reach + lo) / 2)
        reach = max(reach, hi)
    src_x, dst_x = near
    return sorted(candidates, key=lambda c: (abs(c - src_x) + abs(c - dst_x), abs(c - src_x)))


def _gap_lines(boxes: dict[str, Box], top: float, bottom: float) -> list[float]:
    """Centre lines of the row gaps strictly between top and bottom, where a label clears every row."""
    lines = {y - LABEL_GAP / 2 for _x, y, _w, _h in boxes.values()}
    return sorted(line for line in lines if top < line < bottom)


# Where a routed line may leave or enter a card's side: its centre line first, then a step off it,
# so two routed lines into one side never share a line.
SIDE_OFFSETS = (0.0, -24.0, 24.0)
# A routed line that leaves through a card's top or bottom aims at a point this far off the card's
# centre in the row gap, so its first stretch clears the label of a straight line on the axis
# (half a label, 120, plus 80: the slanted exit is still 8px clear at the label's near edge).
PORT_REACH = CONNECTION_LABEL_WIDTH / 2 + 5 * LABEL_MARGIN
# (how a routed line leaves the source, how it enters the target), most preferred first.
ROUTE_ENDS = (
    *((("side", a), ("side", b)) for a, b in itertools.product(SIDE_OFFSETS, SIDE_OFFSETS)),
    *((("bottom", 0.0), ("side", b)) for b in SIDE_OFFSETS),
    *((("top", 0.0), ("side", b)) for b in SIDE_OFFSETS),
    (("side", 0.0), ("top", 0.0)),
    (("bottom", 0.0), ("top", 0.0)),
    (("top", 0.0), ("top", 0.0)),
)


def _port(box: Box, how: tuple[str, float], lane: float) -> tuple[Point, float]:
    """The bend next to a card where a routed line leaves or enters it, and the y of its run to the lane."""
    x, y, width, height = box
    cx, cy = x + width / 2, y + height / 2
    side = math.copysign(1.0, lane - cx)
    kind, offset = how
    if kind == "side":
        return (cx + side * (width / 2 + LABEL_MARGIN), cy + offset), cy + offset
    line = y - LABEL_GAP / 2 if kind == "top" else y + height + LABEL_GAP / 2
    return (cx + side * PORT_REACH, line), line


def _route_bends(
    boxes: dict[str, Box], src: str, dst: str, lane: float, ends: tuple, line: float
) -> tuple[list[Bend], int] | None:
    """Bends for one choice of exit, lane, label line and entry, and the label's bend."""
    start, exit_y = _port(boxes[src], ends[0], lane)
    finish, entry_y = _port(boxes[dst], ends[1], lane)
    if not exit_y < line < entry_y:
        return None
    points: list[Point] = []
    for point in (start, (lane, exit_y), (lane, line), (lane, entry_y), finish):
        if not points or points[-1] != point:
            points.append(point)
    return [{"x": x, "y": y} for x, y in points], points.index((lane, line))


def _side_route(
    boxes: dict[str, Box], src: str, dst: str, labels: list[Box], segments: list[tuple[Point, Point]]
) -> tuple[list[Bend], int | None] | None:
    """Out of the source (its side facing the lane, else its bottom or top), along to a free lane,
    down the lane with the label on a row gap, and into the target (its side, else its top). The
    first choice that crosses no card, runs along no other line and crosses no other label wins;
    failing that, the first choice, which the check then reports. None when the target is not below."""
    source, target = boxes[src], boxes[dst]
    if target[1] < source[1] + source[3] + LABEL_GAP:
        return None
    (src_x, src_y), (dst_x, dst_y) = _centre(source), _centre(target)
    top, bottom = source[1] - LABEL_GAP, target[1] + target[3]
    lanes = _lanes(boxes, src_y, dst_y, (src_x, dst_x))
    wide_lanes = _lanes(boxes, top, bottom, (src_x, dst_x))
    lines = _gap_lines(boxes, top, bottom)
    fallback: tuple[list[Bend], int | None] | None = None
    for ends in ROUTE_ENDS:
        for lane in lanes if ends[0][0] == ends[1][0] == "side" else wide_lanes:
            for line in lines:
                route = _route_bends(boxes, src, dst, lane, ends, line)
                if route is None:
                    continue
                fallback = fallback or route
                if _route_is_clear(boxes, src, dst, *route, labels, segments):
                    return route
    return fallback


def _route_is_clear(
    boxes: dict[str, Box],
    src: str,
    dst: str,
    bends: list[Bend],
    label_index: int,
    labels: list[Box],
    segments: list[tuple[Point, Point]],
) -> bool:
    path = _path(boxes, src, dst, bends)
    label = _path_label(path, bends, label_index)
    own = list(itertools.pairwise(path))
    return (
        not _path_hits_cards(path, boxes, (src, dst))
        and not any(rects_overlap(label, other) for other in [*boxes.values(), *labels])
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

    A connection stays straight when its line and its label clear every other card. Otherwise it is
    routed by _side_route, clear of cards, of other connections' lines and of their labels.
    Duplicate pairs and self-loops keep their spread bends.
    """
    points = {key: (box[0], box[1]) for key, box in boxes.items()}
    spread = bends_for_connections(points, connections)
    result: list[tuple[list[Bend], int | None]] = [([], None)] * len(connections)
    labels: list[Box] = []
    segments: list[tuple[Point, Point]] = []
    blocked: list[int] = []
    for i, ((src, dst), bends) in enumerate(zip(connections, spread, strict=True)):
        if src not in boxes or dst not in boxes:
            continue
        path = _path(boxes, src, dst, bends)
        label = _path_label(path, bends, 0 if bends else None)
        if not bends and (
            _path_hits_cards(path, boxes, (src, dst)) or any(rects_overlap(label, box) for box in boxes.values())
        ):
            blocked.append(i)
            continue
        result[i] = (bends, 0 if bends else None)
        labels.append(label)
        segments.extend(itertools.pairwise(path))
    for i in blocked:
        src, dst = connections[i]
        route = _side_route(boxes, src, dst, labels, segments)
        if route is None:
            continue
        result[i] = route
        path = _path(boxes, src, dst, route[0])
        labels.append(_path_label(path, *route))
        segments.extend(itertools.pairwise(path))
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
    """One occupancy check for every card on a canvas, whatever its kind.

    Pass real card sizes (REAL_CARD_SIZES). With `connections`, each connection as route_connections
    draws it is an obstacle too: its label may not cross any card or other label, and no segment of
    its line (grown by STROKE_MARGIN) may cross a card other than its own two ends. `routes` checks
    connections as drawn elsewhere, such as the straight lines already on a canvas.
    """
    connections = connections or []
    routes = route_connections(boxes, connections) if routes is None else routes
    everything = {**boxes, **connection_label_boxes(boxes, connections, routes)}
    names = list(everything)
    for i, left in enumerate(names):
        for right in names[i + 1 :]:
            if rects_overlap(everything[left], everything[right]):
                raise ValueError(f"{left} at {everything[left][:2]} overlaps {right} at {everything[right][:2]}")
    drawn = []
    for (src, dst), (bends, index) in zip(connections, routes, strict=True):
        if src not in boxes or dst not in boxes:
            continue
        path = _path(boxes, src, dst, bends)
        if hit := _path_hits_cards(path, boxes, (src, dst)):
            raise ValueError(f"connection {src}->{dst} crosses {hit} at {boxes[hit][:2]}")
        drawn.append((f"{src}->{dst}", path, bends, index))
    _assert_lines_apart(drawn)


def _assert_lines_apart(drawn: list[tuple[str, list[Point], list[Bend], int | None]]) -> None:
    """No two connections run along one line, and no routed line crosses another connection's label."""
    labels = {name: _path_label(path, bends, index) for name, path, bends, index in drawn}
    for i, (name, path, bends, _index) in enumerate(drawn):
        segments = list(itertools.pairwise(path))
        for other, other_path, _bends, _other_index in drawn[i + 1 :]:
            for seg in segments:
                if any(_collinear_overlap(seg, other_seg) for other_seg in itertools.pairwise(other_path)):
                    raise ValueError(f"connection {name} runs along connection {other} from {seg[0]} to {seg[1]}")
        if not bends:
            continue
        for other, label in labels.items():
            if other != name and any(_segment_hits(p, q, label) for p, q in segments):
                raise ValueError(f"routed connection {name} crosses the label of connection {other}")


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
    """Topological order, and the edges that run forward in it. A cycle is broken at its earliest node."""
    known = set(nodes)
    unique = list(dict.fromkeys((s, d) for s, d in edges if s in known and d in known and s != d))
    waiting = dict.fromkeys(nodes, 0)
    for _src, dst in unique:
        waiting[dst] += 1
    order: list[str] = []
    done: set[str] = set()
    while len(order) < len(nodes):
        # ponytail: O(n^2) scan, fine for canvas-sized graphs
        node = next((n for n in nodes if n not in done and waiting[n] == 0), None)
        node = node or next(n for n in nodes if n not in done)
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
            shape[0] = (-self._pitch(key) / 2, self._pitch(key) / 2)
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
            widths = [self._pitch(kid) for kid in kids]
            left = -sum(widths) / 2
            for kid, width in zip(kids, widths, strict=True):
                put(subs[kid], left + width / 2, 1)
                left += width
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


def _spread(index: int, count: int) -> float:
    return (index - (count - 1) / 2) * BEND_SPREAD


def _self_loop_bends(origin: Point, index: int) -> list[Bend]:
    x = origin[0] + CARD_WIDTH + 48.0 + index * BEND_SPREAD
    return [
        {"x": x, "y": origin[1] + 24.0},
        {"x": x, "y": origin[1] + CARD_HEIGHT - 24.0},
    ]


def _overlap_bends(src: Point, dst: Point, index: int, count: int) -> list[Bend]:
    spread = _spread(index, count)
    mid_x = (src[0] + dst[0]) / 2 + CARD_WIDTH / 2 + spread
    y1 = src[1] + CARD_HEIGHT + ARROW_INSET
    y2 = dst[1] - ARROW_INSET if dst[1] > src[1] else src[1] - ARROW_INSET
    if y2 == y1:
        y2 = y1 + ROW_GAP / 2
    return [{"x": mid_x, "y": y1}, {"x": mid_x, "y": y2}]


def _bends_for_pair(src: Point, dst: Point, index: int, count: int) -> list[Bend]:
    if src == dst:
        return _self_loop_bends(src, index)
    if count == 1:
        return []
    return _overlap_bends(src, dst, index, count)


def bends_for_connections(
    positions: dict[str, Point],
    connections: list[tuple[str, str]],
) -> list[list[Bend]]:
    """Bend only exact 1:1 overlaps: duplicate (src, dst) pairs, plus self-loops."""
    totals: dict[tuple[str, str], int] = defaultdict(int)
    ranks: list[int] = []
    for pair in connections:
        ranks.append(totals[pair])
        totals[pair] += 1
    result: list[list[Bend]] = []
    for pair, rank in zip(connections, ranks, strict=True):
        src_pos = positions.get(pair[0])
        dst_pos = positions.get(pair[1])
        if src_pos is None or dst_pos is None:
            result.append([])
            continue
        result.append(_bends_for_pair(src_pos, dst_pos, rank, totals[pair]))
    return result


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


def positions_from_flow(flow: dict[str, Any]) -> dict[str, Point]:
    """Layout processors by connection graph; stack child process groups top-down below them.

    Every card goes through one Canvas: funnels, remote process groups and labels stay put and
    are placed first, the processor tree starts below them when it would otherwise land on one,
    and groups stack in flow order clear of everything placed before them.
    """
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
