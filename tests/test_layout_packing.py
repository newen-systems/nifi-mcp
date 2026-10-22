"""Block packing is a geometric translation, never an inferred flow dependency."""
import math
from copy import deepcopy
from itertools import combinations

import pytest

from nifi_mcp.packing import pack_blocks


def _bounds(blocks, offsets):
    return (max(offsets[cid][0] + size[0] for cid, size in blocks.items()),
            max(offsets[cid][1] + size[1] for cid, size in blocks.items()))


def _assert_clear(blocks, offsets, gap=64):
    assert offsets.keys() == blocks.keys()
    for x, y in offsets.values():
        assert x >= 0
        assert y >= 0
        assert x % 8 == y % 8 == 0
    for left, right in combinations(blocks, 2):
        x1, y1 = offsets[left]
        x2, y2 = offsets[right]
        w1, h1 = blocks[left]
        w2, h2 = blocks[right]
        assert (x1 + w1 + gap <= x2 or x2 + w2 + gap <= x1
                or y1 + h1 + gap <= y2 or y2 + h2 + gap <= y1), (left, right)


def test_ten_equal_groups_choose_three_columns_for_better_viewport_scale():
    blocks = {f"id-{i:02}": (384, 176) for i in range(10)}
    offsets = pack_blocks(blocks)
    _assert_clear(blocks, offsets)
    assert len({x for x, _ in offsets.values()}) == 3
    assert len({y for _, y in offsets.values()}) == 4
    width, height = _bounds(blocks, offsets)
    assert (width, height) == (1280, 896)
    assert max(width / (16 / 9), height) < max(1728 / (16 / 9), 656)


@pytest.mark.parametrize("size", [(384, 176), (1700, 200), (200, 1700), (31.3, 12.2)])
def test_single_block_needs_no_padding_or_fake_aspect_ratio(size):
    assert pack_blocks({"only": size}) == {"only": (0, 0)}


def test_empty_blocks():
    assert pack_blocks({}) == {}


def test_tall_cluster_fills_adjacent_space_without_being_split():
    blocks = {"cluster": (384, 1500), **{f"id-{i:02}": (384, 176) for i in range(10)}}
    offsets = pack_blocks(blocks)
    _assert_clear(blocks, offsets)
    width, height = _bounds(blocks, offsets)
    assert sum(w * h for w, h in blocks.values()) / (width * height) > 0.55
    assert any(y > offsets["cluster"][1] and y < offsets["cluster"][1] + 1500
               for cid, (_, y) in offsets.items() if cid != "cluster")
    # Translation preserves a pre-laid-out chain and its return-lane coordinates.
    local = [(0, 0), (0, 240), (0, 480), (360, 600)]
    dx, dy = offsets["cluster"]
    moved = [(x + dx, y + dy) for x, y in local]
    assert [(x - moved[0][0], y - moved[0][1]) for x, y in moved] == local


def test_wide_cluster_and_isolated_groups_fill_viewport():
    blocks = {"cluster": (1600, 240), **{f"id-{i:02}": (384, 176) for i in range(10)}}
    offsets = pack_blocks(blocks)
    _assert_clear(blocks, offsets)
    width, height = _bounds(blocks, offsets)
    assert abs(math.log((width / height) / (16 / 9))) < 0.25
    assert sum(w * h for w, h in blocks.values()) / (width * height) > 0.6


def test_mixed_clusters_pack_without_overlap():
    blocks = {"vertical": (384, 1200), "horizontal": (1400, 240),
              **{f"id-{i:02}": (384, 176) for i in range(8)}}
    offsets = pack_blocks(blocks)
    _assert_clear(blocks, offsets)
    width, height = _bounds(blocks, offsets)
    assert sum(w * h for w, h in blocks.values()) / (width * height) > 0.45


def test_reordered_input_mapping_has_identical_output_and_does_not_mutate():
    blocks = {"z": (640, 1200), "a": (1700, 220), "b": (384, 176), "c": (80, 80)}
    before = deepcopy(blocks)
    offsets = pack_blocks(blocks)
    assert blocks == before
    assert pack_blocks(dict(reversed(list(blocks.items())))) == offsets
    assert pack_blocks(blocks) == offsets
    assert list(offsets) == sorted(blocks)


def test_priorities_put_input_first_and_output_last_without_using_names():
    blocks = {"z": (384, 176), "a": (384, 176), "other": (384, 900), "middle": (1400, 240)}
    order = {"z": -1, "a": 1}
    offsets = pack_blocks(blocks, order=order)
    _assert_clear(blocks, offsets)
    assert offsets["z"][1] + blocks["z"][1] + 64 <= min(offsets[c][1] for c in ("middle", "other"))
    assert max(offsets[c][1] + blocks[c][1] + 64 for c in ("middle", "other")) <= offsets["a"][1]
    assert pack_blocks(dict(reversed(list(blocks.items()))), order=dict(reversed(list(order.items())))) == offsets
    assert order == {"z": -1, "a": 1}


def test_gap_and_nonintegral_dimensions_are_snapped_up():
    blocks = {str(i): (383.2, 175.2) for i in range(8)}
    offsets = pack_blocks(blocks, gap=33)
    _assert_clear(blocks, offsets, gap=40)


@pytest.mark.parametrize("ratio", [1 / 3, 1, 16 / 9, 4])
def test_alternative_viewport_ratios(ratio):
    blocks = {str(i): (200, 200) for i in range(12)}
    offsets = pack_blocks(blocks, target_ratio=ratio)
    _assert_clear(blocks, offsets)
    width, height = _bounds(blocks, offsets)
    assert abs(math.log((width / height) / ratio)) < 0.6


@pytest.mark.parametrize("size", [(0, 1), (1, -1), (float("nan"), 1), (1, float("inf"))])
def test_invalid_sizes_rejected(size):
    with pytest.raises(ValueError, match="dimensions"):
        pack_blocks({"bad": size})


@pytest.mark.parametrize("kwargs", [
    {"target_ratio": 0}, {"target_ratio": float("nan")}, {"target_ratio": float("inf")},
    {"gap": 31}, {"gap": float("inf")}, {"order": {"missing": 1}}, {"order": {"a": float("nan")}},
])
def test_invalid_options_rejected(kwargs):
    with pytest.raises(ValueError):
        pack_blocks({"a": (384, 176)}, **kwargs)


def test_output_cluster_can_share_bottom_shelf_with_isolated_groups():
    blocks = {**{f"id-{i}": (384, 176) for i in range(9)}, "source": (384, 336)}
    offsets = pack_blocks(blocks, bottom={"source"})
    _assert_clear(blocks, offsets)
    width, height = _bounds(blocks, offsets)
    assert offsets["source"][1] + 336 == height
    assert any(offsets[cid][1] + size[1] == height for cid, size in blocks.items() if cid != "source")
    assert (width, height) == (1280, 896)
    assert abs(math.log((width / height) / (16 / 9))) < 0.25
    assert len({x for x, _ in offsets.values()}) == 3
    banded = pack_blocks(blocks, order={"source": 1})
    band_width, band_height = _bounds(blocks, banded)
    assert max(width / (16 / 9), height) < max(band_width / (16 / 9), band_height)
    assert pack_blocks(dict(reversed(list(blocks.items()))), bottom={"source"}) == offsets


def test_multiple_different_height_bottom_blocks_align_without_overlap():
    blocks = {"a": (384, 336), "b": (640, 176), "c": (384, 1200), "d": (384, 176)}
    offsets = pack_blocks(blocks, bottom={"a", "b"})
    _assert_clear(blocks, offsets)
    _, height = _bounds(blocks, offsets)
    assert offsets["a"][1] + 336 == offsets["b"][1] + 176 == height


def test_bottom_anchors_respect_explicit_priority_bands():
    blocks = {"in": (384, 176), "out": (384, 336), "other": (384, 176)}
    offsets = pack_blocks(blocks, order={"in": -1}, bottom={"out"})
    _assert_clear(blocks, offsets)
    assert offsets["in"][1] + 176 + 64 <= min(offsets[c][1] for c in ("out", "other"))
    assert offsets["out"][1] + 336 == _bounds(blocks, offsets)[1]


def test_bottom_rounds_bounds_up_and_keeps_offsets_on_grid():
    blocks = {"out": (383.1, 335.1), "other": (383.1, 175.1)}
    offsets = pack_blocks(blocks, bottom={"out"})
    _assert_clear(blocks, offsets)
    rounded = {cid: (math.ceil(w / 8) * 8, math.ceil(h / 8) * 8) for cid, (w, h) in blocks.items()}
    assert offsets["out"][1] + 336 == _bounds(rounded, offsets)[1]


@pytest.mark.parametrize("kwargs", [
    {"bottom": {"missing"}},
    {"bottom": {"a"}, "order": {"a": -1}},
    {"bottom": {"a", "b"}, "order": {"b": 1}},
])
def test_invalid_bottom_anchors_rejected(kwargs):
    with pytest.raises(ValueError, match="bottom"):
        pack_blocks({"a": (384, 176), "b": (384, 176)}, **kwargs)


def test_global_boundary_allowance_changes_best_fit_without_padding_bodies():
    blocks = {f"id-{i:02}": (384, 176) for i in range(10)}
    ordinary = pack_blocks(blocks)
    offsets = pack_blocks(blocks, extra_height=160)
    _assert_clear(blocks, offsets)
    assert _bounds(blocks, ordinary) == (1280, 896)
    assert _bounds(blocks, offsets) == (1728, 656)
    assert len({x for x, _ in offsets.values()}) == 4
    assert len({y for _, y in offsets.values()}) == 3
    assert min(y for _, y in offsets.values()) == 0
    assert sorted({y for _, y in offsets.values()}) == [0, 240, 480]
    assert max(1728 / (16 / 9), 656 + 160) < max(1280 / (16 / 9), 896 + 160)
    assert pack_blocks(dict(reversed(list(blocks.items()))), extra_height=160) == offsets


def test_boundary_allowance_preserves_bottom_body_anchor():
    blocks = {f"id-{i:02}": (384, 176) for i in range(10)}
    offsets = pack_blocks(blocks, bottom={"id-00"}, extra_height=160)
    _assert_clear(blocks, offsets)
    assert _bounds(blocks, offsets) == (1728, 656)
    assert offsets["id-00"][1] + 176 == 656


def test_boundary_allowance_never_moves_single_block_or_changes_empty_result():
    assert pack_blocks({"only": (384, 176)}, extra_height=160.5) == {"only": (0, 0)}
    assert pack_blocks({}, extra_height=320) == {}
    blocks = {"a": (384, 176), "b": (384, 336)}
    assert pack_blocks(blocks, extra_height=0) == pack_blocks(blocks)


@pytest.mark.parametrize("height", [-1, float("inf"), float("-inf"), float("nan")])
def test_invalid_extra_height_rejected_even_for_empty_input(height):
    with pytest.raises(ValueError, match="extra_height"):
        pack_blocks({}, extra_height=height)


@pytest.mark.parametrize("anchors", [{"id-00"}, {"id-00", "id-01"}, {"id-09"}])
def test_equal_size_bottom_anchors_keep_full_first_rows(anchors):
    blocks = {f"id-{i:02}": (384, 176) for i in range(10)}
    offsets = pack_blocks(blocks, bottom=anchors, extra_height=160)
    _assert_clear(blocks, offsets)
    assert _bounds(blocks, offsets) == (1728, 656)
    row_counts = [sum(y == row for _, y in offsets.values()) for row in sorted({y for _, y in offsets.values()})]
    assert row_counts == [4, 4, 2]
    assert all(offsets[cid][1] + 176 == 656 for cid in anchors)
    assert pack_blocks(dict(reversed(list(blocks.items()))), bottom=anchors, extra_height=160) == offsets


def test_bottom_swap_preserves_exact_rectangles_of_unanchored_grid():
    blocks = {f"id-{i:02}": (384, 176) for i in range(10)}
    ordinary = pack_blocks(blocks, extra_height=160)
    anchored = pack_blocks(blocks, bottom={"id-00"}, extra_height=160)
    assert set(ordinary.values()) == set(anchored.values())
    changed = [cid for cid in blocks if ordinary[cid] != anchored[cid]]
    assert len(changed) == 2
    assert "id-00" in changed
