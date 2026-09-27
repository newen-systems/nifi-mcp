---
title: Canvas layout alternatives for forks and multi-row connections
type: reference
status: accepted
tags: [nifi, mcp, layout]
updated: 2026-09-28
---

# Canvas layout alternatives

The layout in `src/nifi_mcp/layout.py` went through several shapes for forks and for
connections that span more than one row. This note keeps the ones that were set aside,
why, and when each is worth bringing back. The current formulas are in the `layout.py`
docstring and are not repeated here. Changes are named by the commit subject that made them during development.

## Kept design

### G. Perpendicular side children on the fork row (current)

At a fork the main child (deepest subtree, then the one feeding the fork's join, then
relationship order) continues one row down on the parent's axis. The other children keep
relationship order left to right through it. The nearest child on each side sits on the
fork's row, joined by a horizontal line, at least a side pitch out (672 between two
processors: half of each card plus the 240px label and 40px either side, on the 8px
snap). Any further child on that side drops one row, in its own column past the inner
child's family, and is reached by a routed line. A fork whose children are all leaves
has no main child and uses layout A.

Adopted in "Put a fork's side branches on the fork's row" after the demo fork's side branches were moved onto the fork's
row by hand and the overlap went away without a bend. Refined in "Keep one side child per side on a fork row" (one side child
per side per row) and "Give a fork's horizontal line room for its arrowhead" (40px label margin so the arrowhead has room).

## Set aside, with a reason to bring back

### A. Fork children side by side one row down, centred as a family

The first dimension-derived fork layout ("Derive the processor layout from NiFi's real card sizes"). A fork's children sit one row down,
one sibling pitch (512) apart, centred as a family on the parent. Nested forks sit
inside their share of the family width.

Set aside for forks with a continuing branch because a shallow branch next to a deep one
leaves the join edge spanning two rows, crossing the card between (the demo fork's
tag-level2 to MergeContent crossed log-region0). Still used for a fork whose children
are all leaves, where no join or deep branch exists.

Bring back if the canvas must stay narrow (side children cost at least 672 per side
against 512 per sibling here), or if routed lines become more objectionable than wide
forks.

### B. Routed bends for a connection spanning more than one row

Introduced as the primary fix in "Route a connection around cards it would cross". A connection keeps its straight line only when
the line, grown by 8px, and its label clear every other card. Otherwise it is routed down
a free lane: the middle of a gap between card columns, or past the outermost. First
version dropped from the source's centre into the row gap and entered the target from
above. "Route around cards from the side, never along another line" moved exits and entries to the card sides (see E).

Now the fallback: layout G avoids most multi-row edges, and `route_connections` only
fires for a line or label that would still cross a card. The dropped side child of G is
always reached this way. Routing is in a fixed order ("Route connections in a fixed order") so the result does not
depend on the order NiFi lists connections.

Keep it as the safety net. Bring it back as primary only if a layout is chosen that
lets branches of different depth sit in one row again.

### C. Move the shallow branch's source down a row instead of routing

Considered in "Route a connection around cards it would cross" and rejected. Moving tag-level2 one row lower would make
route-level to tag-level2 span two rows, so one connection needs bends either way. It
moves the problem up one edge rather than removing it.

Bring back only for a fork whose parent edge is already routed for another reason, where
the extra row costs nothing.

### D. Strict left/right alternation of side children

Tried in "Put a fork's side branches on the fork's row" (the first side child on the side that keeps relationship order, later
ones alternate). Rejected in "Keep one side child per side on a fork row": on demo-multifork-flow's 4-way route-kind fork it
put log-kind0 and tag-kind3 both left of route-kind on its row, so the straight line to
tag-kind3 and its label ran through log-kind0, and the left-to-right order read kind3,
kind0, kind1, kind2.

Bring back only if relationship order stops mattering to readers and every side child
is guaranteed its own row.

### E. Routed connections leaving from the card centre

The first router ("Route a connection around cards it would cross") dropped from the source's centre into the row gap below.
Replaced in "Route around cards from the side, never along another line" because the routed tag-level2 to MergeContent line then dropped
straight down on top of tag-level2 to log-level2, with its label over that connection's
label. Routed lines now leave the source's side facing the lane, 16px out on the card's
centre line, and enter the target's side the same way. A taken side line steps 24px off
the centre line. "Give a fork's horizontal line room for its arrowhead" made the router try centre lines on every lane before stepping
off any.

Bring back only for a card with a single outgoing connection, where no collinear overlap
is possible and the centre exit reads cleaner.

### F. Row pitch per card type

"Centre every card on its column axis and size rows by the label" set row pitch from each row's real card height: card + 79px label + 16px above
and below, rounded up to the 8px snap. That gave 240 for processor rows and 288 for
child group rows. "Keep one 112px gap between stacked cards of any type" replaced it with one constant gap, 112 = grid(79 + 2 * 16),
added to the tallest card in the row, siblings top-aligned. Rounding the whole pitch
widened the gap under any card whose height is off the snap, so the gap depended on the
card type. For NiFi's 128, 48 and 176px cards the two agree.

Bring back if NiFi card sizes change in a way that needs per-type label clearance, for
example a card type whose connection labels are drawn larger.

## Open option

### H. Keep an extra side child inside its own family on demo-multifork-flow

In G a further side child drops a row into its own column past the inner child's family.
On demo-multifork-flow the route-kind to tag-kind3 line then leaves route-kind's top,
crosses above spread and enters tag-kind3's side: a long route around the outside.

The alternative, not built: place the extra side child inside the inner child's family
(under it, before that family's own children), so its line stays short and local. The
cost is that the inner family grows by a row and the extra child reads as part of that
family rather than a sibling. Try it if long outside routes show up on real flows more
than on the demo.
