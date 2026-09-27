---
title: Natural-language NiFi flow building
type: design
status: active
tags: [nifi, mcp]
updated: 2026-08-28
---

# Natural-language flow building

The MCP is a **workflow surface**, not a 1:1 REST dump. Agents should issue one `nifi_apply_flow_spec` after discovering types, not 40 `create_processor` calls.

## Loop

```
user request
  → nifi_about / nifi_current_user
  → nifi_list_processor_types + nifi_get_processor_definition
  → nifi_apply_flow_spec (new unversioned PG)
  → nifi_get_health
  → nifi_schedule_process_group
  → on failure: nifi_get_bulletins, nifi_list_queue, nifi_get_processor
```

## Safety

- Writes are on. `NIFI_READONLY=true` blocks create/update/delete/schedule.
- Canvas: top-down, forks sideways, pitches derived from real card sizes (see the layout.py docstring), 512x240 for processors. Cards are ~420x200;
  280px horizontal overlaps them. Every gap between stacked cards is 112px: the tallest connection label
  plus 16px either side, on NiFi's 8px snap. A row's pitch is its tallest card plus that gap. Every card is centred on its column axis.
  Bends only for exact 1:1 (source, destination) overlaps, and to route a line or label that would cross a card out of its source's side, down a free lane and into its target's side. Child process groups stack top-down in flow order on a 424x288 lattice.
- Compact JSON by default. Secrets redacted.
- GET may retry 503. POST/PUT/DELETE do not.
- If a deployment reconciles versioned process groups from a registry, prototype unversioned.

## Out of scope for v0.1

- NiFi Registry classic (deprecated here).
- Knox.
- A bundled chat UI.
- Provenance query UI (add when a debug loop needs it).
