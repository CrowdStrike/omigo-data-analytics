# API Design & Versioning

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** API Design & Versioning

**Subtitle:** A public API is a contract with clients you cannot see or update — you may add to it forever, but you can never take anything away

## The Contract With 1,200 Strangers

**Tags:** `core idea` (blue), `Hyrum's law` (orange), `backward compatibility` (green)

- **The API** — a parcel-tracking endpoint `GET /parcels/{id}` returns `{id, status, weight, eta}`
- **The clients** — 1,200 integrations call it: two apps the team owns, 40 partner warehouses, the rest unknown
- **The asymmetry** — adding an optional field or endpoint breaks almost no clients; removing or renaming breaks almost all
- **The disguise** — tightening validation ("weight must now be > 0") is a breaking change wearing a safety vest
- **Hyrum's law** — one partner sorts parcels by the accidental order of the response array; even that is now contract

*Example (italic):* Renaming `weight` to `weight_kg` looks like a one-line cleanup — it would break 1,150 of the 1,200 integrations, and the team can only fix its own two.

**Key point:** With enough clients, every observable behavior of your API — documented or not — is depended on by someone; you can add freely, but never remove, rename, or change meaning.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart: six kinds of change to the parcel endpoint, ranked by how many of the 1,200 client integrations each one breaks.

- **Title (bold 15px, `#1a5276`, top center):** "Same Endpoint, Six Changes: How Many of 1,200 Clients Break".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, scale 1,200 clients = 420px; left-aligned 12px `#444` row labels at x=20.
- **Rows (16px-tall bars, top to bottom at y = 70, 105, 140, 175, 210, 245):**
  - "add optional field": width 0, green `#008300` 12px label "0 break" at x=256
  - "add new endpoint": width 0, green `#008300` 12px label "0 break" at x=256
  - "tighten validation": orange `#d95926` bar width 63, 11px label "180"
  - "make optional field required": orange `#d95926` bar width 143, 11px label "410"
  - "change unit (kg → lb)": red `#e74c3c` bar width 301, 11px label "860"
  - "remove or rename a field": red `#e74c3c` bar width 402, 11px label "1,150"
- **Bar style:** safe rows get a 3px green tick at the baseline; breaking bars solid fill at 0.85 alpha.
- **Annotation (bold 13px `#1a5276`, upper right near x=480, y=60):** "adding is free — taking away never is".
- **Caption (12px `#444`, bottom right):** "client counts illustrative".

## Renaming a Field Without Renaming It

**Tags:** `worked example` (blue), `deprecation` (orange), `dual-write` (green)

- **The goal** — replace ambiguous `weight` with explicit `weight_kg` without breaking anyone
- **Step 1: add** — ship `weight_kg` alongside `weight`, dual-writing the same value to both fields
- **Step 2: announce** — mark `weight` deprecated in docs and headers; new integrations get only the new name
- **Step 3: monitor** — log which API keys still read `weight`; by month 6, 520 of 1,200 clients still do
- **Step 4: contact** — at month 8 the team emails the 260 stragglers directly; 90 never respond or migrate

*Example (italic):* Twelve months after the announcement, 1,110 clients read `weight_kg` — but 90 still read `weight`, so the "renamed" field is never actually deleted.

**Key point:** A rename is really add-new, dual-write, deprecate-old — and deprecation is a monitored process with a long tail, not a delete-key event.

### Visualization (canvas `c2`, 720×300)

Line chart of the 12 months after the deprecation announcement: clients reading the old `weight` field (falling) vs clients reading the new `weight_kg` field (rising), with process markers.

- **Title (bold 15px, `#1a5276`, top center):** "Deprecating `weight`: 12 Months of Dual-Running, 90 Clients Never Leave".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12, 12px `#444` tick labels every 2 months; y = clients 0 to 1,200, gridlines `#e5e9ef` at 300/600/900.
- **Old-field line:** red `#e74c3c` 3px line through months `[0, 1, 2, 4, 6, 8, 10, 12]`, clients `[1200, 1200, 1150, 900, 520, 260, 140, 90]`.
- **New-field line:** green `#008300` 3px line through the same months, clients `[0, 60, 300, 640, 900, 1050, 1100, 1110]`.
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at month 0 labeled "announce + dual-write" and month 8 labeled "email stragglers" (12px `#6b7280`, at top of each line).
- **Labels:** bold 12px red "still reading weight" near (month 3, y≈95); bold 12px green "migrated to weight_kg" near (month 9, y≈95 in chart space above the green line).
- **Annotation (bold 13px `#4a3aa7` violet, near month 11, y=210):** "the long tail: 90 clients, forever".
- **Caption (12px `#444`, bottom right):** "migration counts illustrative".

## Three Ways to Version — and Their Bills

**Tags:** `where it's used` (blue), `versioning strategies` (green)

- **URL versions** — `/v2/parcels` is explicit and cache-friendly, but forks the surface: v1 never dies
- **Header versions** — `Api-Version: 2024-06` hides the fork in a header; same maintenance bill, less visible
- **Additive-only** — one version forever, evolved by adding fields; largely what the big API providers practice
- **The bill** — with URL versioning the team maintains 150 endpoints by year 3; additive-only holds at 58
- **The trade** — versioning buys freedom to redesign; additive-only buys one surface but bans every rename

*Example (italic):* Two years after shipping `/v2/`, the traffic dashboard shows 30% of calls still hitting `/v1/` — the team now fixes every bug twice.

**Key point:** A new version is not an escape hatch from old clients — they stay on the old version, so every version you ship is a surface you maintain until its last client leaves.

### Visualization (canvas `c3`, 720×300)

Line chart over three years: total live endpoints the team must maintain under URL versioning (forking) vs additive-only evolution (one surface).

- **Title (bold 15px, `#1a5276`, top center):** "The Maintenance Bill: Version Forks vs One Evolving Surface".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 0 to 3, 12px `#444` tick labels at each year; y = live endpoints maintained 0 to 160, gridlines `#e5e9ef` at 40/80/120.
- **URL-versioning line:** red `#e74c3c` 3px line through years `[0, 1, 2, 3]`, endpoints `[40, 78, 115, 150]` — v1 stays live while v2 and v3 pile on.
- **Additive-only line:** green `#008300` 3px line through the same years, endpoints `[40, 46, 52, 58]` — one surface, new endpoints added.
- **Markers:** vertical dashed `#6b7280` (dash 4/3) lines at year 1 labeled "v2 ships" and year 2 labeled "v3 ships" (12px `#6b7280` labels at top).
- **Labels:** bold 12px red "every version stays live" near (year 2.2, above the red line); bold 12px green "one version, evolved" near (year 2.2, below the green line).
- **Annotation (bold 13px `#1a5276`, upper left near x=90, y=60):** "old versions never die — you maintain them all".
- **Caption (12px `#444`, bottom right):** "endpoint counts illustrative".

## The Bare Array You'll Regret

**Tags:** `common mistake` (red), `design for evolution` (orange)

- **The mistake** — v1 returns a bare JSON array `[ {...}, {...} ]`; there is nowhere to put anything new
- **The wall** — adding pagination later means changing the response's shape, which breaks every client at once
- **The envelope** — `{ "items": [...] }` costs one wrapper today and gives every future field a home
- **Ids over indexes** — clients that reference "parcel #3 in the list" break on any reorder; stable ids don't
- **Units in names** — `weight_kg` and `timeout_ms` make the meaning part of the name, so it can't drift

*Example (italic):* The team that shipped an envelope adds `next_cursor` and `total` in v1 with zero breakage; the bare-array team is stuck writing `/v2/` for pagination alone.

**Common mistake:** Designing only for today's response. Pagination, envelopes, ids, and explicit units cost almost nothing on day one — retrofitting any of them is a breaking change.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a bare-array response hitting the pagination wall (breaks everyone) vs an envelope response absorbing new fields (breaks no one).

- **Title (bold 15px, `#1a5276`, top center):** "Day-One Shape Decides Whether Pagination Breaks Everyone".
- **Row 1 (boxes centered on y=95), label 12px `#444` at x=20:** "bare array"; blue `#2a78d6` rounded box at x=150 labeled "[ {...}, {...} ]" (12px), 3px arrow to a red `#e74c3c` box at x=390 labeled "paging needs a new shape" with bold 12px red "✗ every client breaks" at x=580.
- **Row 2 (boxes centered on y=205), label:** "envelope"; blue box at x=150 labeled "{ \"items\": [...] }", 3px arrow to a green `#008300` box at x=390 labeled "add next_cursor, total", then bold 12px green "✓ old clients ignore new fields" at x=580.
- **Box style:** 160–180px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text, 1.5px matching borders.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "one wrapper today buys every field you'll ever need".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); client counts, migration curves, and endpoint tallies are invented and labeled illustrative; text numbers (1,200 / 1,150 / 520 / 260 / 90 / 1,110 / 150 / 58) must match the chart arrays exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
