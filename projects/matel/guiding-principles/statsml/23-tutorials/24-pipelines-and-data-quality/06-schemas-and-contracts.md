# Schemas & Contracts

**Page type:** detail page (tutorial page: card-sections, each with a two-column layout table — text left 50% with tag pills / bullets / example / key-point, canvas right 50%)
**HTML title tag:** Schemas & Contracts

**Subtitle:** A schema is a promise between teams about what the data looks like — a written contract plus a check catches a broken promise at the border, not in the CEO's dashboard

## The Report That Broke at 6am

Tags: `core idea` (blue), `running example` (green)

- **The promise** — every order event has a field called `amount` with the price in it
- **The change** — the app team renames `amount` to `total` in a routine Tuesday deploy
- **The silence** — the 1am pipeline loads the events without complaint; nothing checks names
- **The blast** — at 6am, every downstream revenue report shows $0
- **The definition** — a schema is the written list of fields and types a team promises to send

*Example:* The app team renamed one field for code cleanliness; four dashboards two teams away went blank overnight.

**Key point:** A schema is a promise between teams. Renaming a column breaks the promise — even when everyone acted in good faith.

### Visualization (canvas `c1`, 720×300)

Annotated event timeline: one night, hour by hour, from break to discovery.

- **Title (bold 16px, `#1a5276`, top center):** "One Night, Hour by Hour: Where It Broke vs Where It Was Noticed".
- **Axis:** horizontal 2px `#6b7280` line at y=165 from x=60 to x=680 spanning 22:00→10:00 (12 hours); tick labels "22:00", "00:00", "02:00", "04:00", "06:00", "08:00", "10:00" (12px `#6b7280`).
- **Events** (6px filled dot on the axis, thin leader line to a two-line label — line 1 bold 12px, line 2 12px, in the event color; alternating above/below the axis):
  - t=23:04 (above, violet `#4a3aa7`): "23:04 deploy:" / "amount → total"
  - t=01:00 (below, blue `#2a78d6`): "01:00 pipeline runs" / "no error raised"
  - t=06:00 (above, red `#e74c3c`): "06:00 dashboards" / "show $0"
  - t=09:30 (below, orange `#d95926`): "09:30 data team" / "starts debugging"
- **Gap bracket:** red (`#e74c3c`) square bracket at y≈70 spanning from the deploy to the 6am event, with bold 13px red label above: "7 silent hours between the break and anyone noticing".
- **Caption (italic 12px `#6b7280`, bottom center):** "illustrative timeline".

## One Order, Before and After the Rename

Tags: `worked example` (green), `common mistake` (red)

- **Monday's event** — `{ order_id: 1841, amount: 25.00 }`; 12 such orders sum to $482
- **Wednesday's event** — `{ order_id: 1907, total: 25.00 }`; same money, new name
- **The reader** — the report still asks each event for `amount` and gets nothing back
- **The math** — in many pipelines the missing field reads as null, so the sum shows $0, not an error
- **No alarm** — to the pipeline, a missing field is just an empty answer

*Example:* Monday: 12 orders, $482 on the dashboard. Wednesday: 13 orders in the table, $0 on the dashboard.

**Key point:** The money never disappeared — every dollar is still in the events, under `total`. Only the promise about the name broke.

This section's viz cell holds both canvases side by side in a `.viz-pair` flex row.

### Visualization (canvas `c2a`, 310×300)

Two JSON record cards, before and after the rename.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Order, Two Days Apart".
- **Record boxes:** 282×84, fill `#f8f9fa`, 2px stroke in the title color, monospace 13px body text (`#2c3e50`), highlighted line bold in the title color:
  - Box 1 (green `#008300`), caption above: "Monday (12 such orders, $482)"; lines `{ "order_id": 1841,` / ` "amount": 25.00,` (highlighted) / ` "ts": "…" }`.
  - Box 2 (orange `#d95926`), caption: "Wednesday (after the deploy)"; lines `{ "order_id": 1907,` / ` "total": 25.00,` (highlighted) / ` "ts": "…" }`.
- **Annotation between the boxes (bold 12px `#e74c3c`, centered, two lines):** "the report still asks for \"amount\"" / "→ nothing comes back".

### Visualization (canvas `c2b`, 310×300)

Bar chart of daily dashboard revenue with the silent $0 days.

- **Title (bold 15px, `#1a5276`, top center):** "Dashboard Revenue by Day".
- **Data:** days ["Mon", "Tue", "Wed", "Thu"], values `[482, 505, 0, 0]`; y scale max 550.
- **Bars:** 40px wide; non-zero bars fill `rgba(42,120,214,0.45)` with 1.5px `#2a78d6` stroke; zero bars drawn as a 3px red (`#e74c3c`) stub. Value labels "$482", "$505", "$0", "$0" bold 12px above bars (red for zeros); day labels 12px below.
- **Axes:** L-shaped axis in `#6b7280`, left padding 46, bottom padding 46.
- **Annotations:** bold 13px red at y=46: "silent $0 — no error, no alert"; italic 12px `#6b7280` bottom center: "deploy happened Tuesday night".


## Catch It at the Border, Not in the Dashboard

Tags: `rule of thumb` (orange), `where it's used` (blue)

- **The contract** — a small file both teams sign: field names, types, units, nullable or not
- **The check** — at 1am, before loading, compare incoming events against the contract
- **The failure** — Wednesday's events fail loudly: expected `amount`, got `total`
- **The alert** — the check pages the producing team at 1:05am and keeps yesterday's good data
- **The payoff** — one loud failure at the border replaces a quiet lie in every report

*Example:* Same rename with a contract in place: one alert to the app team at 1:05am, zero broken dashboards.

**Key point:** Data breaks at the border between teams. Put the check where the break happens, not where it finally gets noticed.

### Visualization (canvas `c3`, 720×300)

Two-lane flow diagram comparing pipelines without and with a contract check.

- **Title (bold 16px, `#1a5276`, top center):** "Where the Failure Surfaces: No Contract vs Contract at the Border".
- **Lane 1** (label "WITHOUT A CONTRACT", bold 12px `#6b7280`, y≈58): boxes 34px tall connected by gray arrows — "app events" (violet `#4a3aa7`, fill `#f4f2fb`), "pipeline" (blue `#2a78d6`, fill `#eef4fc`), "warehouse" (aqua `#199e70`, fill `#eaf7f2`), "dashboard" (red `#e74c3c`, fill `#fdecea`). Right-side red bold 12px note, two lines: "break rides through — found" / "at 6am, by the CEO".
- **Lane 2** (label "WITH A CONTRACT CHECK AT THE BORDER", y≈168): "app events" (violet) → "contract check" (orange `#d95926`, fill `#fdf3ec`) with a thick red X drawn after it; then a grayed-out "warehouse" box (`#6b7280`, fill `#f5f5f5`) and a green "dashboard" box (`#008300`, fill `#eef8ef`) with green bold caption "keeps yesterday's good data". A dashed orange feedback arrow loops from the contract check back to "app events", labeled bold 12px orange: "1:05am alert: \"expected amount, got total\"".
- **Bottom annotation (bold 13px `#1a5276`, centered):** "same rename, same night — one loud failure at the border instead of a quiet lie downstream".

## Same Name, New Meaning — the Sneakier Break

Tags: `common mistake` (red), `rule of thumb` (orange)

- **Names are not enough** — `amount` can stay `amount` and switch from dollars to cents
- **The symptom** — Wednesday's revenue reads $49,800 instead of the usual ~$500
- **Type changes** — the number 25.00 becoming the text "25.00" breaks math just as quietly
- **Nullability** — a field that was always filled starts arriving empty for some rows
- **Contract depth** — a good contract checks names, types, units, and sane value ranges

*Example:* A name-only check waved the cents change through; a range check ("daily total under $5,000") caught it.

**Key point:** Check values, not just names. The dangerous change is the one that still loads without error.

### Visualization (canvas `c4`, 720×300)

Bar chart of the ~100x unit change plus a check-result panel.

- **Title (bold 16px, `#1a5276`, top center):** "The Column Kept Its Name — the Unit Changed Underneath".
- **Data:** days ["Mon", "Tue", "Wed"], values `[482, 498, 49800]`; y scale max 52,000; plot width 430px starting at left padding 80.
- **Bars:** 90px wide; Mon/Tue fill `rgba(42,120,214,0.45)` with `#2a78d6` stroke; Wed solid red `#e74c3c`. Value labels "$482", "$498", "$49,800" bold 12px above (red for Wed); day labels below.
- **Annotation over the bars (bold 13px `#e74c3c`):** "~100x: \"amount\" now carries cents, not dollars".
- **Right panel** (starting x≈555): heading bold 13px `#1a5276` "What each check says:"; then in 12px:
  - green (`#008300`) "✓ name check:" with `#2c3e50` detail "\"amount\" present"
  - green "✓ type check:" with detail "still a number"
  - bold red "✗ range check:" with red details "daily total $49,800" / "exceeds $5,000 limit"
- **Caption (italic 12px `#6b7280`, bottom center):** "only the value-range rule in the contract catches a unit change".

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) + a layout table. Every section uses `table.layout` (`td.text-col` 50% / `td.viz-col` 50%); section 2 places canvases `c2a`/`c2b` (310×300 each) side by side inside its single viz cell, wrapped in a `.viz-pair` flex row (`display:flex; gap:10px`, each canvas `flex:1 1 0; min-width:0`). Text cell order: `.tags` pill row, `<ul>` bullets (each starting with `<b>bold term</b>` in `#1a5276`; inline `<code>` for field names), italic `.example`, `.key-point` callout.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Tag pills:** `.tag` inline-block 0.72rem bold, padding 2px 10px, radius 10px; blue = bg `rgba(26,82,118,0.12)` / `#1a5276`, green = `rgba(39,174,96,0.15)` / `#27ae60`, red = `rgba(231,76,60,0.12)` / `#e74c3c`, orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Key-point callout:** background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Canvas:** intrinsic width/height read from attributes; shared `setup(id)` helper scales by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates). Monospace chart text uses `ui-monospace, Menlo, monospace`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`, red `#e74c3c`. Doc palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions (this page has none).
