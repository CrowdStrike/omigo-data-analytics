# Search Engine

**Page type:** detail page (obj-table layout: one row per section, text left 45%, canvas right 55% centered)
**HTML title tag:** Search Engine — Collect, Use, Keep, Return

**Subtitle:** Every query is a confession log — health worries, finances, intentions — tied to location and click trails.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** every query you submit — over years this is an intent log covering health scares, money problems, relationships, and things you told no one.
- **Incidental:** location per query (IP or GPS); which results you clicked *and which you were shown but didn't click*; time between query and click; voice query audio; device, browser, and session identifiers.
- **Incidental (surprising):** queries typed but never submitted can register as autocomplete telemetry — each keystroke is a request to the suggestion service.
- **Cross-site:** the same operator's ad network sees your browsing on unrelated sites, joinable to the query log.
- **Inferred:** demographics (age band, gender, income), interest segments, "in-market" purchase-intent labels — derived, never typed.

> **Key point:** Most surprising: the query you typed, stared at, and deleted without pressing Enter may still have been transmitted keystroke by keystroke.

### Visualization (canvas `c1`, 720×430)

Grouped horizontal bar chart: assumed vs realistic collection extent per category.

- **Title (bold 13px `#1a5276`, centered):** "What people assume is collected vs realistic extent (illustrative)"
- **Legend:** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent" (11px `#2c3e50`).
- **Rows** (label, assumed a, realistic b; 0–100 scale, right-aligned 12px labels at x=195, bars start x=205, max width 430, bar height 13, inner gap 3, group gap 15, start y=52; gray `#999` 10px value labels at bar ends):
  - Submitted queries: 80 / 95
  - Queries typed, never sent: 5 / 50
  - Clicked results: 40 / 90
  - Results shown, NOT clicked: 5 / 70
  - Location per query: 25 / 85
  - Voice query audio: 15 / 60
  - Cross-site browsing (ad net): 20 / 90
  - Inferred demographics / intent: 15 / 90
- **Footer caption (gray `#999` 11px, centered):** "Numbers are illustrative — they show the shape of the gap, not measured values."

## How it gets used

- **Provide the service:** answer the query, spell-correct, localize results.
- **Rank & personalize:** your click history and location tune which results you see; unclicked results teach the ranker what failed.
- **Autocomplete:** aggregate query streams power suggestions — your half-typed queries train them.
- **Ad targeting & measurement:** query intent is the most valuable ad signal that exists; search terms map directly to purchase intent.
- **Model training:** query-click pairs are the core training data for ranking and language models.
- **Sharing:** advertisers see the query that triggered their ad; trend products expose aggregates.

### Visualization (canvas `c2`, 720×330)

Flow diagram: data-category boxes on the left funnel into a central hub, which fans out to use boxes on the right. Boxes have colored 1.5px stroke, 12%-alpha fill of the same color, and bold 11px centered colored text; arrows are gray `#bbb` 1.5px lines with filled arrowheads.

- **Title (bold 13px `#1a5276`, centered):** "From data category to use"
- **Left boxes** (x=20, 175×46):
  - y=40, `#1a5276`: "Query log" / "incl. voice + half-typed"
  - y=110, `#2980b9`: "Click trail" / "clicked + not clicked"
  - y=180, `#e67e22`: "Location + device"
  - y=250, `#e74c3c`: "Cross-site browsing" / "via ad network"
- **Hub box** (x=280, y=130, 165×66, `#8e44ad`): "Intent profile +" / "inferred segments". All left boxes arrow into the hub.
- **Right boxes** (x=530, 175×42, hub arrows out to each):
  - y=34, `#27ae60`: "Answer the query"
  - y=96, `#1a5276`: "Ranking +" / "personalization"
  - y=158, `#2980b9`: "Autocomplete"
  - y=220, `#e74c3c`: "Ad targeting +" / "measurement"
  - y=284, `#e67e22`: "Model training · trends"

## How long it's kept

- **Active account:** raw query logs commonly kept many months to years; some categories partially anonymized on a schedule (IP truncation after ~9-18 months is a common pattern).
- **"Delete history":** removes entries from *your view* immediately; server-side copies follow a slower deletion pipeline.
- **De-identified copies:** logs stripped of the account link are often kept indefinitely for research and ranking.
- **Backup tail:** deleted logs persist in backups for an extra window.
- **Ad interaction and billing records:** longer retention for audit; "as required by law" buckets are effectively indefinite.
- **Identifiable vs de-identified:** the two clocks run in opposite directions — logs tied to your account get the shorter, scheduled windows, while the stripped copies carry the longest retention. The catch: removing the account link does not always prevent re-identification from the queries themselves.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars, one per data category, with a dashed "delete history / account deleted" vertical marker.

- **Title (bold 13px `#1a5276`, centered):** "How long each category lives (illustrative)"
- **Plot:** bars start at x=215, plot width 470, first row y=44, bar height 22, gap 16; bars filled at 45% alpha of their color; rows ending at 1.0 get a solid arrowhead (= indefinite); gray `#999` 10px note next to each bar; right-aligned 12px `#2c3e50` row labels.
- **Rows** (label, end fraction, color, note):
  - History in your view, 0.5, `#27ae60`, "gone on delete"
  - Raw query logs (server), 0.68, `#2980b9`, "slow pipeline"
  - Partially anonymized logs, 0.82, `#e67e22`, "IP truncated"
  - Backup copies, 0.75, `#e67e22`, "backup tail"
  - De-identified copies, 1.0, `#8e44ad`, "indefinite"
  - Ad billing / legal holds, 1.0, `#e74c3c`, "indefinite"
- **Marker:** dashed (5/4) red `#e74c3c` 1.5px vertical line at 50% of plot width, bold 11px red label below: "\"delete history\" / account deleted". Gray 11px axis labels: "first query" (left), "years / indefinite →" (right).

## What you get back

- **In a typical export:** your submitted query history, clicked results, saved settings, voice recordings you can review.
- **Not returned:** impressions of results you didn't click, autocomplete keystroke telemetry, inferred demographic and in-market segments, the cross-site browsing profile from the ad network, de-identified log copies, internal quality and abuse scores.

> **Key point:** The asymmetry: the export shows the questions you asked — not the profile of who the system now believes you are, which is the part that gets monetized.

### Visualization (canvas `c4`, 720×340)

Two side-by-side panels comparing export contents vs retained-but-not-returned data. Panels are 320px wide, y=34, height 280, 8%-alpha fill + 2px stroke of panel color, bold 13px colored title, 11px `#2c3e50` item lines at 24px spacing.

- **Title (bold 13px `#1a5276`, centered):** "The export vs what actually exists"
- **Left panel (x=30, green `#27ae60`) "IN THE EXPORT":** Submitted query history / Clicked results / Voice recordings (reviewable) / Saved settings & preferences
- **Right panel (x=380, red `#e74c3c`) "EXISTS BUT NOT RETURNED":** Unclicked result impressions / Autocomplete keystroke telemetry / Inferred demographics / segments / "In-market" intent labels / Cross-site ad-network profile / De-identified log copies / Internal quality / abuse scores
- **Footer caption (gray `#999` 11px, centered):** "You get the questions back — not the answers the system derived about you."

## Regeneration instructions

- **Template/layout:** platform-privacy-policies detail page. h1, `.subtitle`, one `.disclaimer` callout, then a single `.obj-table` (full-width, border-collapse) with four `<tr>` rows — one per section (collected / used / kept / returned). Left `<td>` (45%) holds `.obj-title` + `<ul>` bullets + optional `.key-point` box; right `<td>` (55%, text-align center) holds the canvas.
- **Page CSS:** body -apple-system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. No nav bar, no back/home links.
- **Palette:** `#1a5276` primary blue, `#2980b9` secondary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, gray `#999`/`#666`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper.
- In regenerated HTML, any card links use `.html` extensions.
