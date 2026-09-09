# Video Streaming

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Video Streaming — Collect, Use, Keep, Return

**Subtitle:** Watch time, pause points, abandonment — a taste and attention profile built from what you almost finished.

**Disclaimer callout:** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** profile names, ratings and likes, watchlist additions, search queries, language and subtitle choices.
- **Incidental:** watch time per title down to the second; every pause, rewind, skip, and the exact point you abandoned; time-of-day and binge patterns; what you hovered on but never played; device, network, and playback-quality telemetry.
- **Inferred:** taste and mood clusters; household members from profiles and devices; attention and churn-risk scores; sensitive interests read from viewing choices.

**Key point (callout box):** Most surprising: hovering on a title without ever pressing play is logged — and the thumbnail you see may be an experiment run specifically against your taste cluster.

### Visualization (canvas `c1`, 720×400)

Grouped horizontal bar chart: assumed vs realistic collection extent, two bars per row.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x≈180 and x≈280, 14×10 swatches):** "assumed" — fill `rgba(26,82,118,0.35)`; "realistic extent" — fill `rgba(231,76,60,0.55)`. Legend text 11px `#2c3e50`.
- **Rows** (label, assumed %, realistic %): Titles watched 85/95; Search queries 55/90; Pause / rewind / abandon points 15/90; Hovered but never played 5/75; Time-of-day / binge patterns 20/85; Household members via profiles 15/70; Taste / mood clusters 10/85; Thumbnail experiments on you 5/65.
- **Layout:** right-aligned labels at x=215 (11px `#2c3e50`), bars start at x=225, max width 430px (scale 0–100), bar height 11px, assumed bar on top, realistic bar 3px below, group spacing 40px, first group at y=52.
- **Caption (bottom center, 10px `#999`):** "Numbers are illustrative — they show the shape of the gap, not measured values."

## How it gets used

- **Provide the service:** playback, resume points, adaptive quality per device and network.
- **Rank / recommend:** the home screen is rebuilt per profile from watch, abandon, and hover signals.
- **Artwork experiments:** multiple thumbnails per title are tested against your cluster; you and a neighbor see different covers for the same show.
- **Content decisions:** abandonment curves decide what gets funded, renewed, or cancelled.
- **Ad targeting / measurement:** on ad-supported tiers, viewing behavior becomes audience segments.
- **Model training and sharing** with measurement partners and affiliates.

### Visualization (canvas `c2`, 720×360)

Hub-and-spoke flow diagram: source boxes → central "Attention profile" hub → use boxes, with arrows.

- **Title (bold 13px `#1a5276`, top center):** "From raw signals to uses".
- **Source boxes (left column, 150×36 at x=25, y = 55/120/185/250):** Watch events, Pause / abandon points, Hovers & searches, Device telemetry. Style: `#1a5276` stroke (1.5px), same color fill at 12% alpha, bold 11px centered labels in `#1a5276`.
- **Hub box (x=275, y=130, 160×80):** stroke `#e67e22` 2px, fill `#e67e22` at 12% alpha; bold 12px `#e67e22` label "Attention profile"; below it 10px `#7d5a29` text "taste · mood · churn risk".
- **Use boxes (right column, 190×34 at x=510):** Playback & resume (`#27ae60`, y=45); Home-screen ranking (`#2980b9`, y=95); Thumbnail experiments (`#8e44ad`, y=145); Fund / cancel decisions (`#e67e22`, y=195); Ad targeting (ad tiers) (`#e74c3c`, y=245); Partner measurement (`#e67e22`, y=295). Each stroked/filled in its color (12% alpha fill), bold 11px label in its color.
- **Arrows:** gray `#bbb` (1.5px, filled triangular heads) from each source box to the hub's left-middle; colored arrows (each use box's color) from the hub's right-middle to each use box.

## How long it's kept

- **Active account:** full viewing history for the life of the account; "clear history" often hides it from you, not from the models.
- **After cancellation:** history commonly persists so it is there "if you come back" — cancellation is not deletion.
- **After deletion request:** a backup tail of weeks to months before purging.
- **Billing records:** years, under financial-record law.
- **Aggregated engagement data:** indefinite — it is the business asset. The longest retention applies to these copies stripped of direct identifiers, not the originals, which get the shorter windows; the catch is that a viewing history without a name can still be re-identified.

### Visualization (canvas `c3`, 720×340)

Horizontal retention-timeline bars per data category with two marker lines: subscription cancelled and account deleted.

- **Title (bold 13px `#1a5276`, top center):** "Retention by data category (illustrative)".
- **Axis:** bars start at x=220, axis width 460px; timeline baseline (thin `#999` line) at y=278 with 10px `#666` labels "account opens" (left-aligned at axis start) and "indefinite →" (right-aligned at axis end) at y=310.
- **Rows** (label, bar length as fraction of axis, color, note): Playback telemetry 0.62 `#2980b9` "tail after deletion"; Search & hover logs 0.64 `#2980b9` (no note); Viewing history 0.70 `#e67e22` "outlives cancellation"; Billing records 0.90 `#e67e22` "financial law: years"; Taste clusters / segments 1.0 `#e74c3c` "indefinite"; Aggregated engagement 1.0 `#e74c3c` "indefinite".
- **Bar style:** height 18px, gap 18px, first at y=45; fill in row color at 45% alpha, 1px stroke in row color; full-length (1.0) bars end in a filled triangular arrowhead pointing right. Notes in 10px `#666` just right of the bar end (or right-aligned inside the bar for full-length bars). Labels right-aligned at x=210, 11px `#2c3e50`.
- **Cancellation marker:** vertical dashed orange line (`#e67e22`, 2px, dash 3/4) at 32% of the axis (x≈367), from y=38 to y=280, labeled above in bold 10px `#e67e22` centered: "subscription cancelled".
- **Deletion marker:** vertical dashed red line (`#e74c3c`, 2px, dash 5/4) at 58% of the axis (x≈487), from y=38 to y=280, labeled below in bold 11px `#e74c3c` centered: "account deleted".

## What you get back

- **Included:** viewing history as a title-and-date list, ratings, watchlist, profiles, search history, settings, billing summary.
- **Excluded:** pause / rewind / abandonment telemetry, hover-without-play logs, taste and mood clusters, thumbnail-experiment assignments, churn and attention scores, device fingerprints, data shared with measurement partners.

**Key point (callout box):** The asymmetry: you get the list of what you watched; the platform keeps the model of how you watched — the second-by-second attention profile never leaves.

### Visualization (canvas `c4`, 720×340)

Two side-by-side comparison panels: export contents vs retained data.

- **Title (bold 13px `#1a5276`, top center):** "The data export: what comes back vs what stays behind".
- **Left panel (x=30, y=40, 320×240, green `#27ae60` — 2px stroke, 8% alpha fill), bold 13px title "IN THE EXPORT",** items (11px `#2c3e50`, centered, 22px spacing): Viewing history (title + date) / Ratings & watchlist / Profiles & settings / Search history / Billing summary.
- **Right panel (x=375, y=40, 320×275, red `#e74c3c`), bold 13px title "EXISTS BUT NOT RETURNED",** items: Pause / rewind / abandon telemetry; Hover-without-play logs; Taste & mood clusters; Thumbnail-experiment assignments; Churn & attention scores; Device fingerprints; Data shared with partners.
- **Caption (bottom center, 10px `#999`):** "You get the list of what you watched; the model of how you watched stays behind."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the canvas. Cell borders `1px solid #e0e0e0`, padding 16px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with 6px bottom margin.
- **Callouts:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases are `display: block; margin: 0 auto`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`. No nav bar, no back/home links.
- Note: in regenerated HTML, any card/grid links referencing this page use the `.html` extension.
