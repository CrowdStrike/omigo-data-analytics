# Alert Suppression Before Reviews

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%; single row with multiple titled blocks and two stacked canvases)
**HTML title tag:** Alert Suppression Before Reviews — Common Bad Practices

**Subtitle:** Metric Manipulation — Silence monitoring for 2 weeks before review cycle. Zero incidents on paper.

## Section 1: The Practice

- Performance review approaching in 2 weeks. Silence non-critical monitoring alerts. Issues still exist but aren't visible in the incident log.
- Your "uptime" looks clean. System health appears perfect.
- After review: turn alerts back on. "Discover" the accumulated issues. Fix them — those fixes become wins for NEXT review cycle.

## Section 2: Variant — Threshold Manipulation

- p99 latency alert at 200ms? Change threshold to 500ms for review period. No alerts fire. Reset after.
- Technically: "zero SLA violations during review period!"

## Section 3: Variant — Incident Reclassification

- Reclassify SEV-2 incidents as SEV-3 (which don't count toward your team's reliability score). Same incidents, different labels.

## Section 4: Compounding

- This creates a CYCLE: suppress alerts → clean record → good review → turn alerts on → "find" issues → fix them → good next review.
- Perpetual motion of manufactured heroics from self-created problems.

**Why it persists:** Performance is measured by VISIBLE incidents. If it's not logged, it didn't happen. Alert-based monitoring creates vulnerability: whoever controls thresholds controls the narrative.

**The tell:** Check for threshold changes in weeks before review periods. If alert rules have modification timestamps that cluster pre-review, it's systematic gaming. Also: compare alert volume by week — if review weeks are always quiet, that's not coincidence.

### Visualization (canvas `c1`, 720×340)

Dot-column chart of weekly alert counts over 12 weeks, with a highlighted suppression zone during the review period.

- **Background:** full-canvas `#f9f9f9`; margins top 30, bottom 40, left 40, right 20.
- **Data:** alerts per week W1–W12: `[5, 7, 4, 6, 8, 0, 0, 6, 9, 7, 5, 8]`; suppression zone covers weeks 6–7 (indices 5–6); dot stack scale max 10.
- **Suppression zone:** rectangle over the two suppressed weeks spanning full plot height, fill `rgba(231,76,60,0.08)`, dashed red border (`#e74c3c`, dash 4/3). Bold 18px red label "Review Period" centered above the zone; 16px red label "alerts suppressed" centered below the zone under the axis.
- **Axes:** light gray (`#ccc`, width 1) baseline along the bottom of the plot; 16px gray (`#666`) week labels "W1"…"W12" centered per column; rotated vertical 16px gray y-axis label "Alerts" on the left.
- **Dots:** each week's alerts drawn as a vertical stack of 4px-radius circles, filled solid red `#e74c3c`. In the suppression zone weeks the dots are hollow-style: fill `rgba(231,76,60,0.15)` with red stroke (these weeks show 0 alerts in the data, so no dots render — the styling exists for suppressed-week dots).
- **Caption (bottom center, italic 14px `#555`):** "On paper: zero incidents. Reality: same issues, silenced."

### Visualization (canvas `c2`, 720×300)

Two-cycle sawtooth timeline over 24 weeks: visible logged alerts (bars) vs hidden suppressed backlog (line). Alerts go dark during each review period, the backlog climbs, then a post-review spike of "discovered" issues resets it — the cycle repeats.

- **Background:** full-canvas `#f9f9f9`; margins top 48, bottom 42, left 44, right 16.
- **Data (hardcoded, deterministic):** weeks W1–W24; review periods weeks 9–10 (indices 8–9) and 21–22 (indices 20–21).
  - Visible logged alerts (bars): `[6, 5, 7, 6, 8, 5, 7, 6, 0, 0, 15, 9, 6, 7, 5, 6, 8, 6, 7, 5, 0, 0, 16, 8]`
  - Hidden backlog (line): `[0, 0, 0, 0, 0, 0, 0, 0, 7, 13, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 14, 5, 0]` — line drawn only over segments indices 7–11 and 19–23; 3px-radius filled red dot markers at nonzero points.
- **Y axis:** scale 0–16, horizontal gridlines every 4 units (`#e0e0e0`, width 1), 11px `#666` tick labels (0/4/8/12/16) right-aligned at left; `#ccc` baseline along the bottom of the plot.
- **X axis:** 11px `#666` labels every 4th week ("W1", "W5", "W9", "W13", "W17", "W21") centered under their columns.
- **Review zones:** rectangle over each review period spanning full plot height, fill `rgba(231,76,60,0.08)`, dashed `#e74c3c` border (dash 4/3); bold 12px red "Review" centered above each zone.
- **Bars:** visible alerts, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` width 1, bar width 60% of the column width, centered per column.
- **Line:** hidden backlog, `#e74c3c` width 2.5, round joins.
- **Legend (top-left, in the top margin, 12px `#333`):** row 1 — blue bar swatch + "Visible alerts (logged)"; row 2 — red line-with-dot swatch + "Hidden backlog (suppressed)".
- **Insight annotation (bold 13px `#e67e22`):** "Post-review spike = 'discovered' issues" centered right of the W11 spike, with a short orange arrow (width 2, filled arrowhead) pointing to the top of the W11 bar.
- **Caption (bottom center, italic 12px `#555`):** "Same sawtooth every cycle: silence, accumulate, 'discover', repeat."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with a single `<tr>`; left `<td>` (40%) holds four `.obj-title` blocks (The Practice, Variant — Threshold Manipulation, Variant — Incident Reclassification, Compounding — the latter three with `style="margin-top:14px;"`) each followed by its bullet list, then the two closing `<p>` paragraphs (**Why it persists** / **The tell** with `strong` lead-ins); right `<td>` (60%, centered) holds canvases `c1` and `c2` stacked.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. All chart data is deterministic (hardcoded arrays, no randomness), so resize redraws are stable.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; gray text `#666`/`#555`.
- **Note:** in regenerated HTML, any card links use `.html` extensions (this page has none).
