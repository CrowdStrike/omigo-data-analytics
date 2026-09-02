# Intentional Opacity

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, one row per section)
**HTML title tag:** Intentional Opacity — Common Bad Practices

**Subtitle:** Complexity as Job Security — Build systems only YOU understand. Bus factor = 1 by design.

## Section 1: The Practice

- No documentation (by choice, not accident). Variable names only you understand. Undocumented personal conventions. Critical knowledge in your head only.
- Tribal knowledge you don't share even when asked. You become "essential" — not because of irreplaceable talent, but because you've created a hostage situation.

**The sophisticated version:** Documentation EXISTS but is deliberately incomplete or misleading. Enough to look like you tried, not enough for someone else to actually maintain it. "I documented everything!" (omits the 3 critical edge cases that make it actually work).

### Visualization (canvas `c1`, 720×340)

Network graph: all arrows converge on a central "YOU" node — no direct paths between team and systems.

- **Center node:** filled red `#e74c3c` circle radius 36 at canvas center, stroke `#c0392b` width 3, white bold 16px label "YOU".
- **Team nodes (left column):** 5 circles radius 22 at x=100, starting y=50, 60px vertical gap, labeled `Alice, Bob, Carol, Dave, Eve` — fill `rgba(26,82,118,0.15)`, stroke `#1a5276` width 1.5, label 12px `#1a5276` centered.
- **System nodes (right column):** 5 circles radius 22 at x=620 (w−100), starting y=50, 60px gap, labeled `Deploy, Database, Auth, Pipeline, Monitoring` — fill `rgba(39,174,96,0.15)`, stroke `#27ae60` width 1.5, label 12px `#27ae60`.
- **Arrows:** orange `#e67e22` width-2 lines with filled orange arrowheads — from each team node to the center node, and from the center node to each system node (edge-to-edge along the connecting angle, arrowhead 8px, ±0.4 rad).
- **Labels (centered at bottom):** 12px gray `#999` at y = h−30: "No direct paths exist"; italic 12px `#666` at y = h−10: "Bus factor = 1 (by design, not accident). Every question routes through you."

## Section 2: Variant — Oral Tradition

- Critical decisions explained only in meetings that aren't recorded. "We discussed this in standup 3 months ago." No written record. Knowledge exists only in your memory.

### Visualization (canvas `c2`, 720×300)

Line chart: how much of an unrecorded decision's rationale the team can still reconstruct, month by month, versus a written record.

- **Title (bold 13px `#1a5276`, centered, y=18):** "Decision Rationale the Team Can Still Reconstruct".
- **Plot area:** x from 60 to 690, y from 44 (top, = 100%) to 236 (baseline, = 0%). Y axis: gridlines `#eee` width 1 at 0/25/50/75/100% with 11px `#666` right-aligned labels at x=52; solid `#999` axis lines along left edge and baseline. X axis: months 0–12 mapped linearly across the plot width, 11px `#666` tick labels every 2 months at y=252, plus 11px `#666` centered axis label "Months since the unrecorded meeting" at y=268.
- **Oral-tradition line (red `#e74c3c`, width 2.5):** deterministic exponential decay, `recall(m) = 100·e^(−0.30·m)` for m = 0…12 (100 → 74 → 55 → 41 → 30 → 22 → 17 → 12 → 9 → 7 → 5 → 4 → 3). Filled red circle markers radius 3 at each month; 12px red label "unrecorded (oral only)" to the right of the point at m=2, above the line.
- **Written-record line (green `#27ae60`, width 2):** near-flat reference, `written(m) = 95 − 0.5·m` (95 → 89). Filled green circle markers radius 3 every 2 months; 12px green label "written record" above the line near m=9.
- **Insight annotation:** filled red circle radius 5 at the m=6 point; vertical dashed (4/4) `#e74c3c` width-1 line from that point down to the baseline; bold 12px red `#e74c3c` two-line annotation anchored left at (mid-chart, y≈100): "Month 6: only 17% survives." / ""We discussed this in standup" = 83% already gone."
- **Caption (italic 12px `#666`, centered, y = h−12):** "Unrecorded decisions decay exponentially; a written record barely fades."

## Section 3: Variant — Custom Tooling

- Build personal scripts/tools that only work with your specific setup. The system technically runs on standard tech, but YOUR workflow is the only way to deploy/debug it.

**Why it persists:** "Essential" employees get better reviews, raises, flexibility. Being replaceable is RISKY. Creating irreplaceability through opacity is a rational (if selfish) survival strategy.

**The tell:** Bus factor = 1 for systems complex enough to warrant >3. If one person's vacation causes anxiety, opacity was deliberately cultivated. Try: "can you pair with someone on this for a week?" — if they resist, they're protecting the moat.

### Visualization (canvas `c3`, 720×300)

Two-box comparison: your machine vs the standard setup.

- **Left box ("YOUR Machine"):** rectangle 200×130 at (40, 30), fill `rgba(231,76,60,0.1)`, stroke `#e74c3c` width 2, bold 13px red title "YOUR Machine" centered at x=140. Below, 11px `#333` list lines (16px apart): `deploy.sh (undocumented)`, `~/.magic-env`, `custom aliases`, `local-only certs`, `hardcoded paths`.
- **Right box ("Standard Setup"):** rectangle 200×130 at (480, 30), fill `rgba(39,174,96,0.1)`, stroke `#27ae60` width 2, bold 13px green title "Standard Setup" centered at x=580. Below, 11px gray `#999` lines: "(theoretically works)", "README says: npm start", "Reality: 17 missing steps".
- **Arrow between:** dashed (5/5) red `#e74c3c` width-2 horizontal line from x=250 to x=470 at y=95, with italic 12px red label above at (360, 85): `"Works on my machine"`.
- **Bottom label (italic 12px `#666`, centered, y = h−15):** "The system runs on standard tech — but only YOUR workflow can deploy/debug it."

### Visualization (canvas `c4`, 720×300)

Grouped bar chart: knowledge distribution in a healthy team vs the opacity pattern, with risk meters.

- **Title (bold 13px `#1a5276`, top center):** "Knowledge Distribution".
- **Left group ("Healthy Team", 12px `#555` label at x=180, y=38):** 5 bars for people A–E, heights `[75, 70, 65, 60, 55]`, 25px wide, 35px apart starting at x=100, baseline y=130; fill `rgba(39,174,96,0.4)`, stroke `#27ae60` width 1; 11px `#555` letter labels A–E below each bar.
- **Right group ("Opacity Pattern", label at x=520):** 5 bars, heights `[120, 15, 10, 8, 5]`, starting at x=430; first bar fill `rgba(231,76,60,0.4)` stroke `#e74c3c`, the rest fill `rgba(200,200,200,0.4)` stroke `#999`; bold 12px red annotation `"You"` above the tall first bar (at x=442, y=5).
- **Risk meters:** left — bold 12px green "Risk: LOW" at (180, 162), meter track `rgba(39,174,96,0.3)` 100×12 at (130, 166) with solid `#27ae60` fill 25px wide; right — bold 12px red "Risk: CRITICAL" at (520, 162), track `rgba(231,76,60,0.3)` 100×12 at (470, 166) with solid `#e74c3c` fill 90px wide.
- **Bottom label (bold 12px red `#e74c3c`, centered, y=192):** "The \"essential employee\" pattern: org risk repackaged as personal value."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, three `<tr>` rows (The Practice / Variant — Oral Tradition / Variant — Custom Tooling); left `<td>` (40%) holds `.obj-title` + bullets/paragraphs, right `<td>` (60%, centered) holds the canvas(es) — row 3 stacks `c3` and `c4`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
