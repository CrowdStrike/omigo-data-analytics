# Rewrite Theater

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvases right ~60%, one row per section)
**HTML title tag:** Rewrite Theater — Common Bad Practices

**Subtitle:** Complexity as Job Security — Declare system 'unmaintainable,' rewrite it, own the new version.

## Section 1: The Practice

- Existing system works. It has accumulated complexity. Someone ELSE built it.
- You declare it "tech debt" and "unmaintainable" (meaning: you refuse to learn it).
- Propose rewrite. Rewrite takes 18 months, costs 3x estimate, reproduces 80% of existing behavior with different names and your preferred framework.
- Net value to business: zero. Net value to YOU: the system is now yours.

### Visualization (canvas `c1`, 720×300)

Feature-parity burnup: what the 18-month rewrite actually delivers, against the system it replaced.

- **Title (bold 14px `#1a5276`, top center, y=20):** "18 months of rewrite, measured in features".
- **Plot area:** x = months 0-18 (ticks every 3 months, 11px `#666`, labels "M0"…"M18"), y = % of old system's behavior reproduced, 0-100 (gridlines `#eee` at 25/50/75/100, 55px left margin, labels 11px `#666`).
- **Old-system line:** dashed green `#27ae60` width-2 horizontal line at 100%, left-aligned 11px green label above it: "existing system — already does all of this".
- **Rewrite line (blue `#1a5276`, width 2):** deterministic point per month: `100 * (1 - Math.exp(-m/9)) * 0.93` for m = 0…18 (slow ramp reaching ~80% at month 18). End dot (radius 3.5, filled blue) with bold 11px blue label "80% parity" right of it.
- **Budget marker:** orange `#e67e22` dashed vertical line at month 6, bold 11px orange label "original budget exhausted".
- **Insight annotation (bold 13px red `#e74c3c`, centered around x = month 9, y ≈ 35% height):** "18 months, 3× budget —" / "to reach 80% of what already worked."
- **Caption (bottom center, italic 12px `#666`):** "Illustrative parity curve — the rewrite reproduces existing behavior, it doesn't add capability."

## Section 2: The Key Distinction

- "Won't maintain" does not equal "can't be maintained."
- The rewrite is motivated by ownership transfer, not technical necessity.
- The existing system's "problems" could be fixed incrementally in 3 months. But that doesn't transfer political ownership.

### Visualization (canvas `c2`, 720×280)

Cumulative business value delivered: the 3-month incremental fix vs the 18-month rewrite, over 24 months.

- **Title (bold 14px `#1a5276`, centered, y=20):** "Cumulative value delivered: fix vs rewrite".
- **Plot area:** x = months 0-24 (ticks every 6 months, 11px `#666`), y = cumulative value units 0-100 (gridlines `#eee` at 25/50/75/100, 55px left margin).
- **Incremental-fix line (green `#27ae60`, width 2):** delivers from the start — per month m: `m <= 3 ? m * 10 : 30 + (m - 3) * 3` (30 units by month 3, ~93 by month 24).
- **Rewrite line (red `#e74c3c`, width 2):** flat at 0 through month 18, then `(m - 18) * 2` (reaches 12 by month 24 — parity work delivers almost nothing new).
- **Marker:** gray `#999` dashed vertical at month 18, 11px gray label "rewrite ships".
- **Insight annotation (bold 13px red `#e74c3c`, centered near x = month 12, y ≈ 45% height):** "The 3-month fix out-delivers" / "the rewrite for two years."
- **Caption (bottom center, italic 12px `#666`):** "Illustrative value units — the rewrite's output is parity, which the business already had."

### Visualization (canvas `c3`, 720×300)

Sawtooth of team understanding across three rewrite cycles over 9 years.

- **Title (bold 14px `#1a5276`, top center, y=20):** "The rewrite cycle, measured".
- **Plot area:** x = years 0-9 (ticks yearly, 11px `#666`), y = "% of team that understands the system" 0-100 (gridlines `#eee` at 25/50/75, 55px left margin).
- **Sawtooth line (blue `#1a5276`, width 2):** three identical cycles of 3 years each. Within each cycle (t = years since cycle start, sampled every 0.25 year): `90 - t * 25` (decays from 90% to 15% as the author departs and knowledge rots), then a vertical jump back to 90 at the next cycle start.
- **Cycle labels:** centered bold 12px labels above each cycle's start peak, alternating colors: "System A ships" (red `#e74c3c`, year 0), "System B ships" (orange `#e67e22`, year 3), "System C ships" (blue `#1a5276`, year 6).
- **Trigger markers:** at years 2.8, 5.8, 8.8 (just before each reset), red `#e74c3c` down-pointing filled triangles (6px) on the line with one shared right-aligned 11px red label near the first: "declared 'unmaintainable'".
- **Insight annotation (bold 13px red `#e74c3c`, centered, y ≈ 62% height):** "Every cycle transfers ownership. Zero cycles transfer value."
- **Caption (bottom center, italic 12px `#666`):** "Illustrative — each 'unmaintainable' verdict coincides with the previous author's knowledge, not the code, leaving."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, two `<tr>` rows (The Practice / The Key Distinction); left `<td>` (40%) holds `.obj-title` + bullets, right `<td>` (60%, centered) holds the canvas(es) — row 2 stacks `c2` and `c3`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** declare intrinsic `width`/`height` attributes as given per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
