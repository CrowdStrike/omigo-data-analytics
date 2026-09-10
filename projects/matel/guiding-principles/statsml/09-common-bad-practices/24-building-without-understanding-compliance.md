# Building Without Understanding Compliance

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section, three rows)
**HTML title tag:** Building Without Understanding Compliance — Common Bad Practices

**Subtitle:** Negligence — Ship fast, ask legal later. The pipeline works perfectly — and is illegal. Retrofit costs 10-50× what building it in from day one would have cost. Every day of data collection without compliance is a day of data you might have to delete.

## Section 1: The Practice

- Start collecting user data, training models, shipping features — without understanding the regulatory constraints.
- Assume compliance can be "added later" like a feature flag.
- Discover 6 months post-launch: right-to-deletion is impossible, data crosses borders illegally, consent scope was violated, retention periods exceeded.
- The data you collected? Might need to be deleted entirely. The models trained on it? Might need to be retrained from scratch.

### Visualization (canvas `c1`, 720×320)

Stacked area chart of engineering capacity over 24 months: feature work vs compliance firefighting, with milestone markers.

- **Title (bold 15px, top center, `#1a5276`):** "Engineering Capacity: Ship Fast, Pay Later".
- **Plot area:** left 60, right `w-30`, top 55, bottom 240; `#ccc` left and bottom axes; y labels "0%", "50%", "100%" (11px `#666`, right-aligned); x labels "M0"–"M24" step 6 (11px `#666`) 14px below the axis.
- **Data (25 monthly points, share of capacity on feature work; remainder = compliance firefighting):** `[100,100,100,100,100,100,100,100,98, 10,8,8,8,8,10,10,12,15, 40,55,65,72,78,80,80]` — ~100% feature work months 0–8, flipping to ~90% firefighting months 9–17, partial recovery to ~80% by month 24.
- **Bands:** feature-work band from 0 up to the series, fill `rgba(39,174,96,0.35)`; compliance-firefighting band from the series up to 100, fill `rgba(231,76,60,0.30)`; boundary line `#1a5276` width 2.
- **Legend (above plot, y=30, 12px squares + 11px `#333` text):** at x=60 `rgba(39,174,96,0.7)` "Feature work"; at x=180 `rgba(231,76,60,0.6)` "Compliance firefighting".
- **Milestones** (dashed vertical line, dash 5/3, width 1.5, full plot height; bold 11px label in the milestone color, left-aligned 5px right of the line at 13px below plot top):
  - Month 0: "Launch, no compliance review", green `#27ae60`.
  - Month 9: "Legal discovers non-compliance", orange `#e67e22`.
  - Month 18: "Retrofit complete", red `#e74c3c`.
- **Takeaway (bold 13px red `#e74c3c`, centered at month 13.5, y=145, inside the firefighting band):** "9 months at near-zero feature output — the retrofit tax."
- **Caption (bottom center, italic 12px `#666`):** "Share of engineering capacity by month; illustrative."

## Section 2: Why It's Intentional

Four example boxes (`.example-box` with bold `.ex-title`):

**The Innocent Version** — Regulations changed after you launched. New requirements didn't exist when you built the system. This is a legitimate challenge, not a bad practice.

**The Bad Practice** — You KNOW GDPR/CCPA/HIPAA applies. You choose to ignore it because compliance work is slow, unglamorous, and blocks your launch timeline. "We'll figure it out later." Later = after you've collected 18 months of non-compliant data.

**ML Training Data** — Model trained on user data without proper consent scope. Model ships, improves metrics, gets praised. 9 months later: legal review discovers consent doesn't cover this use case. Options: (a) retrain from scratch without that data, (b) obtain retroactive consent (impossible for churned users), (c) take the regulatory risk and hope.

**Cross-Border Data** — Pipeline moves EU user data to US processing cluster because it's cheaper. Works great for 14 months. Schrems III invalidates the transfer mechanism. Now: re-architect to process in-region, or delete all EU data. Either option: 6+ months of engineering.

### Visualization (canvas `c2`, 720×300)

Three horizontal bars on one honest unit axis (engineering-weeks) comparing build-in, retrofit, and retrofit + fines.

- **Title (bold 15px, top center, `#1a5276`):** "Cost in Engineering-Weeks: Build-In vs Retrofit".
- **Unit axis:** x from 180 to 640 (460px), scale 0–150 weeks; vertical gridlines `#eee` at 0/50/100/150 from y=50 down to the axis line (`#ccc`, y=245); tick labels "0 wk", "50 wk", "100 wk", "150 wk" (11px `#666`, centered) 16px below the axis.
- **Bars:** start x=180, height 40px, vertical spacing 60px, first bar at y=65; bar width = weeks × 460/150. Each bar: solid fill and matching 1.5px stroke; left label bold 14px `#1a5276` right-aligned at x=170; value text bold 15px — white inside the bar if bar wider than 90px, otherwise `#333` just right of the bar.
  - "Build it in:" — 3 weeks, green `#27ae60` (value "3 weeks" drawn outside in `#333`).
  - "Retrofit later:" — 52 weeks, orange `#e67e22`, "52 weeks" inside.
  - "Retrofit + fines:" — 150 weeks, red `#e74c3c`, "150+ weeks" inside.
- **Multiplier annotation (bold 15px red `#e74c3c`, left-aligned 12px right of the retrofit bar's end, vertically centered on that bar):** "10-50× the build-in cost".
- **Caption (bottom center, italic 13px `#666`):** "All three on the same axis; illustrative magnitudes — fines vary by regime."

## Section 3: The Asymmetry

- **Build-in cost:** 2-4 weeks of design work upfront. Data lineage tagging, consent tracking, deletion capabilities, retention policies.
- **Retrofit cost:** 6-18 months. Re-architect storage, re-tag historical data (often impossible), rebuild models, audit everything.
- **Delete-and-restart cost:** Everything built on non-compliant data is now suspect. Months of work invalidated.
- **Fine cost:** GDPR: up to 4% global revenue. HIPAA: up to $1.5M per violation category per year.

### Visualization (canvas `c3`, 720×280)

Area chart: cumulative non-compliant data collected over 18 months, all shaded as at-risk.

- **Title (bold 15px, top center, `#1a5276`):** "Data Collected Without Compliance = Data at Risk".
- **Plot area:** left 80, right `w-40`, top 50, bottom 230; `#ccc` axes (bottom and left).
- **Data:** 18 monthly points, linear growth `value = month_index × 2.5` (0 to 42.5); y scale 0–45 over plot height.
- **Line:** red `#e74c3c`, width 3; area under the line filled `rgba(231,76,60,0.15)`.
- **Discovery marker:** vertical orange `#e67e22` dashed line (dash 5/3, width 2) at month 9, full plot height, labeled "Discovery" (bold 12px orange) above the plot top.
- **Area label (bold 13px red `#e74c3c`, two lines centered around month 4.5 at the y=12 level):** "All this data" / "may need deletion".
- **X labels (11px `#666`):** "M0", "M3", "M6", "M9", "M12", "M15" below the axis.
- **Caption (bottom center, italic 13px `#666`):** "Every month of non-compliant collection = more data you might have to destroy."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (3 rows); left `<td>` (40%) holds `.obj-title` + bullets or `.example-box` divs, right `<td>` (60%, centered, `vertical-align: middle`) holds the canvas.
- **Example boxes:** `.example-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, margin 10px 0, font 0.88em; `.ex-title` bold 700 `#1a5276`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes per chart (c1 720×320, c2 720×300, c3 720×280); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
