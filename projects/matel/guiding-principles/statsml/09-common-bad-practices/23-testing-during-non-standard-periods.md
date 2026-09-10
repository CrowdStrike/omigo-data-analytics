# Testing During Anomalous Periods

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, one row per section, three rows)
**HTML title tag:** Testing During Anomalous Periods — Common Bad Practices

**Subtitle:** Negligence — Running experiments during non-representative windows (holidays, outages, viral events) knowing results won't generalize. Ship the "win" before the data normalizes.

## Section 1: The Practice

- Deliberately launch an A/B test during a known anomalous period (Black Friday, election week, fiscal year-end, viral event).
- User behavior during these periods is non-representative — higher urgency, different demographics, compressed decision timelines.
- Test shows strong positive result. Ship immediately and lock in before steady-state data arrives.
- When performance regresses in normal weeks: "The feature works, something else must have changed."

### Visualization (canvas `c1`, 720×300)

Line chart: weekly conversion rate over 52 weeks with a highlighted holiday spike window where the test was run.

- **Title (bold 15px, top center, `#1a5276`):** "Test Window Selection: Anomalous vs Normal Behavior".
- **Plot area:** left 80, right `w-40`, top 45, bottom 250; axes as thin `#ccc` lines (bottom and left).
- **Data (52 weekly points):** base conversion `4.0 + sin(i*0.3)*0.3` for weeks 0–45 and 51; holiday spike for weeks 46–50: week 48 = 11.2, weeks 47 and 49 = 9.5, weeks 46 and 50 = 8.0. Line `#1a5276`, width 2.
- **Y scale:** value 2 maps to plot bottom, span 8 units over plot height; y-axis labels "2%" through "10%" step 2 (11px `#666`, right-aligned).
- **Anomalous window highlight:** rectangle over weeks 46–51 (5 week-widths), fill `rgba(231,76,60,0.1)`, dashed `#e74c3c` border (dash 4/3, width 1), full plot height.
- **Labels above the window (bold 12px red `#e74c3c`, centered at week 48):** "Test run HERE" / "(Black Friday)".
- **Baseline:** green `#27ae60` dashed horizontal line (dash 3/3, width 1) at the 4.0% level from the left axis to week 44, labeled "Normal baseline: ~4%" (12px green) above it near week 22.
- **Caption (bottom center, italic 13px `#666`):** "Conversion rate over 52 weeks. Test launched in the one window where everything is inflated."

## Section 2: Why It's Intentional

Four example boxes (`.example-box` with bold `.ex-title`):

**The Innocent Version** — Accidentally running a test during a holiday. Realizing afterward that the data is non-representative. Rerunning during a normal period. This is a mistake, not a practice.

**The Bad Practice** — Knowing the period is anomalous. Launching anyway because anomalous behavior FAVORS your hypothesis. Presenting results without mentioning the timing. Shipping before anyone can object. "The numbers speak for themselves."

**E-commerce: Holiday Launch** — New checkout flow tested during Black Friday. Conversion jumps 18% — users are more motivated, less price-sensitive, already committed to buying. Feature shipped. January conversion: +2%. But the launch is permanent.

**Engagement: Viral Moment** — Content algorithm change tested during week when competitor goes down. All traffic inflated. "Algorithm drove 25% more engagement." Competitor recovers — engagement normalizes. Change already shipped and attributed.

### Visualization (canvas `c2`, 720×340)

Bar chart of weekly measured lift decaying after launch, with the reported "+18%" frozen as a dashed line across the whole chart.

- **Title (bold 15px, top center, `#1a5276`):** "Measured Lift, Week by Week After Launch".
- **Plot area:** left 70, right `w-30`, top 50, bottom 272; `#ccc` bottom and left axes; horizontal gridlines `#eee` at 0/5/10/15/20 with y labels "0%"–"20%" (11px `#666`, right-aligned).
- **Y scale:** 0 at plot bottom, 20 at plot top.
- **Bars (12 weeks):** measured lift `[18, 13, 9, 7, 5, 4, 3.5, 3, 2.6, 2.3, 2.1, 2]`; 32px-wide bars centered in equal slots, fill `rgba(26,82,118,0.35)`, stroke `#1a5276` 1px; value label (11px `#333`) 5px above each bar top; week labels "W1"–"W12" (11px `#666`) 15px below the axis.
- **Frozen reported line:** dashed red `#e74c3c` horizontal line (dash 6/4, width 2) at the 18% level spanning the full plot width; bold 12px red label right-aligned above it at the right edge: "Reported in the deck: +18% (frozen)".
- **Takeaway (bold 14px red `#e74c3c`, centered at x=430, y=130):** "The number in the deck never decays; the effect does."
- **Caption (bottom center, italic 13px `#666`):** "Weekly measured lift for the 12 weeks after launch; illustrative."

## Section 3: The Data Cost

- **Wasted experiment slot:** The test window was consumed. Can't re-run the same test easily — novelty effects, political cost of "re-testing a shipped feature."
- **Polluted decision history:** Leadership now believes the feature drives 18% lift. Every forecast, every resource allocation based on that inflated number.
- **48 normal weeks pay for 4 anomalous ones:** Feature lives in production for years based on data from 4 non-representative weeks.
- **Compounding:** Next quarter, another feature tested during another anomaly. The model of "what our product does" diverges further from reality.

### Visualization (canvas `c3`, 720×300)

Two-line divergence chart over 8 quarters: believed cumulative lift vs actual, with the gap shaded.

- **Title (bold 15px, top center, `#1a5276`):** "Believed Product Performance vs Reality (Compounding)".
- **Plot area:** left 80, right `w-50`, top 50, bottom 250; `#ccc` axes.
- **X labels (11px `#666`):** Q1, Q2, Q3, Q4, Q1, Q2, Q3, Q4 (8 evenly spaced points).
- **Series (both width 3, 4px-radius dots at every point):**
  - "What leadership believes" — orange `#e67e22`: `[1.0, 1.18, 1.30, 1.45, 1.60, 1.72, 1.88, 2.05]`.
  - "Actual cumulative lift" — blue `#2980b9`: `[1.0, 1.02, 1.05, 1.08, 1.10, 1.11, 1.13, 1.14]`.
- **Y scale:** value 0.8 at plot bottom, span 1.5 over plot height.
- **Divergence fill:** area between the two lines filled `rgba(231,76,60,0.1)`.
- **Legend (top right):** 12px color squares with 12px `#333` text: orange "What leadership believes", blue "Actual cumulative lift".
- **Gap annotation (bold 13px red `#e74c3c`):** "Gap = 91%" placed at the final quarter, vertically midway between the two lines.
- **Caption (bottom center, italic 13px `#666`):** "Each anomalous-period test adds phantom lift. The believed trajectory diverges further each quarter."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>` per section (3 rows); left `<td>` (40%) holds `.obj-title` + bullets or `.example-box` divs, right `<td>` (60%, centered, `vertical-align: middle`) holds the canvas.
- **Example boxes:** `.example-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 12px 16px, margin 10px 0, font 0.88em; `.ex-title` bold 700 `#1a5276`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes per chart (c1 720×300, c2 720×340, c3 720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, secondary blue `#2980b9`, gray text `#666`/`#333`.
