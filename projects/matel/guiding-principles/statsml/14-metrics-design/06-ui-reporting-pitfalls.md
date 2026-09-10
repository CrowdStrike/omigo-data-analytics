# UI Reporting Pitfalls — How Dashboards Create Wrong Decisions

**Page type:** detail page, metric-testing template (white background; one two-column obj-table per pitfall: text left 40%, canvas right 60%; obj-title heading; canvases 720×200 with devicePixelRatio scaling and redraw on resize)
**HTML title tag:** UI Reporting Pitfalls — How Dashboards Create Wrong Decisions

**Subtitle:** Visual design choices that make stakeholders reach wrong conclusions from correct data.

## 1. Green/Red before test completes

A/B result "+5%" in green after 6 hours (n=200). PM screenshots to CEO. After 2 weeks: "+1.2%, not significant." But green screenshot already set expectations. Early color-coding creates premature certainty from insufficient data.

### Visualization (canvas `canvas1`, 720×300 attribute, drawn 720×200)

Side-by-side mock dashboard tiles (bad left, good right).

- **Left panel (white, `#ccc` border at 10,10 330×180):** header "BAD: After 6 hours" (bold 14px `#666`); solid green `#27ae60` tile (60,50 220×80) with white bold 30px "+5.0%" and 17px "WINNER!"; gray `#999` 13px footnotes "n=200 | 6 hours elapsed" and "No CI shown. No progress bar."
- **Right panel (`#f9f9f9`, `#2980b9` border at 360,10 350×180):** header "GOOD: After 6 hours" (bold 14px `#2980b9`); gray `#95a5a6` tile (420,50 220×60) with white bold 24px "+5.0% (±4.8%)" and 17px "IN PROGRESS"; progress bar: `#ddd` track (420,120 220×16) with `#f39c12` fill at 20% (44px) and 11px `#333` label "20% of required sample"; 13px `#666` footnotes "CI: [-0.2%, +10.2%] | Need n=2000" and "Gray = undecided. Wait for significance."

## 2. Y-axis starting at non-zero

Revenue $9.8M to $10.2M. Y starts at $9.5M: looks like 40% spike. Y starts at $0: barely visible 4% change. Same data, different story. Truncated axes amplify perception of small changes.

### Visualization (canvas `canvas2`, 720×300 attribute, drawn 720×200)

Same line plotted twice with different y-axis ranges.

- **Data:** `[9.8, 9.9, 9.85, 10.0, 10.1, 10.2]` ($M) over months Jan–Jun.
- **Left panel (white, `#ccc` border):** header "MISLEADING: Y starts at $9.5M" (bold 13px `#c0392b`); y-axis labels "$10.5M" / "$10.0M" / "$9.5M" (11px `#666`); line in `#c0392b`, width 3, y mapping 9.5–10.5 over 110px; 17px `#c0392b` annotation ""40% spike!"".
- **Right panel (`#f9f9f9`, `#27ae60` border):** header "HONEST: Y starts at $0" (bold 13px `#27ae60`); y-axis labels "$12M" / "$6M" / "$0"; same data in `#27ae60`, width 3, y mapping 0–12 over 110px; 17px `#27ae60` annotation "4% change (real)".

## 3. Cumulative-only charts

"Total users" always goes up. Even during decline. A failing product's cumulative chart looks like growth. Cumulative metrics are monotonically increasing by definition — they can never reveal decline. Always show the rate chart alongside.

### Visualization (canvas `canvas3`, 720×300 attribute, drawn 720×200)

Cumulative line (left) vs daily bars (right) for the same series.

- **Daily new users:** `[50, 80, 120, 180, 250, 300, 280, 220, 150, 100, 60, 40]` (12 periods); cumulative = running sum.
- **Left panel (white, `#ccc` border):** header "CUMULATIVE: "We're growing!"" (bold 13px `#27ae60`); cumulative line in `#27ae60`, width 3, always rising; small green up-triangle marker and 17px green label "Up & right!".
- **Right panel (`#f9f9f9`, `#c0392b` border):** header "DAILY NEW: Reality - peaked, declining" (bold 13px `#c0392b`); 12 bars (18px wide, scale max 300): indices 0–5 blue `#3498db`, indices 6–11 red `#c0392b`; 17px `#c0392b` label "Peaked month 6!"; red decline arrow (width-2 line from upper-left to lower-right with filled arrowhead).

## 4. No confidence interval shown

"Conversion = 4.2%" — could be ±0.1% or ±2%. Without CI, stakeholders treat noise as signal. A point estimate without uncertainty bounds is an invitation to over-react to random fluctuation.

### Visualization (canvas `canvas4`, 720×300 attribute, drawn 720×200)

Point series without vs with CI error bars.

- **Data points:** `[3.8, 4.0, 4.2, 3.9, 4.1, 4.3, 4.2]` (%); CI half-widths (right panel): `[1.8, 1.5, 1.2, 1.0, 0.8, 0.6, 0.5]`.
- **Left panel (white, `#ccc` border):** header "WITHOUT CI: Looks precise" (bold 13px `#c0392b`); dark `#2c3e50` dots (radius 5, y mapping 3.0–5.0); 17px `#2c3e50` label "4.2%"; 14px `#c0392b` caption ""It dropped! Act now!"".
- **Right panel (`#f9f9f9`, `#27ae60` border):** header "WITH CI: Reveals uncertainty" (bold 13px `#27ae60`); each point (`#2c3e50` dot radius 4) with vertical blue `#3498db` CI bar (width 2) plus 10px end caps; 17px `#27ae60` label "4.2% ± 0.5%"; 14px `#666` caption ""All within noise. No action."".

## 5. No comparison context

"500K sessions today." Good or bad? Without: yesterday (480K), last week (510K), last year (300K), target (550K). A number alone is meaningless. Context turns data into information.

### Visualization (canvas `canvas5`, 720×300 attribute, drawn 720×200)

Naked KPI vs KPI with comparisons.

- **Left panel (white, `#ccc` border):** header "WITHOUT CONTEXT: Meaningless" (bold 13px `#c0392b`); giant bold 42px `#2c3e50` "500K" with 17px `#666` "sessions today"; 14px `#999` lines "Is this good? Bad? Normal?" and "Nobody knows. No action possible."
- **Right panel (`#f9f9f9`, `#27ae60` border):** header "WITH CONTEXT: Actionable" (bold 13px `#27ae60`); bold 28px "500K" + 13px `#666` "sessions today"; blue `#3498db` sparkline (width 2) of `[300, 350, 400, 420, 460, 480, 510, 490, 500, 500]`; comparison lines (14px): green `#27ae60` "↑ +4% vs yesterday (480K)", red `#c0392b` "↓ -2% vs last week (510K)", green "↑ +67% vs last year (300K)", orange `#e67e22` "○ 91% of target (550K)"; 12px `#666` "Verdict: Normal day, slightly below target."

## 6. Average hiding bimodal reality

"Average session: 3 min." Nobody has a 3-min session. 40% bounce at 0s, 50% engage at 5min. Average = a state nobody occupies. The mean of a bimodal distribution describes no actual user.

### Visualization (canvas `canvas6`, 720×300 attribute, drawn 720×200)

Full-width bimodal histogram with a mean line where nobody sits.

- **Frame:** white background, `#2980b9` border (5,5 710×190); title "Session Duration Histogram — The Mean Is A Lie" (bold 14px `#1a5276`).
- **Bins (label: value):** 0s: 40, 30s: 8, 1m: 5, 1.5m: 3, 2m: 2, 2.5m: 2, 3m: 1, 3.5m: 2, 4m: 5, 4.5m: 12, 5m: 35, 5.5m: 20, 6m: 8. Bars 42px wide + 6px gap, scale max 40, baseline y=170, bin labels 10px `#666`.
- **Bar colors:** first two bins red `#e74c3c` (bouncers), bins from 4.5m on blue `#3498db` (engaged), middle bins gray `#bdc3c7`.
- **Mean line:** vertical dashed `#c0392b` (dash 6/4, width 3) at the 3m bin, labeled bold 17px "Mean = 3 min" and 13px "(nobody is here!)".
- **Mode labels (bold 12px):** red "40% Bouncers" bottom left; blue "50% Engaged" bottom right.

## 7. Alert overload (100/day leads to ignore all)

100 metric alerts/day. Team ignores them. Real incident is alert #67. Never investigated. Alert fatigue is a systems failure: when everything is urgent, nothing is. The signal drowns in self-inflicted noise.

### Visualization (canvas `canvas7`, 720×300 attribute, drawn 720×200)

Dark alert-stream grid: 100 dots, one critical.

- **Background:** dark navy `#1a1a2e` full canvas; title "Alert Stream: 100 alerts/day — Which one matters?" (bold 14px `#ecf0f1`).
- **Grid:** 100 dots in 20 columns × 5 rows (34px x-spacing, 28px y-spacing from 35,40); 99 gray `#555` dots (radius 7, alpha 0.6); dot index 66 (alert #67) red `#e74c3c`, radius 10, with white bold 9px "67" inside.
- **Legend (17px):** gray `#777` "● Noise (99 false alarms)" left; red `#e74c3c` "● CRITICAL (buried, ignored)" right.

## 8. Percentage without base

"Support tickets UP 200%!" Sounds terrible. Base: 3 tickets to 9 tickets (tiny absolute number, normal variance). Relative changes on small bases are meaningless noise amplified by math.

### Visualization (canvas `canvas8`, 720×300 attribute, drawn 720×200)

Alarmist percentage vs calm absolute numbers.

- **Left panel (white, `#ccc` border, pale-red `#fdedec` alarm box 30,40 300×100):** header "SCARY: Percentage alone" (bold 13px `#c0392b`); bold 36px `#c0392b` "↑ 200%" and bold 17px "SUPPORT TICKETS!"; 14px `#c0392b` lines "⚠ Emergency meeting scheduled" and "⚠ CEO wants answers by EOD".
- **Right panel (`#f9f9f9`, `#27ae60` border):** header "CALM: With base number" (bold 13px `#27ae60`); 17px `#2c3e50` "Support tickets: 3 → 9"; 14px `#666` "(+6 tickets, from tiny base)" and "Normal weekly variance: ±5 tickets"; two blue `#3498db` bars (30×30 and 90×30) labeled "3" and "9" (11px `#2c3e50`); 13px `#27ae60` "Verdict: Normal. No action needed."

## 9. Stale dashboard with no "last updated" timestamp

Dashboard shows numbers. Are they from today? Last week? Last month? No timestamp. Users trust they're current. Actually: data pipeline broke 2 weeks ago. Numbers are 2 weeks old. Freshness metadata is not optional.

### Visualization (canvas `canvas9`, 720×300 attribute, drawn 720×200)

Fresh-looking KPI tiles vs dark reality panel.

- **Left panel (white, `#ccc` border):** header "APPEARS FRESH: No timestamp" (bold 13px `#c0392b`); four pale-blue `#eaf2f8` KPI tiles (145×55 each) showing bold 20px `#2c3e50` values "$10.2M", "52K", "4.2%", "87" with 11px `#777` captions "Revenue", "Users", "Conversion", "NPS"; 12px `#c0392b` note "No timestamp anywhere!"
- **Right panel (dark `#2c2c2c`, red `#e74c3c` border width 2):** header "REALITY: Data pipeline broken" (bold 13px `#e74c3c`); 17px `#e74c3c` "⚠ Pipeline FAILED"; 14px `#aaa` lines "Last successful run: 14 days ago", "All dashboard numbers are FROZEN", "at 2-week-old values."; 13px `#f39c12` "Decisions made on stale data:" followed by 13px `#eee` bullets "• Budget approved based on old revenue" and "• "Growth" reported that never happened".

## 10. Success-only view (no failure metrics shown)

Revenue up, Users up, Engagement up. All green. Hidden: Churn up, CAC up, NPS down. The dashboard is curated propaganda. Selective metric display creates a false narrative of health while the business deteriorates.

### Visualization (canvas `canvas10`, 720×300 attribute, drawn 720×200)

Two metric lists: shown (all green) vs hidden (all red).

- **Left panel (pale green `#f0fff0`, `#27ae60` border width 2):** header "SHOWN TO STAKEHOLDERS" (bold 13px `#27ae60`); rows with green "↑" (17px), 15px `#2c3e50` metric name, bold 15px green value: Revenue +12%, Users +18%, Engagement +8%, Page Views +22%, App Downloads +15%; bottom caption bold 14px green ""Everything is great!"".
- **Right panel (pale red `#fff0f0`, `#c0392b` border width 2):** header "HIDDEN FROM STAKEHOLDERS" (bold 13px `#c0392b`); rows with red "↓", metric name, bold red value: Churn Rate +45%, CAC (Cost/Acquire) +60%, NPS Score -28pts, Support Tickets +90%, Employee Attrition +35%; bottom caption bold 14px red ""The business is dying."".

## Regeneration instructions

- **Layout:** single `table.obj-table` (full width, border-collapse, `2px solid #2980b9` outer border) with a `<thead>` header row: `<th>` cells "Pitfall & Explanation" and "Visualization" (background `#1a5276`, white text, padding 12px 15px, left-aligned). Body: one `<tr>` per pitfall; left `<td>` (40%) holds `.example-title` (bold `#1a5276` 1.05em, numbered "N. Title") + `.example-desc` paragraph (`#333`, line-height 1.6); right `<td>` (60%) holds the canvas with its inline drawing script. Cell borders `1px solid #2980b9`, padding 15px, vertical-align top; even rows background `#f0f8ff`.
- **Page style:** body -apple-system/Segoe UI/Roboto sans-serif, background `#fafafa`, text `#2c3e50`, padding 20px 10px; h1 `#1a5276`; `.subtitle` `#555` 1.1em. No nav bar, no back/home links.
- **Canvas:** each declares `width="720" height="300"` but the script draws at 720×200; each script sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Most charts are split into a left "bad" panel (0–350px) and right "good" panel (360–710px).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; this page also uses dark red `#c0392b`, mid blue `#3498db` / `#2980b9`, amber `#f39c12`, slate `#2c3e50`, and dark panels `#1a1a2e` / `#2c2c2c`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
