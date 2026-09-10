# Skipping Intermediate Logging

**Page type:** detail page (two-column obj-table layout: text left ~40%, canvas right ~60%, single row containing four titled blocks)
**HTML title tag:** Missing Instrumentation — Common Bad Practices

**Subtitle:** Negligent Practice — Not collecting data at every significant pipeline step. When things go wrong, you cannot trace what happened, when it diverged, or why. Debug cost: weeks instead of minutes.

## The Practice

- 10-step pipeline logs input and output. Steps 2-9: no intermediate state saved. Model accuracy drops. Which step broke? Nobody knows. Start the 2-week investigation.
- "We'll add logging later when we need it." By the time you need it, the data that would have been logged is gone. You needed it BEFORE the bug occurred.
- Model makes a bad prediction. What features did it see? Not logged. Feature values have since been updated in the source system. Cannot reconstruct the decision context.

## Why It's Common

- "Storage costs." — Storage is $0.023/GB/month. One week of engineer debugging time costs $5,000+. The ROI of logging is 100:1.
- "It slows down the pipeline." — Write to a separate async store. Pipeline latency unchanged. Even if it adds 5% latency, that's cheaper than 2 weeks of debugging.
- "We'll know if something breaks from the output." — You'll know THAT something broke. Not WHERE, WHEN, or WHY. Knowing output is wrong without knowing which step caused it is like knowing your car broke down but not whether it's the engine or a flat tire.

## The Data-Specific Cost

- **Cannot determine blast radius:** Bug found today. When did it start? Without historical intermediate checksums, you don't know. Could be yesterday. Could be 3 months ago. All data in that window is suspect.
- **Cannot explain model decisions:** Regulator, business owner, or postmortem asks "why did the system do X?" Answer: "we don't know, we didn't log the inputs." Trust destroyed.
- **Same bug recurs:** Without detailed traces, postmortems are guesswork. "Probably" fixed. Returns 3 months later. Same investigation from scratch.

## Minimum Viable Instrumentation

- Row count at every step boundary (detects silent drops/fanouts).
- Distribution stats per column at each step (mean, p50, p95, null count).
- Schema checksum (detects upstream schema changes).
- Feature vectors at inference time (enables decision explanation).
- Sample of filtered/dropped rows (detects bias in filtering).

**Why it persists:** Logging feels like overhead when things are working. Its value is only visible during outages. Teams that have never had a major data quality incident don't invest in logging. After the first incident costs them 3 months, they log everything. Proactive teams log from day 1.

**The tell:** Ask: "If step 5 of 10 started producing subtly wrong values today, how long until we'd detect it and which step would we identify first?" If the answer is "weeks" and "we'd have to check each one" — you have an instrumentation problem.

### Visualization (canvas `c1`, 720×400)

Two-panel pipeline trace: same 10-step pipeline, per-step row-total checksum, with vs without checkpoints.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 14px, top center, `#1a5276`):** "Tracing a bad output back to the step that broke".
- **Panels:** shared plot band top=62, bottom=300; left panel x 60–340, right panel x 415–695; each panel spans steps 1–10 evenly (9 intervals). Panel titles (bold 13px, centered at y=44): "With checkpoints" in green `#27ae60`, "Without checkpoints" in red `#e74c3c`.
- **Y scale:** value 50 at plot bottom, span 55 units over plot height (checksum metric); horizontal gridlines `#e8e8e8` at 60/70/80/90/100 in both panels; `#ccc` left+bottom axes per panel; y-axis labels 60–100 (11px `#666`) on the left panel only.
- **X labels:** step numbers "1"–"10" (11px `#666`) at y=314 under both panels; "pipeline step" centered under each panel at y=331.
- **Data (both panels):** logged baseline blue `#1a5276` `[100,98,97,96,95,94,93,92,91,90]`; today's run green `#27ae60` `[100,98,97,96,95,78,73,68,64,60]` — identical through step 5, peeling away at step 6.
- **Left panel:** both series drawn as 2px lines with 3px dots. Divergence point (step 6, value 78) circled red `#e74c3c` (radius 11, 2px stroke) with bold 12px red label "found in 5 min" centered below at y=245. Legend (top right of panel): 18px line swatches at x=228 (blue y=70, green y=86) with 11px `#333` text "logged baseline" / "today's run".
- **Right panel:** only endpoints plotted. Steps 2–9 covered by a full-height shaded rectangle, fill `rgba(231,76,60,0.10)` with dashed red border (dash 4/3); bold 15px red "?" marks at each of steps 2–9, alternating y = 185/209; bold 13px red label "2-week search space" centered in the region at y=110. Input endpoint at step 1 (value 100): blue 4px dot with green 7px ring, 11px `#333` label "input: OK" at top left. Output endpoints at step 10: blue dot at 90 labeled "expected" (11px `#1a5276`, right-aligned left of dot), green dot at 60 labeled "actual" (bold 11px `#e74c3c`, right-aligned left of dot).
- **Takeaway (bold 14px red `#e74c3c`, centered, y=362):** "Checkpoints turn a 2-week hunt into a 5-minute diff."
- **Caption (italic 12px `#666`, centered, y=384):** "Row-total checksum logged at each step; illustrative values."

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table, one `<tr>`; left `<td>` (40%) holds four `.obj-title` blocks (the ones after the first get `style="margin-top:14px;"`) each followed by a `<ul>`, then the two `<p><strong>` paragraphs ("Why it persists:", "The tell:"); right `<td>` (60%, centered, `vertical-align: middle`) holds canvas `c1`.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` 0.95em `#333`; `ul` 0.9em `#333`, `li` margin 6px 0; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em, weight 600, `#1a5276`; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="400"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#333`.
