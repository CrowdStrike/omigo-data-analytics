# Mobile Swipe — Temporal Bias, Fatigue & Single-Result Framing

**Page type:** detail page (numbered h2 sections, each with one two-column obj-table row: text left 40%, canvas right 60%)
**HTML title tag:** Mobile Swipe — Temporal Bias, Fatigue & Single-Result Framing

**Subtitle:** TikTok-style single-result interfaces eliminate visual position bias but introduce different measurement problems: temporal decay, attention fatigue, and framing effects from no-comparison context.

## 1. Temporal Position Bias

**Session start vs minute 20 — same content, different engagement**

- **What changed:** No simultaneous rank comparison (slot 1 vs slot 5). But content shown at session start gets higher engagement than content shown 20 minutes in — purely because of when it appears.
- **Why:** User is fresh, curious, high dopamine at session start. By minute 20: habituated, thumb on autopilot, skipping faster. The content didn't change — the user's state did.
- **The measurement error:** Content shown early in sessions gets inflated engagement scores. Content shown late gets deflated. Model learns "early-session content is better" when it's actually "early-session users are more receptive."

**Fix:** Normalize engagement by session position (how many items deep). Compare content only against other content shown at similar session depths. Time-in-session as a covariate.

### Visualization (canvas `ca1`, 720×300)

Decaying line/area chart of engagement rate by session depth.

- **Title (bold 14px `#1a5276`, centered):** "Engagement Rate by Session Depth (Same Content Quality)".
- **Plot margins:** left 70, right 40, top 50, bottom 45; y scale max 80%.
- **Data (item depth → engagement %, points evenly spaced):** #1→72%, #5→65%, #10→55%, #15→42%, #20→33%, #30→24%, #40→18%, #50→14%.
- **Series:** shaded area fill `rgba(26,82,118,0.1)` under a `#1a5276` line (width 3) with 4px dots; each point labeled 10px `#333` with its % above and "#depth" below the axis.
- **Axis caption (gray `#666` 12px, centered):** "Items deep in session".
- **Annotations (bold 11px red `#e74c3c`):** "← fresh user" near top left of plot; "fatigued user →" near bottom right.

## 2. Attention Fatigue & Swipe Velocity

**Engagement drops not because content is worse, but because the user is depleted**

- **The pattern:** First 10 items: average watch time 8s. Items 20-30: 4s. Items 50+: 2s. Swipe velocity increases monotonically. User is in "scroll zombie" mode.
- **What the model sees:** Later items "underperform." But the user isn't evaluating anymore — they're in a motor habit loop. Dwell time measures fatigue state, not content quality.
- **Compounding:** Model demotes content that happened to be shown during fatigue windows → that content gets fewer future impressions → never gets a fair evaluation during fresh sessions.

**Fix:** Session-depth-adjusted scoring. Fatigue detection (swipe velocity spike = stop counting engagement). Only train on first N items per session where attention is plausibly active.

### Visualization (canvas `ca2`, 720×300)

Dual line chart: watch time falling while swipe velocity rises, with a shaded "scroll zombie" zone.

- **Title (bold 14px `#1a5276`, centered):** "Watch Time & Swipe Velocity Over Session".
- **Plot margins:** left 70, right 60, top 50, bottom 40; 10 evenly spaced points.
- **Watch time series (green `#27ae60`, solid, width 2.5, scale max 9s):** `[8.2, 7.5, 6.8, 5.5, 4.2, 3.5, 2.8, 2.2, 1.8, 1.5]`.
- **Swipe velocity series (red `#e74c3c`, dashed 5/3, width 2.5, scale max 35 items/min):** `[6, 7, 9, 12, 16, 19, 23, 27, 30, 33]`.
- **Zombie zone:** from point index 6 to the right edge, fill `rgba(231,76,60,0.06)`, labeled red 10px centered "\"scroll zombie\"".
- **Legend (bold 11px at bottom):** green "— Watch time (sec)"; red "--- Swipe velocity (items/min)".

## 3. Single-Result Framing — No Comparison Context

**One item on screen means binary accept/reject — not relative preference**

- **What's different:** In a ranked list, users compare items against each other. In swipe UI, each item is evaluated in isolation. This changes decision psychology fundamentally.
- **The bias:** Content that "hooks in 0.5s" wins — not content that's best. Thumbnails, first-frame, audio hook dominate. Substantive content that needs 3s to show value gets swiped past.
- **Signal confusion:** A "skip" doesn't mean "bad content." It means "didn't hook fast enough in THIS context at THIS fatigue level." But the model treats it as a negative relevance signal.
- **Contrast effect:** Item quality perception depends on what came before. Great content after 5 great items → skipped (saturation). Mediocre content after 5 bad items → engaged (relief).

**Fix:** Separate "hook quality" from "content quality" (short-term vs long-term engagement). Measure completion rate, replay, share — not just initial dwell. Sequence-aware models that account for preceding content.

### Visualization (canvas `ca3`, 720×300)

Paired horizontal bars (hook % vs quality %) for four content archetypes.

- **Title (bold 14px `#1a5276`, centered):** "Hook Speed vs Content Quality — What Swipe UI Selects For".
- **Rows (bars start at x=200, max width 320px, bar height 28, row pitch 76 from y=50; per row a hook bar on top and quality bar below, values labeled 10px `#333` after each bar):**
  - "Clickbait hook (shallow content)" — Hook 95%, Quality 20% — label color red `#e74c3c`
  - "Strong hook (good content)" — Hook 80%, Quality 75% — label color green `#27ae60`
  - "Slow build (excellent content)" — Hook 25%, Quality 92% — label color blue `#2980b9`
  - "No hook (great content)" — Hook 8%, Quality 85% — label color purple `#8e44ad`
- **Bar fills:** hook bar `rgba(231,76,60,0.5)` when hook >60 else `rgba(26,82,118,0.3)`; quality bar `rgba(39,174,96,0.5)` when quality >60 else `rgba(200,200,200,0.4)`.
- **Labels:** two-line archetype names bold 11px, right-aligned at x=190 in the row color.
- **Bottom line (bold 11px red, centered):** "Swipe UI: only hook matters. Slow-build content is invisible regardless of quality.".

## Regeneration instructions

- **Layout:** h1 + `.subtitle` paragraph, then three numbered `<h2>` sections ("1.", "2.", "3.", 1.3em `#1a5276` with a 2px `#2980b9` bottom border, 6px padding-bottom). Each section holds one `.obj-table` (full width, single `<tr>`): left `<td>` (40%) with `.obj-title` (1.05em, weight 600, `#1a5276`), a `<ul>` (0.9em), and a closing **Fix:** `<p>` (0.95em); right `<td>` (60%, centered) with the canvas (explicit `width="720" height="300"` attributes). Cell borders `1px solid #e0e0e0`, padding 20px 24px, `vertical-align: middle`; even rows `#fafcfe`.
- **Page style:** body system sans-serif, white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; `strong` in `#1a5276`. No nav bar, no back/home links.
- **Canvas:** all 720×300; shared `setup(id)` helper reads width/height attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates. Chart titles are bold 14px `#1a5276`; annotations 10-12px.
- **Palette:** primary blue `#1a5276`, chart blue `#2980b9`, green `#27ae60`, red `#e74c3c`, purple `#8e44ad`, grays `#666`/`#333`.
