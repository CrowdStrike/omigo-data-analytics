# Caching

**Page type:** detail page (one h2 + one-row obj-table per pitfall: text left 50%, canvas right 50%)
**HTML title tag:** Caching - Domain-Specific Pitfalls

**Subtitle:** Statistical pitfalls in caching systems — the false comfort of hit rates, thundering herds, and consistency impossibilities.

## Hit Rate ≠ Correctness

**99.5% Hit Rate Says Nothing About Whether the Hits Are Correct**

- **The headline number:** Cache hit rate = 99.5%, which reads as a system in great health.
- **What's hidden:** 0.5% misses plus 0.1% stale hits serving **WRONG data**.
- **Confidently wrong:** A high hit rate over stale data returns fast answers that are false.
- **Wrong axis:** Hit rate measures lookup success, not accuracy of the cached values.
- **The core flaw:** The metric everyone watches is orthogonal to the metric that matters.

### Visualization (canvas `canvas1`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Single horizontal stacked breakdown bar of hits/wrong/misses with legend and warning.

- **Title (bold 17px `#1a5276`, centered at x=360, y=30):** "Cache Hit Rate Breakdown (99.5% \"hits\")".
- **Bar:** 620px wide × 50px tall starting at (50,70), three segments: 99.4% correct hits green `#27ae60` (white 14px centered label "99.4% Correct Hits"); 0.1% stale/wrong hits red `#e74c3c` (drawn exaggerated ~12px minimum width for visibility); 0.5% misses gray `#95a5a6` (gray `#7f8c8d` 13px label "0.5% Miss" centered under/inside its segment).
- **Wrong-data callout:** bold red 15px "0.1% WRONG" above the red segment with a short red pointer line down to it.
- **Warning (bold red 15px, centered below bar):** "\"Serving wrong data confidently\" — hit rate hides correctness failures".
- **Legend (12×12 swatches at y=170, 13px `#333` text):** green "Correct Hits", red "Stale/Wrong (serving bad data)", gray "Misses".

## TTL Goldilocks Problem

**There Is No Universal "Right TTL" — Only a Freshness/Load Trade**

- **Too short:** Constant cache misses, no benefit, and the backend gets hammered.
- **Too long:** Requests are served data that is minutes or hours stale.
- **What sets the answer:** How fast the underlying data changes and how much staleness is tolerable.
- **Always a trade:** Every TTL buys freshness with performance, or performance with freshness.
- **Moving target:** The optimal value shifts as your data's change rate shifts.

### Visualization (canvas `canvas2`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Two crossing tradeoff curves over TTL duration with a shaded "sweet spot" band.

- **Title (bold 17px `#1a5276`, centered):** "TTL Trade-off: Miss Rate vs Staleness".
- **Axes:** L-shaped `#333` axes, plot from (80,45) to (650,155). X-axis labels (gray `#555` 14px): "Short" near left, "Long" near right, "TTL Duration →" centered below. Rotated vertical y-axis title: "Problem Severity".
- **Miss-rate curve:** red `#e74c3c` (width 2.5), exponential decay 0.9·exp(−i/15) — high at short TTL, dropping fast.
- **Staleness curve:** purple `#8e44ad` (width 2.5), saturating rise 0.85·(1−exp(−i/25)) — low at short TTL, rising.
- **Sweet spot:** vertical band from 25% to 45% of the x-range, filled `rgba(39,174,96,0.15)` with dashed green `#27ae60` border (dash 4/4); bold green 14px label above: "Sweet Spot".
- **Legend (12×12 swatches at top-right, 13px `#333`):** red "Miss Rate (backend load)", purple "Data Staleness".

## Thundering Herd on Expiry

**One Hot Key Expiring Sends 10,000 Requests at the Backend at Once**

- **The mechanism:** Popular key expires, 10,000 simultaneous requests all miss and hit the backend.
- **The amplification:** Backend is overwhelmed, times out, and all 10,000 retry into a storm.
- **Outage trigger:** Expiry of a hot key is a plausible cause of a full outage.
- **Popularity is the risk:** The more popular the key, the worse the thundering herd.
- **The irony:** Caching's success — concentrating load on few keys — creates the vulnerability.

### Visualization (canvas `canvas3`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Backend-load timeline: flat low load, expiry spike far past capacity, retry oscillation, eventual recovery.

- **Title (bold 17px `#1a5276`, centered):** "Thundering Herd: Key Expiry → Backend Overload".
- **Axes:** L-shaped `#333` axes, plot from (80,45) to (660,150). X label "Time →" centered below; rotated y-axis title "Backend Load" (gray `#555` 13px).
- **Normal load:** flat green `#27ae60` line (width 2) near the bottom, from left edge to the expiry point at 30% of the x-range.
- **Expiry marker:** vertical dashed red `#e74c3c` line (dash 4/3, width 2) at 30%, labeled bold red 13px below axis: "Key Expires".
- **Spike:** red line (width 2.5) shooting from the baseline to near the top and plateauing ~30px; bold dark-red `#c0392b` 14px label: "10,000 requests!".
- **Retry oscillation:** continued jagged red line dipping and re-spiking; bold purple `#8e44ad` 13px label: "Retries amplify!".
- **Failure zone:** area right of the expiry line (minus last 100px) shaded `rgba(231,76,60,0.1)`.
- **Capacity line:** horizontal dashed orange `#f39c12` line (dash 6/3, width 1.5) at 40px above the baseline, right-aligned orange 12px label: "Backend Capacity".
- **Recovery:** green line (width 2) descending toward the baseline at far right, green 12px label: "Recovery".

## Cache Poisoning Persists

**One Injection Is Served to Every User for the Whole TTL Window**

- **The attack:** Attacker poisons a DNS or CDN cache with a wrong response.
- **The persistence:** That bad response stays cached for the entire TTL duration.
- **Blast radius:** EVERY user in the window is served wrong or malicious content.
- **The arithmetic:** One successful poison costs TTL duration × users affected.
- **Worse than nothing:** Stale malicious data makes the cache actively harmful, not merely useless.

### Visualization (canvas `canvas4`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Timeline: poison-injection dot, shaded TTL window with poisoned requests, rising cumulative-victims line, TTL-expiry dot.

- **Title (bold 17px `#1a5276`, centered):** "Cache Poisoning: One Injection → Mass Impact".
- **Timeline axis:** horizontal `#333` line (width 1.5) at y=150 from x=80 to x=660; "Time →" centered below (gray `#555` 13px).
- **Poison point:** filled red `#e74c3c` 8px-radius dot at x=140 on the axis, bold red 13px label below: "Poison Injected".
- **TTL window:** rectangle from the poison point to x=580 (y=50–150) filled `rgba(231,76,60,0.15)` with dashed red border (dash 5/3, width 2); bold dark-red `#c0392b` 14px label above: "← TTL Window: Every request gets poisoned response →".
- **Victims line:** purple `#8e44ad` line (width 2.5) rising linearly across the window; purple 13px two-line label right of the window: "Users affected" / "(cumulative)".
- **Poisoned requests:** eight red "✕" glyphs (16px) evenly spaced just above the axis inside the window.
- **TTL expiry:** filled green `#27ae60` 6px-radius dot at the window's right end, green 12px label below: "TTL Expires".
- **Bottom note (dark-red `#c0392b` 13px, centered at y=190):** "Impact = TTL duration × request rate".

## Cold Start After Deployment

**A Deploy Hands the Backend 100% of Traffic It Was Sized for 5% Of**

- **The trigger:** Deploy a new version and all caches start empty.
- **The load jump:** Backend absorbs 100% of traffic instead of the 5% miss rate it was sized for.
- **The outcome:** Instant overload, then cascade failure downstream.
- **Not a slope, a cliff:** The transition from "warm" to "cold" happens in one step.
- **Hidden requirement:** The cache masked true backend capacity needs until it vanished.

### Visualization (canvas `canvas5`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Backend-load time series: 5% warm-cache steady state, vertical spike to 100% at deploy, gradual decay back as cache re-warms.

- **Title (bold 17px `#1a5276`, centered):** "Cold Start: Deployment Empties Cache → Overload".
- **Axes:** L-shaped `#333` axes, plot from (80,45) to (660,155). Y labels (gray `#555` 12px, right-aligned): "100%" (top), "20%", "5%" (near bottom). X label "Time →" centered below.
- **Steady state:** green `#27ae60` line (width 2.5) at the 5% level from the left edge to the deploy point (40% of x-range); green 13px label above it: "Warm cache: 5% load".
- **Deploy marker:** vertical dashed dark `#2c3e50` line (dash 4/3, width 2) at 40%, bold 13px two-line label below the axis: "Deploy" / "(cache cleared)".
- **Spike:** red `#e74c3c` line (width 2.5) jumping to ~100%, holding briefly, then decaying through ~15% back to the 5% level by the right edge.
- **Capacity line:** horizontal dashed orange `#f39c12` line (dash 6/3) at the 20% level, bold orange 12px right-aligned label: "Designed Capacity (20%)".
- **Overload zone:** area above the capacity line right of the deploy marker shaded `rgba(231,76,60,0.08)`.
- **Spike label (bold red 14px, left-aligned near top):** "100% traffic → backend!".

## Write-Through vs Write-Behind Consistency

**Both Write Strategies Keep the Consistency Problem — They Just Relocate It**

- **Write-through:** Slow writes, but the cache is the source of truth.
- **Write-behind:** Fast writes, but the cache can lose data on crash.
- **Read-after-write:** Whether your own write is visible next read depends on which you chose.
- **Compounded:** Distributed cache plus distributed DB gives eventual-consistency nightmares.
- **No escape:** Neither strategy eliminates the problem; each picks a different failure mode.

### Visualization (canvas `canvas6`, 720×200 — HTML attribute height 300 but setup helper forces 720×200)

Side-by-side App→Cache→DB flow diagrams comparing the two write strategies.

- **Title (bold 17px `#1a5276`, centered at y=22):** "Write-Through vs Write-Behind Consistency". Dashed light-gray `#bdc3c7` vertical divider at x=360.
- **Left diagram — header bold 14px blue `#2980b9` "Write-Through":** three boxes outlined `#2980b9` (width 2), fill `#eaf2f8`, dark `#2c3e50` 12px labels: "App" (80,55, 70×30), "Cache" (172,55, 70×30), "DB" (267,55, 55×30); solid green `#27ae60` arrows between all boxes (synchronous).
  - Properties (12px): green "✓ Consistent", green "✓ No data loss on crash", red `#e74c3c` "✕ Slow writes (synchronous)".
- **Right diagram — header bold 14px purple `#8e44ad` "Write-Behind":** same three boxes outlined `#8e44ad`, fill `#f4ecf7`: "App" (440,55), "Cache" (532,55), "DB" (627,55); solid green arrow App→Cache, dashed orange `#f39c12` arrow Cache→DB with italic orange 11px label "async".
  - Properties (12px): green "✓ Fast writes", red "✕ Data loss on crash!", red "✕ Inconsistent reads possible".
  - Red lightning-bolt glyph (width 2 stroke) under the Cache box with red 11px label "CRASH".
- **Bottom summary (bold dark-red `#c0392b` 13px, centered at y=165):** "Neither eliminates consistency problems — they trade speed for safety".

## Regeneration instructions

- **Layout:** domains detail-page convention — h1, `.subtitle`, then per pitfall an unnumbered `<h2>` (1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a one-row `.obj-table`: left `<td>` (40%) with `.obj-title` (a one-line punchline) followed by a `<ul>` of 4-5 `<li>` labeled bullets (`<strong>Label:</strong> phrase`), right `<td>` (60%, centered) with the canvas. Even rows background `#fafcfe`.
- **Page CSS:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` 1.05em weight 600 `#1a5276` margin-bottom 8px; `ul` margin `8px 0 8px 20px`, 0.9em, `#333`; `li` margin `4px 0`; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `strong` `#1a5276`; `.philosophy` callout style defined (background `#f0f4f8`, left border `4px solid #2980b9`) but unused on this page. No nav bar, no back/home links.
- **Canvas:** each `<canvas>` declared with `width="720" height="300"` attributes, but a shared `setupCanvas(id)` helper fixes the drawing size to 720×200 CSS pixels, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`), and calls `ctx.scale` so drawing stays in logical coordinates; default font 17px system sans-serif. These canvases have no separate light plot background fill.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c` (dark red `#c0392b`), orange `#f39c12`, purple `#8e44ad`, grays `#95a5a6`/`#7f8c8d`/`#bdc3c7`, dark `#2c3e50`, gray text `#333`/`#555`.
- Card/grid links elsewhere point to this page as `domains/067-caching.html` in regenerated HTML.
