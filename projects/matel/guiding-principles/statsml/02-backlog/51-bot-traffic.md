# Bot Traffic: The Invisible Majority

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one `.lang-section` per topic; BACKLOG status badge in h1)
**HTML title tag:** Bot Traffic: The Invisible Majority

**Status badge:** BACKLOG (inline in h1)

**Subtitle:** Why a large chunk — sometimes the majority — of ecommerce and high-traffic site data is not human-generated.

**Intro callout:** A large share of web traffic is automated, and on ecommerce sites it can be the majority. Treating it as human behavior contaminates every downstream metric — yet the same traffic is legitimate signal for SEO and content-visibility analysis.

## 1. The Scale Problem

Industry reports consistently show 40–65% of all web traffic is automated. For large ecommerce sites, this can exceed 50%.

- **Known bots** — Googlebot, Bingbot, etc. Respect `robots.txt`, identify themselves via User-Agent.
- **Stealth bots** — scrapers, price monitors, inventory checkers. Spoof headers, rotate IPs, ignore `robots.txt`.
- **Grey-area bots** — SEO crawlers, ad verification, uptime monitors. Legitimate purpose, but not user intent.

**Key point (red left-border box):** If you treat all traffic as user behavior, your conversion rates, engagement metrics, and funnel analyses are contaminated at the source.

### Visualization (canvas `c1`, 720×340)

Horizontal bar chart of traffic composition with a dashed human/bot divider line.

- **Title (bold 13px, `#1a5276`, centered at y=20):** "Typical Ecommerce Traffic Composition".
- **Bars (barX=90, bar height 36, gap 10, startY=50, max width w−180, 0.8 alpha; label 12px `#2c3e50` right-aligned left of bar; value "N%" bold 12px in bar color right of bar):**
  | Label | % | Color |
  |-------|---|-------|
  | Real Users | 38 | `#27ae60` |
  | Search Bots | 22 | `#2980b9` |
  | Stealth Scrapers | 20 | `#e74c3c` |
  | SEO / Ad Verify | 12 | `#e67e22` |
  | Other Automated | 8 | `#8e44ad` |
- **Divider:** vertical dashed red line (`#e74c3c`, dash 3/3) at the 38% mark spanning the bar area, with 10px red label "← human | bot →" near the bottom.
- **Caption (11px `#888`, centered near bottom):** "62% of traffic is non-human — the majority".

## 2. Why It Matters for Data Analysis

Bot traffic distorts every downstream metric:

- **Bounce rate** — bots hit one page and leave, inflating bounce.
- **Session duration** — bots are either instant (scrape & go) or abnormally long (crawl everything).
- **Page popularity** — bots hit product pages systematically, not by interest.
- **Conversion funnels** — denominator is inflated, making real conversion look worse.
- **A/B tests** — bots don't respond to treatments, diluting effect sizes.

**Key point:** The statistical damage: inflated N, biased distributions, attenuated effects. Classic symptoms of measurement contamination.

### Visualization (canvas `c2`, 720×340)

Two-column paired bar comparison: each metric shown raw (with bots, red) vs filtered (green), bars normalized per-metric to the larger value.

- **Title (bold 13px, `#1a5276`, centered at y=20):** "Metric Distortion: With vs Without Bot Traffic".
- **Column headers (11px `#666`, centered at y=40):** "With Bots (raw)" over the left column, "Bots Filtered" over the right column.
- **Rows (startY=50, row height 68; metric name 11px `#2c3e50` at left; bars 20px tall; red bar fill `rgba(231,76,60,0.3)` with value in bold `#e74c3c`; green bar fill `rgba(39,174,96,0.3)` with value in bold `#27ae60`; light `#eee` separator lines between rows):**
  | Metric | With Bots | Without Bots |
  |--------|-----------|--------------|
  | Conversion Rate | 1.2 | 3.1 |
  | Avg Session (s) | 18 | 145 |
  | Bounce Rate % | 72 | 44 |
  | Pages / Session | 2.1 | 4.8 |

## 3. The SEO Paradox

Bot traffic isn't purely noise — it signals what automated systems value:

- Search engine bots determine what gets indexed and ranked.
- Content that bots crawl frequently is content that *search engines care about*.
- Monitoring bot crawl patterns can inform SEO strategy — which pages get crawled, how deep, how often.

**Philosophy callout (blue left-border box):** **The dual nature:** Bot traffic is noise for user behavior analysis, but signal for content visibility analysis. The same data requires different filters depending on the question being asked.

### Visualization (canvas `c3`, 720×300)

Two-circle (Venn-style) dual-nature diagram.

- **Title (bold 13px, `#1a5276`, centered at y=20):** "Dual Nature of Bot Traffic".
- **Left circle (center 32% width, 52% height, radius 85):** fill `rgba(231,76,60,0.12)`, stroke `#e74c3c` width 2. Heading bold 12px `#e74c3c`: "NOISE"; below in 11px `#555`: "User behavior" / "analysis" / "Conversion funnels" / "A/B tests".
- **Right circle (center 68% width, same y, radius 85):** fill `rgba(39,174,96,0.12)`, stroke `#27ae60` width 2. Heading bold 12px `#27ae60`: "SIGNAL"; below in 11px `#555`: "SEO optimization" / "Content visibility" / "Crawl budget" / "Index coverage".
- **Caption (11px `#888`, centered at bottom):** "Same data — different question — different filter".

## 4. Detection Signals

No single signal catches all bots. A layered approach:

- **User-Agent string** — catches honest bots only.
- **Request rate & pattern** — too fast, too regular, or perfectly sequential.
- **Session shape** — no mouse events, no scroll, no dwell time variance.
- **IP reputation** — data center IPs, known proxy ranges.
- **JavaScript execution** — many bots don't execute JS or fail fingerprint challenges.

**Key point:** Stealth bots actively evade each signal individually. Detection requires combining multiple weak signals — a classification problem, not a rule-based filter.

### Visualization (canvas `c4`, 720×340)

Horizontal progress-style bars: bot sophistication level vs evasion rate.

- **Title (bold 13px, `#1a5276`, centered at y=20):** "Bot Sophistication vs Detection Difficulty".
- **Bars (barX=180, max width w−220, bar height 40, gap 10, startY=48; gray `#f4f4f4` background track, filled portion at 0.7 alpha; label 11px `#2c3e50` right-aligned left of bar; value "N% evasion rate" bold 11px in bar color right of fill):**
  | Label | Evasion % | Color |
  |-------|-----------|-------|
  | Declared bots (UA string) | 10 | `#27ae60` |
  | Basic scrapers (rate limit) | 30 | `#2ecc71` |
  | Rotating IPs (IP reputation) | 55 | `#e67e22` |
  | Header spoofing (JS check) | 75 | `#e74c3c` |
  | Full browser emulation | 92 | `#8e44ad` |
- **Caption (11px `#888`, centered at bottom):** "No single rule catches the right tail — requires ensemble classification".

## Regeneration instructions

- **Template/layout:** backlog kusto-style detail page. `<h1>` with inline `<span class="status">BACKLOG</span>` badge, `.subtitle` paragraph, `.intro` callout, then four `.lang-section` blocks, each an `<h2>` ("N. Title") plus a `table.layout` with one row: left `td.text-col` (45%) with intro paragraph, `<ul>` of bold-labeled bullets, and a `.key-point` (sections 1, 2, 4) or `.philosophy` (section 3) callout; right `td.viz-col` (55%) with the canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. h2 1.3rem `#1a5276`, 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` and `.philosophy` background `#f0f4f8`, left border 3px `#2980b9`, 0.9rem. `.key-point` background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem. `.status` badge: background `#fef9e7`, border 1px `#f39c12`, text `#b7950b`, radius 4px. Inline `code`: background `#e8f0f8`, text `#1a5276`, radius 3px. `ul` 0.92rem. Canvases `width: 100%`, border 1px `#e0e0e0`, radius 4px.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange; also `#2980b9` accent blue, `#8e44ad` purple, `#2ecc71` light green.
- **Canvas rendering:** canvases declare intrinsic width/height and are scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper; fonts are -apple-system sans-serif.
- Note: in regenerated HTML any card/page links use `.html` extensions (this page has none).
