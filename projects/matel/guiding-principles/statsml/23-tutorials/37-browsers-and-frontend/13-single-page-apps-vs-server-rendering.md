# Single-Page Apps vs Server Rendering

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Single-Page Apps vs Server Rendering

**Subtitle:** The same web page can be assembled by the server before it is sent, or by JavaScript after it arrives — who builds the HTML changes everything about speed

## Who Builds the Page for the Dark Roast?

**Tags:** `core idea` (blue), `who builds the HTML` (green), `two paths` (orange)

- **The page** — an online coffee shop's product page: Dark Roast 500g, $14, three photos, 128 reviews
- **Server rendering** — the server queries its database and sends finished HTML; the browser just paints it
- **Single-page app** — the server sends a nearly empty HTML shell plus a JavaScript bundle instead
- **The fetch** — the SPA's JavaScript then calls an API for the product data as JSON and builds the HTML itself
- **Same pixels** — both paths end at the identical page; what differs is where the HTML gets assembled

*Example (italic):* View-source on the server-rendered page shows "$14" right in the HTML; on the SPA it shows an empty `<div id="app">` and a script tag.

**Key point:** Server rendering builds the HTML on the server for every request; a single-page app ships JavaScript that builds the HTML in the browser and rewrites the page in place as you click around.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the server-rendered path (HTML built before sending) vs the SPA path (HTML built in the browser), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Same Product Page, Two Builders of the HTML".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20 above the row:** "server-rendered"; blue `#2a78d6` rounded box at x=70 labeled "browser asks /dark-roast" (12px), 3px arrow to a green `#008300` box at x=290 labeled "server: DB query + template → HTML", 3px arrow to a blue box at x=545 labeled "browser paints finished HTML" with bold 12px green "HTML built here ↑ (server)" beneath the middle box.
- **Row 2 (boxes centered on y=215), label:** "single-page app"; blue box at x=70 "browser asks /dark-roast", arrow to a mute `#6b7280` box at x=250 labeled "server: empty shell + 300 KB JS", arrow to an orange `#d95926` box at x=450 labeled "JS runs, calls API for JSON", arrow to an orange box at x=620 labeled "JS builds HTML" with bold 12px orange "HTML built here ↑ (browser)" beneath.
- **Box style:** 130–170px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(107,114,128,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text, 2-line wrap allowed.
- **Annotation (bold 13px ink `#1a5276`, centered near y=280):** "identical final page — the only question is who assembled it".

## First Visit vs the Next Click, in Numbers

**Tags:** `worked example` (blue), `time to first content` (green), `bytes` (orange)

- **Server first visit** — one response of 30 KB finished HTML; first content painted at 0.8s
- **SPA first visit** — 5 KB shell + 300 KB JS bundle + 4 KB JSON; first content painted at 2.6s
- **Server next click** — Espresso Blend is a whole new page: another 30 KB and 0.7s, every click
- **SPA next click** — only 4 KB of JSON moves; JavaScript rewrites the page in place in 0.3s
- **The trade** — the SPA pays a big entry fee once, then navigates cheaply; the server page pays a flat rate

*Example (italic):* Clicking from Dark Roast to Espresso Blend transfers 30 KB on the server-rendered shop but just 4 KB of JSON on the SPA.

**Key point:** The SPA moves cost from every navigation to the first load — 2.6s to start but 0.3s per click after, versus a steady 0.8s then 0.7s per click for server rendering.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: time to first content for "first visit" and "next click", one blue bar (server-rendered) and one orange bar (SPA) per group, with transferred bytes labeled above each bar.

- **Title (bold 15px, `#1a5276`, top center):** "Time to Content: the SPA Pays Up Front, Then Clicks Are Cheap".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; y = seconds 0 to 3, gridlines `#e5e9ef` at 1s/2s with 12px `#444` labels "1s", "2s", "3s"; two x groups centered at x=250 ("first visit") and x=510 ("next click"), 13px `#444` group labels below the baseline.
- **Bars (56px wide, 24px gap within a group):** server-rendered blue `#2a78d6` at heights for seconds `[0.8, 0.7]`; SPA orange `#d95926` at heights for seconds `[2.6, 0.3]` — scale: 60px per second.
- **Value labels (bold 12px, bar color, just above each bar):** "0.8s", "2.6s", "0.7s", "0.3s".
- **Byte labels (11px `#6b7280`, above the value labels):** "30 KB", "309 KB", "30 KB", "4 KB".
- **Legend (12px, top right):** blue swatch "server-rendered", orange swatch "single-page app".
- **Annotation (bold 13px green `#008300`, near x=510, y=95):** "after load-in, SPA clicks cost 4 KB and 0.3s".
- **Caption (12px `#444`, bottom right):** "times and sizes illustrative".

## Content Sites, Dashboards, and the Hybrid Middle

**Tags:** `where it's used` (blue), `SEO` (green), `hybrid` (orange)

- **Search engines** — some crawlers run JS, but slower and less reliably; server HTML is the safe path
- **Slow devices** — a 300 KB bundle must be downloaded and parsed before anything shows on a cheap phone
- **Content sites** — blogs, news, product pages: visitors land, read one page, leave — server rendering fits
- **Dashboards** — analytics tools with constant clicking, filters, and live charts fit the SPA model
- **The hybrid** — SSR + hydration: the server sends real HTML for the first paint, then JS takes over the clicks

*Example (italic):* A news article gets most of its visitors from search, each reading a single page — the first paint is the whole game, so it is rendered on the server.

**Key point:** Choose by visit shape — one-page visits favor server rendering, long interactive sessions favor an SPA, and hybrid SSR-plus-hydration tries to buy both at the cost of extra machinery.

### Visualization (canvas `c3`, 720×300)

Spectrum chart: a horizontal axis from "land, read, leave" to "log in, click all day", with labeled dots for six site types and three shaded fit zones.

- **Title (bold 15px, `#1a5276`, top center):** "Pick by Visit Shape, Not by Fashion".
- **Axis:** horizontal 2px `#1a5276` line at y=170 from x=60 to x=660; 12px `#444` end labels "land, read, leave" (left, below axis) and "log in, click all day" (right, below axis).
- **Zones (rects y=90 to y=250):** left zone x=60–260 fill `rgba(42,120,214,0.10)` with bold 12px blue `#2a78d6` label "server rendering fits" at top; right zone x=460–660 fill `rgba(217,89,38,0.10)` with bold 12px orange `#d95926` label "SPA fits"; middle zone x=260–460 fill `rgba(0,131,0,0.08)` with bold 12px green `#008300` label "hybrid: SSR + hydration".
- **Dots (8px radius on the axis line, 12px `#2c3e50` labels alternating above/below):** blog post at x=95, news article at x=155, product page at x=225, email client at x=490, analytics dashboard at x=560, spreadsheet editor at x=635 — dots colored by their zone (blue / blue / blue / orange / orange / orange).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "the middle is where most modern frameworks live".
- **Caption (12px `#444`, bottom right):** "positions schematic".

## "SPAs Are Faster" Depends on How Many Pages You Read

**Tags:** `common mistake` (red), `break-even` (orange)

- **The claim** — "we rebuilt the shop as an SPA, so it's faster" is only true after enough clicks
- **Cumulative math** — server: 0.8s + 0.7s per extra page; SPA: 2.6s + 0.3s per extra page
- **Break-even** — the running totals cross between page 5 and page 6: the SPA wins only from the 6th page on
- **Real visits** — many shop visitors view one or two pages; they only ever see the SPA's slow side
- **Blank flash** — until the bundle runs, the SPA visitor stares at an empty shell; server pages never do

*Example (italic):* A two-page visit takes 1.5s total server-rendered but 2.9s on the SPA — nearly double, despite the "faster" rebuild.

**Common mistake:** Judging speed from the developer's demo, where the bundle is already cached and every click feels instant — the first-time visitor on a slow connection pays the 300 KB entry fee the demo never shows.

### Visualization (canvas `c4`, 720×300)

Two cumulative-time lines over pages viewed 1 to 8: server-rendered (blue, cheap start, steady climb) vs SPA (orange, expensive start, shallow climb), crossing between page 5 and 6.

- **Title (bold 15px, `#1a5276`, top center):** "Total Waiting Time: the SPA Only Wins From the 6th Page".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 180; x = pages viewed 1 to 8, 12px `#444` tick labels "1"–"8"; y = cumulative seconds 0 to 6, gridlines `#e5e9ef` at 2s/4s with 12px `#444` labels.
- **Server line:** blue `#2a78d6` 3px line with 4px dots through pages `[1, 2, 3, 4, 5, 6, 7, 8]`, cumulative seconds `[0.8, 1.5, 2.2, 2.9, 3.6, 4.3, 5.0, 5.7]`.
- **SPA line:** orange `#d95926` 3px line with 4px dots through the same pages, cumulative seconds `[2.6, 2.9, 3.2, 3.5, 3.8, 4.1, 4.4, 4.7]`.
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at x between page 5 and 6, 12px `#6b7280` label "break-even" at its top.
- **Line labels (bold 12px, line color, at the right ends):** blue "server-rendered", orange "single-page app".
- **Annotation (bold 13px red `#e74c3c`, near page 2, y=80):** "a 2-page visit: 1.5s vs 2.9s — the SPA is the slow one".
- **Caption (12px `#444`, bottom right):** "per-page times illustrative, cumulative sums exact".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); load times (0.8s / 2.6s first visit, 0.7s / 0.3s per click) and transfer sizes (30 KB HTML, 5 KB shell, 300 KB bundle, 4 KB JSON) are invented and labeled illustrative; the c4 cumulative arrays are the exact running sums of those per-page times, so the break-even at page 6 follows from them.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
