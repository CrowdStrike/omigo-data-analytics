# AJAX & fetch

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** AJAX & fetch

**Subtitle:** AJAX lets a page ask the server for just the data it needs and update in place — the dashboard number changes without the whole page reloading

## The Dashboard That Stopped Blinking

**Tags:** `core idea` (blue), `no reload` (green), `browser` (orange)

- **The screen** — a coffee shop's wall dashboard shows one big number: orders today, currently 409
- **The old way** — to see a fresh count the page reloads: white flash, everything redraws, 2.4 seconds
- **The waste** — the logo, menu, and charts are re-downloaded even though only one number changed
- **The AJAX way** — the page quietly asks the server "orders now?" and swaps just the number in place
- **The feel** — the count ticks 409 → 412 in about 90 ms; the rest of the page never blinks

*Example (italic):* At 2:03pm the dashboard updates from 409 to 412 orders and nobody watching can tell a request happened.

**Key point:** AJAX means the page fetches data in the background and edits itself in place — a request no longer has to mean a full page reload.

### Visualization (canvas `c1`, 720×300)

Two-row timeline comparing what the user sees during one update: full reload (long white-screen gap) vs AJAX fetch (page stays visible, number swaps).

- **Title (bold 15px, `#1a5276`, top center):** "One Update: Full Reload Blanks the Page for 2.4s, fetch Swaps a Number in 90ms".
- **Axes:** origin x=60, plot width 600; x = time 0 to 3 seconds, 2px `#999` baseline at y=245, 12px `#444` tick labels "0s"–"3s" every 0.5s; two horizontal lanes at y=95 ("full reload", 12px `#444` label at x=20) and y=185 ("fetch", same style).
- **Full reload lane:** green `#008300` segment (14px tall bar) from 0 to 0.2s labeled "page visible (409)"; red `#e74c3c` bar from 0.2s to 2.6s labeled bold 12px red "white screen — 2.4s"; green bar from 2.6s to 3s labeled "page back (412)".
- **Fetch lane:** one continuous green `#008300` bar from 0 to 3s labeled "page visible the whole time"; blue `#2a78d6` tick (4px wide) at 0.2s with 12px blue label "request sent"; violet `#4a3aa7` marker at 0.29s with bold 12px violet label "409 → 412 (90ms)".
- **Annotation (bold 13px green `#008300`, right side near y=150):** "same new number, zero blank screen".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## One fetch Call, One Number Changes

**Tags:** `worked example` (blue), `JSON` (green)

- **The call** — `fetch('/api/orders/today')` sends a background GET request from the page's script
- **The reply** — the server answers with a tiny JSON body: `{"orders": 412}` — ~0.06 KB with headers
- **The parse** — `response.json()` turns those bytes into a JavaScript object the script can read
- **The swap** — `document.getElementById('order-count').textContent = data.orders` edits one DOM node
- **Hand-check** — a full reload moves 1,200 KB (40 HTML + 120 CSS + 600 JS + 440 images); 0.06 KB is 20,000× less

*Example (italic):* Refreshing the count by full reload costs 1,200 KB; the fetch reply carrying `{"orders": 412}` costs ~0.06 KB with headers — 20,000× smaller.

**Key point:** The pattern is always the same three steps — fetch a URL, parse the JSON, write the value into one element — and only the data travels, not the page.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of bytes transferred for the same update: full reload stacked by asset type vs the JSON-only fetch sliver.

- **Title (bold 15px, `#1a5276`, top center):** "Bytes to Update One Number: 1,200 KB Reload vs 0.06 KB JSON".
- **Axis:** left edge x=170, bars extend right, max width 480; 2px `#999` vertical baseline at x=170; rows labeled 12px `#444` at x=20.
- **Row 1 (y=90), "full reload — 1,200 KB":** one 26px-tall stacked bar, segment widths proportional to hardcoded KB `[40, 120, 600, 440]`: HTML blue `#2a78d6` width 16, CSS aqua `#199e70` width 48, JS violet `#4a3aa7` width 240, images yellow `#c98500` width 176 (total 480); 11px `#444` segment labels "HTML 40" / "CSS 120" / "JS 600" / "img 440" above or inside segments.
- **Row 2 (y=190), "fetch — 0.06 KB":** green `#008300` bar drawn 3px wide (true scale would be invisible), 12px green label "{\"orders\": 412} — 0.06 KB" to its right.
- **Annotation (bold 13px magenta `#d55181`, near x=400, y=240):** "20,000× less data for the same new number".
- **Caption (12px `#444`, bottom right):** "asset sizes illustrative; ratio computed from them".

## The 2005 Shift to Live Pages

**Tags:** `where it's used` (blue), `history` (orange)

- **Before** — through the early 2000s almost every click on the web meant a full page round-trip
- **The name** — the term "AJAX" was coined in a 2005 essay describing pages that update in place
- **The engine** — the underlying browser object, `XMLHttpRequest`, had shipped years earlier (~1999)
- **The cleanup** — the `fetch()` API (standardized mid-2010s) replaced it with a simpler promise-based call
- **Today** — every SPA, dashboard, autocomplete box, and infinite scroll is this one idea repeated

*Example (italic):* A data-science dashboard polling `/api/metrics` every 30 seconds is running the same pattern as a 2005 webmail inbox.

**Key point:** AJAX is the dividing line between the web as linked documents and the web as applications — a data scientist meets it in every live dashboard they build or read.

### Visualization (canvas `c3`, 720×300)

Horizontal milestone timeline 1995–2020 with markers for the key steps from full-reload pages to fetch.

- **Title (bold 15px, `#1a5276`, top center):** "From Page Flips to Live Apps: 25 Years in Four Steps".
- **Axis:** 3px `#999` horizontal line at y=170 from x=60 to x=660; 12px `#444` year ticks at 1995/2000/2005/2010/2015/2020, evenly spaced (120px per 5 years).
- **Markers (10px filled circles on the line, each with a bold 12px label above or below, staggered to avoid overlap):**
  - 1999, blue `#2a78d6`: "XMLHttpRequest ships" (label above, y=130)
  - 2005, green `#008300`: "'AJAX' named — pages update in place" (label below, y=210)
  - 2006, orange `#d95926`: "libraries wrap it for everyone" (label above, y=100)
  - 2015, violet `#4a3aa7`: "fetch() standard — promises, cleaner code" (label below, y=240)
- **Era shading:** `rgba(42,120,214,0.08)` band from 1995 to 2005 labeled 11px `#6b7280` "full-reload web"; `rgba(0,131,0,0.08)` band from 2005 to 2020 labeled "in-place updates".
- **Annotation (bold 13px ink `#1a5276`, near x=500, y=70):** "same idea, friendlier API".
- **Caption (12px `#444`, bottom right):** "milestone years approximate".

## fetch Doesn't Throw on a 404

**Tags:** `common mistake` (red), `error handling` (orange)

- **The trap** — `fetch` rejects only when the network itself fails; a 404 or 500 reply resolves normally
- **The result** — the code happily calls `.json()` on an error page and the parse fails or returns junk
- **The symptom** — the dashboard silently keeps showing 412 (or "undefined") while the API is down
- **The fix** — check `response.ok` (true only for status 200–299) before touching the body
- **The habit** — on a bad status, branch: keep the last good number and show a small "stale" badge

*Example (italic):* The API starts returning 500 at 2:10pm; without a `response.ok` check the wall dashboard shows a frozen 412 for the rest of the day and nobody notices.

**Common mistake:** Assuming a resolved fetch means success. To fetch, "the server answered" and "the server answered with your data" are different things — only `response.ok` tells them apart.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a 500 reply flowing through code without an `ok` check (silent stale number) vs with one (stale badge shown).

- **Title (bold 15px, `#1a5276`, top center):** "Server Returns 500: With and Without the response.ok Check".
- **Row 1 (y=95), label 12px `#444` at x=20:** "no ok check"; orange `#d95926` rounded box at x=150 labeled "reply: 500 error page" (12px), 3px arrow to a red `#e74c3c` box at x=390 labeled ".json() fails silently", arrow to a red box at x=580 labeled "shows stale 412" with bold 12px red "✗ looks fine, is wrong".
- **Row 2 (y=205), label:** "with ok check"; orange box "reply: 500 error page", 3px arrow to a green `#008300` box at x=360 labeled "response.ok? no → branch", arrow to a green box at x=570 labeled "412 + 'stale' badge" with bold 12px green "✓ honest".
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(217,89,38,0.12)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "a wrong number that looks right is worse than an error message".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); order counts (409 → 412), reload/fetch timings (2.4s vs 90ms), and asset sizes (40/120/600/440 KB vs 0.06 KB JSON) are invented and labeled illustrative; the 20,000× ratio follows from those sizes; timeline years (1999 XMLHttpRequest, 2005 AJAX coined, 2015 fetch standard) are documented history, marked approximate in the caption.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
