# Cookies, localStorage, Sessions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cookies, localStorage, Sessions

**Subtitle:** HTTP forgets you the instant a page loads — cookies, localStorage, and server sessions are three places the web stashes a note so the next request still knows who you are

## The Cart That Survives a Refresh

**Tags:** `core idea` (blue), `stateless web` (green), `cookies` (orange)

- **The shop** — an online tea shop; you add 3 tins to the cart, then click to another page
- **The amnesia** — HTTP is stateless: to the server, the second page load is a total stranger
- **The note** — on your first visit the server's reply includes `Set-Cookie: cart=CART-1`
- **The echo** — your browser attaches `Cookie: cart=CART-1` to every later request, automatically
- **The memory** — the server reads `K7`, looks up your cart, and the 3 tins are still there

*Example (italic):* You refresh the checkout page five times; each request quietly carries `cart=CART-1`, so the 3 tins never vanish.

**Key point:** The web remembers you by making your browser repeat a small note back on every request — the server never remembers on its own; it re-recognizes you each time.

### Visualization (canvas `c1`, 720×300)

Two-row request/response flow diagram: the first visit where the server issues the cookie, and a later page load where the browser echoes it back and the cart survives.

- **Title (bold 15px, `#1a5276`, top center):** "One Small Note Makes a Stateless Server Remember".
- **Row 1 (y=105), label 12px `#444` at x=20:** "first visit"; blue `#2a78d6` rounded box at x=140 labeled "browser: add 3 tins" (12px), 3px arrow right to an ink `#1a5276` box at x=420 labeled "server replies Set-Cookie: cart=CART-1", 12px mute `#6b7280` label "no cookie yet" above the arrow.
- **Row 2 (y=215), label:** "next page load"; blue box at x=140 labeled "browser sends Cookie: cart=CART-1", 3px arrow right to a green `#008300` box at x=420 labeled "server looks up" / "CART-1 → 3 tins" on two lines, bold 12px green "✓ cart intact" at x=610.
- **Box style:** 170–190px wide, 44px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(26,82,118,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "the browser repeats the note on every request — that repetition IS the memory".
- **Caption (12px `#444`, bottom right):** "cart id is a made-up placeholder; tin count illustrative".

## Three Places to Stash the Cart

**Tags:** `worked example` (blue), `sizes & lifetimes` (green), `three stores` (orange)

- **The cart data** — 3 tins as a JSON blob is about 180 bytes; the visit makes 25 requests
- **Cookie cart** — the whole 180-byte blob rides every request: 25 × 180 = 4,500 bytes upstream
- **Session cookie** — only a 40-byte id rides along (25 × 40 = 1,000 bytes); the cart lives on the server
- **localStorage cart** — the blob sits in the browser; 0 bytes ride along until checkout posts it once
- **Capacity** — a cookie tops out around 4 KB; localStorage gives roughly 5 MB per site; the server store is as big as its database

*Example (italic):* The same 3-tin cart costs 4,500 bytes of cookie traffic, 1,000 bytes as a session id, or 0 bytes in localStorage — identical cart, three price tags.

**Key point:** Cookies travel on every request, localStorage never travels, and a server session travels as a tiny id — pick by asking "who needs to see this data, and how often?".

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: upstream bytes spent carrying the same 180-byte cart across a 25-request visit, one bar per storage choice, with capacity noted per row.

- **Title (bold 15px, `#1a5276`, top center):** "Same Cart, Three Costs: Bytes Sent Over a 25-Request Visit".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 = 4,500 bytes; light `#e5e9ef` gridlines at 1,500 and 3,000 bytes with 11px `#6b7280` labels.
- **Rows (top to bottom at y = 90, 155, 220), each with a left-aligned 12px `#444` two-line label at x=20:**
  - "cookie holds cart / limit ~4 KB": orange `#d95926` bar width 440, 12px white label "4,500 B" right-aligned inside the bar end
  - "session id cookie / cart on server": blue `#2a78d6` bar width 98, 12px label "1,000 B"
  - "localStorage / limit ~5 MB per site": green `#008300` bar width 3 (sliver), 12px green label "0 B until checkout"
- **Bar style:** 26px tall, solid fills, 4px radius.
- **Annotation (bold 13px magenta `#d55181`, right side near y=60):** "cookies bill you on every single request".
- **Caption (12px `#444`, bottom right):** "cart bytes and request count illustrative; 4 KB / 5 MB are typical browser limits".

## Logins, Analytics, and Remembered Preferences

**Tags:** `where it's used` (blue), `auth` (green), `personalization` (orange)

- **Login** — after you sign in, a session cookie is the proof; lose it and every page demands a password
- **Server timeout** — the tea shop's server session expires after 30 idle minutes, logging you out
- **Analytics** — a persistent cookie with a visitor id, set to expire in 30 days, links your return visits
- **Preferences** — dark mode and "sort by price" live happily in localStorage for years, never sent anywhere
- **Tab scratchpad** — sessionStorage holds a half-filled form for one tab and dies when the tab closes

*Example (italic):* You return after 10 days: the 30-day analytics cookie still knows you, your dark-mode flag is still in localStorage, but the 30-minute server session is long gone — so you log in again.

**Key point:** Each mechanism has a natural lifetime — minutes for server sessions, days for cookies, years for localStorage — and each job (auth, analytics, preferences) matches one of them.

### Visualization (canvas `c3`, 720×300)

Horizontal lifetime chart: how long each storage mechanism survives, log-feel achieved by hardcoded pixel widths, one row per mechanism with its typical job.

- **Title (bold 15px, `#1a5276`, top center):** "How Long Each Memory Lives (and What It's Used For)".
- **Axis:** vertical 2px `#999` baseline at x=250, bars extend right, max width 430; log-feel via hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 75, 120, 165, 210, 255), each with a left-aligned 12px `#444` label at x=20:**
  - "sessionStorage — tab scratchpad": aqua `#199e70` bar width 60, 11px label "until tab closes"
  - "server session — login": blue `#2a78d6` bar width 110, 11px label "30 idle min"
  - "session cookie — login proof": violet `#4a3aa7` bar width 170, 11px label "until browser closes"
  - "persistent cookie — analytics id": orange `#d95926` bar width 290, 11px label "30 days"
  - "localStorage — preferences": green `#008300` bar width 430, 11px label "until cleared (years)"
- **Bar style:** 18px tall, solid fills, 4px radius.
- **Annotation (bold 13px ink `#1a5276`, top right near y=55):** "the login gets one of the shortest memories; the longest remembers dark mode".
- **Caption (12px `#444`, bottom right):** "bar widths schematic (log-feel); 30-min and 30-day lifetimes illustrative defaults".

## localStorage Is Not a Session

**Tags:** `common mistake` (red), `who can see it` (orange)

- **The confusion** — "I saved the login token in localStorage, so the server knows I'm logged in"
- **The reality** — localStorage never rides a request; the server cannot see it unless your code sends it
- **Cookies auto-send** — a cookie goes along on every request without a single line of your code
- **The safety flip side** — an `HttpOnly` cookie is invisible to page scripts; localStorage is readable by any script on the page
- **The symptom** — the cart looks full on screen (drawn from localStorage) but checkout arrives at the server empty

*Example (italic):* The tea shop's page shows 3 tins from localStorage, but the checkout request carries no cart data — the server charges for zero tins.

**Common mistake:** Treating localStorage as something the server can read. It is browser-only memory; only cookies travel automatically, and server sessions only work because a cookie carries the id.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a request leaving a browser that keeps the cart in localStorage (server sees nothing) vs one using a cookie (cart id arrives), shown as boxes flowing to the server.

- **Title (bold 15px, `#1a5276`, top center):** "What Actually Travels: Cookie Rides Along, localStorage Stays Home".
- **Row 1 (y=105), label 12px `#444` at x=20:** "localStorage only"; green `#008300` rounded box at x=150 labeled "browser: cart in localStorage (3 tins)" (12px), 3px arrow right labeled "request: no cart data" (11px mute `#6b7280`) to a red `#e74c3c` box at x=440 labeled "server sees empty cart" with bold 12px red "✗ charges 0 tins".
- **Row 2 (y=215), label:** "cookie attached"; blue `#2a78d6` box at x=150 labeled "browser: Cookie: cart=CART-1", 3px arrow right labeled "request: cart=CART-1 rides along" to a green `#008300` box at x=440 labeled "server finds 3 tins" with bold 12px green "✓".
- **Box style:** 180–200px wide, 44px tall, 8px radius, fills `rgba(0,131,0,0.12)` / `rgba(231,76,60,0.12)` / `rgba(42,120,214,0.15)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "localStorage is the browser's memory, not the server's — nothing leaves unless you send it".
- **Caption (12px `#444`, bottom right):** "tin counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the cart (3 tins, 180-byte blob), 25-request visit, upstream bytes (4,500 / 1,000 / 0), 40-byte session id, 30-minute idle timeout, and 30-day cookie expiry are invented and labeled illustrative; the ~4 KB per-cookie and ~5 MB per-site localStorage limits are typical documented browser limits; c3 bar widths (60 / 110 / 170 / 290 / 430) and c2 bar widths (440 / 98 / 3) are the schematic pixel values listed above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
- Page footnote (italic 12px, muted `#6b7280`, after the last card-section): "Note: session ids, cookie values, tracking ids, token parts, and example passwords on this page are made-up placeholders — for illustration only, and to avoid false positives from secret scanners."
