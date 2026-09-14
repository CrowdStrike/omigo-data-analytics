# Cross-Site Scripting (XSS)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cross-Site Scripting (XSS)

**Subtitle:** When a website pastes user text into a page without escaping it, that text becomes code — and it runs in every visitor's browser with the site's full privileges

## The Comment That Ran in Every Visitor's Browser

**Tags:** `core idea` (blue), `data becomes code` (red), `browser` (orange)

- **The site** — a recipe site lets anyone leave a comment; comments render straight into the page
- **The comment** — one "review" is `<script>send(document.cookie)</script>` instead of praise
- **The render** — the server pastes the comment into HTML unescaped; the browser sees a script tag
- **The run** — every visitor's browser executes it as the site's own code: read cookies, act as them
- **The twin** — same disease as SQL injection, but the fooled interpreter is the viewer's browser

*Example (italic):* The comment is posted at 9:00am; by midnight the recipe page's 4,000 viewers have each unknowingly run the attacker's script inside their own logged-in session.

**Key point:** XSS is data becoming code: user-supplied text rendered into HTML without escaping is parsed as markup, so any script inside it runs as if the site itself had written it.

### Visualization (canvas `c1`, 720×300)

Flow diagram of stored XSS: one poisoned comment enters the database, then fans out to three viewer browsers that each execute it.

- **Title (bold 15px, `#1a5276`, top center):** "One Poisoned Comment, Executed by Every Browser That Loads the Page".
- **Attacker box:** rounded rect at x=25, y=118, 165×52, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border; 12px `#2c3e50` text lines "attacker posts comment" / "`<script>send(cookie)</script>`" (second line 11px monospace).
- **Database box:** rounded rect at x=265, y=118, 160×52, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px text "comments table" / "stored verbatim"; 3px `#6b7280` arrow from attacker box to it, 12px `#6b7280` label "save" above the arrow.
- **Viewer boxes:** three rounded rects at x=530, y = 48 / 124 / 200, each 165×46, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, 12px text "viewer's browser" / "script RUNS as the site"; 3px `#6b7280` arrows fan from the database box to all three, 12px label "page render (unescaped)" along the middle arrow.
- **Annotation (bold 13px red `#e74c3c`, centered near x=360, y=278):** "4,000 page views that day = 4,000 executions".
- **Caption (12px `#444`, bottom right):** "viewer counts illustrative; payload is a textbook pattern".

## Stored, Reflected, DOM: Three Delivery Routes

**Tags:** `worked example` (blue), `three flavors` (green)

- **Stored** — the comment sits in the database and fires for every one of the 4,000 daily viewers
- **Reflected** — the payload rides a URL parameter (`?q=<script>...`) and the site echoes it back
- **The lure** — the attacker mails the crafted link to 150 users; only clickers get hit
- **DOM-based** — client-side JS reads the URL fragment and injects it; the server never sees it
- **Hand-check** — 4,000 ÷ 150 ≈ 27: one stored post outreaches a whole reflected mail campaign 27-fold

*Example (italic):* The stored comment reaches 4,000 viewers on its own; the reflected link reaches only the 150 people it was mailed to — and the DOM version never even appears in the server logs.

**Key point:** All three flavors run identically in the browser; they differ only in the delivery route — saved in the database, bounced off a URL parameter, or assembled by client-side rendering.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart comparing the reach of one payload per flavor, with a callout that the DOM route bypasses the server entirely.

- **Title (bold 15px, `#1a5276`, top center):** "Reach of One Payload: Stored Hits Everyone, Reflected Only the Clickers".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=200, max bar width 470, bars 26px tall; vertical 2px `#999` baseline at x=200 from y=60 to y=255.
- **Rows (bar top y = 75 / 145 / 215):**
  - "stored — 1 comment": red `#e74c3c` bar width 470 (= 4,000 viewers), bold 12px red value label "4,000 viewers" at the bar end
  - "reflected — 1 mailed link": orange `#d95926` bar width 18 (= 150 clickers, proportional), 12px `#d95926` label "150 clickers"
  - "DOM-based — 1 fragment link": violet `#4a3aa7` bar width 18 (= 150), 12px `#4a3aa7` label "150 — server logs show nothing"
- **Annotation (bold 13px `#1a5276`, near x=340, y=260):** "the database does the attacker's distribution for free".
- **Caption (12px `#444`, bottom right):** "reach numbers illustrative; bar widths proportional to viewers".

## Escape on Output, Then Layer the Backstops

**Tags:** `where it's used` (blue), `defense in depth` (green), `CSP` (orange)

- **Escape on output** — turn `<` into `&lt;` and `&` into `&amp;` at render time; script becomes text
- **Five characters** — `<` `>` `&` `"` `'` map to `&lt;` `&gt;` `&amp;` `&quot;` `&#39;`
- **Frameworks help** — React escapes by default; the danger lives in escape hatches like `innerHTML`
- **CSP** — a Content-Security-Policy header is a browser-enforced allowlist of what scripts may run
- **HttpOnly** — cookies flagged HttpOnly are invisible to script, so a stray XSS can't lift the token

*Example (italic):* The escaped comment renders on the page as the literal text `<script>send(...)` — visible, ugly, and completely inert.

**Key point:** Output encoding is the fix; CSP and HttpOnly are backstops that shrink the blast radius when an escape hatch lets one payload through.

### Visualization (canvas `c3`, 720×300)

Funnel diagram of 100 illustrative payloads passing through three defense layers, with the count surviving each layer.

- **Title (bold 15px, `#1a5276`, top center):** "100 Payloads vs Three Layers (illustrative)".
- **Layer bands:** three horizontal rounded rects centered at x=360, at y = 62 / 137 / 212, each 30px tall; widths 480 / 120 / 48 (schematic, shrinking); fills `rgba(0,131,0,0.15)` / `rgba(42,120,214,0.15)` / `rgba(230,126,34,0.15)` with 2px borders `#008300` / `#2a78d6` / `#e67e22`.
- **Band labels (12px `#2c3e50`, centered in each band):** "Layer 1 — escape output by context" / "Layer 2 — CSP script allowlist" / "Layer 3 — HttpOnly cookies".
- **Flow counts (bold 12px, right of each band at its y):** green `#008300` "100 in, 96 made inert — 4 pass" after layer 1; blue `#2a78d6` "4 in, 3 blocked by the browser — 1 passes" after layer 2; orange `#d95926` "1 runs, but reads no session token" after layer 3.
- **Arrows:** 3px `#6b7280` vertical arrows between bands at x=360.
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "each layer catches what the one above missed".
- **Caption (12px `#444`, bottom right):** "counts and widths schematic — layers are real, numbers invented".

## Why "We Sanitize Input" Isn't Enough

**Tags:** `common mistake` (red), `context matters` (orange)

- **The claim** — "we sanitize at the door": strip `<script>` tags when the comment is saved
- **The gap** — safe for the HTML body is not safe for an attribute, a URL, or a script block
- **No brackets needed** — `" onmouseover="steal()` escapes an attribute without a single `<` or `>`
- **URL context** — `javascript:steal()` in an href fires on click; a tag filter never sees it
- **The rule** — escape at output, per context; an input filter can't know where the text will land

*Example (italic):* A profile name with no angle brackets at all sails past the input filter, then detonates the moment a template drops it inside a double-quoted attribute.

**Common mistake:** Sanitizing once at input assumes one context. The same string is inert in an HTML body, executable inside an attribute, and a live link in a URL — only the output site knows which escaping applies.

### Visualization (canvas `c4`, 720×300)

Three-row diagram: the same filter-passing strings landing in three output contexts, with a verdict per row.

- **Title (bold 15px, `#1a5276`, top center):** "Same Filter, Three Landing Spots: 'Safe' Depends on Context".
- **Layout:** three rows at y = 80 / 150 / 220; left 12px `#444` context label at x=20, monospace 11px payload box starting at x=170 (rounded rect 300×36, fill `rgba(42,120,214,0.10)`, 1.5px `#2a78d6` border), verdict at x=505.
- **Row 1 — "HTML body":** payload box "`&lt;script&gt; (escaped)`"; bold 13px green `#008300` verdict "✓ renders as plain text".
- **Row 2 — "attribute value":** payload box "`\" onmouseover=\"steal()`"; bold 13px red `#e74c3c` verdict "✗ runs — no brackets used".
- **Row 3 — "href URL":** payload box "`javascript:steal()`"; bold 13px red `#e74c3c` verdict "✗ fires on click".
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "the input filter passed all three — only output escaping knows the context".
- **Caption (12px `#444`, bottom right):** "payloads are textbook illustration patterns".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts are the hardcoded values above (no randomness); the 4,000 viewers, 150 clickers, and 100-payload funnel (96 / 3 / 1) are invented and labeled illustrative; the five escape mappings (`&lt;` `&gt;` `&amp;` `&quot;` `&#39;`) are exact. Any HTML-escaped payload text inside canvas labels is drawn as literal characters via `fillText` (no HTML parsing risk in canvas).
- **Framing:** defensive/educational only — payloads shown are canonical textbook illustrations (`document.cookie`, `onmouseover`, `javascript:`), kept schematic and non-operational.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
