# Copy-Paste Queries

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Copy-Paste Queries

**Subtitle:** Long, exact strings pasted into the search box — each one looks unique, but a few templates hide underneath, and matching the literal string is the whole game

## An Error Message Becomes the Query

**Tags:** `core idea` (blue), `pasted not typed` (orange), `exact match` (green)

- **The moment** — an app crashes; the developer copies the error text straight into the search box
- **Nobody typed it** — a program composed the string; every colon and quote arrives intact
- **Long and spiky** — 13 tokens and 9 punctuation marks, against 4 plain words in a typed query
- **Once-only** — seen once in 30 days; the typed version repeats 1,200 times (illustrative)
- **Beyond code** — a tracking number, an appliance error code, a pasted line of song lyrics
- **What wins** — the page containing the exact string, not the page about the general topic

*Example (italic):* A developer pastes `TypeError: cannot read properties of undefined (reading 'map') at renderList (app.js:214)` and wants the one page where someone hit that same line.

**Key point:** A copy-paste query is program output reused as a query — long, exact, once-only, and won by matching the literal string, not the topic.

### Visualization (canvas `c1`, 720×300)

Three mini-panels comparing one typed query against one pasted query on length, punctuation, and reuse — two bars per panel, each panel with its own scale.

- **Title (bold 15px, ink `#1a5276`, top center):** "Typed vs Pasted: Same Search Box, Different Animal (illustrative)".
- **Legend (12px, centered under the title at y≈44):** blue swatch "typed: fix map error react", orange swatch "pasted: the TypeError line".
- **Three panels** at x-origins 40, 275, 510, each 180 wide, baseline y=245, plot height 150, dashed `#e5e9ef` vertical dividers between panels; panel titles bold 12px `#2c3e50` centered at y=70: "tokens", "punctuation marks", "repeats in 30 days".
- **Bars per panel (width 55, gap 30):** typed bar fill `rgba(42,120,214,0.55)` border blue `#2a78d6`; pasted bar fill `rgba(217,89,38,0.55)` border orange `#d95926`. Panel scales and values: tokens max 15, values 4 vs 13; punctuation max 10, values 0 vs 9 (zero drawn as baseline label only); repeats max 1300, values 1,200 vs 1 (the 1 drawn as a 2px sliver).
- **Value labels:** bold 12px in each bar's border color above the bar: "4"/"13", "0"/"9", "1,200"/"1"; below the baseline 11px `#6b7280` labels "typed" and "pasted" under each bar.
- **Annotation (bold 12px orange `#d95926`, centered at y=290):** "3x longer, all punctuation, and it never repeats".

## Five Pastes, One Template

**Tags:** `worked example` (blue), `templating` (green)

- **Same bug, five pastes** — five developers hit one bug; each pastes their own error verbatim
- **Identical head** — every string starts with the same TypeError phrase, word for word
- **Different tails** — only function, file, and line differ: app.js:214, checkout.js:88, cart.js:129
- **The recipe** — replace file paths, line numbers, and hex ids with placeholders; keep the fixed words
- **The collapse** — five once-only strings become one template with a count of 5
- **One step more** — abstract 'map' to 'X' and cousin errors reading 'name' or 'id' join the cluster

*Example (italic):* Strip `renderList (app.js:214)` from one string and `showCart (cart.js:129)` from another — the two become character-for-character identical.

**Key point:** Templating means stripping the variable parts and keeping the fixed words — a highlighter and five printouts are enough to redo this by hand.

### Visualization (canvas `c2`, 720×300)

Funnel diagram: five raw pasted strings listed on the left, variable tails highlighted, arrows converging into a single green template box on the right.

- **Title (bold 15px, ink `#1a5276`, top center):** "Five Raw Pastes Collapse to One Template".
- **Five rows (11px monospace, left-aligned at x=15, y = 75, 112, 149, 186, 223):** shared head `TypeError: … (reading 'map') at ` in mute `#6b7280`, then the variable tail in bold orange `#d95926`: `renderList (app.js:214)`, `renderList (checkout.js:88)`, `drawTable (main.js:41)`, `showCart (cart.js:129)`, `loadFeed (bundle.min.js:1)`.
- **Arrows:** 1.5px `#999` lines from x=445 at each row's y to the template box's left edge midpoint (500, 149).
- **Template box:** x=505, y=112, 200×74, fill `rgba(0,131,0,0.08)`, 2px green `#008300` border; inside, three centered 11px monospace ink lines: "TypeError: cannot read" / "properties of undefined" / "(reading 'map')"; bold 12px green "count: 5" above the box at y=100.
- **Annotations:** bold 12px green `#008300` centered under the box at y=210: "5 once-only strings, 1 template"; 11px mute `#6b7280` at x=15, y=252: "orange = variable part (function, file, line)".
- **Caption (bold 12px ink `#1a5276`, centered at y=285):** "strip paths, line numbers, ids — keep the fixed words".

## One-Off Strings, Fat Clusters

**Tags:** `where it's used` (blue), `long tail` (orange), `normalization trap` (red)

- **Tail dwellers** — each raw string sits in the extreme tail of the query log: frequency one, forever
- **Fat clusters** — templated, 20 once-only strings become 4 templates; the top one covers 9
- **Phrase beats bag** — treating the paste as one exact phrase outranks any bag-of-words match
- **Synonyms hurt** — expanding 'undefined' to near-synonyms buries the page with the literal string
- **Normalization bites** — lowercasing and punctuation-stripping erase quotes, dots, and 0x-style codes
- **Cache futility** — result caching keyed on the raw string never fires; every paste is a miss

*Example (italic):* A ranker that strips punctuation turns (reading 'map') into reading map — and starts matching pages about geography.

**Key point:** The tokens standard text pipelines throw away — punctuation, case, code symbols — are exactly the tokens these queries are made of.

### Visualization (canvas `c3`, 720×300)

Two-panel bar chart: the same 20 queries counted as raw strings (20 bars of height 1) and counted as templates (4 fat bars), split by a dashed divider.

- **Title (bold 15px, ink `#1a5276`, top center):** "Same 20 Queries, Two Ledgers (illustrative)".
- **Divider:** dashed `#bdc3c7` vertical line at x=360 from y=40 to y=280.
- **Left panel:** y-axis at x=50 with ticks 0, 5, 10 (12px `#444`) and light `#e5e9ef` gridlines, baseline y=235, plot height 155 (max 10); 20 bars (width 11, gap 3, from x=58), all height 1, fill `rgba(42,120,214,0.55)`; panel title bold 12px `#2c3e50` at y=56: "counted as raw strings"; annotation bold 12px red `#e74c3c` at y≈150: "every bar = 1 — looks hopeless"; x label 11px `#6b7280` at y=255: "20 distinct pasted strings".
- **Right panel:** y-axis at x=410, same scale and gridlines; 4 bars (width 50, gap 18, from x=420), values `[9, 6, 3, 2]`, fill `rgba(0,131,0,0.55)` border green `#008300`, bold 12px green value labels above each bar; category labels 11px `#444` below: "'map'", "null ref", "timeout", "syntax"; panel title bold 12px `#2c3e50` at y=56: "counted as templates"; annotation bold 12px green at y≈80: "top template covers 9 of 20".
- **Caption (11px `#6b7280`, bottom right at y=290):** "illustrative — same 20 queries, two ways of counting".

## Rare Query, Common Need

**Tags:** `common mistake` (red), `rule of thumb` (blue)

- **The trap** — "this query appeared once, so nobody cares" — the string is rare, the need is not
- **At scale** — 1,000,000 once-only pasted errors collapse to about 5,000 templates (illustrative)
- **Deeper still** — those templates trace back to roughly 300 underlying causes (illustrative)
- **Dashboard blindness** — head-query reports show none of this; the traffic hides in the long tail
- **Fix once, win daily** — repairing results for one template fixes every future paste of that bug

*Example (italic):* Every day a fresh batch of never-seen strings arrives, and every day most of them are the same few hundred bugs wearing new file names.

**Common mistake:** Rare query is not rare need — count templates and causes, not raw strings, before declaring the tail unimportant.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart on a log scale: distinct raw strings, templates after normalization, and underlying causes, shrinking by orders of magnitude.

- **Title (bold 15px, ink `#1a5276`, top center):** "A Million Pastes, a Few Hundred Causes (illustrative)".
- **Axis:** horizontal log₁₀ scale from x=180 to x=680 (log 0 to 6); 1px `#999` baseline at y=240; tick labels 12px `#444` at log 0, 2, 4, 6: "1", "100", "10k", "1M"; light `#e5e9ef` vertical gridlines at each tick from y=60 to y=240.
- **Rows (bar height 34, tops at y = 80, 138, 196), row labels bold 12px ink `#1a5276` right-aligned at x=170:** "distinct raw strings" bar to log 6.00 fill `rgba(42,120,214,0.55)` border `#2a78d6`; "after templating" bar to log 3.70 fill `rgba(74,58,167,0.45)` border violet `#4a3aa7`; "underlying causes" bar to log 2.48 fill `rgba(0,131,0,0.55)` border green `#008300`.
- **Value labels:** bold 13px in each bar's border color just right of the bar end: "1,000,000", "5,000", "~300".
- **Annotation (bold 13px red `#e74c3c`, near x=520, y=215):** "rare string ≠ rare need".
- **Caption (11px `#6b7280`, centered at y=268):** "queries in one month, log scale — illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border (no index number), `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label — "Key point:" for sections 1–3, "Common mistake:" for section 4).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline `code` in 0.85em monospace; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. No font below 11px.
- **Data:** every number is a hardcoded literal (no `Math.random`); all counts are invented and labeled "illustrative" in chart titles or captions; c1 values (13 tokens, 9 punctuation, 1 vs 1,200 repeats), c3 values (20 strings → templates 9/6/3/2), and c4 values (1,000,000 / 5,000 / 300 at log widths 6.00 / 3.70 / 2.48) must match the text bullets exactly.
- The pasted error string is ordinary program output — keep it generic; nothing on the page may resemble keys, tokens, or passwords.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
