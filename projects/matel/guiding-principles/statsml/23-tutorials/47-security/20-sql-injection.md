# SQL Injection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** SQL Injection

**Subtitle:** When a program builds a query by gluing strings together, user input can cross the line from data to code — one quote mark rewrites the question the database is asked

## One Quote Mark Away From Logged In

**Tags:** `core idea` (blue), `data vs code` (red), `defensive` (green)

- **The form** — a login page takes a username and looks it up: `WHERE username = '<input>'`
- **The glue** — the vulnerable version (what NOT to do) pastes the raw input into the query string
- **The trick** — the textbook input `' OR '1'='1` starts with a quote that closes the string early
- **The crossing** — everything after that quote is no longer a name; the database reads it as SQL
- **The result** — the WHERE clause becomes `username='' OR '1'='1'`, which is true for every row

*Example (italic):* A user types `' OR '1'='1` into the username box; the concatenated query matches every account, and the site logs them in as the first user it finds.

**Key point:** SQL injection is not a database bug — it is a boundary failure: string concatenation lets untrusted DATA re-enter the query as CODE.

### Visualization (canvas `c1`, 720×300)

Flow diagram: a trusted code template and an untrusted input box merge via string concatenation into one assembled query, with the injected fragment rendered as code.

- **Title (bold 15px, `#1a5276`, top center):** "String Concatenation: User Input Lands Inside the Code".
- **Row 1 (boxes at y=70, 44px tall, 8px radius):** blue `#2a78d6` box at x=55, width 300, fill `rgba(42,120,214,0.15)`, 12px label "CODE (trusted): SELECT ... WHERE username = '␣'"; red `#e74c3c` box at x=420, width 245, fill `rgba(231,76,60,0.12)`, 12px label "DATA (untrusted): ' OR '1'='1".
- **Merge arrows:** two 3px `#6b7280` arrows from the bottom centers of both boxes converging to (360, 175).
- **Row 2 (y=185):** single ink-bordered `#1a5276` box at x=90, width 540, 46px tall, fill `#f8f9fa`, 13px monospace text "WHERE username = '' OR '1'='1'" — the substring "OR '1'='1'" drawn in bold red `#e74c3c`, the rest in `#2c3e50`.
- **Callout label:** bold 12px red `#e74c3c` "quote closes the string here" near (150, 158), with a short 2px red arrow pointing at the `''` in the row-2 box.
- **Annotation (bold 13px red `#e74c3c`, centered near y=270):** "everything after the quote is executed as SQL — data became code".
- **Caption (12px `#444`, bottom right):** "classic textbook example, shown as what NOT to do".

## Hand-Tracing the Flaw and the Fix

**Tags:** `worked example` (blue), `parameterized queries` (green)

- **The table** — a `users` table with 50 rows; a real login should match exactly 0 or 1 of them
- **Concatenated** — paste in `' OR '1'='1` and the WHERE clause is true on all 50 rows: check any row by hand
- **The fix** — a parameterized query sends `WHERE username = ?` and the input separately
- **Compiled first** — the database parses and plans the query BEFORE the input arrives
- **Bound as data** — the input is attached to `?` as one literal string; it can never grow new SQL
- **The rerun** — the same attack string now matches 0 of 50 rows: no user is literally named `' OR '1'='1`

*Example (italic):* Against the concatenated query the attack string matches 50 of 50 rows; against `WHERE username = ?` with the input bound as a parameter, it matches 0 of 50.

**Key point:** Parameterization fixes the root cause: query structure is fixed at compile time and input is bound afterward as pure data — there is no moment where the input could be re-parsed as code.

### Visualization (canvas `c2`, 720×300)

Two-row pipeline comparison: the concatenated path (attack matches all rows) vs the parameterized path (attack matches none), same input, same 50-row table.

- **Title (bold 15px, `#1a5276`, top center):** "Same Input, Two Pipelines: 50 of 50 Rows vs 0 of 50".
- **Row 1 (y=95), left label 12px `#444` at x=15:** "concatenate (NOT this)"; red-tinted box at x=160, width 190, fill `rgba(231,76,60,0.12)`, 12px "glue string, then parse" → 3px arrow → box at x=390, width 150, 12px "WHERE always TRUE" → arrow → bold 13px red `#e74c3c` result at x=575: "50 / 50 rows".
- **Row 2 (y=205), left label:** "parameterize"; green-tinted box at x=160, width 190, fill `rgba(0,131,0,0.12)`, 12px "parse `? `, THEN bind input" → 3px arrow → box at x=390, width 150, 12px "input is one literal" → arrow → bold 13px green `#008300` result at x=575: "0 / 50 rows".
- **Box style:** 42px tall, 8px radius, 12px `#2c3e50` text, borders 2px in the row color (`#e74c3c` / `#008300`).
- **Divider:** 1px `#e5e9ef` horizontal line at y=150 across x=15..705.
- **Annotation (bold 12px violet `#4a3aa7`, centered near y=268):** "order is the fix: structure first, data second — not escaping, not sanitizing".
- **Caption (12px `#444`, bottom right):** "50-row users table illustrative".

## Two Decades at the Top of the Charts

**Tags:** `where it's used` (blue), `defense in depth` (orange)

- **The record** — injection has sat in the OWASP Top 10 in every edition since 2003, often at #1
- **Why it persists** — the vulnerable version is the EASY version: one line of string glue works fine in tests
- **The asymmetry** — the flaw only shows under adversarial input, which normal QA never types
- **ORMs help** — query builders and ORMs parameterize by default, so idiomatic code is safe code
- **Least privilege** — the app's DB account should not own DROP or admin rights; a breach then reads less
- **Validation too** — input checks (length, charset) are a good SECOND layer, never the primary fix

*Example (italic):* Injection ranked #6 in the 2004 OWASP Top 10, #2 in 2007, #1 in 2010, 2013, and 2017, and still #3 in 2021 — two decades on the board.

**Key point:** SQL injection endures because writing the vulnerable version is easier than writing the safe one; the cure is to make parameterization the default path, with least privilege and validation as backup layers.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart of the Injection category's rank in successive OWASP Top 10 editions — inverted so rank #1 is the tallest bar.

- **Title (bold 15px, `#1a5276`, top center):** "OWASP Top 10 Rank of Injection, 2004–2021 (#1 = tallest)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 175; y encodes inverted rank: bar height = (11 − rank) / 10 × 175; y-axis 12px `#444` guide labels "#1" at the top gridline and "#10" near the baseline, gridlines `#e5e9ef` at heights for ranks 1, 3, 6.
- **Bars (width 62, centered at x = 130, 225, 320, 415, 510, 605), editions and ranks hardcoded:** `["2004","2007","2010","2013","2017","2021"]`, ranks `[6, 2, 1, 1, 1, 3]`.
- **Bar colors:** rank 1 bars red `#e74c3c` fill `rgba(231,76,60,0.55)`; rank 2–3 bars orange `#d95926` fill `rgba(217,89,38,0.45)`; rank 6 bar blue fill `rgba(42,120,214,0.35)`.
- **Labels:** 12px `#444` edition year under each bar at y=262; bold 13px rank label "#6 #2 #1 #1 #1 #3" atop each bar in the bar's color.
- **Annotation (bold 13px ink `#1a5276`, near x=340, y=55):** "three editions in a row at #1".
- **Caption (12px `#444`, bottom right):** "ranks from published OWASP Top 10 editions; 2021 merges injection categories".

## Escaping Is Not the Fix

**Tags:** `common mistake` (red), `generalizes` (orange)

- **The mistake** — "just escape the quotes" treats symptoms; encodings, edge cases, and dialects leak through
- **The difference** — escaping edits the data and re-parses it with the code; parameterization never re-parses
- **The escape hatch** — ORMs offer raw-SQL methods; every raw call with glued strings reopens the hole
- **Same disease** — shell commands built by concatenation and `eval` on user strings fail the same way
- **Same cure** — pass data through argument slots (exec arrays, parameters), never through the code text

*Example (italic):* A team "fixes" injection by escaping single quotes, then a numeric field with no quotes at all (`id = 1 OR 1=1`) walks straight past the filter.

**Common mistake:** Reaching for escaping or sanitizing as the primary defense. The only structural fix is keeping data out of the code channel — parameterized queries for SQL, argument arrays for shells, and never `eval` on user input.

### Visualization (canvas `c4`, 720×300)

Three-column "same disease, same cure" panel: SQL, shell, and eval each shown as a vulnerable pattern (red) over its safe channel (green).

- **Title (bold 15px, `#1a5276`, top center):** "One Disease, Three Hosts: Data Interpreted as Code".
- **Columns (headers bold 13px `#1a5276` at y=60, centered at x = 140, 360, 580):** "SQL", "shell command", "eval".
- **Red row (boxes at y=85, width 190, 52px tall, 8px radius, fill `rgba(231,76,60,0.12)`, border 2px `#e74c3c`, 12px monospace):** "\"...WHERE u='\" + input", "\"rm -rf \" + path", "eval(userExpr)" — each with a small bold 11px red tag "code + data glued" beneath at y=145.
- **Down arrows:** 3px `#6b7280` arrow from each red box to its green box, 12px `#444` label "same cure" beside the middle arrow.
- **Green row (boxes at y=185, same size, fill `rgba(0,131,0,0.12)`, border 2px `#008300`, 12px monospace):** "WHERE u = ?  (bind input)", "exec([\"rm\",\"-rf\",path])", "parse, don't eval" — each with bold 11px green tag "data stays in a slot" beneath at y=245.
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "wherever data can be re-read as code, give it a slot instead of a seam".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded values above (no randomness); the 50-row users table and its 50/50 vs 0/50 match counts are invented and labeled illustrative; OWASP edition ranks (2004 #6, 2007 #2, 2010 #1, 2013 #1, 2017 #1, 2021 #3) reflect the published Top 10 editions.
- **Framing:** strictly defensive/educational — the only attack string shown is the canonical textbook `' OR '1'='1`, always presented as what NOT to do, paired immediately with the parameterized fix.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
