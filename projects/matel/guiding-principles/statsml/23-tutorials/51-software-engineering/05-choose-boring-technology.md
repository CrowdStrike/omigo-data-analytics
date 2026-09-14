# Choose Boring Technology

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Choose Boring Technology

**Subtitle:** Dan McKinley's rule: a company gets about three innovation tokens — spend novelty where your product is different, and pick boring, well-understood tools everywhere else

## Three Tokens for a Food-Delivery Startup

**Tags:** `core idea` (blue), `innovation tokens` (green), `McKinley` (orange)

- **The essay** — Dan McKinley's "Choose Boring Technology" (2015) gives a company roughly 3 innovation tokens
- **The startup** — a food-delivery app must pick a language, database, queue, cache, search, and deploy tool
- **The differentiator** — real-time courier routing is the actual product; that is where novelty earns its keep
- **The spend** — the three tokens go to the routing engine, the dispatch ML model, and live map streaming
- **Everything else** — Postgres, a plain queue, cron, and HTTP: mature, deeply documented, easy to hire for

*Example (italic):* The team spends its three tokens on routing, dispatch ML, and live tracking — and ships the rest of the stack on tools their last three jobs also used.

**Key point:** Innovation tokens are a budget for novelty: you can only do a small number of new things well, so spend them where being new differentiates the product — and be boring everywhere else.

### Visualization (canvas `c1`, 720×300)

Token-allocation board: eight stack decisions drawn as boxes in two rows, three carrying a gold "token" coin, five stamped boring.

- **Title (bold 15px, `#1a5276`, top center):** "Three Tokens, Eight Stack Decisions".
- **Layout:** two rows of four rounded boxes (150×54, 8px radius), row centers y=105 and y=205, box left edges x = 30, 205, 380, 555.
- **Token boxes (row 1, first three):** "Routing engine", "Dispatch ML", "Live map streaming" — fill `rgba(201,133,0,0.15)`, 2px `#c98500` border, bold 12px `#2c3e50` labels; a gold coin (filled circle `#c98500`, radius 12) overlaps each box's top-right corner with bold 12px white "T" centered inside.
- **Boring boxes (row 1 fourth + all of row 2):** "Database: Postgres", "Queue: RabbitMQ", "Cache: Redis", "Deploys: plain scripts", "Jobs: cron" — fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border, 12px `#2c3e50` labels, small 11px `#6b7280` "boring" tag under the label inside each box.
- **Legend (12px `#444`, top left at x=30, y=55):** gold coin + "token spent"; blue square + "boring choice".
- **Annotation (bold 13px green `#008300`, centered at y=270):** "novelty only where the product is different".
- **Caption (12px `#444`, bottom right):** "stack illustrative".

## The One-More-Database Bill

**Tags:** `worked example` (blue), `total cost` (green)

- **The pitch** — a hot new datastore promises faster reads for the restaurant menu cache
- **The forever bill** — every added technology costs monitoring, backups, upgrades, and on-call runbooks for life
- **Known ops** — the boring stack costs the 6-person team about 10 ops hours per month, steady (illustrative)
- **The extra** — the new store adds ~8 known hours/month plus unknown-unknown incidents found only in production
- **The tally** — by month 12 the boring stack has cost 120 hours; with the extra store, 240 hours — double

*Example (italic):* Over 24 months the extra datastore costs about 320 additional ops hours — roughly 13 hours every month, paid whether or not the faster reads ever mattered.

**Key point:** The essay's "one more database" math: a technology's cost is not the install weekend — it is operations, expertise, and integration paid every month, forever, by everyone.

### Visualization (canvas `c2`, 720×300)

Cumulative ops-hours line chart over 24 months: boring stack (flat slope) vs boring stack plus one novel datastore (steeper, with incident steps).

- **Title (bold 15px, `#1a5276`, top center):** "Cumulative Ops Hours: One Extra Datastore, 24 Months".
- **Axes:** origin x=65, baseline y=245, plot width 600, plot height 180; x = months 0 to 24 with 12px `#444` tick labels every 6 months; y = cumulative hours 0 to 600, gridlines `#e5e9ef` at 150/300/450.
- **Boring line:** blue `#2a78d6` 3px line through months `[0, 3, 6, 9, 12, 15, 18, 21, 24]`, hours `[0, 30, 60, 90, 120, 150, 180, 210, 240]` — straight, 10 hrs/month.
- **With-new-store line:** red `#e74c3c` 3px line through the same months, hours `[0, 55, 112, 170, 240, 320, 395, 475, 560]` — steeper, with visible bends where surprise incidents landed.
- **Incident markers:** filled red dots (radius 4) at months 12 and 15; 12px red label "unknown-unknown incidents" pointing at them from y≈95.
- **Gap bracket:** thin `#6b7280` vertical bracket at month 24 between 240 and 560 with bold 13px magenta `#d55181` label "+320 hrs over 2 years".
- **Line labels:** bold 12px blue "boring stack" near month 20 under the blue line; bold 12px red "+1 novel datastore" near month 18 above the red line.
- **Caption (12px `#444`, bottom right):** "hours illustrative".

## Why 2am Favors the Boring Tool

**Tags:** `where it's used` (blue), `unknown unknowns` (red)

- **The 2am test** — when boring tech breaks at 2am, the error message has a decade of Stack Overflow answers
- **Known failures** — mature tools fail in cataloged ways; the fix is usually one search away
- **Unknown unknowns** — new tech's true cost is the failure modes nobody has hit yet; you discover them live
- **Hiring pool** — thousands of engineers already run the boring tool; the shiny one has a tiny pool
- **The graveyard** — trendy datastores adopted in hypergrowth get painfully migrated off years later

*Example (italic):* A widely repeated industry pattern: a hot datastore adopted during hypergrowth, then a multi-year migration back to a boring relational database once the surprises pile up.

**Key point:** Boring does not mean bad — boring means the failure modes are known and documented. The real price of new technology is the unknown-unknowns that only surface in production.

### Visualization (canvas `c3`, 720×300)

McKinley-style known/unknown circles: for each technology an outer circle is all its failure modes, the inner circle is the ones the world already understands.

- **Title (bold 15px, `#1a5276`, top center):** "What You Know About a Technology's Failures".
- **Left circle pair (center 200,170):** outer circle radius 90, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c` border; inner circle radius 78, fill `rgba(42,120,214,0.25)`, 2px `#2a78d6` border; bold 13px `#1a5276` label "20-year-old database" centered above at y=62.
- **Right circle pair (center 520,170):** outer circle radius 90, same red styling; inner circle radius 30, same blue styling; bold 13px `#1a5276` label "6-month-old datastore" centered above at y=62.
- **In-circle labels:** 12px `#2c3e50` "known failure modes" inside each blue disc (left one fits; right one uses a 12px callout line to text at x≈600, y≈150); 12px `#e74c3c` "unknown unknowns" in each red ring (left ring label sits just inside the outer edge at y≈250).
- **Annotation (bold 13px orange `#d95926`, centered at y=285):** "the red ring is where 2am gets long".
- **Caption (12px `#444`, bottom right):** "circle areas schematic, illustrative".

## Tokens Are Meant to Be Spent

**Tags:** `common mistake` (red), `deliberate adoption` (orange)

- **Not a ban** — tokens exist to be spent; a company that never adopts anything forfeits real advantages
- **The wrong reason** — "the team was bored" is how one-off tech sneaks in and becomes a permanent ops bill
- **The sanctioned path** — adopt deliberately, company-wide, when the boring option genuinely can't do the job
- **One question** — write down how the boring tool would solve it; adopt new tech only when that answer fails
- **Both ditches** — zero novelty stagnates; unlimited novelty drowns operations — the budget is the point

*Example (italic):* The routing team shows Postgres genuinely cannot stream 50,000 live courier positions — so the company adopts one streaming store, everywhere, on purpose.

**Common mistake:** Reading "choose boring" as "never choose new." The rule is a budget, not a ban — spend tokens deliberately where boring truly fails, and never spend them out of boredom.

### Visualization (canvas `c4`, 720×300)

Inverted-U curve: product advantage as a function of how many novel technologies the stack carries, peaking near three.

- **Title (bold 15px, `#1a5276`, top center):** "Product Advantage vs Novel Technologies in the Stack".
- **Axes:** origin x=65, baseline y=245, plot width 600, plot height 180; x = novel technologies 0 to 10, 12px `#444` integer tick labels every 2; y = product advantage 0 to 100 (schematic), no y tick labels, gridlines `#e5e9ef` at 25/50/75.
- **Curve:** violet `#4a3aa7` 3px line through novel-tech counts `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, advantage `[20, 55, 80, 90, 82, 65, 48, 33, 21, 12, 6]`.
- **Peak marker:** filled green `#008300` dot (radius 5) at (3, 90); bold 13px green label "≈3 tokens" just above it.
- **Left ditch annotation (bold 12px `#6b7280`, near x=0.4, y=200):** "stagnation — boring everything, no edge".
- **Right ditch annotation (bold 12px red `#e74c3c`, near x=7.5, y=175):** "ops drown in unknown-unknowns".
- **Annotation (bold 13px magenta `#d55181`, centered near y=60, x≈430):** "the budget is the point — spend it, then stop".
- **Caption (12px `#444`, bottom right):** "curve schematic, illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); ops hours, the example stack, circle areas, and the advantage curve are invented and labeled illustrative; "roughly three tokens" and the one-more-database framing come from Dan McKinley's essay/talk "Choose Boring Technology" (2015), credited in the page text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
