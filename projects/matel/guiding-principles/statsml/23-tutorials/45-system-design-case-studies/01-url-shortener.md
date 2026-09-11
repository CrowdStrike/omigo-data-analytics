# URL Shortener

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** URL Shortener

**Subtitle:** The hello world of system design — a service that turns long links into 7-character codes, and one small exercise that makes every layer of the standard toolkit visible

## Seven Characters Are Enough

**Tags:** `core idea` (blue), `base-62` (green), `capacity math` (orange)

- **The service** — paste a long link, get back a tiny one like `sho.rt/0b3Fk9Q`; visiting it redirects
- **Three asks** — shorten a URL, redirect a code back to it, and let users claim aliases like `/summer-sale`
- **The alphabet** — 0-9, a-z, A-Z gives 62 symbols per character; a code is just a number written in base 62
- **The space** — 7 characters give 62^7 = 3,521,614,606,208 codes, about 3.5 trillion (exact)
- **The comfort** — even minting 1,000 new links per second, 3.5 trillion codes last over 100 years

*Example (italic):* A 200-character campaign URL full of tracking parameters becomes `sho.rt/0b3Fk9Q` — 7 characters drawn from a pool of 3.5 trillion.

**Key point:** A URL shortener is a lookup table with two operations — write (long URL in, code out) and read (code in, long URL out) — and base-62 math is what keeps the codes short.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart of code-space size by code length, showing the ×62 jump per extra character.

- **Title (bold 15px, `#1a5276`, top center):** "Each Extra Character Multiplies the Code Space by 62".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440; log-feel achieved by hardcoded pixel widths, not a real log axis.
- **Rows (top to bottom at y = 85, 150, 215), each with a left-aligned 12px `#444` label at x=20:**
  - "5 chars — 916 million": blue `#2a78d6` bar width 195
  - "6 chars — 56.8 billion": blue bar width 314
  - "7 chars — 3.52 trillion": green `#008300` bar width 432, bold 12px green label "3,521,614,606,208 codes" at bar end
- **Bar style:** 22px tall, fills `rgba(42,120,214,0.30)` for blue rows and `rgba(0,131,0,0.30)` for the green row, 2px solid edges in the row color, 11px `#444` count labels at bar ends (green row's label bold green as above).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=265):** "7 characters: enough for 100+ years at 1,000 links/second".
- **Caption (12px `#444`, bottom right):** "counts exact: powers of 62; bar widths schematic (log-feel)".

## Minting a Code: Three Recipes

**Tags:** `worked example` (blue), `id generation` (green)

- **Counter + encode** — keep a global counter; link #3000 encodes in base 62 as `Mo`, padded to `00000Mo`
- **Hand-check** — 3000 = 48×62 + 24; digit 48 is `M`, digit 24 is `o` (digits run 0-9, a-z, A-Z)
- **Random + check** — draw 7 random characters, look the code up, retry on the (rare) collision
- **Hash + truncate** — hash the URL, base-62 encode, keep 7 chars; the same URL maps to the same code
- **The trade** — counters are guessable in sequence, random needs a lookup, truncation invites collisions

*Example (italic):* With the counter scheme, the 3,000th link ever created gets code `00000Mo` and the 3,001st gets `00000Mp` — unique by construction and cheap to compute.

**Key point:** All three recipes map a link to one of the 3.5 trillion codes — they differ in who guarantees uniqueness: the counter (always), the lookup (retry on hit), or luck (truncation alone).

### Visualization (canvas `c2`, 720×300)

Three-row flow diagram: each generation strategy as a left-to-right pipeline of rounded boxes ending in a verdict.

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways to Mint `00000Mo`".
- **Row 1 (y=85), label 12px `#444` at x=20:** "counter + encode"; blue `#2a78d6` rounded box at x=160 labeled "counter = 3000", 3px arrow to a blue box at x=340 labeled "base-62 encode", arrow to a green `#008300` box at x=530 labeled "00000Mo" with bold 12px green "✓ unique by construction".
- **Row 2 (y=165), label:** "random + check"; blue box at x=160 labeled "7 random chars", arrow to a blue box at x=340 labeled "lookup: taken?", arrow to a green box at x=530 labeled "code" with 12px green "✓ retry on rare hit".
- **Row 3 (y=245), label:** "hash + truncate"; blue box at x=160 labeled "hash(long URL)", arrow to a blue box at x=340 labeled "keep first 7", arrow to an orange `#d95926` box at x=530 labeled "code?" with bold 12px orange "⚠ collisions unchecked".
- **Box style:** 130–150px wide, 36px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text; arrows 3px `#6b7280`.
- **Caption (12px `#444`, bottom right):** "3000 = 48×62 + 24 → digits M, o (exact)".

## A Hundred Reads for Every Write

**Tags:** `where it's used` (blue), `read-heavy` (green), `scaling path` (orange)

- **The skew** — a link is created once but clicked many times; redirects outnumber creates ~100:1
- **Cache first** — a code never changes once minted, so cached redirects can never go stale
- **The disk** — at ~500 bytes per row, 1 billion links is ~500 GB; storage is the easy part
- **The path** — single DB → read replicas → cache in front → shard by code when one box can't hold it
- **Every layer** — this one toy design exercises ID generation, caching, replication, and sharding at once

*Example (italic):* An illustrative day: 1 million new links but 100 million redirects — a cache answering 90% of redirects leaves the database only 11 million queries.

**Key point:** Because redirects dwarf creates and never change, the winning shape is a big cache in front of a boring database — the scaling path just adds layers as traffic grows.

### Visualization (canvas `c3`, 720×300)

Two-part canvas: top half is a horizontal bar chart of where one illustrative day's 101 million requests land; bottom strip is the four-stage scaling path.

- **Title (bold 15px, `#1a5276`, top center):** "Redirects Outnumber Creates ~100:1 — Let the Cache Absorb Them".
- **Bars (top half):** vertical 2px `#999` baseline at x=250, bars extend right, max width 420, linear scale (420px = 90M); rows at y = 70, 110, 150 with left-aligned 12px `#444` labels at x=20:
  - "redirects served by cache — 90M": green `#008300` bar width 420
  - "redirects hitting the DB — 10M": blue `#2a78d6` bar width 47
  - "creates (writes to DB) — 1M": orange `#d95926` bar width 5
- **Bar style:** 20px tall, fills `rgba(0,131,0,0.30)` / `rgba(42,120,214,0.30)` / solid orange, 11px `#444` count labels at bar ends.
- **Annotation (bold 13px green `#008300`, right side near y=110):** "DB sees 11M of 101M requests".
- **Scaling strip (bottom, y=245):** 12px `#444` label "the scaling path" at x=20; four rounded boxes left to right at x = 160, 300, 440, 580, each 110×36, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text: "single DB", "+ replicas", "+ cache", "+ shards", joined by 3px `#6b7280` arrows.
- **Caption (12px `#444`, bottom right):** "traffic ratios illustrative".

## Truncated Hashes Collide Early

**Tags:** `common mistake` (red), `birthday problem` (orange)

- **The hope** — "hashes are unique, so the first 7 characters of a hash must be unique too"
- **The math** — truncated codes collide like shared birthdays: the odds grow with the square of codes issued
- **The cliff** — with 3.5 trillion codes, 50% collision odds arrive after only ~2.2 million links
- **The scale** — 2.2 million is 0.00006% of the space; collisions start long before the table looks full
- **The fix** — treat truncation like random: check the table and re-salt on a hit, or just use the counter

*Example (italic):* At 1 million issued codes the collision odds are already ~13%; at 5 million they are ~97% — a "unique" hash scheme with no collision check starts corrupting links within weeks.

**Common mistake:** Confusing a big code space with collision-free generation — uniqueness comes from checking or counting, never from truncating a hash into a 3.5-trillion-slot space.

### Visualization (canvas `c4`, 720×300)

Line chart of birthday-problem collision probability vs number of codes issued, for a 62^7 code space.

- **Title (bold 15px, `#1a5276`, top center):** "Collision Odds for Hash+Truncate in a 3.5-Trillion Space".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = codes issued 0 to 5 million, 12px `#444` tick labels "0", "1M", "2M", "3M", "4M", "5M"; y = collision probability 0 to 100%, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Curve:** red `#e74c3c` 3px line through issued millions `[0, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5]`, probability % `[0, 3.5, 13.2, 27.4, 43.3, 58.8, 72.1, 89.7, 97.1]` (p = 1 − exp(−k²/2N), N = 62^7).
- **50% marker:** horizontal dashed `#6b7280` (dash 4/3) line at 50%; vertical dashed line at 2.2M meeting the curve, bold 13px red `#e74c3c` label "50% at ~2.2M codes" beside the intersection.
- **Annotation (bold 12px violet `#4a3aa7`, near x=3.5M, y=200):** "the space is 0.00006% full here".
- **Caption (12px `#444`, bottom right):** "birthday math exact for 62^7 space".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); base-62 counts (62^5 = 916,132,832; 62^6 = 56,800,235,584; 62^7 = 3,521,614,606,208), the encode 3000 → `Mo`, and the birthday probabilities/2.2M crossover are exact and labeled exact; the 100:1 read/write ratio, daily traffic split, 500 bytes/row, and cache hit rate are invented and labeled illustrative. Keep the design generic — no claims about how any real shortener company operates internally.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
