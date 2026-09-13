# Sieve of Eratosthenes

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sieve of Eratosthenes

**Subtitle:** To find every prime up to n, don't test numbers one at a time — keep each survivor and cross out its multiples; whatever never gets crossed out is prime

## Crossing Out Lockers, One Skip-Count at a Time

**Tags:** `core idea` (blue), `running example` (green), `skip counting` (orange)

- **The hallway** — a school hallway has 30 numbered lockers, and the class wants every prime up to 30
- **The slow way** — testing lockers one by one means asking "does anything divide 17?" thirty times over
- **The sieve idea** — keep locker 2, then stamp every 2nd locker after it: 4, 6, 8, ..., 30 can't be prime
- **One pass, many answers** — that single walk stamps 14 lockers without doing a single division
- **Repeat with survivors** — the next unstamped locker (3) leads the next walk; stamped lockers never lead
- **The name** — this cross-out-the-multiples routine is the Sieve of Eratosthenes, about 2,200 years old

*Example (italic):* Locker 15 is never tested on its own — the walk led by 3 stamps it in passing, three lockers after 12.

**Key point:** Instead of asking every number "are you prime?", the sieve lets each prime shout "my multiples aren't" — one skip-count clears many lockers at once.

### Visualization (canvas `c1`, 720×300)

Locker grid, numbers 1–30 in a 10×3 grid, showing only the first pass: locker 2 kept, every even locker after it stamped, everything else untouched.

- **Title (bold 15px, `#1a5276`, top center):** "One Walk Down the Hall: Stamp Every 2nd Locker After 2".
- **Grid geometry:** 10 columns × 3 rows of rounded rects (4px radius), cell 54 wide × 48 tall, 6px gaps; top-left cell at x=75, y=60; numbers 1–30 left-to-right, top-to-bottom, centered bold 13px in each cell.
- **Locker 1:** fill `rgba(107,114,128,0.15)`, number in mute `#6b7280` — sits out (neither prime nor composite).
- **Locker 2 (kept):** fill `rgba(0,131,0,0.15)`, 3px green `#008300` border, bold green number.
- **Stamped lockers** `[4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]`: fill `rgba(217,89,38,0.18)`, one 2px orange `#d95926` diagonal slash corner-to-corner, number in `#6b7280`.
- **Untouched lockers** `[3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29]`: white fill, 1px `#e5e9ef` border, number in `#2c3e50`.
- **Annotation (bold 13px orange `#d95926`, centered below the grid at y=250):** "one pass, 14 lockers stamped — no division done".
- **Caption (12px `#444`, bottom right):** "lockers 1–30; pass of 2 only".

## Sieving 1 to 30 by Hand

**Tags:** `worked example` (blue), `step by step` (green)

- **Pass of 2** — keep 2, stamp 4, 6, 8, ..., 30: that is 14 stamps
- **Pass of 3** — keep 3, stamp 9, 12, ..., 30: 8 stamps, but only 9, 15, 21, 27 land on fresh lockers
- **Pass of 5** — keep 5, stamp 25 and 30: 2 stamps, and only 25 is new
- **Stop** — the next survivor is 7, and 7 × 7 = 49 is past locker 30, so the walking is over
- **Read off survivors** — 2, 3, 5, 7, 11, 13, 17, 19, 23, 29: the ten primes up to 30
- **Bookkeeping** — 24 stamps total; 19 stamped lockers + 10 primes + locker 1 accounts for all 30

*Example (italic):* Locker 27 survives the pass of 2, but the pass of 3 catches it — 9, 12, 15, 18, 21, 24, 27, stamped.

**Key point:** Three short walks (led by 2, 3, 5) and 24 stamps expose every prime up to 30 — no number was ever tested individually.

### Visualization (canvas `c2`, 720×300)

The finished sieve: the same 10×3 locker grid, every locker colored by the FIRST walk that stamped it, primes highlighted as the untouched survivors.

- **Title (bold 15px, `#1a5276`, top center):** "The Finished Sieve: Who Stamped Whom".
- **Grid geometry:** identical to `c1` — 10×3 rounded cells 54×48, 6px gaps, top-left at x=75, y=60, bold 13px centered numbers.
- **Locker 1:** fill `rgba(107,114,128,0.15)`, mute `#6b7280` number.
- **Stamped by 2** `[4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30]`: fill `rgba(217,89,38,0.18)`, 2px orange `#d95926` slash, `#6b7280` number.
- **First stamped by 3** `[9, 15, 21, 27]`: fill `rgba(213,81,129,0.18)`, 2px magenta `#d55181` slash, `#6b7280` number.
- **First stamped by 5** `[25]`: fill `rgba(25,158,112,0.18)`, 2px aqua `#199e70` slash, `#6b7280` number.
- **Primes (never stamped)** `[2, 3, 5, 7, 11, 13, 17, 19, 23, 29]`: fill `rgba(0,131,0,0.15)`, 2px green `#008300` border, bold green numbers.
- **Legend (12px `#444`, single row at y=248, starting x=75):** four 12×12 swatches with labels — orange "stamped by 2", magenta "first stamped by 3", aqua "first stamped by 5", green "prime — never stamped".
- **Annotation (bold 13px green `#008300`, bottom right near y=280):** "10 survivors = the primes up to 30".

## Why One Sweep Beats Thirty Interrogations

**Tags:** `where it's used` (blue), `speed` (orange)

- **Head-to-head** — trial division on 1–30 needs 40 division checks; the sieve needed 24 stamps
- **Stamps are cheap** — a stamp is "jump ahead and mark"; a division check is real arithmetic every time
- **It scales** — the gap widens fast: for big ranges the sieve does many times less work per number
- **Where it shows up** — prime tables for hashing, factorization helpers, and countless coding puzzles
- **The pattern** — precompute answers for a whole range at once instead of solving each query from scratch

*Example (italic):* To label every number up to 30 as prime or not, trial division makes 40 checks; the sieve makes 24 stamps — 14 for the 2-walk, 8 for the 3-walk, 2 for the 5-walk.

**Key point:** The sieve's trick — let each found prime do bulk work for the whole range — is the same precompute-once idea behind lookup tables everywhere.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison: total work to classify every number up to 30 — one plain bar for trial division's 40 checks, one stacked bar splitting the sieve's 24 stamps by pass.

- **Title (bold 15px, `#1a5276`, top center):** "Work to Classify Every Number Up to 30".
- **Axes:** origin x=90, baseline y=250, plot width 560, plot height 190; y = operations 0 to 45, light `#e5e9ef` gridlines every 10 with 12px `#444` labels "0", "10", "20", "30", "40"; no x axis ticks, category labels under the bars.
- **Bar 1 (trial division):** 120px wide, centered at x=260, value 40 (height 169px at 190px per 45 units), fill blue `#2a78d6` at `rgba(42,120,214,0.35)` with 2px blue border; bold 13px blue value label "40 checks" above; 12px `#444` label below baseline: "testing one at a time".
- **Bar 2 (sieve, stacked):** 120px wide, centered at x=520, segments bottom-up: pass of 2 = 14 (orange `rgba(217,89,38,0.35)`, 2px `#d95926` border), pass of 3 = 8 (magenta `rgba(213,81,129,0.35)`, 2px `#d55181` border), pass of 5 = 2 (aqua `rgba(25,158,112,0.35)`, 2px `#199e70` border); bold 13px `#1a5276` total label "24 stamps" above; 12px segment labels — "pass of 2: 14" and "pass of 3: 8" centered inside their segments in `#2c3e50`, "pass of 5: 2" outside to the right at 11px with a short leader line; 12px `#444` label below baseline: "sieve of Eratosthenes".
- **Annotation (bold 13px green `#008300`, upper right near x=420, y=70):** two lines: "same 10 primes, 40% less work —" / "and the gap explodes as n grows".
- **Caption (12px `#444`, bottom right):** "check counts for n = 30; trial division by primes up to √k — illustrative of the general gap".

## Start at p², Stop at √n

**Tags:** `common mistake` (red), `rule of thumb` (orange)

- **The wasted walking** — beginners start the 5-walk at 10 and keep walking for 7, 11, 13, ... for nothing
- **Start at p²** — 10, 15, 20 were already stamped by 2 or 3; locker 25 is the 5-walk's first fresh stamp
- **Stop at √n** — any composite up to 30 has a factor of at most √30 ≈ 5.5, so leaders 2, 3, 5 suffice
- **Proof by pair** — if c = a × b with both a and b above √n, then a × b beats n: impossible
- **The 7-walk** — 7's first fresh stamp would be 49, past the end of the hallway: zero work left

*Example (italic):* Walking the hallway for 7 visits 14, 21, 28 — all already stamped; its first fresh victim, 49, isn't even in the building.

**Common mistake:** Starting each pass at 2p and looping p all the way up to n. Start each pass at p² and stop the sieve once p² > n — the same answer with a fraction of the walking.

### Visualization (canvas `c4`, 720×300)

Two walk-rows above a shared number line 1–30: the 5-walk showing 10/15/20 already stamped and 25 as the first fresh stamp, and the 7-walk finding nothing fresh before running off the end.

- **Title (bold 15px, `#1a5276`, top center):** "Why the 5-Walk Starts at 25 and the 7-Walk Never Starts".
- **Number line:** horizontal 2px `#999` line at y=245 from x=120 to x=690 (numbers 1 to 30 mapped linearly); 12px `#444` tick labels at 1, 5, 10, 15, 20, 25, 30.
- **Row 1 (5-walk, y=130), 12px `#444` row label "walk of 5" at x=20:** green `#008300` 8px filled dot above position 5 with 12px green label "leader"; grey `#6b7280` 7px hollow dots at 10, 15, 20, 30 with 11px grey labels "done by 2", "done by 3", "done by 2", "done by 2" (staggered above/below the dots to avoid overlap); orange `#d95926` 8px filled dot at 25 with bold 12px orange label "first NEW stamp = 5 × 5".
- **Row 2 (7-walk, y=190), row label "walk of 7" at x=20:** green 8px dot at 7 labeled "leader" (12px green); grey 7px hollow dots at 14, 21, 28, each with 11px grey label "already stamped"; dashed violet `#4a3aa7` (dash 6/4) 2px arrow from position 28 to x=705 with arrowhead, bold 12px violet label above it: "49 = 7 × 7 is past 30 — stop".
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at position 5.5 from y=90 down to the number line, 11px `#6b7280` label at its top: "√30 ≈ 5.5".
- **Annotation (bold 13px violet `#4a3aa7`, upper right near x=430, y=70):** "leaders past √30 have no new work".
- **Caption (12px `#444`, bottom right):** "positions to scale, n = 30".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every locker list, bar value, and dot position is the hardcoded literal above (no randomness); stamp counts (14 + 8 + 2 = 24) and the 40 trial-division checks are exact for n = 30, and the numbers in the text match the numbers in the charts.
- **Grid cells (`c1`, `c2`):** draw with a rounded-rect helper (fill then stroke); a "stamp" is one 2px diagonal line from the cell's top-left to bottom-right inset by 6px.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
