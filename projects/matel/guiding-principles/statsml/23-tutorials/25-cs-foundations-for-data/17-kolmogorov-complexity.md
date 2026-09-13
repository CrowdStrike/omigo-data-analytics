# Kolmogorov Complexity

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kolmogorov Complexity

**Subtitle:** The complexity of a piece of data is the length of the shortest recipe that reproduces it exactly — patterned data has a short recipe, truly random data has none shorter than itself

## Two Strings, One Phone Call

**Tags:** `core idea` (blue), `shortest recipe` (green), `patterns` (orange)

- **The call** — Mia must dictate 48-character strings to a friend over the phone, exactly, no photos
- **String A** — "ababababab..." for 48 characters; she just says "repeat ab 24 times" — 18 characters
- **String B** — "123123123..." for 48 characters; "repeat 123 16 times" does it in 19 characters
- **String C** — "q7g2vkx9..." random gibberish; the only way is spelling out all 48 characters
- **The measure** — Kolmogorov complexity is the length of the shortest recipe that recreates the data
- **No pattern, no shortcut** — string C's shortest recipe is the string itself; that is what random means

*Example (italic):* All three strings are 48 characters long, yet A takes an 18-character sentence to transmit and C takes all 48 — same length, very different complexity.

**Key point:** The complexity of data is not its size — it is the size of the shortest instruction that rebuilds it exactly.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: three string groups on the x axis, each with a grey bar for the string's length (all 48) and a colored bar for its shortest phone recipe (18, 19, 48).

- **Title (bold 15px, `#1a5276`, top center):** "Same 48-Character Strings, Very Different Recipes".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = characters 0 to 50 with 12px `#444` tick labels "0", "10", "20", "30", "40", "50" and light `#e5e9ef` gridlines at each; x = three groups centered at x=170, 370, 570 with 12px `#444` labels below the baseline: "A: ababab...", "B: 123123...", "C: q7g2vk...".
- **Bars:** per group two bars 50px wide, 10px apart; left bar = string length, all `[48, 48, 48]`, fill `rgba(107,114,128,0.30)` with 12px `#6b7280` value label "48" on top; right bar = shortest recipe length `[18, 19, 48]`, fills blue `#2a78d6`, green `#008300`, orange `#d95926`, bold 13px matching-color value labels "18", "19", "48" on top.
- **Recipe captions (11px `#6b7280`, under each group label):** "\"repeat ab 24 times\"", "\"repeat 123 16 times\"", "spell out all 48".
- **Annotation (bold 13px orange `#d95926`, near x=430, y=75):** two lines: "the random string has no shortcut —" / "its shortest recipe is itself".
- **Caption (12px `#444`, bottom right):** "recipes counted in plain-English characters, illustrative".

## Measuring Five Recipes by Hand

**Tags:** `worked example` (blue), `count it yourself` (green)

- **The game** — take five 48-character strings, write the shortest English recipe, count its letters
- **All a's** — "aaaa...a" (48 a's): recipe "repeat a 48 times" = 17 characters
- **Two-beat** — "abab...ab": recipe "repeat ab 24 times" = 18 characters
- **Four-beat** — "aabbaabb...": recipe "repeat aabb 12 times" = 20 characters
- **Counting** — "123456789101112..." cut at 48: recipe "count up from 1, first 48 digits" = 32 characters
- **Gibberish** — "q7g2vkx9..." random: no recipe beats spelling it out, so 48 characters

*Example (italic):* Rank the five recipes — 17, 18, 20, 32, 48 — and the strings sort themselves from most patterned to pure noise.

**Key point:** You just estimated Kolmogorov complexity by hand: the shorter the winning recipe, the more pattern the string contains.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart: five strings as rows, bar length = shortest recipe length in characters, on a shared 0–50 axis, with a dashed line at 48 marking "spell it out".

- **Title (bold 15px, `#1a5276`, top center):** "Shortest Recipe Length for Five 48-Character Strings".
- **Axis:** horizontal 2px `#999` line at y=255 from x=250 to x=690 (width 440), scale = characters 0 to 50; tick labels "0", "10", "20", "30", "40", "50" (12px `#444`) below.
- **Rows (top to bottom at y = 70, 110, 150, 190, 230), each with a left-aligned 12px `#444` label at x=20:**
  - "all a's — \"repeat a 48 times\"": bar to 17, fill blue `#2a78d6`
  - "abab... — \"repeat ab 24 times\"": bar to 18, fill blue `#2a78d6`
  - "aabb... — \"repeat aabb 12 times\"": bar to 20, fill aqua `#199e70`
  - "1234567891011... — \"count up from 1...\"": bar to 32, fill yellow `#c98500`
  - "q7g2vkx9... — spell it out": bar to 48, fill orange `#d95926`
- **Bar style:** 16px-tall rounded bars; bold 12px matching-color value labels "17", "18", "20", "32", "48" just right of each bar end.
- **Guide line:** vertical dashed `#6b7280` (dash 4/3) line at 48 from y=55 to the axis, 11px `#6b7280` label "48 = no compression at all" at its top.
- **Annotation (bold 13px green `#008300`, near x=420, y=95):** "more pattern → shorter recipe".

## Zip Files, Randomness, and Occam's Razor

**Tags:** `where it's used` (blue), `compression` (green), `model selection` (orange)

- **Uncomputable** — no algorithm can find the true shortest program, so in practice we approximate it
- **Zip as a ruler** — a compressor's output size is a practical stand-in for Kolmogorov complexity
- **Three files** — 10,000 characters each: all a's zips to 120 bytes, English prose to 4,100, noise to 10,050
- **Randomness test** — data that a good compressor cannot shrink is, for practical purposes, random
- **Occam's razor** — MDL model selection picks the model that gives the data the shortest total recipe
- **Anomaly flags** — a log file that suddenly compresses much worse has new structure-free content

*Example (italic):* An analyst zips two 10,000-character sensor logs: one drops to 800 bytes, the other barely moves — the second is carrying far more irreducible information (or noise).

**Key point:** True Kolmogorov complexity is uncomputable, but compressed size is a workable everyday estimate — and "incompressible" is the practical definition of random.

### Visualization (canvas `c3`, 720×300)

Vertical bar chart: three 10,000-character files with their zipped sizes in bytes, against a dashed line at the original 10,000, showing pattern-rich data collapsing and noise refusing to shrink.

- **Title (bold 15px, `#1a5276`, top center):** "Zip Size as a Complexity Estimate (three 10,000-char files)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 190; y = bytes 0 to 11,000 with 12px `#444` tick labels "0", "2,500", "5,000", "7,500", "10,000" and light `#e5e9ef` gridlines at each; x = three bars 90px wide centered at x=190, 375, 560 with 12px `#444` labels below: "all a's", "English prose", "random noise".
- **Bars:** heights `[120, 4100, 10050]` bytes; fills blue `#2a78d6`, green `#008300`, orange `#d95926`; bold 13px matching-color value labels "120", "4,100", "10,050" on top of each bar.
- **Original-size line:** horizontal dashed `#6b7280` (dash 4/3) line at 10,000 bytes across the plot; 12px `#6b7280` label "original size 10,000" at its left end.
- **Annotation (bold 13px orange `#d95926`, near x=430, y=90):** two lines: "random data does not compress —" / "the zip is the data".
- **Caption (12px `#444`, bottom right):** "zip sizes illustrative".

## Looks Random Is Not the Same as Is Random

**Tags:** `common mistake` (red), `hidden structure` (orange)

- **Two digit strings** — 48 digits of pi "314159265358979..." next to 48 digits from a lottery machine
- **Same look** — both pass every eyeball test: no repeats, no runs, digits roughly evenly spread
- **Different recipes** — pi's string has the 21-character recipe "first 48 digits of pi"; the lottery string has none
- **The gap** — 21 versus 48: one string hides a tiny generator, the other genuinely needs all its digits
- **One-way proof** — finding a short recipe proves low complexity; failing to find one proves nothing

*Example (italic):* A reviewer calls a 48-digit column "random noise", but it is pi's digits — a 21-character recipe reproduces the whole thing, so its complexity is low.

**Common mistake:** Declaring data random because you cannot see the pattern. Kolmogorov complexity is about the shortest recipe that exists, not the shortest one you found.

### Visualization (canvas `c4`, 720×300)

Two-row arrow chart on a shared character axis: each row shows a digit string's length dot (48) with an arrow pulling back to its shortest known recipe length — a long pull for pi, no pull at all for the lottery digits.

- **Title (bold 15px, `#1a5276`, top center):** "Two Strings That Look the Same — Recipes of 21 vs 48".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (width 450), scale = characters 0 to 50; tick labels "0", "10", "20", "30", "40", "50" (12px `#444`) below.
- **String previews (11px monospace `#6b7280`, above each row):** row 1 "3141592653589793..." at y=95, row 2 "8305174296150387..." at y=175.
- **Row 1 (y=115), label 12px `#444` at x=20:** "digits of pi — \"first 48 digits of pi\""; grey `#6b7280` 7px dot at 48, 3px green `#008300` arrow to a green 7px dot at 21 with arrowhead; bold 12px labels: grey "48" above the start, green "recipe: 21" above the end.
- **Row 2 (y=195), label:** "lottery digits — no recipe found"; grey 7px dot at 48, no arrow, orange `#d95926` 7px ring drawn around the same dot; bold 12px orange label "recipe: 48 (itself)" above it.
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "same look to the eye — a 27-character gap in complexity".
- **Caption (12px `#444`, bottom right):** "lottery digits illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, recipe lengths, and dot positions are the hardcoded arrays above (no randomness); recipe lengths 17/18/19/20/21/32/48 are literal character counts of the quoted English recipes; zip byte sizes in c3 are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
