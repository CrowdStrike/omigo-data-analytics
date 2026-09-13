# String Searching

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** String Searching

**Subtitle:** Finding a word inside a long text without re-reading letters — Rabin-Karp turns each window into a number it can update cheaply, and KMP remembers what it already matched so it never steps backward

## Finding a Word by Sliding a Stencil

**Tags:** `core idea` (blue), `sliding window` (green), `naive search` (orange)

- **The task** — find every "ana" inside "banana", like Ctrl+F hunting a word in a long document
- **The stencil** — slide the 3-letter pattern along the text one position at a time, four stops in all
- **Letter checks** — at each stop compare left to right and quit at the first letter that differs
- **The count** — the four stops cost 1 + 3 + 1 + 3 = 8 letter-looks and find matches at positions 2 and 4
- **The waste** — the two matches overlap, so the naive slide reads letters 3 and 4 twice each

*Example (italic):* Ctrl+F for "ana" in "banana" stops 4 times, peeks at 8 letters in total, and reports hits starting at letters 2 and 4.

**Key point:** Slide-and-compare always works, but it re-reads letters after every shift — on long texts that re-reading is the whole cost, and both Rabin-Karp and KMP exist to remove it.

### Visualization (canvas `c1`, 720×300)

Alignment diagram: the text "banana" as a row of letter boxes, with four pattern rows below it — one per shift — coloring each compared letter green (match) or red (mismatch) and dashing letters never looked at.

- **Title (bold 15px, `#1a5276`, top center):** "Naive Search: Slide 'ana' Along 'banana' — 8 Letter-Looks".
- **Text row:** six 52px-wide, 36px-tall boxes starting at x=200, top y=52; 1px `#999` border, `#f8f9fa` fill; letters "b a n a n a" bold 16px `#2c3e50` centered; 11px `#6b7280` position numbers "1"–"6" above each box.
- **Shift rows (pattern boxes 52px wide, 30px tall, aligned under text columns; box tops at y = 110, 155, 200, 245), each with a 12px `#444` label at x=20:**
  - "shift 0 — 1 look, stop": box under column 1 red (`#e74c3c` border, `rgba(231,76,60,0.15)` fill, 12px red "×" in corner); columns 2–3 dashed `#6b7280` boxes with mute letters (never looked at)
  - "shift 1 — 3 looks, MATCH": three green boxes (`#008300` border, `rgba(0,131,0,0.18)` fill) under columns 2–4
  - "shift 2 — 1 look, stop": red box under column 3; columns 4–5 dashed mute
  - "shift 3 — 3 looks, MATCH": three green boxes under columns 4–6
- **Annotation (bold 12px orange `#d95926`, right side near x=545, y=170):** two lines: "8 looks total —" / "letters 3 and 4 read twice".
- **Caption (11px `#444`, bottom right):** "exact counts for this example".

## Rabin-Karp: Compare Fingerprints, Not Letters

**Tags:** `worked example` (blue), `rolling hash` (green), `fingerprints` (orange)

- **Letter values** — score each letter by its alphabet position: a=1, b=2, n=14
- **Fingerprint** — add up a window's scores: the pattern "ana" scores 1 + 14 + 1 = 16
- **Four windows** — "ban"=17, "ana"=16, "nan"=29, "ana"=16; only the 16s can possibly match
- **Rolling trick** — next fingerprint = old − leaving letter + entering letter: 17 − 2 + 1 = 16
- **Confirm** — a fingerprint hit still gets one letter-by-letter check, because "naa" also scores 16

*Example (italic):* Moving the window from "ban" to "ana" costs two tiny sums instead of re-reading three letters — that per-slide shortcut is the whole speed-up.

**Key point:** Rabin-Karp replaces most letter comparisons with one cheap number update per slide; real versions use positional hashes so two different words almost never share a fingerprint.

### Visualization (canvas `c2`, 720×300)

Bar chart of the four window fingerprints against a dashed target line at the pattern's fingerprint, with the rolling update spelled out as an annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Rabin-Karp: One Fingerprint per Window — Only the 16s Get Checked".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 185; y = fingerprint 0 to 30 with light `#e5e9ef` gridlines at 10, 20, 30 and 12px `#444` labels; four bars 80px wide centered at x = 150, 290, 430, 570.
- **Bars (heights = value/30 × 185):** "ban" 17 and "nan" 29 in blue (`rgba(42,120,214,0.35)` fill, `#2a78d6` 2px border); the two "ana" 16s in green (`rgba(0,131,0,0.30)` fill, `#008300` 2px border). Bold 13px value labels above each bar: "17", "16", "29", "16" (green for the 16s, blue otherwise).
- **Bar labels:** bold 13px `#2c3e50` window text below baseline ("ban", "ana", "nan", "ana"); 11px `#6b7280` letter positions beneath ("1–3", "2–4", "3–5", "4–6").
- **Pattern line:** horizontal dashed orange `#d95926` (dash 6/4) line at value 16 across the plot; bold 12px orange label at its right end: "pattern 'ana' = 16".
- **Annotation (bold 12px violet `#4a3aa7`, near x=180, y=70):** two lines: "roll: 17 − b(2) + a(1) = 16" / "two sums, no re-reading".
- **Caption (12px `#444`, bottom right):** "letter scores a=1, b=2, n=14 — illustrative toy hash".

## KMP: Never Re-Read a Letter

**Tags:** `worked example` (blue), `prefix table` (green), `no backtracking` (orange)

- **One pointer** — KMP walks "banana" left to right and never moves the text pointer backward
- **The memory** — after finding "ana", its last "a" can already begin the next match; KMP keeps it
- **Prefix table** — for "ana" the table is [0, 0, 1]: after a full match, carry 1 letter forward
- **The scan** — "b" misses, "a n a" matches (hit), keep the "a", then "n a" completes the second hit
- **The count** — 6 letters, 6 looks; naive needed 8 because it re-read letters 3 and 4

*Example (italic):* After the first "ana" is found, KMP does not restart from scratch — it treats the trailing "a" as already matched and only checks "n", then "a".

**Key point:** KMP's text pointer never moves backward — the prefix table records how much of a partial match survives a slide, so total work stays about one pass over the text.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: for each letter of "banana", how many times naive search reads it versus how many times KMP reads it.

- **Title (bold 15px, `#1a5276`, top center):** "Reads per Letter: Naive Re-Reads, KMP Never Does".
- **Axes:** origin x=70, baseline y=245, plot width 580, plot height 175; y = times read 0 to 3, light `#e5e9ef` gridlines at 1, 2, 3 with 12px `#444` labels; six letter groups centered at x = 130, 220, 310, 400, 490, 580.
- **Group labels:** bold 14px `#2c3e50` letters "b a n a n a" below baseline; 11px `#6b7280` positions "1"–"6" beneath.
- **Bars (30px wide, 6px gap within a group):** naive on the left, blue (`rgba(42,120,214,0.35)` fill, `#2a78d6` border), values `[1, 1, 2, 2, 1, 1]`; KMP on the right, green (`rgba(0,131,0,0.30)` fill, `#008300` border), values `[1, 1, 1, 1, 1, 1]`. 12px value labels above every bar.
- **Legend (12px, top left near x=80, y=60):** blue swatch "naive (8 total)", green swatch "KMP (6 total)".
- **Annotation (bold 12px green `#008300`, near x=310, y=85):** "KMP reads every letter exactly once — the pointer never steps back".
- **Caption (11px `#444`, bottom right):** "exact counts for 'ana' in 'banana'".

## Why Fast Search Matters

**Tags:** `where it's used` (blue), `common mistake` (red)

- **Everywhere** — Ctrl+F, log grepping, plagiarism checkers, and DNA motif scans are all this problem
- **The scale** — naive worst case on 1,000 letters with a 10-letter pattern: about 10,000 looks
- **Linear instead** — KMP (and Rabin-Karp on average) need about 1,010 — roughly one look per letter
- **Pick Rabin-Karp** — fingerprints shine when hunting many patterns at once: hash all, scan once
- **Pick KMP** — no collision worries and a guaranteed worst case; a dependable single-pattern default

*Example (italic):* A log monitor scanning 1,000-letter lines for a 10-letter error code does about 10,000 letter-looks naively but about 1,010 with KMP — same answer, a tenth of the work.

**Common mistake:** Treating a fingerprint match as a real match. Different windows can share a fingerprint ("naa" scores 16 just like "ana"), so Rabin-Karp must confirm every hit letter by letter — skip that and you report ghosts.

### Visualization (canvas `c4`, 720×300)

Two-line growth chart: letter-looks versus text length for naive worst-case search and for KMP / Rabin-Karp, with the gap at 1,000 letters called out.

- **Title (bold 15px, `#1a5276`, top center):** "Letter-Looks vs Text Length (10-letter pattern)".
- **Axes:** origin x=80, baseline y=245, plot width 570, plot height 180; x = text length 0 to 1,000 with 12px `#444` tick labels every 200; y = letter-looks 0 to 10,000 with light `#e5e9ef` gridlines at 2,500 / 5,000 / 7,500 / 10,000 labeled "2.5k", "5k", "7.5k", "10k" (12px `#444`).
- **Naive worst-case line:** red `#e74c3c` 3px line through lengths `[0, 200, 400, 600, 800, 1000]`, looks `[0, 2000, 4000, 6000, 8000, 10000]`; 12px red label "naive worst case" above the line near length 620.
- **KMP / Rabin-Karp line:** green `#008300` 3px line through the same lengths, looks `[0, 210, 410, 610, 810, 1010]`; bold 12px green label "KMP / Rabin-Karp ≈ one look per letter" below the line near length 560.
- **Endpoint markers:** 6px dots at (1000, 10000) red and (1000, 1010) green, with bold 13px value labels "10,000" (red) and "1,010" (green) to their left.
- **Annotation (bold 13px `#1a5276`, near x=250, y=95):** "same answer, ~10× fewer looks".
- **Caption (12px `#444`, bottom right):** "illustrative — worst-case counts for a 10-letter pattern".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, read counts, and line points are the hardcoded arrays above (no randomness); fingerprints use the toy sum hash a=1, b=2, n=14 exactly as stated; c1–c3 numbers are exact for "ana" in "banana", c4 is labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
