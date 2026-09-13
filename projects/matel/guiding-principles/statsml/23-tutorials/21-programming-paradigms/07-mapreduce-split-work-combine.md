# MapReduce: Split, Work, Combine

**Page type:** detail page (tutorial card-sections: h2 per section, two-column table.layout with text left ~50% / canvas right ~50%; one section uses a 3-col 38/31/31 layout with two canvases)
**HTML title tag:** MapReduce: Split, Work, Combine

**Subtitle:** Counting word frequencies across 1,000 books: give each of 10 machines a pile, let each count its own pile, then merge the counts

## 1,000 Books, One Impossible Evening

Tags: `core idea` (blue pill), `running example` (green pill)

- **The job** — count how often every word appears across 1,000 books
- **One machine** — reading all 1,000 books alone takes ~5 hours (illustrative)
- **Split** — deal the books out: 10 machines get 100 books each
- **Work** — each machine counts words in its own 100 books, ~30 minutes, all at once
- **Combine** — merge the 10 count tables into one grand total

*Example (italic):* Ten friends each count their own bookshelf, then shout their tallies to one scorekeeper.

**Key point:** The trick is that counting is splittable — no machine needs to see another machine's books to do its share.

### Visualization (canvas `c1`, 720×300)

Flow diagram: split 1,000 books across 10 machines, combine counts.

- **Title (bold 16px, ink `#1a5276`, top center):** "Split the Books, Not the Job Description"
- **Left box** at (30,105), 100×70: fill `rgba(42,120,214,0.15)`, stroke blue `#2a78d6`; two centered blue bold lines "1,000" / "books" at x=80.
- **Middle column — machines:** 4 boxes drawn (representing 10) at x=244, width 118, height 40, y positions [52, 105, 158, 211]; fill `rgba(0,131,0,0.10)`, stroke green `#008300`. Labels bold green 12px: "machine 1", "machine 2", "machine 3", "machine 10"; second line dark text 11px "100 books". Gray arrows from books box (132,140) to each machine. Bold gray "..." ellipsis at (303,205) between machine 3 and machine 10.
- **Local count tables:** 4 boxes at x=438, width 108, height 40, same y positions; fill `rgba(74,58,167,0.10)`, stroke violet `#4a3aa7`; violet 11px text lines "local counts" / "word : n". Gray arrows from each machine to its table.
- **Combine box** at (612,105), 92×62: fill `rgba(217,89,38,0.12)`, stroke orange `#d95926`; bold orange 12px lines "grand" / "totals". Gray arrows from each local-counts box (548, y+20) converging to (610,135).
- **Bottom annotation (bold 13px magenta `#d55181`, centered at x=380, y=282):** "alone: ~5 h  |  10 machines at once: ~30 min (illustrative)"

## Two Machines, Three Sentences — By Hand

Tags: `worked example` (green pill)

Layout note: this section uses the 3-col layout — text column 38%, two viz columns 31% each (canvases `c2a` and `c2b`).

- **Machine A holds** — "the cat sat" and "the dog ran"
- **Machine B holds** — "the cat ran"
- **Map** — each machine counts locally: A gets the:2, B gets the:1
- **Shuffle** — all counts for the same word travel to the same place
- **Reduce** — sum each word's pile: the: 2+1 = 3
- **Check it** — nine words total across both machines; the five totals sum to 9

*Example (italic):* "the" appears twice on A and once on B — shuffle puts [2, 1] together, reduce says 3.

**Key point:** Map never sees other machines; reduce never sees raw text. The shuffle in the middle is the only hand-off.

### Visualization (canvas `c2a`, 420×340)

Diagram: MAP — two machines count locally.

- **Title (bold 15px ink, top center):** "Map: Count Your Own Pile"
- **Machine A box** at (25,44), 175×130: fill `rgba(0,131,0,0.08)`, stroke green `#008300`. Centered at x=112: bold green 13px "Machine A"; italic 12px dark text lines `"the cat sat"` and `"the dog ran"`; bold violet 12px lines "the:2  cat:1  sat:1" / "dog:1  ran:1". Small gray downward arrow from (112,108) to (112,120) between sentences and counts.
- **Machine B box** at (220,44), 175×130: fill `rgba(42,120,214,0.08)`, stroke blue `#2a78d6`. Centered at x=307: bold blue 13px "Machine B"; italic 12px `"the cat ran"`; bold violet 12px "the:1  cat:1  ran:1". Gray arrow (307,108)→(307,120).
- **Annotations (centered):** bold magenta 13px two lines at y=218/236: "neither machine has seen" / "the other's sentences"; gray 12px two lines at y=272/290: "map output: (word, count) pairs," / "one small table per machine".

### Visualization (canvas `c2b`, 400×340)

Diagram: SHUFFLE + REDUCE — group by word, then sum.

- **Title (bold 15px ink, top center):** "Shuffle Groups, Reduce Sums"
- **Column headers (bold 12px, y=54):** aqua `#199e70` "shuffle: pile per word" at x=165; orange `#d95926` "reduce" at x=330.
- **Five rows** (y = 68 + i×36), data:

| word | pile | total |
|------|------|-------|
| the | [2, 1] | 3 |
| cat | [1, 1] | 2 |
| ran | [1, 1] | 2 |
| sat | [1] | 1 |
| dog | [1] | 1 |

- Each row: word right-aligned bold dark 13px at x=90; pile box (105,y) 120×25 fill `rgba(25,158,112,0.10)` stroke aqua with pile text centered; gray arrow to total box (296,y) 68×25 fill `rgba(217,89,38,0.12)` stroke orange with bold orange total.
- **Bottom annotations (centered):** bold magenta 13px at y=278: "totals: 3+2+2+1+1 = 9 words"; gray 12px at y=296: "matches the 9 words we started with".

## You Already Use This Pattern

Tags: `where it's used` (blue pill)

- **GROUP BY** — `SELECT word, SUM(n) GROUP BY word` is map, shuffle, reduce
- **pandas** — `df.groupby('word').sum()` is the same pattern on one machine
- **Spark and Hadoop** — the same three steps, spread across hundreds of machines
- **Works when combinable** — sum, count, max: pieces merge cleanly into a total
- **Struggles when not** — an exact median needs all values in one place; no clean pieces

*Example (italic):* Every groupby you have ever written was a tiny MapReduce that never left your laptop.

**Key point:** Big-data engines are not magic — they are this split-work-combine pattern plus machinery for moving the pieces.

Below the canvas in the viz column, a `pre.code` block (verbatim):

```
# the same computation, three spellings
SELECT word, SUM(n) FROM counts GROUP BY word;   -- SQL

df.groupby("word")["n"].sum()                    # pandas

counts.reduceByKey(lambda a, b: a + b)           # Spark
```

### Visualization (canvas `c3`, 720×300)

Matrix diagram: groupby engines all follow split / work / combine.

- **Title (bold 16px ink, top center):** "One Pattern, Many Tools"
- **Column headers (bold 13px, y=58, centered at column x+45):** "split" green `#008300` at x=250, "work" violet `#4a3aa7` at x=400, "combine" orange `#d95926` at x=550.
- **Three tool rows** (y = 78 + i×52): right-aligned at x=200, bold dark 13px label plus gray 11px note below:
  - "SQL GROUP BY" — "one server"
  - "pandas groupby" — "your laptop"
  - "Spark / Hadoop" — "100s of machines"
- Each row×column cell: box 90×32 at column x, fill `rgba(42,120,214,0.06)`, stroke in the column color, centered bold 14px "✓" in column color; light-gray (`#e5e9ef`) arrows between adjacent cells.
- **Bottom annotations (centered):** bold magenta 14px at y=262: "every groupby is a tiny MapReduce — the engines differ only in scale"; gray 12px at y=282: "shuffle is the hidden middle step: same key, same place".

## The Average-of-Averages Trap

Tags: `common mistake` (red pill), `worked example` (green pill)

- **The confusion** — "reduce = average the machines' averages" — it is not
- **Machine A** — 10 orders, average $4.00 (sum $40)
- **Machine B** — 90 orders, average $8.00 (sum $720)
- **Average of averages** — (4 + 8) / 2 = $6.00 — wrong
- **True average** — (40 + 720) / (10 + 90) = 760 / 100 = $7.60
- **The fix** — machines send (sum, count) pairs; only the final step divides

*Example (italic):* B carries 9x the orders of A, but averaging averages weighs the machines equally.

**Common confusion (key-point callout):** Reduce needs combinable pieces. Averages don't combine as averages — carry (sum, count) instead; exact medians and percentiles don't combine at all.

### Visualization (canvas `c4`, 720×300)

Split diagram: average-of-averages vs true average.

- **Title (bold 16px ink, top center):** "Combining Averages Goes Wrong"
- **Vertical dashed divider** at x=330 (light gray `#e5e9ef`, dash 4/3) from y=40 to bottom.
- **Left half — the two machines:**
  - Machine A box at (40,60), 240×60: fill `rgba(0,131,0,0.08)`, stroke green; bold green 13px "Machine A", dark 12px "10 orders   sum $40   avg $4.00".
  - Machine B box at (40,135), 240×60: fill `rgba(42,120,214,0.08)`, stroke blue; bold blue 13px "Machine B", dark 12px "90 orders   sum $720   avg $8.00".
  - Gray 12px centered two lines at (160, 232/250): "B has 9x the orders of A —" / "any fair total must weight it 9x".
- **Right half — two horizontal candidate bars** (x0=375, max width 230px, scale max value $8, bar height 30):
  - Bar 1 at y=76: label above "avg of avgs: (4+8)/2", value 6.0, red fill `#e74c3c`, bold label right of bar "$6.00  WRONG".
  - Bar 2 at y=154: label above "true avg: 760/100", value 7.6, green fill `#008300`, bold label "$7.60  RIGHT".
- **Bottom-right annotations (centered at x=520):** bold magenta 13px at y=250: "ship (sum, count) pairs; divide once at the end"; gray 12px at y=270: "(40+720) / (10+90) = $7.60".

## Regeneration instructions

- **Template/layout:** tutorials detail page. `<h1>` + `.subtitle`, then four `.card-section` blocks each with `<h2>` (1.3rem `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse, td padding 12px, vertical-align top). Two-col rows use `td.text-col` 50% / `td.viz-col` 50%; the 3-col row uses `td.text-col3` 38% and two `td.viz-col3` 31%.
- **Text column structure:** `.tags` pill row first (pills 0.72rem bold, 2px 10px padding, 10px radius: blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`); then a `<ul>` (0.92rem) of one-line bullets each opening with `<b>` in `#1a5276`; one italic `.example` line (0.9rem `#555`); one `.key-point` callout (background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem).
- **Code blocks:** `pre.code` — background `#f8f9fa`, left border 3px solid `#1a5276`, monospace 0.78rem; inline `code` uses `#f4f6f8` background monospace.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Canvases have `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` and `arrow()` drawing helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- In regenerated HTML, any card/page links use `.html` extensions.
