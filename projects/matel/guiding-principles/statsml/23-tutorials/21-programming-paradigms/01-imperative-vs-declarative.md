# Imperative vs Declarative

**Page type:** detail page (tutorial card-sections: h2 with blue underline per section, two-column layout table — text left 50%, canvas right 50%; section 1's viz cell also holds a monospace code block)
**HTML title tag:** Imperative vs Declarative

**Subtitle:** Two ways to ask for the top 5 customers by spend — spell out every step yourself, or state the result you want and let the engine work out the steps

## One Question, Two Ways to Ask It

**Tags:** `core idea` (blue), `running example` (green)

- **The task** — "get me the top 5 customers by spend" from a table of customers
- **Imperative** — you say HOW: loop over rows, compare, keep a running list, sort, cut
- **Declarative** — you say WHAT: "sorted by spend, largest first, give me 5"
- **Same answer** — both produce the exact same 5 names; only the author of the steps differs
- **Who decides** — imperative: you pick every step; declarative: the engine picks them

*Example:* "Bake me a chocolate cake" is declarative; the 14-step recipe you follow at home is imperative.

**Key point:** Imperative code describes a procedure; declarative code describes a result. The result leaves the engine free to choose a better procedure than yours.

### Code block (`.payload`, below the canvas in the viz cell)

```
# Imperative — you write the steps
top5 = []
for row in customers:          # visit every row yourself
    top5.append(row)
    top5.sort(key=spend, reverse=True)
    top5 = top5[:5]            # keep only 5

# Declarative — you write the result
SELECT name, spend FROM customers
ORDER BY spend DESC LIMIT 5;
```

### Visualization (canvas `c1`, 720×300)

Flow diagram: imperative step stack on the left vs declarative WHAT-to-engine flow on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Same Question, Two Authors of the Steps".
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=360.
- **Left column (imperative), centered at x=180:** header bold 13px orange `#d95926` "Imperative: you say HOW"; a vertical stack of 4 boxes (240×30, at x=60, starting y=62, 46px pitch, fill `#fdf0e6`, border orange 1.5px) each with 12px `#2c3e50` text: "visit every row in a loop", "compare spend, keep a top-5 list", "sort the list, cut back to 5", "repeat until rows run out"; orange down-arrows between boxes; footer bold 12px orange "4 steps, written by you" (y=272).
- **Right column (declarative), centered at x=540:** header bold 13px green `#008300` "Declarative: you say WHAT"; a green box (260×44 at (410, 66), fill `rgba(0,131,0,0.08)`, green border) containing two monospace 12px lines "SELECT name, spend" / "ORDER BY spend DESC LIMIT 5"; green down-arrow to a blue box (220×40 at (430, 138), fill `#eef4fb`, blue `#2a78d6` border) labeled bold 12px blue "engine invents the steps" with 12px mute `#6b7280` sub-line "(scan? index? parallel?)"; blue down-arrow to a green box (180×34 at (450, 206)) labeled 12px "the same 5 customers".
- **Footer (bold 13px violet `#4a3aa7`, centered at x=540, y=272):** "identical answer — the engine wrote the loop for you".

## Eight Customers You Can Sort by Hand

**Tags:** `worked example` (green)

- **The table** — 8 customers: Ana $420, Ben $180, Cara $610, Dev $95
- **...and** — Eva $340, Fay $720, Gil $260, Hana $150
- **Sort descending** — Fay 720, Cara 610, Ana 420, Eva 340, Gil 260, Ben, Hana, Dev
- **Cut to 5** — top 5 are Fay, Cara, Ana, Eva, Gil
- **Check the total** — 720 + 610 + 420 + 340 + 260 = $2,350 of $2,775 overall

*Example:* The loop version and the SQL version both return exactly Fay, Cara, Ana, Eva, Gil.

**Key point:** With 8 rows you can trace either version by hand — the paradigm changes who does the work, never the answer.

### Visualization (canvas `c2`, 720×300)

Sorted bar chart of the 8 customers with the top 5 highlighted and a LIMIT-5 cut line.

- **Title (bold 15px, `#1a5276`, top center):** "Customers Sorted by Spend — Then Cut to 5".
- **Data:** names `['Fay', 'Cara', 'Ana', 'Eva', 'Gil', 'Ben', 'Hana', 'Dev']`, spend `[720, 610, 420, 340, 260, 180, 150, 95]`; y scale max 800.
- **Axes:** padding top 52, bottom 48, left 58, right 20; axis lines `#999`; x-axis caption "customers ranked by spend, largest first" (12px mute `#6b7280`, bottom center).
- **Bars:** width 52px, evenly gapped; first 5 bars filled green `#008300`, last 3 `rgba(107,114,128,0.35)`; dollar value labels 12px `#2c3e50` above bars (e.g. "$720"), name labels below.
- **Bracket:** green bracket (width 2) spanning the top-5 bars just above the plot area.
- **Cut line:** dashed (5/4) magenta `#d55181`, width 2, vertical between rank 5 and rank 6, labeled bold 12px magenta "LIMIT 5 cuts here".
- **Annotation (bold 13px green, at ~36% plot width near top):** "top 5 = $2,350 of $2,775 total (85%)".

## Why a Data Scientist Should Care

**Tags:** `where it's used` (blue), `rule of thumb` (green)

- **The engine optimizes** — a database can use an index or parallel workers; your loop can't
- **Less code, fewer bugs** — one SQL line replaces ~10 loop lines with edge cases
- **Your daily tools** — SQL, pandas `nlargest`, dplyr, Spark are all declarative
- **Scales silently** — the same query works on 8 rows or 10 million; the loop crawls
- **Readable intent** — a reviewer sees WHAT you wanted, not 10 lines of bookkeeping

*Example:* In pandas the same ask is one line: `df.nlargest(5, 'spend')`.

**Key point:** Every step you don't spell out is a step the engine is free to do faster — that freedom is the whole payoff of declarative style.

### Visualization (canvas `c3`, 720×300)

Two mini bar panels: lines of code (left) and runtime on 10M rows (right).

- **Title (bold 15px, `#1a5276`, top center):** "What You Save by Saying WHAT".
- **Divider:** dashed (4/3) vertical line `#bdc3c7` at x=360.
- **Left panel, centered at x=185:** header bold 13px ink "Lines of code you maintain"; two bars (70px wide, baseline y=240, scale max 12 over 150px): "loop version" = 10 lines in orange `#d95926`, "SQL / pandas" = 1 line in green `#008300`; value labels bold 12px ("10 lines", "1 line") above, category labels 12px below; baseline line `#999`; footer bold 12px green "10x less code to get wrong" (y=272).
- **Right panel, centered at x=540:** header bold 13px ink "Runtime on 10M rows (illustrative)"; two bars (70px wide, same baseline, scale max 14, minimum 4px height): "Python loop" = 12 s in orange, "database" = 0.3 s in blue `#2a78d6`; value labels bold 12px ("12 s", "0.3 s"); baseline line `#999`; footer bold 12px blue "the engine used an index + parallel workers" (y=272).

## The Common Confusion: Declarative Is Not Vague

**Tags:** `common mistake` (red), `trade-off` (orange)

- **Not fuzzy** — "top 5 by spend" is exact; declarative means precise WHAT, silent HOW
- **Not magic** — a bad question (missing filter, wrong column) is bad in both styles
- **Imperative still wins** — simulations, custom step-by-step logic, glue code
- **Ties, for example** — if two customers tie at rank 5, YOU must say which to keep
- **Mix freely** — real work is imperative scripts calling declarative queries

*Example:* A Monte Carlo simulation has no "WHAT" to declare — it is a procedure, so write a loop.

**Key point:** Pick the paradigm per task, not per religion — declare data shaping, script the procedures around it.

### Visualization (canvas `c4`, 720×300)

Two side-by-side checklist boxes: when to declare vs when to script.

- **Title (bold 15px, `#1a5276`, top center):** "Pick the Paradigm per Task".
- **Left box (declarative):** rectangle at (45, 48), 300×180, fill `rgba(0,131,0,0.06)`, green `#008300` border 1.5px; header bold 13px green "Declare it (say WHAT)"; four check items (green "✓" at x=62, 12px `#2c3e50` text at x=80, 26px pitch): "filter rows: WHERE spend > 100", "join tables on customer id", "aggregate: totals per region", "rank and cut: top 5 by spend"; footer italic 12px mute `#6b7280` centered: "data shaping: the engine can optimize".
- **Right box (imperative):** rectangle at (375, 48), 300×180, fill `#fdf0e6`, orange `#d95926` border 1.5px; header bold 13px orange "Script it (say HOW)"; four check items (orange "✓"): "simulate 10,000 coin-flip runs", "retry an API call until it works", "custom step-by-step business rule", "glue: download, unzip, load, email"; footer italic 12px mute centered: "procedures: the steps ARE the point".
- **Captions (bottom center):** bold 13px violet `#4a3aa7` "real pipelines mix both: an imperative script wrapping declarative queries" (y=262); 12px mute "declarative is exact about the result — it is silent only about the steps" (y=284).

## Regeneration instructions

- **Layout:** tutorial detail page. `<h1>` + `.subtitle`, then 4 `.card-section` blocks, each with an `<h2>` (1.3rem, `#1a5276`, 2px solid `#2980b9` bottom border) and a `table.layout` (width 100%, border-collapse) with one `<tr>`: left `td.text-col` (50%) holding `.tags` pills, a `<ul>` of bullets, an italic `.example` line, and a `.key-point` callout; right `td.viz-col` (50%) holding a `<canvas>` 720×300 at `width:100%` with 1px `#e0e0e0` border, 4px radius. Section 1's viz cell additionally holds a `<pre class="payload">` code block below the canvas.
- **Page CSS:** body system-ui sans, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `<ul>` 0.92rem; `li b` colored `#1a5276`; inline `code` in ui-monospace on `#f4f6f8` background, 3px radius.
- **Code block style:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, `overflow-x: auto`, line-height 1.45, left-aligned.
- **Tag pills:** `.tag` inline-block 0.72rem weight 600, padding 2px 10px, radius 10px; blue = `rgba(26,82,118,0.12)` bg / `#1a5276` text; green = `rgba(39,174,96,0.15)` / `#27ae60`; red = `rgba(231,76,60,0.12)` / `#e74c3c`; orange = `rgba(230,126,34,0.15)` / `#e67e22`.
- **Callout style:** `.key-point` — background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `.example` — italic, `#555`, 0.9rem.
- **Canvas:** logical size 720×300 per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; shared `box()` and `arrowDown()` drawing helpers for the diagram charts. Hardcoded data arrays, no `Math.random()`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette (JS object `P`):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
