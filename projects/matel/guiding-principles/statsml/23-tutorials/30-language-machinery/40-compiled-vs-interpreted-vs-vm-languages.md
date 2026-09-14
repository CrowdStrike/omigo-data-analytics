# Compiled vs Interpreted vs VM Languages

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Compiled vs Interpreted vs VM Languages

**Subtitle:** Your code is a recipe the computer can't read — the only real difference between compiled, interpreted, and VM languages is when and where someone translates it

## One Recipe, Three Ways to Translate It

**Tags:** `core idea` (blue), `translation` (green), `three styles` (orange)

- **The recipe** — a cook finds a great recipe written in French, but the kitchen only reads English
- **Compiled** — hire a translator once, get a full English copy, cook from that copy forever after
- **Interpreted** — the translator stands beside the cook and translates each line aloud, every dinner
- **VM style** — translate once into a compact kitchen shorthand any shorthand-reader can follow
- **Same dish** — all three paths end in the same meal; they differ only in when translation happens
- **In code** — C compiles ahead, classic Python interprets as it runs, Java compiles to VM bytecode

*Example (italic):* The French line "faites revenir les oignons" becomes "brown the onions" either once on paper (compiled), aloud mid-cooking (interpreted), or once as shorthand "BRN-ONI" (VM).

**Key point:** Compiled, interpreted, and VM are not different kinds of code — they are three answers to one question: when does the translation to machine steps happen?

### Visualization (canvas `c1`, 720×300)

Three horizontal pipeline rows (compiled, interpreted, VM), each a chain of labeled boxes and arrows from the recipe to the cook, showing where the translation step sits in each path.

- **Title (bold 15px, `#1a5276`, top center):** "Where the Translation Happens: Three Paths from Recipe to Cook".
- **Rows:** three lanes at y = 95, 165, 235 (box centers); each lane starts with a bold 12px row label at x=15 — blue `#2a78d6` "COMPILED", orange `#d95926` "INTERPRETED", green `#008300` "VM / BYTECODE".
- **Box style:** rounded rectangles 118 wide, 36 tall, 1.5px border in the row color, fill white; box text 12px `#2c3e50` centered; arrows 2px `#6b7280` with small arrowheads between boxes.
- **Compiled row (blue), box left edges at x=130, 285, 440, 580:** "French recipe" → "translator (once)" → "English copy" → "cook reads it"; the "translator (once)" box gets fill `rgba(42,120,214,0.15)`.
- **Interpreted row (orange), box left edges at x=130, 315, 520:** "French recipe" → "translator beside cook (every dinner)" (box 170 wide, fill `rgba(217,89,38,0.15)`) → "cook follows aloud".
- **VM row (green), box left edges at x=130, 285, 440, 580:** "French recipe" → "translator (once)" (fill `rgba(0,131,0,0.15)`) → "shorthand card" → "shorthand reader".
- **Annotation (bold 12px `#1a5276`, right side near x=560, y=55):** "the shaded box is the translation — early, constant, or once-to-shorthand".
- **Caption (12px `#444`, bottom right):** "code analogue: C compiles ahead, Python interprets, Java runs bytecode on the JVM".

## Six Dinners: Counting the Translation Minutes

**Tags:** `worked example` (blue), `setup vs per-run` (green)

- **The plan** — the cook will make this dish for 6 dinners; count only the extra translation minutes
- **Compiled** — 30 minutes to translate the whole recipe once, then 0 extra minutes per dinner
- **Interpreted** — 0 minutes of prep, but the live translator adds 10 minutes to every single dinner
- **VM** — 6 minutes once into shorthand, plus 2 minutes of shorthand-reading per dinner
- **Totals** — after 6 dinners: compiled 30, interpreted 10×6 = 60, VM 6 + 2×6 = 18 minutes
- **Crossover** — at dinner 3 interpreted has also spent 30; from dinner 4 on, compiling was cheaper

*Example (italic):* Cook the dish just once and interpreting wins (10 vs 30 vs 8 minutes) — cook it six times and the order flips completely (60 vs 30 vs 18).

**Key point:** Compiled pays a big cost once, interpreted pays a small cost every run, VM splits the difference — which is cheapest depends on how many times you run the code.

### Visualization (canvas `c2`, 720×300)

Single-panel line chart: cumulative extra translation minutes after each of 6 dinners, one line per style, with the dinner-3 crossover between compiled and interpreted marked.

- **Title (bold 15px, `#1a5276`, top center):** "Total Translation Minutes After Each Dinner".
- **Axes:** origin x=60, baseline y=245, plot width 580, plot height 180; x = dinner number 1 to 6 with 12px `#444` tick labels "1".."6"; y = cumulative minutes 0 to 60, light `#e5e9ef` gridlines every 10 with 12px `#444` labels "0".."60"; y-axis title 12px `#444` rotated "total extra minutes".
- **Compiled line:** blue `#2a78d6` 3px line with 5px dots through (dinner, minutes) = `[[1, 30], [2, 30], [3, 30], [4, 30], [5, 30], [6, 30]]`; 12px blue label "compiled (30 once)" left of the line start.
- **Interpreted line:** orange `#d95926` 3px line with 5px dots through `[[1, 10], [2, 20], [3, 30], [4, 40], [5, 50], [6, 60]]`; 12px orange label "interpreted (+10 each)" near its top end.
- **VM line:** green `#008300` 3px line with 5px dots through `[[1, 8], [2, 10], [3, 12], [4, 14], [5, 16], [6, 18]]`; 12px green label "VM (6 once, +2 each)" below its right end.
- **Crossover marker:** 9px hollow `#1a5276` circle at dinner 3, 30 minutes, where the blue and orange lines meet.
- **Annotation (bold 12px `#1a5276`, near x=250, y=75):** two lines: "dinner 3: interpreting has now cost" / "as much as compiling once".
- **Caption (12px `#444`, bottom right):** "minutes are illustrative — the shape is the lesson".

## Why Your Python Loop Is Slow but NumPy Isn't

**Tags:** `where it's used` (blue), `data science` (green), `speed gap` (orange)

- **Daily life** — a data scientist meets all three before lunch: Python, NumPy's C core, Spark's JVM
- **The loop** — a plain Python `for` loop is interpreted: the translator re-reads every line, every pass
- **The escape** — `numpy.sum` hands the whole array to compiled C code, translated long ago
- **The JIT** — VMs like the JVM watch running bytecode and compile the hot parts on the fly
- **The gap** — summing 10 million numbers: Python loop 1.2 s, JVM ~0.05 s, compiled C ~0.02 s
- **The habit** — vectorize: replace interpreted loops with calls into compiled library code

*Example (italic):* Rewriting `for x in data: total += x` as `numpy.sum(data)` on 10 million numbers cuts 1.2 seconds to about 0.02 — a 60× speedup from moving the translation, not the math.

**Key point:** Slow Python loops and fast NumPy are the same lesson: the math never changed, only where the translation to machine steps happens.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: time to sum 10 million numbers under three translation styles, with the tiny compiled bars labeled at their ends and the 60× gap called out.

- **Title (bold 15px, `#1a5276`, top center):** "Summing 10 Million Numbers: Same Math, Different Translator".
- **Axes:** vertical 2px `#999` axis at x=250, bars extend right to max x=680 (plot width 430); x scale 0 to 1.3 seconds, light `#e5e9ef` gridlines at 0.25, 0.5, 0.75, 1.0, 1.25 with 12px `#444` labels "0.25 s".."1.25 s" below y=255.
- **Bars (26px tall, centered at y = 95, 155, 215), left-aligned 12px `#444` category labels at x=15:**
  - "Python for-loop (interpreted)": orange `#d95926` bar to 1.2 s, bold 12px orange value label "1.2 s" just past the bar end
  - "Java on the JVM (bytecode + JIT)": green `#008300` bar to 0.05 s, bold 12px green label "0.05 s" right of the bar
  - "C / NumPy core (compiled)": blue `#2a78d6` bar to 0.02 s, bold 12px blue label "0.02 s" right of the bar
- **Annotation (bold 13px `#1a5276`, near x=430, y=185):** two lines: "60× gap between the loop and NumPy —" / "the translator changed, not the math".
- **Caption (12px `#444`, bottom right):** "timings illustrative — typical order of magnitude, varies by machine".

## Compiled or Interpreted Is About the Translator, Not the Language

**Tags:** `common mistake` (red), `language vs implementation` (orange)

- **The mistake** — calling Python "an interpreted language" as if the language itself decides
- **Reality** — a language is a grammar; a specific implementation compiles or interprets it
- **Python** — CPython interprets bytecode, but PyPy JIT-compiles the very same Python programs
- **Java** — compiled by `javac`, yet the output is bytecode that runs on a VM: both at once
- **The spectrum** — most modern languages mix stages; pure one-style implementations are rare

*Example (italic):* The same Python file runs interpreted under CPython and JIT-compiled under PyPy — the file didn't change, so "interpreted" was never a fact about the language.

**Common mistake:** Treating "compiled vs interpreted" as a property of the language. It is a property of the implementation, and most real implementations sit somewhere on a spectrum, not at the ends.

### Visualization (canvas `c4`, 720×300)

Single horizontal timeline of "when translation happens", with four labeled stops from ahead-of-time to line-by-line and familiar implementations placed as dots — Java and Python each appearing where their implementation actually sits.

- **Title (bold 15px, `#1a5276`, top center):** "One Axis: When Does the Translation Happen?".
- **Timeline:** horizontal 3px `#6b7280` line at y=165 from x=70 to x=670, small arrowhead at the right end; 12px `#6b7280` end labels below the line: "earliest" under x=70, "latest" under x=670.
- **Stops (7px dots on the line at x = 130, 300, 470, 620), each with a bold 12px stop label above at y=135:**
  - x=130, blue `#2a78d6`: "ahead of time"
  - x=300, green `#008300`: "at load, to bytecode"
  - x=470, violet `#4a3aa7`: "while running (JIT)"
  - x=620, orange `#d95926`: "line by line"
- **Implementation tags (12px `#2c3e50`, two staggered rows below each stop at y=200 and y=222):** under x=130: "C, Rust, Go"; under x=300: "javac, CPython .pyc"; under x=470: "JVM hot spots, PyPy"; under x=620: "shell scripts".
- **Bracket:** thin 1.5px `#1a5276` bracket above the line spanning x=300 to x=470 at y=105, with bold 12px `#1a5276` label centered above it: "Java lives at BOTH stops: compiled to bytecode, then JIT'd".
- **Annotation (bold 13px magenta `#d55181`, centered near y=272):** "same language, different dot — the implementation picks the spot, not the language".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Color coding is consistent across all four charts:** blue = compiled / ahead-of-time, orange = interpreted / line-by-line, green = VM / bytecode, violet = JIT.
- **Data:** all bar lengths, line points, dot positions, and box coordinates are the hardcoded literals above (no randomness); the six-dinner minute totals in the text (30 / 60 / 18, crossover at dinner 3) and the timing bars (1.2 s / 0.05 s / 0.02 s, 60×) must match the chart values exactly; invented timings carry "illustrative" captions.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
