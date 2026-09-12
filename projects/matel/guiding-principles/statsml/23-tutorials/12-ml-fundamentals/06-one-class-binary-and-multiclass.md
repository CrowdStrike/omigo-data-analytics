# One-Class, Binary & Multiclass

**Page type:** detail page (tutorial: 4 card-sections; sections 1, 3, 4 two-column table.layout 45/55, section 2 three-column 38/31/31 with two canvases)
**HTML title tag:** One-Class, Binary & Multiclass

**Subtitle:** Before picking an algorithm, count your classes: only normal examples (one-class), exactly two answers (binary), or one of many (multiclass) — each shape needs different tools

## One Bank, Three Question Shapes

Tags: `core idea` (blue), `running example` (green)

- **Binary** — two labeled classes, one boundary between them: "is this transaction fraud — yes or no"
- **Multiclass** — k labeled classes, space carved into k regions: "which of 5 spend categories is it?"
- **One-class** — only normal examples exist: learn their shape, flag whatever falls outside it
- **The count decides** — the number of labeled classes picks the algorithm family, not taste
- **Same bank** — one transaction table can pose all three questions at once

*Example (italic):* The same $300 charge is "fraud? no", "category: travel", and "inside the normal fence" at once.

**Key point:** Before choosing an algorithm, count the classes you actually have labels for: one, two, or many.

### Visualization (canvas `c1`, 720×300)

Three mini-panels side by side: binary boundary, multiclass regions, one-class fence.

- **Title (bold 15px, `#1a5276`, top center):** "Three Shapes of Classification — Count the Labeled Classes"
- **Dividers:** vertical dashed gray lines (`#bdc3c7`, dash 4/3) at x=240 and x=480, from y=36 to y=250.
- **Panel headers (bold 13px, centered at x=120/360/600, y=52):** "BINARY — 2 labels" in blue `#2a78d6`; "MULTICLASS — k labels" in green `#008300`; "ONE-CLASS — normals only" in violet `#4a3aa7`.
- **Binary panel (left):** legit cloud, 5 blue `#2a78d6` 5px dots at (60,200), (80,215), (75,185), (100,205), (90,225); fraud cloud, 4 orange `#d95926` 5px dots at (140,110), (160,95), (150,125), (175,110); dashed violet boundary line (`#4a3aa7`, dash 6/4, width 2) from (45,150) to (210,168); bold 12px labels: blue "legit" at (78,172), orange "fraud" at (158,80).
- **Multiclass panel (middle):** three clouds of 3 dots each (5px): green `#008300` at (285,115), (305,100), (298,130); blue `#2a78d6` at (395,115), (415,105), (405,135); orange `#d95926` at (340,215), (360,200), (350,228); dashed ink region boundaries (`#1a5276`, dash 6/4, width 2), three segments from hub (350,165) to (350,75), to (275,235), to (425,235); bold 12px labels: green "groceries" at (295,155), blue "travel" at (405,155), orange "dining" at (350,190).
- **One-class panel (right):** normal cloud, 6 blue `#2a78d6` 5px dots at (560,150), (580,165), (575,135), (600,155), (590,180), (610,140); dashed green fence ellipse (`#008300`, dash 6/4, width 2.5) center (585,157) radii 58×45; blue bold 12px label "normal transactions" at (585,220); a lone red `#e74c3c` 5px dot at (665,95) with red bold 12px "flagged!" at (665,80).
- **Panel captions (bold 12px, centered at x=120/360/600, y=262):** blue "two sides, one boundary"; green "space carved into k regions"; violet "learn the fence, flag outsiders".
- **Footnote (11px `#6b7280`, right-aligned at x=710, y=290):** "illustrative sketches".

## Three Yes/No Scorers Make a Category Picker

Tags: `worked example` (green)

- **The task** — sort a charge into groceries, travel, or dining from just amount and hour of day
- **One-vs-rest** — train 3 binary scorers: groceries-vs-not, travel-vs-not, dining-vs-not
- **New charge** — $35 at 8pm gets three scores: 0.20, 0.05, 0.85 (illustrative)
- **Highest wins** — dining scores 0.85, so the charge is filed as dining
- **Softmax** — neural nets do the same trick in one shot; one-vs-one pairs classes instead

*Example (italic):* Nine past charges, three per category, are enough to redo the whole thing by hand.

**Key point:** Multiclass often IS just binary run k times — one yes/no scorer per class, highest score wins.

### Visualization (canvas `c2a`, 420×340)

Scatter of the 9 training charges with the three binary boundaries drawn on one map.

- **Title (bold 15px, `#1a5276`, top center):** "Three binary fences, one map"
- **Axes:** x = amount $ (0–350), y = hour of day (0–24); L-shaped gray `#999` axis; padding top 46 / bottom 58 / left 58 / right 20; axis caption (12px `#444`, bottom center): "amount $ (x), hour of day (y)".
- **Training points (6px dots):** groceries green `#008300` at (42,10), (55,12), (48,11); travel blue `#2a78d6` at (280,9), (320,15), (260,20); dining orange `#d95926` at (24,19), (52,22), (70,21).
- **Travel boundary:** vertical dashed blue line (`#2a78d6`, dash 6/4, width 2) at amount=150, full plot height; bold blue 12px label "travel vs not" just right of the line near the top.
- **Dining boundary:** horizontal dashed orange line (`#d95926`, dash 6/4, width 2) at hour=17, from amount=0 to amount=150; bold orange 12px label "dining vs not" above its left end.
- **Groceries region:** bold green 12px label "groceries vs not: high here" centered at data point (75, 5).
- **New point:** violet `#4a3aa7` filled diamond (7px half-diagonal) at (35, 20), bold violet 12px label "new: $35, 8pm" to its right.
- **Caption (bold 12px violet `#4a3aa7`, centered below axis):** "each fence answers one yes/no question"

### Visualization (canvas `c2b`, 400×340)

Score bars for the new charge; the highest of the three binary scores picks the category.

- **Title (bold 15px, `#1a5276`, top center):** "Score the new charge, highest wins"
- **Subtitle (bold 13px violet `#4a3aa7`, centered, y=50):** "new charge: $35 at 8pm"
- **Three horizontal bars:** rows at y=90/150/210, bar track from x=155 to x=360 (score 0→1), bar height 26, light gray track `#e5e9ef` with 1px `#c9d4de` border; left labels (bold 12px, right-aligned at x=145, vertically centered): green `#008300` "groceries vs not", blue `#2a78d6` "travel vs not", orange `#d95926` "dining vs not"; fills in the label color at scores 0.20 / 0.05 / 0.85; bold 13px score text ("0.20", "0.05", "0.85") in the same color just right of each bar end.
- **Winner mark:** the dining bar gets a 2px ink `#1a5276` outline and a bold 12px ink "WINNER" tag above its right end.
- **Caption (bold 12px green `#008300`, centered, y=268):** "highest score wins: filed as dining"
- **Footnote (11px `#6b7280`, centered, y=290):** "scores illustrative".

## Where Each Shape Shows Up

Tags: `where it's used` (blue), `rule of thumb` (blue)

- **Binary jobs** — spam, fraud yes/no, churn, medical screening: two answers, one threshold
- **Binary tools** — logistic regression, SVM, trees: nearly every algorithm speaks binary natively
- **Multiclass jobs** — digit recognition, product categories, ticket routing, language ID
- **Multiclass tools** — trees, kNN, Naive Bayes, softmax nets; one-vs-rest wraps the rest
- **One-class jobs** — fraud, intrusion, defect and novelty detection when bad examples are rare
- **One-class tools** — one-class SVM, isolation forest, autoencoder error, density thresholds

*Example (italic):* A bank runs all three: a fraud yes/no gate, a category filer, and a normal-shape fence.

**Key point:** The problem shape, not the dataset size, decides which shelf of tools you reach for.

### Visualization (canvas `c3`, 720×300)

Three-column board: one colored box per problem shape listing use cases and tools.

- **Title (bold 15px, `#1a5276`, top center):** "Use Cases and Tools, by Shape"
- **Boxes:** three 210×200 boxes at x=30/255/480, y=52; fill `rgba(0,0,0,0.02)`, 2px colored border: binary blue `#2a78d6`, multiclass green `#008300`, one-class violet `#4a3aa7`.
- **Box content (all text centered on the box):** bold 13px colored header at y+22 ("BINARY", "MULTICLASS", "ONE-CLASS"); three 12px `#333` use-case lines at y+48/+66/+84; a thin colored divider line at y+100 (inset 20px each side); a bold 11px `#6b7280` "TOOLS" label at y+118; three bold 12px colored tool lines at y+138/+156/+174.
  - Binary uses: "spam filtering" / "fraud yes-no, churn" / "medical screening"; tools: "logistic regression" / "SVM, decision trees" / "almost every algorithm".
  - Multiclass uses: "digit recognition" / "product categorization" / "ticket routing, language ID"; tools: "trees, kNN, Naive Bayes" / "softmax nets" / "one-vs-rest wraps binary".
  - One-class uses: "fraud & intrusion detection" / "defect spotting" / "novelty detection"; tools: "one-class SVM, isolation forest" / "autoencoder error" / "density thresholds".
- **Bottom caption (bold 13px orange `#d95926`, centered, y=282):** "one-class earns its keep when bad examples are rare or keep changing"

## Not Imbalanced Binary, Not Multi-Label

Tags: `common mistake` (red)

- **The trap** — "rare fraud, so it's just imbalanced binary" — only if you HAVE fraud examples
- **One-class truth** — no usable second-class examples, or tomorrow's fraud won't match yesterday's
- **Second trap** — multiclass gives each row exactly ONE of k labels, never several
- **Multi-label** — several tags true at once: a movie is both comedy AND romance
- **Quick test** — ask "do I have both classes?" and "can two labels be true together?"

*Example (italic):* A new fraud pattern walks past a binary model trained on old fraud; the one-class fence still flags it.

**Common mistake (key-point callout):** One-class is not imbalanced binary, and multiclass is not multi-label — mixing them up picks the wrong tool.

### Visualization (canvas `c4`, 720×300)

Two panels contrasting the look-alike pairs, each ending in a bold verdict line.

- **Title (bold 15px, `#1a5276`, top center):** "Two Look-Alikes That Are Not the Same"
- **Divider:** vertical dashed gray line (`#bdc3c7`, dash 4/3) at x=360, from y=36 to y=285.
- **Left panel header (bold 13px violet `#4a3aa7`, centered at x=180, y=54):** "one-class ≠ imbalanced binary"
- **Left row 1:** bold 12px ink `#1a5276` label "imbalanced binary:" left-aligned at (55, 84); 8 blue `#2a78d6` 5px dots in a row at y=105, x=70 to 210 step 20; one red `#e74c3c` 5px dot at (235,105) with red bold 11px "fraud" at (235,122); 12px `#333` centered caption at (180,140): "rare — but labeled fraud exists".
- **Left row 2:** bold 12px ink label "one-class:" at (55, 164); 8 blue 5px dots in a row at y=190, x=70 to 210 step 20; dashed green `#008300` fence rectangle (dash 6/4, width 2) around them from (58,176) to (222,204); bold 16px gray `#6b7280` "?" at (250,196); 12px `#333` centered caption at (180,222): "zero fraud examples to learn from".
- **Left verdict (bold 12px violet, centered at x=180, y=252):** "no usable second class → one-class"; 11px `#6b7280` line below at y=270: "tomorrow's fraud won't match yesterday's".
- **Right panel header (bold 13px green `#008300`, centered at x=540, y=54):** "multiclass ≠ multi-label"
- **Right row 1:** bold 12px ink label "multiclass — exactly one label:" at (395, 84); pill row at y=98 (rects 26px tall, 1px radius-free strokeRect): gray-outline `#6b7280` pills "groceries" (x=400, w=78) and "travel" (x=486, w=60), filled pill "dining" (x=554, w=62) with fill `rgba(217,89,38,0.15)`, 2px orange `#d95926` border, bold orange text; 12px `#333` centered caption at (540,148): "one and only one box gets ticked".
- **Right row 2:** bold 12px ink label "multi-label — several can be true:" at (395, 172); pill row at y=186: filled pill "comedy" (x=400, w=72, fill `rgba(0,131,0,0.12)`, 2px green border, bold green text), filled pill "romance" (x=480, w=78, fill `rgba(213,81,129,0.12)`, 2px magenta `#d55181` border, bold magenta text), gray-outline pill "horror" (x=566, w=62); 12px `#333` centered caption at (540,236): "two boxes ticked at once is fine".
- **Right verdict (bold 12px green, centered at x=540, y=264):** "can two labels be true together? → multi-label"

## Regeneration instructions

- **Template:** tutorials topic-page skeleton (most-powerful-signals compact style). Each `.card-section` has an `<h2>` (1.3rem `#1a5276`, 2px bottom border `#2980b9`) and a `table.layout`; standard rows use `.text-col` (50%) / `.viz-col` (50%); the two-chart row uses `.text-col3` (38%) with two `.viz-col3` cells (31% each).
- **Left column per section:** `.tags` pill row first (0.72rem bold, 10px radius pills — blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`), then a `<ul>` of one-line bullets each opening with `<b>` term in `#1a5276`, then an italic `.example` line (`#555`, 0.9rem), then a `.key-point` callout (background `#f8f9fa`, left border 3px `#e74c3c`, 0.9rem).
- **Page style:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px bottom border `#2980b9`; `.subtitle` `#666` 0.95rem. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes as given per chart (720×300, 420×340, 400×340), CSS `width:100%`, 1px border `#e0e0e0` radius 4px; scaled via `window.devicePixelRatio` in a shared `setup(id)` helper reading width/height attributes (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange. Red reserved for genuine alarm points (the flagged outlier, the rare fraud dot).
- **Data:** shared literal arrays for the worked example — GROC amounts `[42,55,48]` hours `[10,12,11]`, TRAV amounts `[280,320,260]` hours `[9,15,20]`, DIN amounts `[24,52,70]` hours `[19,22,21]`, NEW point `[35,20]`, one-vs-rest scores `[0.20, 0.05, 0.85]`; c1 and c4 sketch dots are literal coordinate arrays; no `Math.random()`.
- In regenerated HTML, any card links use `.html` extensions.
