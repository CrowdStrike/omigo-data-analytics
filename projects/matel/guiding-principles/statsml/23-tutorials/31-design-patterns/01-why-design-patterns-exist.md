# Why Design Patterns Exist

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Why Design Patterns Exist

**Subtitle:** Recurring problems produce recurring solution shapes — a design pattern is such a shape with a name, so teams can reuse it instead of rediscovering it

## Every New Coffee Shop Reinvents the Same Fixes

**Tags:** `core idea` (blue), `recurring problem` (green), `coffee shop` (orange)

- **The shop** — a new coffee shop opens and hits the same three headaches every shop before it hit
- **Waiting crowd** — customers hover at the counter, asking again and again whether their drink is done
- **Menu explosion** — every add-on combo (oat milk, extra shot, syrup) threatens to become its own menu line
- **One machine** — every order needs the single espresso machine, and baristas collide over it
- **The shapes** — every shop lands on the same fixes: a callout board, add-ons on a base drink, one queue

*Example (italic):* A shop in a new town, with new staff and a new menu, independently reinvents the exact same pickup callout board.

**Key point:** Recurring problems produce recurring solution shapes. A design pattern is simply such a shape, given a name so it can be reused instead of rediscovered from scratch.

### Visualization (canvas `c1`, 720×300)

Flow diagram: three recurring shop problems on the left, each with a 3px arrow to the recurring solution shape it converges on.

- **Title (bold 15px, `#1a5276`, top center):** "Three Recurring Headaches, Three Recurring Shapes".
- **Rows (box centers at y = 85, 160, 235):** left problem boxes at x=40, 280px wide, 44px tall, 8px radius; right shape boxes at x=430, 250px wide, 44px tall; 3px arrow from x=320 to x=430 at each row's center.
- **Row 1 (blue `#2a78d6`, fill `rgba(42,120,214,0.15)`):** "customers keep asking: is mine done?" → "callout board — announce once".
- **Row 2 (aqua `#199e70`, fill `rgba(25,158,112,0.14)`):** "add-on combos explode the menu" → "base drink + stackable add-ons".
- **Row 3 (violet `#4a3aa7`, fill `rgba(74,58,167,0.12)`):** "everyone needs the one machine" → "one shared queue for the machine".
- **Box text:** 12px `#2c3e50`, centered.
- **Annotation (bold 13px green `#008300`, centered near y=278):** "same problem, same shape — in every shop".
- **Caption (12px `#444`, bottom right):** "shop details illustrative".

## Counting Interruptions: Ask-Me-Again vs the Callout Board

**Tags:** `worked example` (blue), `hand-check` (green)

- **The setup** — 12 customers wait at the 2pm rush; each walks up to ask "is mine ready?" every 30 seconds
- **Asking rate** — 12 customers × 2 asks per minute = 24 barista interruptions every minute
- **The callout** — instead the barista announces each finished drink once: 3 drinks per minute = 3 callouts
- **Hand-check** — 24 interruptions vs 3 callouts is 8× less noise, and no customer misses a drink
- **Scaling** — with 4 or 8 waiting customers the asks are 8 or 16 per minute; callouts stay at 3

*Example (italic):* At the 2pm rush, 12 waiting customers generate 24 asks a minute; the callout board cuts the barista's interruptions to 3.

**Key point:** The callout shape — announce once, everyone listens — wins because its cost tracks finished drinks, not crowd size. Software knows this exact shape as Observer.

### Visualization (canvas `c2`, 720×300)

Grouped bar chart: barista interruptions per minute at three crowd sizes, ask-me-again (blue) vs callout board (green).

- **Title (bold 15px, `#1a5276`, top center):** "Barista Interruptions per Minute: Asking vs the Callout Board".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = 0 to 24 with gridlines `#e5e9ef` at 6/12/18 and 12px `#444` tick labels; x = three groups labeled "4 waiting", "8 waiting", "12 waiting" (12px `#444`) centered at x = 170, 370, 570.
- **Ask bars (blue `#2a78d6`):** 60px wide, left of each group center, heights for values `[8, 16, 24]`.
- **Callout bars (green `#008300`):** 60px wide, right of each group center, heights for values `[3, 3, 3]`.
- **Value labels:** bold 12px in each bar's color above each bar top: "8", "16", "24" and "3", "3", "3".
- **Legend (12px, top left inside plot):** blue swatch "each customer asks", green swatch "callout board".
- **Annotation (bold 13px green `#008300`, near x=400, y=90):** "callouts stay at 3 no matter how many wait".
- **Caption (12px `#444`, bottom right):** "rates illustrative: each customer asks every 30s, 3 drinks finish per minute".

## The Same Shapes Turn Up in Data Work

**Tags:** `where it's used` (blue), `shared vocabulary` (green), `data work` (orange)

- **Named shapes** — the coffee-shop fixes reappear in code under names: Observer, Decorator, Singleton
- **Observer** — a dashboard that refreshes when new data lands is the callout board in software
- **Decorator** — wrapping a model call with logging and caching is add-ons on a base drink
- **Singleton** — one shared database connection pool is the single espresso machine and its queue
- **Vocabulary** — saying "make it an observer" in code review replaces a paragraph of explanation

*Example (italic):* A data scientist wiring a retrain job to fire when fresh data arrives is choosing the callout-board shape, whether or not they know its name.

**Key point:** Patterns matter less as code recipes and more as shared vocabulary — a name lets a team point at an entire solution shape in one word.

### Visualization (canvas `c3`, 720×300)

Three-column mapping diagram: coffee-shop shape → software pattern name → where a data scientist meets it, one row per pattern.

- **Title (bold 15px, `#1a5276`, top center):** "The Shop Shape, Its Software Name, Where You Meet It".
- **Column headers (bold 12px `#1a5276`, y=58, centered):** "shop shape" at x=120, "pattern name" at x=355, "data work" at x=595.
- **Rows (box centers at y = 100, 165, 230):** col-1 boxes at x=30, 180px wide; col-2 boxes at x=280, 150px wide; col-3 boxes at x=490, 210px wide; all 40px tall, 8px radius, 12px `#2c3e50` text; 2px `#6b7280` arrows between columns.
- **Row 1 (blue `#2a78d6`, fill `rgba(42,120,214,0.15)`):** "callout board" → "Observer" → "dashboard refreshes on new data".
- **Row 2 (aqua `#199e70`, fill `rgba(25,158,112,0.14)`):** "add-ons on a base drink" → "Decorator" → "logging + caching on a model call".
- **Row 3 (violet `#4a3aa7`, fill `rgba(74,58,167,0.12)`):** "one machine, one queue" → "Singleton" → "shared DB connection pool".
- **Pattern-name boxes:** bold 12px text in the row's color.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "one name replaces a paragraph in code review".

## Forcing a Pattern Where No Problem Recurs

**Tags:** `common mistake` (red), `over-engineering` (orange)

- **The job** — a one-off script reads a CSV and prints one total: 5 lines, runs once, done
- **The temptation** — "proper design" adds a Factory (22 lines), then a Singleton (38), then a Strategy (60)
- **The check** — the problem never recurred, so no recurring shape was ever needed
- **The smell** — 60 lines is 12× the code, and it prints exactly what the 5-line script printed
- **The rule** — reach for a pattern when you meet the problem the second or third time, not the first

*Example (italic):* The 5-line CSV totaler grows to 60 lines of Factory + Singleton + Strategy and still prints the same single number.

**Common mistake:** Applying patterns to problems that never recur. A pattern earns its complexity only when the recurring problem it solves has actually shown up — patterns are answers, not decoration.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: lines of code for the same one-CSV-total job as each unneeded pattern is layered on.

- **Title (bold 15px, `#1a5276`, top center):** "Same Job, More Pattern: Lines of Code for One CSV Total".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, max width 440 (60 lines ≈ 7.33 px per line); left-aligned 12px `#444` row labels at x=20.
- **Rows (bar centers at y = 75, 130, 185, 240), bars 16px tall, 8px radius ends optional:**
  - "plain script — 5 lines": green `#008300` bar width 37
  - "+ Factory — 22 lines": blue `#2a78d6` bar width 161
  - "+ Singleton — 38 lines": orange `#d95926` bar width 279
  - "+ Strategy — 60 lines": red `#e74c3c` bar width 440, with bold 12px red label "12× the code, same output" at the bar end
- **Line-count labels:** 11px in each bar's color just past each bar end: "5", "22", "38", "60".
- **Annotation (bold 13px orange `#d95926`, centered near y=272):** "the job never changed — only the ceremony did".
- **Caption (12px `#444`, bottom right):** "line counts illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); interruption rates (asks `[8, 16, 24]` vs callouts `[3, 3, 3]` at 4/8/12 waiting customers) follow from the stated 30-second ask interval and 3 drinks per minute and are labeled illustrative; the lines-of-code sequence `[5, 22, 38, 60]` is invented and labeled illustrative; the 8× and 12× ratios in the text are exact arithmetic on those numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
