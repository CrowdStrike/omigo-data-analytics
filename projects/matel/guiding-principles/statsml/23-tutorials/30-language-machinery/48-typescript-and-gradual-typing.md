# TypeScript & Gradual Typing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** TypeScript & Gradual Typing

**Subtitle:** TypeScript retrofits type labels onto plain JavaScript — you can label one file at a time, and the checker verifies only what's labeled

## The Receipt That Said 3.500.60

**Tags:** `core idea` (blue), `retrofitted types` (green), `JavaScript` (orange)

- **The app** — a coffee shop's checkout is 40 files of plain JavaScript, no types anywhere
- **The bug** — the web form hands over the price and the tip as the texts "3.50" and "0.60"
- **The plus sign** — JavaScript glues texts together: "3.50" + "0.60" prints 3.500.60 on the receipt
- **The retrofit** — TypeScript adds type labels to the same JavaScript; the code still runs as JS
- **The catch** — with `price: number` declared, the checker flags the bad line in the editor

*Example (italic):* A customer orders a 3.50 latte and tips 0.60; the receipt says total 3.500.60 instead of 4.10, and nobody's tests covered it.

**Key point:** TypeScript is JavaScript plus optional type labels — that is gradual typing: annotate some of the code, and the checker verifies exactly the part you labeled.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the same checkout line without types (bug reaches the printed receipt) vs with a TypeScript label (bug caught in the editor before running).

- **Title (bold 15px, `#1a5276`, top center):** "One Type Label Turns a Runtime Receipt Bug into an Editor Error".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain JS"; blue `#2a78d6` rounded box at x=150 labeled "form sends \"3.50\", \"0.60\"" (12px), 3px arrow to a blue box at x=370 labeled "price + tip", 3px arrow to a red `#e74c3c` box at x=545 labeled "receipt: 3.500.60" with bold 12px red "✗ customer sees it".
- **Row 2 (y=205), label:** "TypeScript"; blue box at x=150 "declare price: number", 3px arrow to a red-outlined box at x=370 labeled "editor: string + string" with bold 12px red squiggle-style underline, 3px arrow to a green `#008300` box at x=545 labeled "fix → total 4.10" with bold 12px green "✓ never ships".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "same code, same bug — only the moment of discovery changes".
- **Caption (12px `#444`, bottom right):** "prices illustrative; 3.500.60 is real JavaScript string gluing".

## Migrating 40 Files Five at a Time

**Tags:** `worked example` (blue), `any` (orange), `strictness dial` (green)

- **The start** — rename a .js file to .ts and it compiles unchanged; unlabeled values default to `any`
- **any** — the escape-hatch type meaning "trust me"; the checker skips whatever it touches
- **The plan** — the shop converts 5 of its 40 checkout files each week, adding real labels per file
- **Hand-check** — after week 3 that is 15 typed and 25 untyped files; week 8 reaches 40 of 40
- **The dial** — strict flags like `noImplicitAny` are switched on last, once `any` is nearly gone

*Example (italic):* By week 4 the checkout is exactly half migrated — 20 typed files call 20 untyped ones, and the whole program still runs every day.

**Key point:** Gradual typing means typed and untyped code coexist in one running program — migration happens file by file, never as a big-bang rewrite.

### Visualization (canvas `c2`, 720×300)

Stacked area chart of the 8-week migration: files still untyped .js (blue, shrinking) vs converted .ts (green, growing), total constant at 40.

- **Title (bold 15px, `#1a5276`, top center):** "The 8-Week Crossover: 40 Files Migrate Five at a Time".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = weeks 0 to 8, tick labels "wk 0"–"wk 8" every 2 weeks (12px `#444`); y = files 0 to 40, gridlines `#e5e9ef` at 10/20/30.
- **.ts area (bottom):** green fill `rgba(0,131,0,0.30)` under a 2px `#008300` line through weeks `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, files `[0, 5, 10, 15, 20, 25, 30, 35, 40]`.
- **.js area (top):** blue fill `rgba(42,120,214,0.25)` between the .ts line and the constant total 40, 2px `#2a78d6` upper edge.
- **Labels:** bold 12px blue "still untyped .js" at (x≈wk 1.5, y≈95 in the upper band); bold 12px green "typed .ts" at (x≈wk 6, y≈200 in the lower band).
- **Annotation (bold 12px violet `#4a3aa7`, near x=wk 4, y=60):** "the app ships every week of the migration".
- **Caption (12px `#444`, bottom right):** "5 files/week schedule illustrative".

## The Same Dial Exists in Python

**Tags:** `where it's used` (blue), `type hints` (green), `pipelines` (orange)

- **Python too** — type hints plus a checker like mypy retrofit types onto Python the same way
- **Pipelines** — a column declared `price: float` can't silently arrive as a string of digits
- **Refactors** — rename a field and the checker lists every call site, instead of a 2am crash
- **The ceiling** — types catch shape errors, not logic errors; a wrong tip formula still checks
- **The count** — of 20 illustrative checkout bugs, strict checking catches 15 before anything runs

*Example (italic):* The shop's sales-by-hour script crashed weekly on a text price column; one hint, `price: float`, made the checker point straight at the loader.

**Key point:** A type checker is the cheapest test suite you will ever run — it executes in the editor, before the code does, and it never gets skipped under deadline pressure.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: of the same 20 checkout bugs, how many are caught before running at three strictness settings.

- **Title (bold 15px, `#1a5276`, top center):** "Same 20 Bugs, Three Strictness Settings".
- **Axis:** horizontal 2px `#999` baseline at x=230, bars extend right, max width 440 representing all 20 bugs (22px per bug).
- **Rows (top to bottom at y = 80, 145, 210), each with a left-aligned 12px `#444` label at x=20:**
  - "no checker — 0 of 20 caught": blue `#2a78d6` bar width 440 (all 20 bugs), no overlay, 12px red `#e74c3c` label "all 20 reach runtime"
  - "loose TS, any allowed — 9 of 20": blue bar width 440, overlay green `#008300` bar width 198 (9 bugs)
  - "strict mode — 15 of 20": blue bar width 440, overlay green bar width 330 (15 bugs)
- **Bar style:** 16px tall, total bars fill `rgba(42,120,214,0.30)`, caught overlay bars solid green, 11px count labels at overlay ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=255):** "the last 5 are logic bugs no type system can see".
- **Caption (12px `#444`, bottom right):** "bug counts illustrative".

## Types Vanish When the Code Runs

**Tags:** `common mistake` (red), `runtime boundary` (orange)

- **Erased** — the compiler deletes every type label; the running program is plain JavaScript
- **The trust** — an interface saying `price: number` is a promise the checker never verifies live
- **The boundary** — API responses, files, and form input arrive untyped whatever you declared
- **as** — writing `response as Order` silences the checker without checking anything at all
- **The fix** — validate at the boundary (parse, check, convert), then let types cover the inside

*Example (italic):* The payment API starts sending price as "3.50" again; the code compiled clean, and the 3.500.60 receipt bug is back — at runtime.

**Common mistake:** Believing a compiled TypeScript program cannot have type errors. Types are checked before running and erased after — anything crossing a boundary must be validated by real code.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: trusting a type label on API data (compiles, then breaks at runtime) vs validating at the boundary (converts, then safe), shown as order data flowing through the checkout.

- **Title (bold 15px, `#1a5276`, top center):** "The Boundary: Where Type Labels Stop Being Checked".
- **Row 1 (y=95), label 12px `#444` at x=20:** "trust the label"; blue `#2a78d6` rounded box at x=150 labeled "API: { price: \"3.50\" }" (12px), 3px arrow to a blue box at x=370 labeled "as Order — compiles", 3px arrow to a red `#e74c3c` box at x=545 labeled "runtime: 3.500.60" with bold 12px red "✗ bug is back".
- **Row 2 (y=205), label:** "validate first"; blue box at x=150 "API: { price: \"3.50\" }", 3px arrow to a green `#008300` box at x=370 labeled "check + Number(price)", 3px arrow to a green box at x=545 labeled "total 4.10" with bold 12px green "✓".
- **Box style:** 150–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "types guard the inside; only code can guard the door".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the receipt prices (3.50 + 0.60 = 4.10, string-glued 3.500.60), the migration schedule (40 files, 5/week over weeks `[0..8]` giving typed counts `[0,5,10,15,20,25,30,35,40]`), and the bug counts (0 / 9 / 15 of 20 caught) are invented and labeled illustrative; the string-concatenation result 3.500.60 is genuine JavaScript `+` behavior on two strings.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
