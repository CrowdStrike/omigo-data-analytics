# Duck, Structural & Nominal Typing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Duck, Structural & Nominal Typing

**Subtitle:** Three ways a language decides "is this the right type?" — trust the name on the label, check the list of parts, or just try it and see what happens

## Three Ways to Ask "Is This a Blender?"

**Tags:** `core idea` (blue), `three tests` (green), `type checks` (orange)

- **The recipe** — a smoothie recipe needs "a blender"; the kitchen must decide which gadgets qualify
- **Name test (nominal)** — only gadgets whose label literally says "Blender" count, whatever they can do
- **Checklist test (structural)** — anything with a blend, pulse, and clean-cycle button counts, name ignored
- **Try-it test (duck)** — pour the fruit in and press blend; if a smoothie comes out, it qualified
- **Same question** — all three answer "is this the right type?"; they just accept different evidence

*Example (italic):* A food processor fails the name test, passes the checklist test, and blends the smoothie perfectly — one gadget, three tests, two different verdicts.

**Key point:** Nominal trusts the label, structural trusts the parts list, duck trusts the behavior — three answers to the same "right type?" question.

### Visualization (canvas `c1`, 720×300)

Three-lane gate diagram: one gadget (a food processor) is judged by the three tests stacked as horizontal lanes, each lane showing what the test inspects and its verdict badge.

- **Title (bold 15px, `#1a5276`, top center):** "One Food Processor, Three Gates".
- **Gadget box:** rounded rect at x=20–150, y=130–190, 1.5px `#1a5276` border, fill `rgba(42,120,214,0.10)`; bold 13px `#1a5276` text "FoodProcessor" centered, 11px `#6b7280` line below it: "blend · pulse · clean-cycle".
- **Lanes (y centers 95, 165, 235):** from each lane a 2px `#6b7280` arrow leaves the gadget box (fan out from x=150) into an inspection box at x=250–520 (rounded rect, 1px `#e5e9ef` border, white fill), then a 2px arrow to a verdict badge at x=560–690.
- **Lane 1 — name test:** bold 12px blue `#2a78d6` label "NAME (nominal)" above the box; box text 12px `#2c3e50`: "label says 'Blender'? — no, it says 'FoodProcessor'"; verdict badge: rounded rect, fill `rgba(231,76,60,0.12)`, bold 13px red `#e74c3c` text "REJECT".
- **Lane 2 — checklist test:** bold 12px green `#008300` label "CHECKLIST (structural)"; box text: "has blend, pulse, clean-cycle? — yes, all 3 buttons"; verdict badge fill `rgba(0,131,0,0.12)`, bold 13px green `#008300` text "PASS".
- **Lane 3 — try-it test:** bold 12px orange `#d95926` label "TRY IT (duck)"; box text: "press blend with fruit in — a smoothie comes out"; verdict badge fill `rgba(0,131,0,0.12)`, bold 13px green "PASS".
- **Annotation (bold 12px violet `#4a3aa7`, bottom center near y=285):** "same gadget — the verdict depends on which question you ask".
- **Caption (11px `#444`, bottom right):** "illustrative gadget".

## Eight Gadgets, Three Verdicts

**Tags:** `worked example` (blue), `pass counts` (green)

- **The drawer** — eight gadgets audition for the recipe; the recipe itself only ever presses blend
- **Name test** — 2 pass: BrandBlender and KnockoffBlender, the only two whose label says "Blender"
- **Checklist test** — 3 pass: BrandBlender, FoodProcessor, LabHomogenizer carry all three buttons
- **Try-it test** — 5 pass: those 3 plus ImmersionStick and CafeStation, whose blend button works
- **The knockoff** — KnockoffBlender passes the name test yet jams the moment blend is pressed
- **No single winner** — 2, 3, and 5 are all "correct"; each test simply defines "blender" differently

*Example (italic):* The same drawer holds 2 blenders by name, 3 by checklist, and 5 by behavior — the gadgets never changed, only the definition did.

**Key point:** The pass counts 2 (nominal), 3 (structural), 5 (duck) come straight from the table — redo the three rules by hand on the eight rows.

### Visualization (canvas `c2`, 720×300)

Pass/fail matrix: eight gadget rows against three test columns, green dots for pass and red crosses for fail, with a bold totals row at the bottom reading 2 / 3 / 5.

- **Title (bold 15px, `#1a5276`, top center):** "Eight Gadgets vs Three Tests — 2, 3, and 5 Pass".
- **Column headers (bold 12px, y=68):** "name (nominal)" in blue `#2a78d6` centered at x=360, "checklist (structural)" in green `#008300` at x=505, "try it (duck)" in orange `#d95926` at x=645.
- **Rows (y = 92, 113, 134, 155, 176, 197, 218, 239), 12px `#2c3e50` labels left-aligned at x=20:** `["BrandBlender", "KnockoffBlender", "FoodProcessor", "LabHomogenizer", "ImmersionStick", "CafeStation", "HandMixer", "Toaster"]`.
- **Matrix data (1 = pass, 0 = fail), hardcoded:** nominal = `[1, 1, 0, 0, 0, 0, 0, 0]`, structural = `[1, 0, 1, 1, 0, 0, 0, 0]`, duck = `[1, 0, 1, 1, 1, 1, 0, 0]`.
- **Marks:** pass = 6px-radius filled circle green `#008300`; fail = 11px red `#e74c3c` cross (two 2px strokes) — drawn at each column's x center on the row's y.
- **Row separators:** 1px `#e5e9ef` horizontal lines between rows from x=20 to x=690; heavier 1.5px `#999` line above the totals row at y=252.
- **Totals row (y=268):** bold 13px label "pass count" at x=20 in `#1a5276`; bold 14px totals "2" (blue) at x=360, "3" (green) at x=505, "5" (orange) at x=645.
- **Annotation (bold 12px violet `#4a3aa7`, right-aligned near x=690, y=285):** "same drawer — 2, 3, or 5 blenders depending on the question".
- **Caption (11px `#444`, bottom left):** "illustrative gadgets".

## Where Your Code Meets the Three Tests

**Tags:** `where it's used` (blue), `error timing` (orange)

- **Nominal languages** — Java and C# accept a value only when its declared class or interface name matches
- **Structural languages** — TypeScript and Go accept any value whose methods match the required shape
- **Duck languages** — Python and Ruby never pre-check anything; each call is itself the test, at runtime
- **Error timing** — name and checklist mismatches fail before the program starts; duck fails mid-run
- **Data science life** — pass anything DataFrame-like to a Python function and it works, until it doesn't

*Example (italic):* A 20-minute Python pipeline ran fine for 14 minutes before one object turned out to lack `.fillna()` — a mismatch Go or Java would have flagged before the run began.

**Key point:** The stricter the check, the earlier the news: nominal and structural mismatches surface at compile time, duck typing fails wherever the bad call finally happens.

### Visualization (canvas `c3`, 720×300)

Timeline chart: three language rows over a 20-minute run clock, showing where the same type mismatch is caught — before the run for nominal and structural, at minute 14 for duck.

- **Title (bold 15px, `#1a5276`, top center):** "When the Same Mistake Is Caught (20-Minute Pipeline)".
- **Axis:** horizontal 2px `#999` line at y=250 from x=230 to x=680 (run time 0 to 20 min); 12px `#444` tick labels "0", "5", "10", "15", "20 min" every 5 minutes below the line.
- **Compile zone:** light band fill `rgba(26,82,118,0.06)` from x=160 to x=230, full plot height (y=60 to 250); vertical dashed `#6b7280` (dash 4/3) line at x=230 with 12px `#6b7280` label "run starts" above at y=55; 11px `#6b7280` label "checks before run" centered in the band near y=70.
- **Rows (y = 105, 160, 215), 12px `#2c3e50` labels left-aligned at x=20:** "nominal — Java", "structural — Go / TypeScript", "duck — Python".
- **Rows 1 and 2:** bold 14px red `#e74c3c` cross at x=195 (inside the compile zone) on the row's y; 12px red label "caught here" to the left of the crosses (shared, between the two rows near y=132); no bar to the right — the run never starts.
- **Row 3:** blue `#2a78d6` progress bar 10px tall with rounded ends from x=230 (minute 0) to x=545 (minute 14); bold 16px red `#e74c3c` cross at x=545; bold 13px red label above it: "crash at minute 14".
- **Annotation (bold 12px orange `#d95926`, near x=560, y=190):** two lines: "duck typing ships the doubt" / "into the run itself".
- **Caption (11px `#444`, bottom right):** "illustrative 20-minute run".

## Duck and Structural Are Not the Same

**Tags:** `common mistake` (red), `duck vs structural` (orange)

- **The mix-up** — both ignore names, so people treat "duck typing" and "structural typing" as synonyms
- **When it checks** — structural inspects the full checklist before running; duck checks nothing upfront
- **How much it checks** — duck only tests the buttons actually pressed; unused buttons never matter
- **ImmersionStick** — one blend button, no pulse or clean-cycle: structural rejects it, duck accepts it
- **PromoBlender** — a display model with all three buttons but no motor: structural accepts, duck rejects
- **Two-way split** — each test accepts a gadget the other refuses, so they cannot be the same rule

*Example (italic):* The store's display model passes every checklist inspection yet makes zero smoothies — only the try-it test catches a motorless machine.

**Common mistake:** Calling Python "structurally typed". Structural typing is an upfront shape check; duck typing is no check at all until the moment each method is actually called.

### Visualization (canvas `c4`, 720×300)

Two-by-two verdict grid: two gadgets (rows) against the structural and duck tests (columns), with opposite verdicts on each diagonal proving the two rules disagree in both directions.

- **Title (bold 15px, `#1a5276`, top center):** "Two Gadgets That Split the Two Tests".
- **Column headers (bold 13px, y=75):** "checklist (structural)" in green `#008300` centered at x=390, "try it (duck)" in orange `#d95926` centered at x=590.
- **Row labels (12px `#2c3e50`, left-aligned at x=20, vertically centered on each row):** row 1 (y center 135) "ImmersionStick" with 11px `#6b7280` second line "blend only"; row 2 (y center 225) "PromoBlender" with 11px `#6b7280` second line "display model, no motor".
- **Cells:** rounded rects 180×70 centered on (390, 135), (590, 135), (390, 225), (590, 225); 1px `#e5e9ef` borders.
- **Cell (ImmersionStick, structural):** fill `rgba(231,76,60,0.10)`, bold 13px red `#e74c3c` "REJECT", 11px `#444` second line "has 1 of 3 buttons".
- **Cell (ImmersionStick, duck):** fill `rgba(0,131,0,0.10)`, bold 13px green `#008300` "PASS", 11px `#444` second line "blend() works".
- **Cell (PromoBlender, structural):** fill `rgba(0,131,0,0.10)`, bold 13px green "PASS", 11px `#444` second line "3 of 3 buttons present".
- **Cell (PromoBlender, duck):** fill `rgba(231,76,60,0.10)`, bold 13px red "REJECT", 11px `#444` second line "blend() does nothing".
- **Annotation (bold 13px magenta `#d55181`, centered near y=283):** "opposite verdicts both ways — different rules, not synonyms".
- **Caption (11px `#444`, bottom right):** "illustrative gadgets".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all gadget names, pass/fail matrices, totals (2 / 3 / 5), and timeline positions are the hardcoded literal values above (no randomness); every invented number carries an "illustrative" caption; text counts must equal chart counts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
