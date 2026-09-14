# Go's Functional Options

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Go's Functional Options

**Subtitle:** Instead of a constructor with a dozen parameters, Go passes small functions that each tweak one setting — the builder pattern rebuilt from closures

## Ordering Coffee Without a Twelve-Argument Function

**Tags:** `core idea` (blue), `closures` (green), `Go` (orange)

- **The menu** — a coffee shop's ordering code has ten knobs: size, milk, shots, syrup, decaf, to-go…
- **The old way** — one constructor taking all ten arguments; callers pass eight they don't care about
- **The option** — an `Option` is just a function that takes the order and changes one field
- **The constructor** — `NewCoffee(opts ...Option)` builds a default order, then runs each option on it
- **The call** — `NewCoffee(OatMilk, Shots(2))` reads like the customer actually talking

*Example (italic):* A regular latte is `NewCoffee()` — zero arguments — because every knob already has a sensible default.

**Key point:** A functional option is a closure that edits one setting; the constructor applies defaults, then each option in order — no giant parameter list, no half-built object.

### Visualization (canvas `c1`, 720×300)

Two-row diagram comparing the same order written as a positional constructor call vs a functional-options call, each in a rounded code box with a verdict label.

- **Title (bold 15px, `#1a5276`, top center):** "The Same Latte, Two Ways to Ask for It".
- **Row 1 (y=95), label 12px `#444` at x=20:** "positional"; wide red-tinted box (fill `rgba(231,76,60,0.12)`, 1.5px `#e74c3c` border, 8px radius, x=110, width 500, height 44) with 13px monospace `#2c3e50` text `NewCoffee("med","oat",2,false,"",true,0,nil)`; bold 12px red `#e74c3c` label under it: "✗ which argument was decaf?".
- **Row 2 (y=205), label:** "functional options"; green-tinted box (fill `rgba(0,131,0,0.12)`, 1.5px `#008300` border, x=110, width 340, height 44) with 13px monospace text `NewCoffee(OatMilk, Shots(2))`; bold 12px green `#008300` label under it: "✓ unset knobs keep their defaults".
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "each option names exactly the one thing it changes".
- **Caption (12px `#444`, bottom right):** "code schematic, illustrative".

## Folding Three Options Into a $5.85 Latte

**Tags:** `worked example` (blue), `defaults first` (green), `left to right` (orange)

- **The defaults** — a plain latte starts at medium, whole milk, one shot: $4.00 on the register
- **Option one** — `OatMilk` runs first and adds $0.60: the order now reads $4.60
- **Option two** — `Shots(2)` adds one extra shot at $0.75: the order reads $5.35
- **Option three** — `Large` upsizes for $0.50: the final coffee costs $5.85
- **Hand-check** — 4.00 + 0.60 + 0.75 + 0.50 = 5.85; each closure touched exactly one field

*Example (italic):* The barista's ticket shows the same fold: base $4.00, then three one-line edits, total $5.85.

**Key point:** The constructor is a fold — start from the default struct, apply each option function left to right; the finished object is the sum of small edits.

### Visualization (canvas `c2`, 720×300)

Waterfall chart of the price as each option is applied: a full base bar, three floating increment bars, and a full total bar.

- **Title (bold 15px, `#1a5276`, top center):** "NewCoffee(OatMilk, Shots(2), Large): a Fold From $4.00 to $5.85".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = dollars 0 to 6, gridlines `#e5e9ef` at $1.50/$3.00/$4.50 with 12px `#444` tick labels; x = five slots labeled 12px `#444`: "defaults", "OatMilk", "Shots(2)", "Large", "total".
- **Bars (each 72px wide, value scale 30px per dollar):** "defaults" solid blue `#2a78d6` from 0 to 4.00; "OatMilk" floating green `#008300` from 4.00 to 4.60; "Shots(2)" floating aqua `#199e70` from 4.60 to 5.35; "Large" floating violet `#4a3aa7` from 5.35 to 5.85; "total" solid ink `#1a5276` from 0 to 5.85.
- **Value labels (bold 12px, bar color, above each bar):** "$4.00", "+$0.60", "+$0.75", "+$0.50", "$5.85".
- **Connectors:** dashed 1px `#6b7280` horizontal lines linking each bar's top to the next bar's base.
- **Annotation (bold 13px green `#008300`, upper left area near y=70):** "each option edits one field — the object is the fold".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## One Constructor Instead of Thirty-Two

**Tags:** `where it's used` (blue), `API design` (green), `backward compatible` (orange)

- **Telescoping pain** — covering every combination of n optional knobs needs up to 2^n constructors
- **The count** — 5 optional settings means 32 constructor variants; functional options need exactly 1
- **Real APIs** — Go client libraries for RPC, HTTP, and logging all take a trailing `opts ...Option`
- **Data science** — training-job and database-client configs with 20 knobs are exactly this shape
- **No breakage** — adding option #6 later breaks zero existing calls; a new positional arg breaks all

*Example (italic):* A database client adds a `WithRetries` option in v1.4 — every v1.3 call site compiles unchanged.

**Key point:** Functional options keep one constructor forever: new settings arrive as new functions, so the API grows without breaking a single caller.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: constructor variants needed to cover n optional settings, telescoping constructors (2^n) vs functional options (always 1).

- **Title (bold 15px, `#1a5276`, top center):** "Constructors Needed to Cover n Optional Settings".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = groups at 1–5 optional settings, 12px `#444` labels "1".."5" under each group plus axis label "optional settings"; y = variant count 0 to 32, gridlines `#e5e9ef` at 8/16/24 with 12px `#444` tick labels.
- **Telescoping bars (orange `#d95926`, 34px wide):** counts `[2, 4, 8, 16, 32]`, pixel heights `[11, 22, 45, 90, 180]` (scale 180px = 32); bold 12px orange count label above each bar.
- **Options bars (green `#008300`, 34px wide, right of each orange bar with 6px gap):** counts `[1, 1, 1, 1, 1]`, pixel height 6 each; one bold 12px green label "always 1" above the rightmost green bar.
- **Legend (12px, top left inside plot):** orange swatch "telescoping constructors", green swatch "functional options".
- **Annotation (bold 13px magenta `#d55181`, near x=290, y=55):** "one constructor, no matter how many knobs".
- **Caption (12px `#444`, bottom right):** "2^n combination counts exact".

## Options Don't Commute: Discount Before or After the Shot?

**Tags:** `common mistake` (red), `order matters` (orange)

- **Not commutative** — options run left to right; two options touching the same math disagree by order
- **The setup** — base latte $4.00; `ExtraShot` adds $0.75; `Discount20` multiplies the total by 0.8
- **Order A** — shot then discount: (4.00 + 0.75) × 0.8 = $3.80
- **Order B** — discount then shot: 4.00 × 0.8 + 0.75 = $3.95
- **Last one wins** — passing `Large` twice is harmless, but `Small, Large` quietly ends up large

*Example (italic):* Two tickets list the same two options in opposite order and ring up 15 cents apart — $3.80 vs $3.95.

**Common mistake:** Assuming options commute. They are closures applied in sequence; when two touch the same field or the same total, argument order is part of the result.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: the same two options applied in opposite orders, shown as price boxes flowing left to right to different totals.

- **Title (bold 15px, `#1a5276`, top center):** "Same Options, Different Order: $3.80 vs $3.95".
- **Row 1 (y=95), label 12px `#444` at x=20:** "shot, then discount"; blue `#2a78d6` rounded box at x=170 labeled "base $4.00" (12px), 3px arrow to an aqua `#199e70` box at x=330 labeled "ExtraShot → $4.75", 3px arrow to a green `#008300` box at x=510 labeled "Discount20 → $3.80".
- **Row 2 (y=205), label:** "discount, then shot"; blue box at x=170 "base $4.00", arrow to a green box at x=330 labeled "Discount20 → $3.20", arrow to an orange `#d95926` box at x=510 labeled "ExtraShot → $3.95" with bold 12px orange "15¢ apart" beside it.
- **Box style:** 130–150px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(25,158,112,0.12)` / `rgba(0,131,0,0.12)` / `rgba(217,89,38,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "options are a sequence, not a set — order is part of the call".
- **Caption (12px `#444`, bottom right):** "prices illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); coffee prices ($4.00 base, +$0.60 oat milk, +$0.75 extra shot, +$0.50 large, $5.85 total; order-dependence pair $3.80 vs $3.95) are invented and labeled illustrative; constructor variant counts (2 / 4 / 8 / 16 / 32) are the exact 2^n combination counts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
