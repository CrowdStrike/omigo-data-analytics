# Prototypal Inheritance (JS)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Prototypal Inheritance (JS)

**Subtitle:** In JavaScript an object can inherit straight from another object — no class needed — and a missing property is simply looked up on the object it links to

## The Family Recipe Card

**Tags:** `core idea` (blue), `object links object` (green), `lookup chain` (orange)

- **Grandma's card** — one pancake recipe card holds every step: flour, milk, sugar, cooking time
- **Your card** — you write a new card with ONLY your changes and clip it on top of grandma's
- **The link** — your card doesn't copy her steps; it just points at her card for anything missing
- **Reading a step** — check your card first; if the step isn't there, follow the clip and read hers
- **That is a prototype** — in JS the "card underneath" is the object's prototype, set by `Object.create`
- **No class anywhere** — you inherited from a concrete card, not from a blueprint for cards

*Example (italic):* Your card says only "sugar: 40g, add chocolate: 60g" — asked for the milk amount, you flip to grandma's card and read 250ml.

**Key point:** Prototypal inheritance means an object links to another live object, and any property it lacks is looked up on that object — inherit from a thing, not a class.

### Visualization (canvas `c1`, 720×300)

Two recipe-card boxes side by side with a bold "looks up when missing" arrow from the child card to the parent card, and one dashed lookup path traced for the `milk` property.

- **Title (bold 15px, `#1a5276`, top center):** "Your Card Links to Grandma's — It Doesn't Copy It".
- **Parent card (grandma):** rounded rect x=70, y=70, w=250, h=180, 2px `#2a78d6` border, fill `rgba(42,120,214,0.08)`; bold 13px blue header "grandma's card (prototype)"; 12px `#2c3e50` lines inside: "flour: 200g", "milk: 250ml", "sugar: 25g", "cook: 3 min/side".
- **Child card (yours):** rounded rect x=430, y=90, w=220, h=120, 2px `#008300` border, fill `rgba(0,131,0,0.08)`; bold 13px green header "your card (child)"; 12px lines: "sugar: 40g", "chocolate: 60g".
- **Prototype link:** 3px `#d95926` arrow from the child card's left edge (x=430, y=150) to the parent card's right edge (x=320, y=150) with arrowhead; bold 12px orange label above the shaft: "looks up when missing".
- **Lookup trace:** dashed `#6b7280` (dash 4/3) path from a 12px `#6b7280` label "milk?" under the child card (x≈500, y=235) up into the child card, then along the arrow to the parent's "milk: 250ml" line; that line circled with a 2px `#d95926` ellipse.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=280):** "one live link, not a copy — the chain answers what the child can't".
- **Caption (12px `#444`, bottom right):** "illustrative recipe amounts".

## Looking Up Three Ingredients by Hand

**Tags:** `worked example` (blue), `Object.create` (green)

- **The base object** — `pancake = { flour: 200, milk: 250, sugar: 25 }`, amounts in g/ml
- **The child** — `choco = Object.create(pancake)`, then set `choco.sugar = 40` and `choco.chocolate = 60`
- **Lookup 1** — `choco.sugar` → found on choco itself → 40 (its own value shadows grandma's 25)
- **Lookup 2** — `choco.milk` → not on choco → follow the link to pancake → 250
- **Lookup 3** — `choco.butter` → not on choco, not on pancake, chain ends → `undefined`
- **Two objects total** — choco stores only 2 values of its own; the other 3 live on pancake

*Example (italic):* Three reads, three routes: `sugar` stops at the child (40), `milk` travels one hop (250), `butter` falls off the end (`undefined`).

**Key point:** Every property read walks the chain — own object first, then each prototype in turn — and stops at the first hit or returns `undefined` at the end.

### Visualization (canvas `c2`, 720×300)

Three-row lookup diagram: each row is one property read (`sugar`, `milk`, `butter`) moving left to right through boxes "choco" → "pancake" → "end of chain", with a colored stop marker where each lookup resolves.

- **Title (bold 15px, `#1a5276`, top center):** "Three Lookups, Three Stopping Points".
- **Column boxes:** three columns of rounded rects at x=190, x=390, x=590 (each w=140, h=44), rows at y=80, y=145, y=210; column headers bold 13px `#1a5276` at y=60: "choco (own)", "pancake (prototype)", "end of chain"; row labels 12px `#444` at x=25: "choco.sugar", "choco.milk", "choco.butter".
- **Row 1 (`sugar`):** first box fill `rgba(0,131,0,0.15)` with 2px `#008300` border and bold 13px green text "40 ✓ (shadows 25)"; remaining boxes 1px `#e5e9ef` border, 12px `#6b7280` text "not reached".
- **Row 2 (`milk`):** first box 1px `#e5e9ef` border with 12px `#6b7280` text "miss"; 2.5px `#d95926` arrow from box 1 to box 2; second box fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px blue text "250 ✓"; third box muted "not reached".
- **Row 3 (`butter`):** boxes 1 and 2 both muted "miss" with 2.5px `#d95926` arrows box 1 → box 2 → box 3; third box 2px `#d55181` border, bold 13px magenta text "undefined".
- **Annotation (bold 12px orange `#d95926`, near x=390, y=270):** "first hit wins — own value 40 hides the prototype's 25".
- **Caption (12px `#444`, bottom right):** "amounts in g/ml, illustrative".

## Why One Shared Object Beats Forty Copies

**Tags:** `where it's used` (blue), `sharing` (green), `rule of thumb` (orange)

- **The menu problem** — a coffee-shop app has 40 drink objects; all share the same 10 base fields
- **Copy world** — cloning the base into every drink stores 40 × 10 = 400 fields to keep in sync
- **Prototype world** — 40 drinks each hold 2 own overrides, plus 10 shared fields: 90 stored fields
- **One fix, everywhere** — correct a base field once on the prototype and all 40 drinks see it
- **Where you meet it** — arrays and dates get `.map` and `.getDay` from prototypes, not from copies
- **JS classes too** — `class` syntax is a coat of paint; methods still land on a shared prototype

*Example (italic):* 40 drinks × 10 copied fields = 400 stored values, versus 40 × 2 own + 10 shared = 90 — and the copy version has 40 places for a typo to hide.

**Key point:** The prototype keeps one shared copy of the common stuff, so children stay tiny and a base fix lands everywhere at once.

### Visualization (canvas `c3`, 720×300)

Two-bar comparison chart: total stored fields for the copy approach (400) versus the prototype approach (90), the prototype bar split into its own-fields and shared-fields parts.

- **Title (bold 15px, `#1a5276`, top center):** "40 Drinks, 10 Base Fields: Copies vs One Prototype".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 185; y = stored fields 0 to 400 with 12px `#444` tick labels "0", "100", "200", "300", "400" and light `#e5e9ef` gridlines; x has two category labels (13px `#2c3e50`, centered under bars): "copy into every drink", "link to one prototype".
- **Bar 1 (copies):** x center 250, width 130, height for value 400, fill `rgba(213,81,129,0.35)`, 2px `#d55181` border; bold 13px magenta value label "400 fields" above.
- **Bar 2 (prototype):** x center 500, width 130, stacked: bottom segment value 80 fill `rgba(0,131,0,0.35)` with 2px `#008300` border and 12px green side label "80 own (40 × 2)"; top segment value 10 fill `rgba(42,120,214,0.45)` with 2px `#2a78d6` border and 12px blue side label "10 shared"; bold 13px green total label "90 fields" above.
- **Annotation (bold 13px green `#008300`, near x=380, y=95):** two lines: "same menu," / "310 fewer places to update".
- **Caption (12px `#444`, bottom right):** "illustrative field counts".

## It's a Link, Not a Copy

**Tags:** `common mistake` (red), `live link` (orange)

- **The wrong picture** — people imagine `Object.create` snapshots the parent's values at birth
- **The live link** — the child holds a pointer; every read walks to the parent as it is NOW
- **Edit the parent** — change `pancake.milk` from 250 to 300 and `choco.milk` reads 300 next time
- **Edit the child** — `choco.milk = 200` adds an OWN field; grandma's card still says 300
- **The check** — `Object.hasOwn(choco, 'milk')` tells you whose card a value is really written on
- **The trap** — "fixing" a value on one child hides the shared one only for that child

*Example (italic):* Grandma updates her card's milk from 250ml to 300ml — every child card that never wrote its own milk instantly answers 300, no notification needed.

**Common mistake:** Treating inherited values as frozen copies. Reads are live lookups, and writing on the child shadows the parent instead of changing it — check `Object.hasOwn` before you trust where a value lives.

### Visualization (canvas `c4`, 720×300)

Before/after panel pair: the pancake→choco link drawn twice, left panel before the parent edit (`milk: 250`), right panel after (`milk: 300`), showing the child's answer change without the child being touched.

- **Title (bold 15px, `#1a5276`, top center):** "Edit the Prototype Once — Every Linked Child Answers Differently".
- **Panel split:** vertical 1px `#e5e9ef` divider at x=360; 13px bold `#444` panel labels at y=55: "before" (x≈180), "after grandma's edit" (x≈540).
- **Left panel:** parent box (rounded rect x=60, y=75, w=180, h=70, 2px `#2a78d6` border) with 12px text "pancake — milk: 250"; child box (x=90, y=185, w=150, h=55, 2px `#008300` border) with 12px text "choco — (no own milk)"; 2px `#6b7280` arrow child → parent; 12px `#2c3e50` readout under the child at y=265: "choco.milk → 250".
- **Right panel:** same two boxes at x=420/x=450; parent text "pancake — milk: 300" with the 300 circled by a 2px `#d95926` ellipse; child box UNCHANGED text "choco — (no own milk)"; same arrow; bold 13px orange readout at y=265: "choco.milk → 300".
- **Edit marker:** bold 12px orange `#d95926` label between panels near y=110 with a small arrow onto the right parent box: "250 → 300 edited here only".
- **Annotation (bold 13px `#e74c3c`, centered near y=290):** "the child never changed — the link did the work".
- **Caption (12px `#444`, bottom right):** "illustrative amounts in ml".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all box positions, arrows, field counts (400 / 80 / 10 / 90), and ingredient amounts (200g, 250ml, 25g, 40g, 60g, 250→300) are the hardcoded literals above (no randomness); worked-example numbers in the text match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
