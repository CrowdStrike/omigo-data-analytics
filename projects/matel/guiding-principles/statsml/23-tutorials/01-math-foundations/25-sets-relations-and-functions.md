# Sets, Relations & Functions

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Sets, Relations & Functions

**Subtitle:** A set is a bag of distinct things, a relation is any list of pairs between two bags, and a function is a relation with one promise: every input gets exactly one answer

## One Coffee Shop, Two Bags of Things

**Tags:** `core idea` (blue), `vocabulary` (green)

- **The shop** — a coffee shop tracks its regulars: Ana, Ben, Cara, Dev, Eli — that list is a set
- **Set** — a bag of distinct things; no duplicates, no order, only "in or out" matters
- **Second set** — the menu is another set: latte, mocha, tea, drip
- **Membership** — "Ana ∈ Customers" just says Ana is in the bag; ∈ reads "is a member of"
- **Everywhere** — a column's distinct values, a table's ids, a menu — all of them are sets

*Example (italic):* Running SELECT DISTINCT on the drink column turns a messy order log into the clean four-item menu set.

**Key point:** A set answers exactly one question — is this thing in or out? It has no counts, no order, and no repeats. Everything below is built from these bags.

### Visualization (canvas `c1`, 720×300)

Two set "bags" drawn as rectangles — the Customers set (left) and the Drinks set (right) — each filled with white member pills, with a membership annotation in the middle gap.

- **Title (bold 15px, `#1a5276`, top center):** "Two Sets: the Regulars and the Menu".
- **Data:** customers `['Ana', 'Ben', 'Cara', 'Dev', 'Eli']`; drinks `['latte', 'mocha', 'tea', 'drip']`.
- **Customers box:** rect at x=70, y=46, 240×218; 2px blue `#2a78d6` stroke, fill `rgba(42,120,214,0.08)`; heading bold 13px blue "Customers = { ... }" centered at box top + 22; five white pills (120×24, blue outline, via a rounded-`pill()` helper) at x=box+60, y starting box+36 stepping 35, each with the customer name 13px `#2c3e50` centered.
- **Drinks box:** rect at x=410, y=46, 240×218; 2px green `#008300` stroke, fill `rgba(0,131,0,0.07)`; heading bold 13px green "Drinks = { ... }"; four white pills (120×24, green outline) at x=box+60, y starting box+44 stepping 40, drink names 13px `#2c3e50`.
- **Middle annotations (centered at x=360):** violet `#4a3aa7` bold 13px "Ana ∈ Customers" at y=120; mute `#6b7280` 12px '"is a member of"' at y=138; orange `#d95926` bold 12px two lines "no order," (y=180) / "no duplicates" (y=196).
- **Caption (12px `#6b7280`, bottom center):** "a set only knows who is in the bag — nothing else".

## The Order Log Is a Relation

**Tags:** `worked example` (blue), `many-to-many` (orange)

- **The log** — this week's orders are pairs: (Ana, latte), (Ana, tea), (Ben, mocha), and so on
- **Relation** — a relation is nothing more than a set of such pairs between two sets
- **Any pattern** — Ana appears in two pairs, drip in only one; relations put no limits on repeats
- **Many-to-many** — one customer with many drinks and one drink with many customers is allowed
- **In tables** — a two-column table of distinct pairs is a relation; hence "relational" databases

*Example (italic):* The full week: Ana–latte, Ana–tea, Ben–mocha, Cara–latte, Dev–drip, Dev–mocha, Eli–tea — seven pairs, one relation.

**Key point:** If you can write it down as a table of pairs, it is a relation. No rule about how the pairs connect is required — any wiring between the two sets counts.

### Visualization (canvas `c2`, 720×300)

Bipartite arrow diagram: five customer dots on the left, four drink dots on the right, seven colored arrows drawing the week's order-log pairs.

- **Title (bold 15px, `#1a5276`, top center):** "This Week's Order Log: Seven Pairs, One Relation".
- **Data:** customers `['Ana', 'Ben', 'Cara', 'Dev', 'Eli']` at y `[70, 115, 160, 205, 250]`, dots at x=200; drinks `['latte', 'mocha', 'tea', 'drip']` at y `[80, 135, 190, 245]`, dots at x=470.
- **Pairs (customer index, drink index):** `[[0,0],[0,2],[1,1],[2,0],[3,3],[3,1],[4,2]]` — i.e., Ana→latte, Ana→tea, Ben→mocha, Cara→latte, Dev→drip, Dev→mocha, Eli→tea.
- **Arrow colors (per pair):** `[blue #2a78d6, blue #2a78d6, orange #d95926, aqua #199e70, violet #4a3aa7, violet #4a3aa7, magenta #d55181]`; 2px lines with filled 9px arrowheads (shared `arrow()` helper), drawn from x=lx+12 to x=rx−14.
- **Node styling:** customer names 13px `#2c3e50` right-aligned at lx−8 with 5px ink `#1a5276` dots; drink names 13px `#2c3e50` left-aligned at rx+10 with 5px green `#008300` dots.
- **Annotations:** blue bold 12px "Ana: two arrows out" left-aligned at (46, 52); green bold 12px "latte: two arrows in" right-aligned at (690, 60).
- **Takeaway (bold 13px orange `#d95926`, bottom center):** "any wiring is allowed — this one is many-to-many".

## Favorite Drink: The Function Rule

**Tags:** `the function rule` (blue), `where it's used` (green)

- **The survey** — each regular names one favorite: Ana→latte, Ben→mocha, Cara→latte, Dev→drip, Eli→tea
- **Exactly one** — every input gets exactly one arrow out; that single promise makes a function
- **Broken** — if Ana names both latte and tea, the mapping is a relation but not a function
- **Sharing is fine** — Ana and Cara both pick latte; outputs may repeat, inputs may not
- **Familiar forms** — a dictionary, a lookup table, and a dataframe column are all functions

*Example (italic):* favorite(Dev) = drip — one question, one guaranteed answer; "f maps customers to drinks" means exactly this arrow picture.

**Key point:** Function = relation + a promise: every input appears in exactly one pair. That promise is why a dictionary lookup never returns two conflicting values.

### Visualization (canvas `c3`, 720×300)

Dual-panel bipartite diagram: the favorite-drink mapping as a valid function (left) vs the same mapping plus one extra Ana→tea arrow, breaking the rule (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "The Function Rule: Exactly One Arrow Out of Every Input".
- **Data (both panels):** customers `['Ana', 'Ben', 'Cara', 'Dev', 'Eli']` at y `[78, 118, 158, 198, 238]`; drinks `['latte', 'mocha', 'tea', 'drip']` at y `[88, 132, 176, 220]`; favorites (drink index per customer) `[0, 1, 0, 3, 2]` — Ana→latte, Ben→mocha, Cara→latte, Dev→drip, Eli→tea.
- **Panel helper:** `panel(lx, rx, extraAnaTea, arrowColor)` draws five 2px arrows from lx+10 to rx−12; customer labels 12px `#2c3e50` right-aligned with 4px ink `#1a5276` dots; drink labels 12px left-aligned with 4px green `#008300` dots.
- **Left panel (function):** `panel(110, 255, false, green #008300)`; heading green bold 13px "favorite drink: a function ✓" centered at (180, 52); caption mute `#6b7280` 12px "sharing outputs is fine (Ana, Cara → latte)" centered at (180, h−12).
- **Right panel (not a function):** `panel(470, 615, true, mute #6b7280)` — the five arrows in mute gray plus one extra 2.5px red `#e74c3c` arrow Ana→tea; heading red bold 13px "Ana names latte AND tea ✗" centered at (540, 52); caption red bold 12px "two arrows from one input: not a function" centered at (540, h−12).
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−30.

## One-to-One vs Onto: When You Can Go Backwards

**Tags:** `common mistake` (red), `one-to-one` (orange)

- **Card ids** — loyalty cards 101–105 give each customer their own id, never shared: one-to-one
- **Onto** — every output gets hit: all 5 card ids are used, every drink is someone's favorite
- **Reversible** — one-to-one plus onto is a bijection: card 104 identifies Dev and nobody else
- **Many-to-one** — favorites cannot run backwards: "who loves latte?" returns both Ana and Cara
- **The trap** — joining tables on a key you assumed one-to-one silently duplicates rows
- **Check first** — before any join, ask: is this key one-to-one, many-to-one, or many-to-many?

*Example (italic):* Card 103 → Cara works in both directions; latte → ? has two answers — that asymmetry is the whole distinction.

**Common mistake:** Assuming every function is reversible. Only a one-to-one and onto map (a bijection) has a full inverse; a many-to-one lookup loses information, and joins built on it inflate row counts.

### Visualization (canvas `c4`, 720×300)

Dual-panel bipartite diagram: the loyalty-card mapping as a one-to-one and onto function — a bijection (left) vs the favorite-drink mapping as onto but not one-to-one, with the latte collision highlighted (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One-to-One Runs Backwards, Many-to-One Does Not".
- **Left panel (card ids, bijection):** customers `['Ana', 'Ben', 'Cara', 'Dev', 'Eli']` at y `[78, 118, 158, 198, 238]` map straight across to ids `['101', '102', '103', '104', '105']` at the same y; five horizontal green `#008300` 2px arrows from lx=110+10 to rx=255−12; customer labels 12px `#2c3e50` right-aligned with 4px ink dots, id labels left-aligned with 4px green dots; heading green bold 13px "card id: one-to-one + onto" centered at (180, 52); caption green bold 12px "a bijection — reversible: 104 → Dev" centered at (180, h−12).
- **Right panel (favorites, onto but not one-to-one):** same customers at lx2=470, drinks `['latte', 'mocha', 'tea', 'drip']` at y `[88, 132, 176, 220]` at rx2=615; favorites `[0, 1, 0, 3, 2]`; the two arrows into latte (Ana, Cara) drawn magenta `#d55181`, the other three mute `#6b7280`, all 2px; "latte" label and its dot in bold magenta, the other drink labels 12px `#2c3e50` with green dots; heading magenta bold 13px "favorite: onto, not one-to-one" centered at (540, 52); caption magenta bold 12px "latte → Ana or Cara? no inverse" centered at (540, h−12).
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h−30.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared helpers: `pill(ctx,x,y,w,h,fill,stroke)` draws a rounded pill shape; `arrow(ctx,x1,y1,x2,y2,color,lw)` draws a line with a filled 9px arrowhead. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
