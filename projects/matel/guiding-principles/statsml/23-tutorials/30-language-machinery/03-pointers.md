# Pointers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Pointers

**Subtitle:** A pointer is a variable that holds the address of another variable — a claim ticket that tells you where the coat hangs, not the coat itself

## A Ticket Is Not the Coat

**Tags:** `core idea` (blue), `address` (green), `indirection` (orange)

- **The coat check** — Maya hands over her coat; the attendant hangs it on hook 7 and gives her a ticket
- **The ticket** — the ticket says only "hook 7": it is a slip of paper holding a location, not a coat
- **The pointer** — a pointer is exactly that ticket: a variable whose value is where another variable lives
- **Copying the ticket** — photocopy Maya's ticket and both slips still say "hook 7"; there is still one coat
- **Following it** — to actually get the coat you must walk to hook 7; using a pointer means following it

*Example (italic):* Maya's ticket says "hook 7" and Leo's says "hook 3" — swap the two tickets and no coat moves an inch, only the directions to them.

**Key point:** A pointer stores a location, not a value — copying or swapping pointers rearranges directions, never the things they point to.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram: a row of eight numbered coat hooks on top, three claim tickets below, arrows from each ticket up to its hook — two tickets pointing at the same coat.

- **Title (bold 15px, `#1a5276`, top center):** "The Claim Ticket Is an Address: 'hook 7', Not a Coat".
- **Hooks:** eight hooks along y=100, centers at x = `[95, 170, 245, 320, 395, 470, 545, 620]`; each hook a 2px `#6b7280` stem with a small circle; 12px `#444` labels "hook 1" ... "hook 8" at y=68 above each.
- **Coats:** simple rounded-rect coat shapes (34×40) hanging below hooks 3 and 7 only — hook 3 coat filled blue `#2a78d6`, hook 7 coat filled green `#008300`; all other hooks empty.
- **Tickets:** three 130×46 rounded rects (fill `#f8f9fa`, 2px border) at y=225: at x=90 "Maya's ticket / hook 7" (green `#008300` border, bold 12px text), at x=295 "photocopy / hook 7" (green dashed border, 12px text), at x=500 "Leo's ticket / hook 3" (blue `#2a78d6` border, bold 12px text).
- **Arrows:** 2.5px arrows with arrowheads from ticket tops to their hooks — green from both hook-7 tickets to hook 7 (x=545), blue from Leo's ticket to hook 3 (x=245).
- **Annotation (bold 12px orange `#d95926`, near x=340, y=165):** two lines: "two tickets, one coat —" / "copying a pointer copies the address only".
- **Caption (12px `#444`, bottom right):** "illustrative — a coat check with 8 hooks".

## Six Numbered Boxes: x, p, and Box 104

**Tags:** `worked example` (blue), `memory cells` (green)

- **Memory is boxes** — picture six boxes with street numbers 100, 104, 108, 112, 116, 120
- **A plain variable** — `x` lives in box 104 and holds the number 42; ask for `x`, get 42
- **A pointer** — `p` lives in box 116 and holds 104 — the address of `x`, not 42 itself
- **Following p** — "go where p says" means: read 104 out of box 116, walk to box 104, find 42
- **Writing through p** — store 99 at the box p names, and `x` becomes 99; p itself still holds 104
- **Two ways in** — box 104 now answers 99 whether you ask via the name `x` or via the ticket `p`

*Example (italic):* Before: box 104 holds 42, box 116 holds 104. Write 99 through p and only box 104 changes — x reads 99, p still reads 104.

**Key point:** p's value is 104, an address; the 42 (later 99) lives one hop away in box 104 — the pointer never changes when the thing it points to does.

### Visualization (canvas `c2`, 720×300)

Two rows of six memory boxes (before and after writing through p), with a curved arrow from p's box to x's box in each row, showing that only box 104's content changes.

- **Title (bold 15px, `#1a5276`, top center):** "p Holds 104 — the Address of x, Not Its Value".
- **Box grid:** six boxes per row, each 88×46 with 1.5px `#6b7280` borders; box left edges at x = `[100, 196, 292, 388, 484, 580]`; row 1 boxes at y=78, row 2 at y=200; addresses "100", "104", "108", "112", "116", "120" in 12px `#6b7280` centered under each box.
- **Row labels (12px `#444`, left at x=12):** "before" beside row 1 (y=101), "after *p = 99" beside row 2 (y=223).
- **Variable names:** bold 13px `#1a5276` "x" centered above the 104 box and "p" centered above the 116 box (row 1 only, y=66).
- **Contents (bold 16px, centered):** row 1 — box 104 shows "42" in `#2c3e50`, box 116 shows "104" in blue `#2a78d6`, other boxes empty; row 2 — box 104 shows "99" in green `#008300`, box 116 shows "104" in blue, others empty.
- **Arrows:** in each row a 2.5px blue `#2a78d6` curved arrow from the top of the 116 box arcing left to the top of the 104 box, with arrowhead; 11px blue label "points to" at the arc's midpoint (row 1 only).
- **Annotation (bold 12px green `#008300`, near x=520, y=270):** two lines: "wrote through p: x is now 99," / "p itself never changed".

## Hand Over the Address, Not the Warehouse

**Tags:** `where it's used` (blue), `cheap sharing` (green), `linked structures` (orange)

- **Big data, small ticket** — 10,000 rows × 8 bytes = 80,000 bytes; its address is 8 bytes (64-bit machine)
- **Passing to a function** — hand over the pointer and the function works on the one real table
- **Shared edits** — two names holding the same address see each other's changes; that is a feature and a trap
- **Linked structures** — lists, trees, and graphs are just boxes whose contents are addresses of other boxes
- **Hidden everywhere** — Python and Java pass object references around; pointers wear a costume but never left

*Example (italic):* Copying the 10,000-row table moves 80,000 bytes; handing over its address moves 8 — same table either way, 10,000 times less carrying.

**Key point:** Pointers let programs share one big thing by passing a tiny address — the cost of handing it over stays 8 bytes no matter how large the thing grows.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart with two bars comparing bytes moved when passing a 10,000-row table by full copy versus by pointer, with the pointer bar almost invisible at the same scale.

- **Title (bold 15px, `#1a5276`, top center):** "Handing a 10,000-Row Table to a Function: Bytes That Move".
- **Axis:** horizontal 2px `#999` baseline at x=230 from y=80 to y=235; value axis runs right from x=230 to x=690 (width 460) for 0 to 80,000 bytes; 12px `#444` tick labels "0", "20k", "40k", "60k", "80k" along y=255 with light `#e5e9ef` vertical gridlines.
- **Bar 1 (y=95, height 44):** label 12px `#444` right-aligned at x=220: "copy the table"; orange `#d95926` bar from x=230 to x=690 (value 80,000); bold 13px orange value label "80,000 bytes" inside the bar's right end.
- **Bar 2 (y=175, height 44):** label "pass a pointer"; blue `#2a78d6` bar from x=230 drawn 4px wide (true width under 1px at this scale); bold 13px blue value label "8 bytes" just right of the bar.
- **Annotation (bold 13px violet `#4a3aa7`, near x=400, y=200):** two lines: "the pointer stays 8 bytes" / "no matter how big the table gets".
- **Caption (12px `#444`, bottom right):** "illustrative — 10,000 rows × 8 bytes per row".

## The Ticket That Outlived the Coat

**Tags:** `common mistake` (red), `dangling pointer` (orange)

- **Printing the ticket** — printing p shows 104, an address; beginners expect 42 and think the pointer is broken
- **One hop matters** — p is the box's number, "follow p" is the box's contents; mixing the two is the classic slip
- **The coat leaves** — box 104 gets reclaimed when x's scope ends, but p still cheerfully holds 104
- **Dangling** — following that stale address reads garbage or crashes: a ticket to a hook that was cleared
- **Null as honesty** — setting p to null after the box dies makes "there is no coat" checkable instead of a trap

*Example (italic):* The attendant clears hook 7 at closing time, but Maya's ticket still says "hook 7" — the ticket looks fine right up until she follows it.

**Common mistake:** Treating the pointer as proof the thing still exists. p holding 104 only means it holds 104 — check for null, and null it out when box 104 is gone.

### Visualization (canvas `c4`, 720×300)

Single row of six memory boxes where box 104 has been reclaimed (grayed out, contents unknown) while p still holds 104, its arrow now dashed red into the dead box.

- **Title (bold 15px, `#1a5276`, top center):** "Dangling: p Still Says 104, but Box 104 Is Gone".
- **Box grid:** same six-box layout as `c2`, one row at y=130; box left edges at x = `[100, 196, 292, 388, 484, 580]`, boxes 88×46; addresses "100"–"120" in 12px `#6b7280` below.
- **Dead box:** the 104 box filled `#eceff3` with a 1.5px dashed `#6b7280` border, bold 16px `#6b7280` "?" centered inside; 11px `#6b7280` label "reclaimed" under its address; faded 12px `#6b7280` strikethrough "x" above it.
- **Live pointer:** box 116 normal border, bold 16px blue `#2a78d6` "104" inside, bold 13px `#1a5276` "p" above.
- **Arrow:** 2.5px dashed (dash 6/4) red `#e74c3c` curved arrow from the top of box 116 arcing to the top of dead box 104, with red arrowhead.
- **Annotation (bold 13px red `#e74c3c`, near x=170, y=245):** two lines: "the ticket outlived the coat —" / "following it now is undefined behavior".
- **Caption (12px `#444`, bottom right):** "illustrative — addresses 100–120, 4 bytes apart".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red is used only in `c4` for the genuine error state (dangling pointer).
- **Data:** all box addresses, contents, hook numbers, ticket labels, bar values, and pixel positions are the hardcoded literals above (no randomness); the 80,000-vs-8-bytes comparison assumes 10,000 rows × 8 bytes and is labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
