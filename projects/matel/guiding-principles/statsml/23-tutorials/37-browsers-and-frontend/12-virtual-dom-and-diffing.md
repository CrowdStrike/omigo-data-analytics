# Virtual DOM & Diffing

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Virtual DOM & Diffing

**Subtitle:** A framework keeps a cheap in-memory copy of the page, compares the old copy to the new one after every change, and patches only what changed — never the whole screen

## One Cell Changes in a 100-Row Orders Table

**Tags:** `core idea` (blue), `patch not rebuild` (green), `orders table` (orange)

- **The screen** — a coffee shop dashboard shows a live table of 100 orders, 3 cells per row: id, item, status
- **The change** — order #47 finishes brewing; its status cell flips from "brewing" to "ready"
- **The naive way** — throw the table away and rebuild all 100 rows: 300 cells re-created for one word
- **The virtual DOM** — the framework first redraws a lightweight JavaScript copy of the table, not the real page
- **The diff** — it compares the new copy to the old copy, finds the one text that differs, and patches just that cell
- **The payoff** — real DOM work is the slow part; the diff turns 300 cell rebuilds into 1 text update

*Example (italic):* When order #47 turns "ready", the browser touches exactly one text node — the other 299 cells are never rebuilt.

**Key point:** The virtual DOM is a cheap description of the page; diffing old vs new descriptions lets the framework send the real DOM only the minimal patch — patch only what changed.

### Visualization (canvas `c1`, 720×300)

Horizontal bar chart comparing real-DOM operations for the same one-word update: full re-render vs virtual-DOM diff and patch.

- **Title (bold 15px, `#1a5276`, top center):** "Order #47 Turns Ready: 300 Cells Rebuilt vs 1 Cell Patched".
- **Layout:** left-aligned 12px `#444` row labels at x=20, bars start at x=230, max bar width 460, bars 22px tall.
- **Row 1 (y=110):** label "full re-render — 300 cells"; orange `#d95926` bar width 460, 12px `#444` value label "300 DOM ops" at the bar end.
- **Row 2 (y=190):** label "diff + patch — 1 cell"; green `#008300` bar width 2 (minimum visible sliver), 12px `#008300` value label "1 DOM op" just right of it.
- **Annotation (bold 13px green `#008300`, centered near y=250):** "same screen, 1/300th of the work — 299 cells were identical".
- **Caption (12px `#444`, bottom right):** "cell counts exact for a 100-row × 3-cell table; timings not shown".

## Diffing a Four-Row Slice by Hand

**Tags:** `worked example` (blue), `node by node` (green)

- **The slice** — zoom into rows 46–49 of the same table: 1 table node + 4 row nodes + 12 cell nodes = 17 nodes
- **The walk** — the differ pairs old and new trees top-down: table vs table, row 46 vs row 46, cell by cell
- **The matches** — 16 of the 17 pairs are identical, so the differ emits nothing for them
- **The hit** — one pair differs: row 47's status cell reads "brewing" in the old tree, "ready" in the new
- **The output** — the whole diff of this slice emits exactly 1 operation: set that cell's text to "ready"
- **Hand-check** — 17 pairs compared, 16 equal, 1 different, 1 patch op: you can redo the walk on paper

*Example (italic):* Comparing 17 node pairs by hand takes a minute and yields a one-line patch list: setText(row 47, status, "ready").

**Key point:** Diffing is just a paired walk of two trees — compare each old node with its new counterpart, keep the matches, and emit one patch per mismatch.

### Visualization (canvas `c2`, 720×300)

Two mini trees side by side (old left, new right), same 17-node shape, with the single differing cell highlighted and a patch arrow between the trees.

- **Title (bold 15px, `#1a5276`, top center):** "Old Tree vs New Tree: 17 Pairs Compared, 1 Patch Emitted".
- **Old tree (centered on x=190):** rounded box "table" (70×26, fill `rgba(42,120,214,0.15)`, 1px `#2a78d6` border, 12px `#2c3e50` text) at y=60; four boxes "tr 46".."tr 49" (56×24) evenly spaced across x=60–320 at y=130, 1px `#6b7280` connector lines from the table box; under each tr, three 16×16 squares at y=195 (12 squares, fill `rgba(42,120,214,0.15)`) with 1px connectors.
- **New tree (centered on x=530):** identical layout shifted right by 340, boxes across x=400–660.
- **The mismatch:** in both trees, the third square under "tr 47" is magenta — fill `rgba(213,81,129,0.20)`, 2px `#d55181` border; 11px `#d55181` labels "brewing" under the old square and "ready" under the new square.
- **Patch arrow:** dashed `#d55181` (dash 4/3) horizontal arrow between the two magenta squares, bold 12px `#d55181` label "1 patch: setText" above it.
- **Tally line (12px `#444`, centered at y=262):** "compared: 17 pairs   •   equal: 16   •   patch ops: 1".
- **Annotation (bold 13px violet `#4a3aa7`, top right near y=45):** "everything equal is skipped".
- **Caption (12px `#444`, bottom right):** "4-row slice of the 100-row table, illustrative".

## Why Framework Screens Stay Under the Frame Budget

**Tags:** `where it's used` (blue), `60 fps` (green), `keys` (orange)

- **The budget** — a smooth screen redraws every 16 ms (60 fps); miss it and typing or scrolling stutters
- **Cheap copies** — virtual nodes are plain JavaScript objects, so rebuilding the copy is fast; real DOM is slow
- **The scaling** — the cheap virtual diff scales with tree size; slow real-DOM patches scale with the change
- **Keys in lists** — a `key` (like the order id) tells the differ which old row matches which new row when rows move
- **The feel** — this is why a framework dashboard with 1,000 rows still updates one cell without a visible hitch

*Example (italic):* At 1,000 rows a full rebuild costs about 30 ms and drops frames, while diff + patch stays near 4 ms.

**Key point:** Diffing keeps the slow real-DOM work proportional to the change — the cheap virtual walk still grows with the page, and that split is what keeps big framework screens inside the 16 ms frame budget.

### Visualization (canvas `c3`, 720×300)

Line chart of update time vs table size: full re-render grows past the frame budget, diff + patch stays flat and low.

- **Title (bold 15px, `#1a5276`, top center):** "Update Cost as the Table Grows: Rebuild vs Diff + Patch".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = table rows 100 to 1000 with 12px `#444` tick labels at 100/250/500/750/1000; y = ms per update 0 to 32, gridlines `#e5e9ef` at 8/16/24 with 12px `#444` labels.
- **Rebuild line:** orange `#d95926` 3px line through rows `[100, 250, 500, 750, 1000]`, ms `[3, 7.5, 15, 22.5, 30]` — grows linearly with row count.
- **Diff line:** green `#008300` 3px line through the same rows, ms `[0.5, 1.1, 2.2, 3.2, 4.2]` — near flat.
- **Budget line:** horizontal dashed `#6b7280` (dash 4/3) line at y for 16 ms, 12px `#6b7280` label "16 ms frame budget (60 fps)" above its right end.
- **Annotation (bold 13px orange `#d95926`, near rows≈550, above the crossing):** "full rebuild blows the frame budget past ~530 rows".
- **Line labels:** bold 12px orange "full re-render" near the right end of the orange line; bold 12px green "diff + patch" near the right end of the green line.
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Index Keys Put Yesterday's State on Today's Row

**Tags:** `common mistake` (red), `list keys` (orange)

- **The list** — the dashboard's "in progress" panel shows five orders: #12, #13, #14, #15, #16
- **The change** — order #12 completes and drops off; the new list is #13, #14, #15, #16
- **Index keys** — keying rows by position makes the differ think slot 0 is "the same row" as before
- **The waste** — it rewrites the text of all 4 surviving slots and deletes the last: 5 ops instead of 1
- **The bug** — row-level DOM state (an expanded-details toggle open on #12) stays in slot 0 and now shows on #13
- **Stable keys** — keying by order id yields 1 delete op, 4 rows reused untouched, and the toggle stays gone

*Example (italic):* With index keys, closing out order #12 leaves its expanded-details panel sitting open on order #13.

**Common mistake:** Using the array index as the key tells the differ "position is identity" — when the list shifts, old rows are reused for the wrong items, dragging their input and toggle state along.

### Visualization (canvas `c4`, 720×300)

Two-row diagram: the same delete-first-order update diffed with index keys (4 rewrites + 1 delete, state on wrong row) vs stable id keys (1 delete, 4 reuses).

- **Title (bold 15px, `#1a5276`, top center):** "Order #12 Leaves the List: Index Keys vs Id Keys".
- **Row 1 (boxes at y=95), label 12px `#444` at x=20:** "key = index"; four rounded boxes (88×40, 8px radius) at x = 130/240/350/460, fill `rgba(217,89,38,0.15)`, 1px `#d95926` border, 12px `#2c3e50` two-line text "#13 / rewrite", "#14 / rewrite", "#15 / rewrite", "#16 / rewrite"; a fifth dashed-border `#6b7280` box at x=570 labeled "slot 4 / delete"; bold 12px `#d55181` note under the first box: "expanded flag lands on #13".
- **Row 2 (boxes at y=205), label:** "key = order id"; four boxes at the same x positions, fill `rgba(0,131,0,0.12)`, 1px `#008300` border, text "#13 / reused", "#14 / reused", "#15 / reused", "#16 / reused"; dashed `#6b7280` box at x=570 labeled "#12 / delete".
- **Op tallies (bold 12px, right edge x=690, right-aligned):** orange `#d95926` "5 ops" beside row 1, green `#008300` "1 op" beside row 2.
- **Annotation (bold 13px magenta `#d55181`, centered near y=275):** "index keys: 5 ops and state on the wrong row — id keys: 1 op".
- **Caption (12px `#444`, bottom right):** "op counts exact for this 5-row list".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the 300-vs-1 op counts follow exactly from a 100-row × 3-cell table with one text change, and the 17-pair / 16-equal / 1-patch tally from the 4-row slice; the update-time curves (rebuild ms `[3, 7.5, 15, 22.5, 30]`, diff ms `[0.5, 1.1, 2.2, 3.2, 4.2]` at rows `[100, 250, 500, 750, 1000]`) are invented and labeled illustrative; the 5-ops-vs-1-op key comparison is exact for the 5-row list shown.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
