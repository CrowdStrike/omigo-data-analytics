# LSM Trees

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** LSM Trees

**Subtitle:** When new data arrives faster than you can file it, jot everything at the end of a notepad and sort it later in batches — that trade is the whole idea of an LSM tree

## The Pizza Shop Notepad vs the Filing Cabinet

**Tags:** `core idea` (blue), `append-only` (green), `write-heavy` (orange)

- **The shop** — a pizza shop's phone rings nonstop on game night; every order must be written down instantly
- **The cabinet** — filing each order alphabetically means shuffling earlier slips to make room, every time
- **The notepad** — the cashier instead jots each order at the bottom of the pad: one motion, always the same cost
- **Sort later** — when the pad fills up, its orders get copied onto one sorted page in a single quiet pass
- **The name** — databases that work this way (append fast, merge sorted batches later) are LSM trees

*Example (italic):* Order #8 lands on the notepad in 1 step, but filing it alphabetically would mean shifting 5 slips first — the pad never makes the phone wait.

**Key point:** An LSM tree accepts writes at the cheapest possible spot — the end of a log — and pays the sorting bill later, in batches, off the critical path.

### Visualization (canvas `c1`, 720×300)

Grouped bar chart: for each of 8 arriving orders, the steps needed to record it — appending to the notepad (flat) vs inserting into an alphabetically sorted file (growing).

- **Title (bold 15px, `#1a5276`, top center):** "Recording Order #1 through #8: Append vs File-in-Place".
- **Axes:** origin x=60, baseline y=245, plot width 620, plot height 180; x axis = order number 1–8 with 12px `#444` labels "#1"…"#8" centered under each group; y axis = steps 0 to 6, light `#e5e9ef` gridlines at 1–5 with 12px `#444` labels.
- **Bars:** two bars per order, 26px wide, 6px gap between pair, groups evenly spaced; append steps (blue `#2a78d6`, fill `rgba(42,120,214,0.35)`, 2px blue border) = `[1, 1, 1, 1, 1, 1, 1, 1]`; sorted-insert steps (orange `#d95926`, fill `rgba(217,89,38,0.30)`, 2px orange border) = `[1, 1, 2, 2, 3, 4, 4, 5]`.
- **Legend (12px, top left inside plot):** blue swatch "notepad: jot at the end", orange swatch "cabinet: shift slips, insert sorted".
- **Annotation (bold 13px blue `#2a78d6`, near x=420, y=95):** two lines: "appending stays flat at 1 step —" / "sorting is postponed, not skipped".
- **Caption (12px `#444`, bottom right):** "step counts illustrative — shifts grow with slips already filed".

## One Flush and One Merge, by Hand

**Tags:** `worked example` (blue), `flush & merge` (green)

- **The pad fills** — five jotted orders, arrival order: Mia 2, Sam 1, Leo 3, Sam 2, Ana 1
- **Sam twice** — Sam called back to change 1 pizza to 2; the pad just gets a second Sam line
- **The flush** — copy the pad to a sorted page, newest line per name wins: Ana 1, Leo 3, Mia 2, Sam 2
- **An older page** — last hour's sorted page already reads: Ben 2, Mia 1, Zoe 1
- **The merge** — zip the two sorted pages into one: Ana 1, Ben 2, Leo 3, Mia 2, Sam 2, Zoe 1
- **Shrinkage** — the flush drops Sam's stale line (5 → 4); the merge feeds 4 + 3 = 7 lines, writes 6

*Example (italic):* Mia appears on both pages (2 new, 1 old) — the merge keeps the newer "Mia 2" and drops the stale line, no eraser needed.

**Key point:** Flush = sort one small batch; merge = zip two already-sorted pages front to front — both are single left-to-right passes anyone can redo by hand.

### Visualization (canvas `c2`, 720×300)

Flow diagram of four labeled boxes with the actual order lines: the unsorted notepad flushes into a sorted page, which merges with the older sorted page into one master page.

- **Title (bold 15px, `#1a5276`, top center):** "Notepad → Sorted Page → Merged Page".
- **Notepad box (x=25, y=55, w=150, h=170):** 2px blue `#2a78d6` border, fill `rgba(42,120,214,0.10)`, bold 12px blue header "notepad (arrival order)"; 12px `#2c3e50` lines: "Mia 2", "Sam 1", "Leo 3", "Sam 2", "Ana 1" — "Sam 1" struck through in `#6b7280` with an 11px `#d95926` note "updated" beside it.
- **Flushed page box (x=235, y=55, w=150, h=150):** 2px green `#008300` border, fill `rgba(0,131,0,0.08)`, bold 12px green header "flushed page (sorted)"; lines: "Ana 1", "Leo 3", "Mia 2", "Sam 2".
- **Older page box (x=235, y=215, w=150, h=70):** 2px `#6b7280` border, fill `#f8f9fa`, bold 12px `#6b7280` header "last hour's page"; one 12px line: "Ben 2 · Mia 1 · Zoe 1" — "Mia 1" in `#6b7280` strike-through.
- **Merged page box (x=475, y=75, w=190, h=170):** 3px violet `#4a3aa7` border, fill `rgba(74,58,167,0.08)`, bold 12px violet header "merged page (sorted)"; 12px lines: "Ana 1", "Ben 2", "Leo 3", "Mia 2", "Sam 2", "Zoe 1".
- **Arrows:** 3px blue arrow notepad→flushed page labeled bold 12px blue "flush: sort 5 lines"; 3px violet arrows from flushed page and older page converging into the merged box, joint label bold 12px violet "merge: 7 lines in, 6 out".
- **Annotation (bold 12px orange `#d95926`, near x=475, y=270):** "newest line per name wins".
- **Caption (12px `#444`, bottom right):** "illustrative orders — one flush, one merge".

## Why Write-Heavy Systems Choose This

**Tags:** `where it's used` (blue), `trade-off` (orange)

- **The flood** — chat messages, sensor readings, click logs: millions of tiny writes, few slow lookups
- **Write win** — the notepad design takes roughly 4x more orders per minute than file-in-place (below)
- **Read cost** — a lookup now checks the pad first, then newer pages, then older ones, newest first
- **Real homes** — this is the storage engine inside Cassandra, RocksDB, LevelDB, and HBase
- **The other camp** — read-heavy systems keep the filing cabinet (B-trees): slower writes, one-stop reads

*Example (italic):* On game night the shop logs 180 orders/min on the pad but would manage about 45/min filing each alphabetically — while a lookup now flips through 3 pages instead of 1 (illustrative).

**Key point:** LSM trees buy cheap, steady writes by making reads check a few sorted pages — the right trade exactly when writes outnumber reads.

### Visualization (canvas `c3`, 720×300)

Two-panel horizontal bar chart: left panel compares write throughput of the two designs, right panel compares how many places one lookup must check.

- **Title (bold 15px, `#1a5276`, top center):** "The Trade: Faster Writes, Busier Reads".
- **Left panel (x=60 to x=360):** bold 13px `#2c3e50` header "orders logged per minute"; two horizontal bars 26px tall at y=110 and y=170, drawn from x=110, scale 0–200 mapped to 230px; "notepad (LSM)" = 180, blue `#2a78d6` fill `rgba(42,120,214,0.35)` with bold 13px blue value label "180" at bar end; "cabinet (B-tree)" = 45, `#6b7280` fill `rgba(107,114,128,0.30)` with 13px `#6b7280` label "45"; 12px `#444` row labels left of each bar.
- **Right panel (x=400 to x=690):** bold 13px `#2c3e50` header "places checked per lookup"; same bar style, scale 0–4 mapped to 220px from x=460; "notepad (LSM)" = 3, orange `#d95926` fill `rgba(217,89,38,0.30)`, bold 13px orange label "3" with a 12px orange second line "pad + 2 pages" below it; "cabinet (B-tree)" = 1, green `#008300` fill `rgba(0,131,0,0.25)`, 13px green label "1".
- **Divider:** 1px `#e5e9ef` vertical line at x=380 from y=70 to y=250.
- **Annotation (bold 13px blue `#2a78d6`, centered near y=272):** "~4x the write speed, paid for with a 3-stop read".
- **Caption (12px `#444`, bottom right):** "throughput and stop counts illustrative".

## Deleting Without an Eraser

**Tags:** `common mistake` (red), `tombstones` (orange)

- **The cancel** — Sam calls to cancel; the cashier cannot erase Sam's line from an already-sorted page
- **The tombstone** — instead a new line goes on the pad: "Sam — CANCELLED", dated after Sam's order
- **Newest wins** — a lookup meets the cancel note first, stops, and reports "no order for Sam"
- **Still on disk** — the old "Sam 2" line sits on the older page until a later merge drops both lines
- **The mistake** — assuming a delete frees space at once; before the merge it actually adds a line

*Example (italic):* Right after the cancel the pages hold 9 lines about 8 customers — Sam's order plus Sam's tombstone — and only the next merge shrinks it to 7 lines.

**Common mistake:** Treating deletes as instant removal. In an LSM tree a delete is one more write — a tombstone — and the data truly disappears only when compaction merges past it.

### Visualization (canvas `c4`, 720×300)

Layered-pages diagram: three stacked page bars (newest on top) with a lookup arrow for "Sam" stopping at the tombstone on the top layer, the stale order visible on the layer below.

- **Title (bold 15px, `#1a5276`, top center):** "Looking Up 'Sam' After a Cancel: Newest Note Wins".
- **Layer bars (x=170, w=420, h=48) at y = 70, 135, 200, top to bottom:** each with a 12px `#444` label at x=25 — "notepad (newest)", "merged page", "oldest page".
- **Top layer:** 2px red `#e74c3c` border, fill `rgba(231,76,60,0.10)`; 12px `#2c3e50` content "Ana 1 · Sam — CANCELLED" with the Sam entry in bold 12px red.
- **Middle layer:** 2px `#6b7280` border, fill `#f8f9fa`; content "Ben 2 · Leo 3 · Mia 2 · Sam 2 · Zoe 1" with "Sam 2" in `#6b7280` strike-through.
- **Bottom layer:** 2px `#6b7280` border, fill `#f8f9fa`; content "Kim 1 · Raj 2" in 12px `#6b7280`.
- **Lookup arrow:** 3px blue `#2a78d6` arrow entering from x=640 y=45 down to the top layer, bold 12px blue label "lookup: Sam?" beside it; a red 8px X marker on the tombstone entry with bold 12px red label below the top layer: "stop here — answer: cancelled".
- **Dashed skip lines:** dashed (dash 4/3) `#6b7280` 1px arrows from the lookup down past layers 2 and 3, 11px `#6b7280` label "never read for Sam".
- **Annotation (bold 13px violet `#4a3aa7`, near x=170, y=278):** "the delete added a line; the next merge removes both Sam lines".
- **Caption (12px `#444`, bottom right):** "illustrative — tombstone shown on the newest layer".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar heights, order lines, throughput numbers, and layer contents are the hardcoded literals above (no randomness); step counts, throughput, and lookup-stop counts are invented and carry "illustrative" captions; the c2 order lines must exactly match the worked-example bullets (5 pad lines, 3 old lines, 6 merged lines).
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
