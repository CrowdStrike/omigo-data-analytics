# Garbage Collection

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Garbage Collection

**Subtitle:** When no ticket in anyone's hand can reach a stored coat, that coat can never be claimed again — garbage collection finds such items and frees the space automatically

## The Coat No One Can Ever Claim

**Tags:** `core idea` (blue), `reachability` (green), `automatic cleanup` (orange)

- **The coat check** — a theater cloakroom has 10 hooks; every stored coat is claimed with a numbered ticket
- **Lost tickets** — patrons leave and toss tickets; a coat whose ticket no one holds can never be claimed
- **Garbage** — that unclaimed coat is garbage: not worthless, just unreachable — no path leads back to it
- **The sweep** — at intermission the attendant checks which tickets are still in hands and clears the rest
- **In a program** — objects are the coats, variables are the tickets: GC frees objects no variable can reach

*Example (italic):* Hooks 4, 5, and 8 still hold coats, but their tickets went out with the trash an hour ago — no one on Earth can retrieve those coats, so the hooks may as well be emptied.

**Key point:** Memory is garbage the moment nothing points to it — unreachable means unusable, so freeing it is always safe.

### Visualization (canvas `c1`, 720×300)

Single-panel diagram: three claim tickets in patrons' hands on top, a row of ten numbered hooks below, arrows from each ticket down to its hook — three coats reachable, three coats stranded, four hooks empty.

- **Title (bold 15px, `#1a5276`, top center):** "Ten Hooks, Three Tickets Left in Hands — Which Coats Are Garbage?".
- **Hooks:** ten 52×60 rounded rects along y=150, left edges at x = `[70, 128, 186, 244, 302, 360, 418, 476, 534, 592]`; 12px `#444` labels "1" ... "10" centered below each at y=232.
- **Coats:** hooks 2, 4, 5, 7, 8, 9 hold a simple coat shape (30×36) inside the rect; hooks 1, 3, 6, 10 are empty (fill `#f8f9fa`).
- **Coat colors:** reachable coats on hooks 2, 7, 9 filled green `#008300`; stranded coats on hooks 4, 5, 8 filled orange `#d95926`.
- **Tickets:** three 86×34 rounded rects (fill `#f8f9fa`, 2px green `#008300` border, bold 12px `#1a5276` text) at y=52, centered above hooks 2, 7, 9 (centers x=96, 444, 560), labeled "ticket 2", "ticket 7", "ticket 9"; 12px `#6b7280` heading "tickets still in hands" at x=250, y=40.
- **Arrows:** 2.5px green `#008300` arrows with arrowheads from each ticket bottom (y=86) to its hook top (y=150).
- **Annotation (bold 12px orange `#d95926`, near x=250, y=120):** two lines: "coats 4, 5, 8: no ticket anywhere —" / "unreachable, safe to clear".
- **Caption (12px `#444`, bottom right):** "illustrative — a cloakroom with 10 hooks".

## The Intermission Sweep, Step by Step

**Tags:** `worked example` (blue), `mark and sweep` (green)

- **Setup** — 8 hooks, all holding coats; patrons hold tickets to hooks 3 and 6 only (the "roots")
- **Pocket tickets** — the coat on hook 6 has tickets for hooks 2 and 8 in its pocket: coats can point to coats
- **Mark** — start from hand tickets and mark 3 and 6; open marked pockets and mark 2 and 8 — four reachable
- **Sweep** — every unmarked hook is cleared: coats on 1, 4, 5, 7 go; 4 of the 8 hooks come back
- **Redo it by hand** — any coat you can reach ticket-by-ticket survives; everything else is freed

*Example (italic):* Hook 8's ticket is in no one's hand, but it sits in the pocket of reachable coat 6 — so coat 8 is reachable in two hops and survives the sweep.

**Key point:** Mark-and-sweep = follow every chain of tickets from what is in hand, mark what you touch, free the rest — here 4 marked, 4 freed.

### Visualization (canvas `c2`, 720×300)

Single-panel diagram: two root tickets on top, eight full hooks in a row, straight arrows from roots to hooks 3 and 6, curved arrows from hook 6 down and across to hooks 2 and 8 — marked coats green, swept coats orange with an X.

- **Title (bold 15px, `#1a5276`, top center):** "Mark from the Tickets in Hand, Then Sweep the Rest".
- **Hooks:** eight 60×64 rounded rects along y=140, left edges at x = `[80, 152, 224, 296, 368, 440, 512, 584]`; 12px `#444` labels "1" ... "8" centered below each at y=226; every hook holds a coat shape (34×40).
- **Root tickets:** two 90×34 rounded rects (fill `#f8f9fa`, 2px green `#008300` border, bold 12px `#1a5276` text) at y=50, centered above hooks 3 and 6 (centers x=254, 470), labeled "ticket 3", "ticket 6"; 12px `#6b7280` heading "in hands (roots)" at x=330, y=38.
- **Root arrows:** 2.5px green `#008300` arrows with arrowheads from each ticket bottom (y=84) to its hook top (y=140).
- **Pocket arrows:** two 2.5px dashed (dash 6/4) aqua `#199e70` quadratic arcs below the row, from hook 6 bottom-center (x=470, y=204) to hook 2 bottom-center (x=182, y=204) and to hook 8 bottom-center (x=614, y=204), control points dipping to y=262; 11px aqua label "tickets found in coat 6's pocket" at x=290, y=272.
- **Coat colors:** marked coats on hooks 2, 3, 6, 8 filled green `#008300`; swept coats on hooks 1, 4, 5, 7 filled orange `#d95926` with a bold 13px `#e74c3c` "X" drawn over each.
- **Annotation (bold 12px `#008300`, near x=90, y=110):** two lines: "mark: 3, 6, then 2, 8 via pockets" / "sweep: 1, 4, 5, 7 freed".
- **Caption (12px `#444`, bottom right):** "illustrative — 4 marked, 4 freed".

## Why Your Program Doesn't Fill Up and Crash

**Tags:** `where it's used` (blue), `memory over time` (green), `gc pause` (orange)

- **Constant churn** — a running program creates millions of short-lived objects: strings, lists, temp results
- **Without GC** — in C the programmer must free each one by hand; forget, and memory climbs to a crash
- **With GC** — Python, Java, and JavaScript sweep automatically: memory rises, a sweep runs, memory drops
- **The sawtooth** — usage climbs between sweeps and falls at each one; the jagged line is a healthy heartbeat
- **The cost** — each sweep briefly pauses the program (a "GC pause"), which is why cleanups run in batches

*Example (italic):* A small web server climbs from 40 MB to 100 MB, a sweep drops it back to 42 MB, and the pattern repeats — the sawtooth, not a flat line, is what normal looks like.

**Key point:** GC trades a little pause time for never having to free memory by hand — rising-then-dropping memory is the system working, not failing.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: memory used (MB) over 12 seconds of a healthy program, a sawtooth that climbs to the sweep threshold and drops back down three times.

- **Title (bold 15px, `#1a5276`, top center):** "A Healthy Heap: the GC Sawtooth".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = seconds 0 to 12 with 12px `#444` tick labels "0s", "2s", ..., "12s" every 2; y = MB 0 to 120 with 12px `#444` labels "0", "40", "80", "120" and light `#e5e9ef` gridlines at 40, 80, 120.
- **Memory line:** blue `#2a78d6` 3px line through hardcoded points (seconds, MB) = `[[0, 40], [1, 55], [2, 70], [3, 85], [4, 100], [4.05, 42], [5, 56], [6, 71], [7, 86], [8, 100], [8.05, 44], [9, 59], [10, 73], [11, 88], [12, 100]]`; fill under the line `rgba(42,120,214,0.15)`.
- **Threshold line:** horizontal dashed `#6b7280` (dash 4/3) line at 100 MB across the plot; 12px `#6b7280` label "sweep triggers near 100 MB" above its left end.
- **Drop markers:** green `#008300` 6px dots at the two drop bottoms (4.05 s, 42 MB) and (8.05 s, 44 MB); 12px green labels "42" and "44" beside them.
- **Annotation (bold 13px green `#008300`, near x=330, y=95):** two lines: "each cliff = one garbage collection" / "the floor stays low — no leak".
- **Caption (12px `#444`, bottom right):** "illustrative — MB values are invented".

## Reachable Is Not the Same as Needed

**Tags:** `common mistake` (red), `memory leak` (orange)

- **The trap** — GC frees only the unreachable; a coat you will never wear but still hold a ticket for stays
- **In code** — objects parked in a global list, cache, or map stay reachable forever, so GC never frees them
- **GC-language leaks** — Java and Python programs still leak this way: forgotten references, not lost frees
- **The tell** — a sawtooth whose valleys keep rising — 42 MB, then 55, then 68 — each sweep frees less
- **The fix** — drop the reference (clear the cache, null the field); the very next sweep does the rest

*Example (italic):* A lookup cache that is never trimmed holds a ticket to every result it ever computed, so after each sweep the floor rises: 42 MB, then 55, then 68.

**Common mistake:** Expecting GC to free memory you no longer need. It frees only what you can no longer reach — and one forgotten cache keeps everything it holds reachable.

### Visualization (canvas `c4`, 720×300)

Single-panel line chart: the leaking sawtooth — memory still drops at every sweep, but each valley lands higher than the last, with a dashed trend line through the rising floor.

- **Title (bold 15px, `#1a5276`, top center):** "The Leak GC Cannot Fix: a Rising Floor".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = seconds 0 to 12 with 12px `#444` tick labels "0s", "2s", ..., "12s" every 2; y = MB 0 to 120 with 12px `#444` labels "0", "40", "80", "120" and light `#e5e9ef` gridlines at 40, 80, 120.
- **Memory line:** orange `#d95926` 3px line through hardcoded points (seconds, MB) = `[[0, 40], [1, 60], [2, 80], [3, 100], [3.05, 42], [4, 58], [5, 73], [6, 88], [7, 100], [7.05, 55], [8, 70], [9, 85], [10, 100], [10.05, 68], [11, 84], [12, 100]]`; fill under the line `rgba(217,89,38,0.12)`.
- **Valley dots:** red `#e74c3c` 6px dots at the three drop bottoms (3.05 s, 42), (7.05 s, 55), (10.05 s, 68); bold 12px `#e74c3c` labels "42", "55", "68" below each dot.
- **Floor trend:** dashed red `#e74c3c` (dash 4/3) 2px line through the three valley dots, extended slightly right; 12px `#e74c3c` label "the floor keeps rising" at its right end.
- **Threshold line:** horizontal dashed `#6b7280` (dash 4/3) line at 100 MB; 12px `#6b7280` label "sweep threshold" above its left end.
- **Annotation (bold 13px `#e74c3c`, near x=120, y=90):** two lines: "sweeps still run, but a cache" / "keeps old objects reachable".
- **Caption (12px `#444`, bottom right):** "illustrative — MB values are invented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all hook/ticket layouts and both time-series arrays are the hardcoded literals above (no randomness); c3 and c4 memory numbers are invented and labeled illustrative; the text's numbers (10 hooks, 3 tickets; 4 marked / 4 freed; 40→100→42; valleys 42, 55, 68) must match the charts exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
