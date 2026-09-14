# RAII & Smart Pointers

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** RAII & Smart Pointers

**Subtitle:** Tie a resource to an owning object — acquired at check-in, given back at checkout — so the room is freed the moment its owner's life ends, down every exit path

## The Key Card Comes Back at Checkout

**Tags:** `core idea` (blue), `ownership` (green), `scope` (orange)

- **Check-in** — a guest arrives and the front desk hands over the one key card to room 214
- **The owner** — the card lives in the guest's pocket: whoever holds the card holds the room
- **Checkout** — when the guest leaves for good, the desk takes the card back and frees room 214
- **No forgetting** — the desk collects the card at every kind of exit: checkout, fire alarm, eviction
- **RAII** — that is RAII: the constructor takes the resource, the destructor gives it back, always
- **unique_ptr** — a `unique_ptr` is the single card: one owner, handed over (moved) but never copied

*Example (italic):* The guest dashes out during a fire alarm without stopping at the desk — the card still comes back, because leaving the building is exactly what returns it.

**Key point:** Bind the resource to an owning object — taken in the constructor, returned in the destructor — and room 214 is freed the instant its owner's life ends, however that end comes.

### Visualization (canvas `c1`, 720×300)

Paired-lifetime bar diagram: two stay scenarios (normal checkout and a fire-alarm exit), each drawn as an owner bar and a room bar on a shared time axis, the two bars in every pair starting and ending at exactly the same x.

- **Title (bold 15px, `#1a5276`, top center):** "Owner and Room: Two Lifetimes That Always Start and End Together".
- **Time axis:** horizontal 2px `#999` line at y=252 from x=160 to x=680; 12px `#444` labels below: "check-in" at x=170, "midday" at x=420, "checkout" at x=660.
- **Scenario 1 (normal stay), label 12px `#444` at x=20, y=95:** "normal stay"; owner bar 18px tall at y=78 from x=170 to x=640, fill `rgba(42,120,214,0.35)` with 2px blue `#2a78d6` border, 11px blue text inside left: "guest object (owner)"; room bar 18px tall at y=104, same x span 170–640, fill `rgba(0,131,0,0.25)` with 2px green `#008300` border, 11px green text inside left: "room 214 (resource)".
- **Scenario 2 (fire-alarm exit), label at x=20, y=192:** "fire-alarm exit"; identical owner bar at y=175 and room bar at y=201, both spanning x=170 to x=420 only.
- **Start marker:** vertical dashed `#6b7280` (dash 4/3) line at x=170 from y=60 to the axis; 12px `#6b7280` label at its top: "constructor takes room 214".
- **End markers:** small green `#008300` down-ticks at the right end of each bar pair (x=640 and x=420); 12px green label "key returned" beside each.
- **Annotation (bold 13px green `#008300`, near x=470, y=232):** two lines: "ends early or on time —" / "the key comes back with the owner".
- **Caption (12px `#444`, bottom right):** "illustrative — one guest, room 214".

## Room 214 and a Family of Cards

**Tags:** `worked example` (blue), `reference count` (green)

- **Copies allowed** — a `shared_ptr` is a card the desk will photocopy: parent, kid A, kid B
- **The tally** — the desk counts live cards for room 214: check-in makes it 1, the copies make it 2, then 3
- **Cards return** — kid A leaves (3 → 2), kid B leaves (2 → 1), the parent finally leaves (1 → 0)
- **Zero means free** — the instant the tally hits 0, the desk cleans and releases room 214
- **Hand-check** — six events, tally 1, 2, 3, 2, 1, 0: the room is freed exactly once, at the very end

*Example (italic):* Swap the order so the parent leaves before the kids — the tally still walks down to 0 exactly once, and only the last card returned frees the room.

**Key point:** Shared ownership is a live-card tally: copying a card adds 1, returning one subtracts 1, and whichever owner drives the count to 0 is the one whose destructor frees the room.

### Visualization (canvas `c2`, 720×300)

Stepped line chart of the desk's card tally for room 214 across six events, with a dot and count label at each event and the final drop to zero marked as the moment the room is freed.

- **Title (bold 15px, `#1a5276`, top center):** "The Desk's Tally for Room 214: Live Cards After Each Event".
- **Axes:** origin x=70, baseline y=240, plot width 590, plot height 170; y = card count 0 to 3 with light `#e5e9ef` gridlines at 1, 2, 3 and 12px `#444` tick labels; x = six events centered at x = `[120, 220, 320, 420, 520, 620]`, 12px `#444` labels below the baseline: "check-in", "+ kid A", "+ kid B", "kid A out", "kid B out", "parent out".
- **Step line:** blue `#2a78d6` 3px stepped line through tally values `[1, 2, 3, 2, 1, 0]` (horizontal segment at each level, vertical riser between events).
- **Dots:** 7px blue dot at each event, bold 13px blue count label above each: "1", "2", "3", "2", "1", "0"; the final dot at (620, count 0) drawn green `#008300` with its "0" label in bold green.
- **Freed marker:** vertical dashed green (dash 4/3) line at x=620 from y=90 to the baseline.
- **Annotation (bold 13px green `#008300`, near x=400, y=95):** two lines: "only the last returned card" / "frees the room — exactly once".
- **Caption (12px `#444`, bottom right):** "illustrative — one parent, two kids, six events".

## Four Ways Out of a Function

**Tags:** `where it's used` (blue), `leaks` (green), `exception safety` (orange)

- **Manual freeing** — with raw `new`/`delete`, giving the room back is a line YOU must write at every exit
- **Four exits** — one function, four ways out: a normal return, two early returns, one thrown exception
- **One delete** — a single `delete` at the bottom covers only 1 of the 4 exits; the other 3 leak
- **Leaks add up** — a leaked room is never rentable again; a server doing this all day runs out of rooms
- **RAII wins** — an owning object's destructor runs on all 4 exits, exception included, for free
- **Everywhere** — files, locks, sockets, database handles: anything you must give back fits this pattern

*Example (italic):* A request handler opens a file, hits bad input, and returns early — with RAII the file closes itself; with manual code that early return quietly leaks it.

**Key point:** Nobody remembers cleanup at every exit forever — RAII moves the give-back into the destructor, so the language remembers for you on all 4 paths, exceptions included.

### Visualization (canvas `c3`, 720×300)

Check-and-cross grid: four exit-path rows against two columns (manual delete at the end vs an RAII owner), with a per-column tally showing the manual version frees on only 1 of 4 paths.

- **Title (bold 15px, `#1a5276`, top center):** "One Function, Four Ways Out: Who Remembers to Free the Room?".
- **Column headers (bold 13px `#1a5276`, y=65):** "manual delete at the end" centered at x=430, "RAII owner" centered at x=615.
- **Rows at y = 100, 138, 176, 214, each with a 12px `#444` left-aligned label at x=20:** "normal return", "early return — bad input", "exception thrown", "early return — timeout"; light 1px `#e5e9ef` separator line under each row from x=20 to x=690.
- **Manual column (centered x=430):** row 1 bold 16px green `#008300` "✓"; rows 2–4 bold 16px red `#e74c3c` "✗" each with an 11px red label "leaks" just to its right.
- **RAII column (centered x=615):** bold 16px green `#008300` "✓" on all four rows.
- **Tallies (bold 13px, y=248):** red `#e74c3c` "frees 1 of 4" centered under the manual column, green `#008300` "frees 4 of 4" centered under the RAII column.
- **Annotation (bold 13px red `#e74c3c`, near x=40, y=282):** "3 of the 4 exits leak room 214 in the manual version".
- **Caption (12px `#444`, bottom right):** "illustrative — one function with four exit paths".

## Two Rooms Holding Each Other's Cards

**Tags:** `common mistake` (red), `reference cycle` (orange)

- **The trap** — a booking holds a card to the guest profile, and the profile holds a card back
- **Stuck at 1** — everyone outside leaves, yet each tally still reads 1: the two cards prop each other up
- **Never freed** — neither count can reach 0 first, so neither room is ever released: a cycle leak
- **The fix** — make one direction a `weak_ptr`: a card that lets you look but does not hold the room
- **Not magic** — smart pointers automate the give-back; you still must decide who really owns whom

*Example (italic):* After the customer logs out, both tallies read 1 forever — the booking keeps the profile alive and the profile keeps the booking alive, with nobody using either.

**Common mistake:** Believing `shared_ptr` can never leak. Two objects holding owning cards to each other never reach count 0 — break the loop with a `weak_ptr` in one direction.

### Visualization (canvas `c4`, 720×300)

Two side-by-side box-and-arrow diagrams: on the left the leak (booking and profile each holding an owning card to the other, both tallies stuck at 1), on the right the fix (one direction downgraded to a dashed weak card, both tallies able to reach 0).

- **Title (bold 15px, `#1a5276`, top center):** "The Cycle: Two Owning Cards Keep Both Tallies Stuck at 1".
- **Panel headers (bold 13px, y=62):** red `#e74c3c` "the leak" centered at x=175, green `#008300` "the fix" centered at x=530; vertical 1px `#e5e9ef` divider at x=355 from y=55 to y=265.
- **Left panel boxes:** two 180×52 rounded rects with 2px `#6b7280` borders, fill `#f8f9fa` — "booking" centered at (175, 105) and "guest profile" centered at (175, 215); bold 13px `#1a5276` names inside; bold 12px orange `#d95926` badge "tally: 1" at each box's right edge.
- **Left panel arrows:** 2.5px blue `#2a78d6` curved arrows with arrowheads — one from the booking box's left side arcing down to the profile box, one from the profile box's right side arcing up to the booking box; 11px blue label "owning card" beside each.
- **Right panel boxes:** same two boxes centered at (530, 105) and (530, 215); booking badge bold 12px green `#008300` "tally → 0, freed", profile badge green "freed next".
- **Right panel arrows:** downward 2.5px solid blue arrow (booking → profile) labeled 11px blue "owning card"; upward 2.5px dashed (dash 5/4) aqua `#199e70` arrow (profile → booking) labeled 11px aqua "weak card — no hold".
- **Annotation (bold 12px orange `#d95926`, near x=45, y=282):** "everyone logged out, yet neither count can reach 0 first".
- **Caption (12px `#444`, bottom right):** "illustrative — two objects, one card each way".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red appears only in `c3` (leaked exit paths) and the `c4` "the leak" header — genuine error states.
- **Data:** all bar spans, tally values `[1, 2, 3, 2, 1, 0]`, event labels, exit-path rows, check/cross placements, box positions, and pixel coordinates are the hardcoded literals above (no randomness); the hotel, room 214, and the four-exit function are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
