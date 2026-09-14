# Protocols & Dunder Methods

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Protocols & Dunder Methods

**Subtitle:** In Python, `len(x)`, `x[0]`, and `x + y` are not magic — each one quietly calls a double-underscore method on your object, so defining those methods makes your own class behave like a built-in

## A Playlist That Wants to Act Like a List

**Tags:** `core idea` (blue), `dunder methods` (green), `operator = method call` (orange)

- **The playlist** — a `Playlist` class holds someone's morning songs, but `len(morning)` crashes with a TypeError
- **The secret** — `len(x)` never counts anything itself; it just calls `x.__len__()` and returns the answer
- **Dunders** — methods named with double underscores (`__len__`, `__getitem__`, `__add__`) are Python's hooks
- **The fix** — add `def __len__(self): return len(self.songs)` and `len(morning)` suddenly works
- **Every operator** — `x[i]` dials `__getitem__`, `x + y` dials `__add__`, `song in x` dials `__contains__`
- **Not magic** — operator syntax is a fixed phone book: each symbol maps to one method name

*Example (italic):* `len(morning)` fails on a fresh `Playlist`, works the moment `__len__` exists — the built-in was only ever forwarding the call.

**Key point:** Operators and built-ins are fixed method calls in disguise: define the dunder with the matching name and your class picks up the syntax.

### Visualization (canvas `c1`, 720×300)

Two-column mapping diagram: four "what you write" code boxes on the left, arrows to four "what Python actually calls" boxes on the right, one row per operator.

- **Title (bold 15px, `#1a5276`, top center):** "Every Operator Dials a Dunder".
- **Left column ("what you write"):** four rounded rects (radius 6) at x=60, width 210, height 34, centered at y = 85, 135, 185, 235; fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border; 13px monospace (`Menlo, monospace`) `#1a5276` centered labels: `len(morning)`, `morning[0]`, `"Sunrise" in morning`, `morning + evening`.
- **Right column ("what Python calls"):** four matching rounded rects at x=430, width 250, same y centers; fill `rgba(0,131,0,0.10)`, 1.5px `#008300` border; 13px monospace `#008300` labels: `morning.__len__()`, `morning.__getitem__(0)`, `morning.__contains__(...)`, `morning.__add__(evening)`.
- **Arrows:** 2px `#6b7280` horizontal arrows with small solid arrowheads from each left box (x=270) to its right box (x=430), at each row's center y.
- **Column headers (bold 12px `#6b7280`):** "what you write" above the left column (y=55), "what Python actually calls" above the right column (y=55).
- **Annotation (bold 13px orange `#d95926`, centered near y=278):** "the syntax is fixed — the phone book maps each symbol to one method name".

## Three Dunders, Seven Songs

**Tags:** `worked example` (blue), `hand-checkable` (green)

- **Morning list** — 3 songs: "Sunrise" (4 min), "Coffee Run" (3 min), "Daybreak" (5 min) — 12 minutes total
- **Evening list** — 4 songs: "Sunset" (3), "Night Drive" (4), "Moonlight" (2), "Stars" (5) — 14 minutes
- **`__len__`** — returns the song count, so `len(morning)` gives 3 and `len(evening)` gives 4
- **`__getitem__`** — returns the song at an index, so `morning[0]` gives "Sunrise"
- **`__add__`** — builds a new `Playlist` from both song lists: `morning + evening` has 7 songs, 26 minutes
- **Check by hand** — combined index 3 is "Sunset", the first evening song, because 3 morning songs sit before it

*Example (italic):* `combined = morning + evening`, then `len(combined)` is 3 + 4 = 7 and `combined[3]` is "Sunset" — every answer checkable on your fingers.

**Key point:** Three short dunders — `__len__`, `__getitem__`, `__add__` — and the playlist counts, indexes, and concatenates exactly like a built-in list: 3 + 4 songs = 7, 12 + 14 minutes = 26.

### Visualization (canvas `c2`, 720×300)

Three-row block chart: each playlist drawn as a strip of song blocks whose widths are proportional to minutes, with the `+` visually producing the combined strip.

- **Title (bold 15px, `#1a5276`, top center):** "morning + evening → one new 7-song Playlist".
- **Layout:** rows at y = 80, 140, 225 (block height 30); blocks start at x=180; scale 18px per minute; row labels left-aligned 12px monospace `#444` at x=20: `morning  (3 songs)`, `evening  (4 songs)`, `combined (7 songs)`.
- **Morning row:** 3 adjacent rounded blocks (2px gap), widths = minutes × 18 from `[4, 3, 5]` (72, 54, 90 px); fill `rgba(42,120,214,0.30)`, 1.5px `#2a78d6` border; 11px `#1a5276` centered labels "Sunrise 4", "Coffee Run 3", "Daybreak 5"; 12px `#2a78d6` total "12 min" right of the strip.
- **Evening row:** 4 blocks, widths from `[3, 4, 2, 5]` (54, 72, 36, 90 px); fill `rgba(0,131,0,0.25)`, 1.5px `#008300` border; 11px `#008300` labels "Sunset 3", "Night Drive 4", "Moonlight 2", "Stars 5"; 12px `#008300` total "14 min".
- **Operator glyphs:** bold 16px `#6b7280` "+" centered between rows 1 and 2 (x=160, y=125), bold 16px "=" between rows 2 and 3 (x=160, y=195).
- **Combined row:** the same 7 blocks in order, minutes `[4, 3, 5, 3, 4, 2, 5]` (total width 26 × 18 = 468 px), morning blocks blue-filled, evening blocks green-filled, same 11px labels; 12px `#444` total "26 min".
- **Index marker:** thin dashed (dash 4/3) `#d95926` vertical line just left of combined block 4 (the 3-song boundary, x = 180 + 216), bold 12px orange label above: "combined[3] = 'Sunset'".
- **Caption (12px `#444`, bottom right):** "song names and minutes are illustrative".

## One Protocol, a Whole Ecosystem for Free

**Tags:** `where it's used` (blue), `duck typing` (green), `protocols` (orange)

- **A protocol** — an informal deal: any object with `__len__` and `__getitem__` counts as a sequence
- **No paperwork** — no base class to inherit, no interface to declare; having the methods IS the membership
- **Free features** — the playlist now works in `for song in p`, `sorted(p)`, `random.choice(p)`, `list(p)`
- **Duck typing** — libraries check behavior, not ancestry: "if it indexes like a list, treat it as one"
- **Where you meet it** — pandas DataFrames, NumPy arrays, and Path objects all speak these same protocols
- **The payoff** — one class plays nicely with decades of library code that has never heard of `Playlist`

*Example (italic):* `random.choice(morning)` picks one of the 3 songs — the `random` module was written years before `Playlist`, yet they cooperate through the sequence protocol.

**Key point:** Implement the protocol's dunders and every function written against that protocol accepts your object — the interface is the method names, nothing more.

### Visualization (canvas `c3`, 720×300)

Hub-and-spoke diagram: the playlist's two dunders in a center box, spokes out to the built-in behaviors they unlock, each spoke labeled with working code.

- **Title (bold 15px, `#1a5276`, top center):** "Two Dunders In, an Ecosystem Out".
- **Hub:** rounded rect (radius 8) centered at (360, 165), width 200, height 56; fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border; two centered lines: bold 13px `#1a5276` "Playlist defines" / 13px monospace "__len__  +  __getitem__".
- **Spokes:** six rounded rects (width 168, height 32, fill white, 1.5px `#2a78d6` border, 12px monospace `#2a78d6` centered labels) placed at (115, 75), (115, 165), (115, 255), (605, 75), (605, 165), (605, 255) — labels: `for song in p:`, `sorted(p, key=...)`, `random.choice(p)`, `list(p)`, `reversed(p)`, `"Stars" in p`.
- **Connectors:** 1.5px `#6b7280` lines from the hub's left/right edges to each spoke box's near edge, small solid arrowheads pointing outward at the spoke end.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=290):** "no inheritance, no registration — the method names are the interface".

## Who Answers `x + y`? The Two-Step Lookup

**Tags:** `common mistake` (red), `__radd__` (orange)

- **The surprise** — `["Intro"] + morning` works even though the built-in `list` knows nothing about playlists
- **Step one** — Python first asks the left side: `list.__add__(morning)` shrugs and returns `NotImplemented`
- **Step two** — Python then asks the right side's mirror method: `morning.__radd__(["Intro"])` builds the answer
- **The result** — a new 4-song playlist: 1 intro track + the 3 morning songs
- **Both decline** — if `__add__` and `__radd__` both return `NotImplemented`, only then comes the TypeError
- **Direct calls** — writing `p.__len__()` in your code skips this machinery; always write `len(p)` instead

*Example (italic):* `["Intro"] + morning` has `len` 4 — the list declined, Python quietly asked `morning.__radd__`, and nobody wrote a special case anywhere.

**Common mistake:** Raising an error inside `__add__` when the other operand is a foreign type. Return `NotImplemented` instead — raising kills step two, and the other object never gets its `__radd__` turn.

### Visualization (canvas `c4`, 720×300)

Top-to-bottom flowchart of the `+` lookup: the expression box, the left operand's attempt, the right operand's rescue, and the dead-end TypeError branch shown dashed.

- **Title (bold 15px, `#1a5276`, top center):** "How Python Resolves  ['Intro'] + morning".
- **Box 1 (expression):** rounded rect centered at (240, 78), width 300, height 36; fill `rgba(26,82,118,0.10)`, 2px `#1a5276` border; 13px monospace `#1a5276` label `["Intro"] + morning`.
- **Box 2 (step 1):** rounded rect centered at (240, 155), width 340, height 44; fill `rgba(217,89,38,0.10)`, 1.5px `#d95926` border; two lines 12px monospace `#d95926`: `step 1: list.__add__(morning)` / `returns NotImplemented`.
- **Box 3 (step 2):** rounded rect centered at (240, 240), width 340, height 44; fill `rgba(0,131,0,0.10)`, 2px `#008300` border; two lines: 12px monospace `#008300` `step 2: morning.__radd__(["Intro"])` / bold 12px `#008300` `→ new Playlist, len = 4`.
- **Flow arrows:** 2px `#6b7280` vertical arrows with arrowheads: box 1 → box 2 (11px `#6b7280` side label "ask the left side first"), box 2 → box 3 (11px side label "left declined — ask the right side").
- **Dead-end branch:** dashed (dash 5/4) 1.5px `#e74c3c` arrow from box 3's right edge to a rounded rect centered at (590, 240), width 170, height 44, dashed 1.5px `#e74c3c` border, 12px `#e74c3c` centered label "TypeError — only if BOTH decline".
- **Annotation (bold 13px magenta `#d55181`, near x=470, y=115):** two lines: "NotImplemented is a polite pass," / "not an error — it keeps step 2 alive".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline code in bullets uses `<code>` styled 0.88em monospace on `#f4f6f8`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the genuine error state (the TypeError dead end in c4).
- **Chart text:** code snippets inside canvases render in 11–13px monospace (`Menlo, monospace`); all other chart text ≥11px; draw rounded rects with a small `rrect(x, y, w, h, r)` helper and arrowheads with a `arrow(x1, y1, x2, y2)` helper shared across the four charts.
- **Data:** all diagrams are drawn from the hardcoded literals above (song minute arrays `[4, 3, 5]` and `[3, 4, 2, 5]`, box coordinates, labels) — no randomness; song names and minutes are invented and labeled illustrative in c2's caption; the counts in the text (3, 4, 7 songs; 12, 14, 26 minutes; `combined[3]` = "Sunset"; `len` 4 in c4) must match the chart literals exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
