# Defer, Finally, With

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Defer, Finally, With

**Subtitle:** Three keywords from three languages that make one promise: the cleanup you schedule next to the setup will run no matter how the code exits — success, error, or early bail

## Hanging the Keys on the Exit Door

**Tags:** `core idea` (blue), `guaranteed cleanup` (green), `exit paths` (orange)

- **The shop** — a coffee shop owner unlocks at 7am and, whatever the day brings, must lock up on the way out
- **Three endings** — a normal 6pm close, a fire alarm at 2pm, a burst pipe that empties the shop at noon
- **The trick** — the moment she unlocks, she hangs the keys on the exit door handle: leaving forces the lock-up
- **In code** — `defer` (Go), `finally` (Java, Python), `with` (Python) hang the cleanup at the moment of setup
- **The promise** — however the function leaves — normal return, thrown error, early exit — the scheduled cleanup runs

*Example (italic):* She never plans to lock up "at the end of the day"; she rigs the exit so that leaving IS locking up — that rigging is exactly what defer, finally, and with do.

**Key point:** Cleanup attached to the setup runs on every exit path; cleanup written on the last line runs only on the one lucky path that reaches the last line.

### Visualization (canvas `c1`, 720×300)

Funnel diagram: three differently-colored exit paths (normal day, fire alarm, burst pipe) all converging on one shared "lock the door" box before reaching "outside".

- **Title (bold 15px, `#1a5276`, top center):** "Three Ways the Day Can End — One Lock-Up".
- **Left boxes (170×42, 6px radius, 2px border, bold 12px centered label, at x=40):** "normal close, 6pm" (blue `#2a78d6`, y=62), "fire alarm, 2pm" (red `#e74c3c`, y=132), "pipe bursts, noon" (orange `#d95926`, y=202); each fill = its border color at 0.10 alpha.
- **Arrows:** 2px lines in each box's color from its right edge to the left edge of the center box, with small solid arrowheads.
- **Center box (170×54 at x=330, y=126):** green `#008300` 3px border, fill `rgba(0,131,0,0.10)`, bold 13px green label in two lines: "lock the door" / "(the cleanup)".
- **Right box (110×42 at x=575, y=132):** mute `#6b7280` 2px border, 12px `#444` label "outside"; green 2px arrow from center box to it.
- **Annotation (bold 12px green `#008300`, two lines, near x=330, y=225):** "every exit path passes" / "through the same cleanup".
- **Caption (12px `#444`, bottom right):** "illustrative — one shop, three possible endings".

## Tracing Five Lines, One of Them Poison

**Tags:** `worked example` (blue), `error exit` (red)

- **The script** — open `orders.txt`, process its 5 lines one at a time, then close the file
- **The poison line** — line 4 is corrupted; processing it throws an error and the function exits right there
- **Naive version** — `close()` sits on the last line of the function; the error on line 4 means it never runs
- **Guarded version** — the open sits inside `with` (or the close inside `finally` / a `defer`): it closes mid-error
- **Same trace** — both versions run open, line 1, line 2, line 3, hit the line-4 error; only the exit differs

*Example (italic):* Same crash on line 4 both times — the guarded version leaves the file closed behind it, the naive version leaves it dangling open forever.

**Key point:** `with`/`finally`/`defer` did not stop the crash — the line-4 error still escaped — it only guaranteed the close happened on the way out.

### Visualization (canvas `c2`, 720×300)

Two-lane step timeline: identical steps in both lanes up to the line-4 error, then the naive lane skips the close while the guarded lane runs it.

- **Title (bold 15px, `#1a5276`, top center):** "Same Steps, Same Crash — Different Exit".
- **Lane labels (bold 12px `#444`, left-aligned at x=15):** "close on last line" at y=115, "close in with/finally" at y=215.
- **Shared steps (both lanes):** 8px dots at x = `[160, 250, 340, 430, 520]` labeled below in 12px `#444`: "open", "line 1", "line 2", "line 3", "line 4"; dots blue `#2a78d6` except the x=520 dot red `#e74c3c` with a bold 13px red "error!" label above it; 2px `#6b7280` connector line through each lane's dots (lane 1 dots at y=110, lane 2 at y=210).
- **Naive exit (lane 1):** dashed (dash 4/3) `#6b7280` 2px arrow from (520, 110) to (630, 110); 12px `#6b7280` two-line label at its end: "close skipped —" / "file left open".
- **Guarded exit (lane 2):** solid green `#008300` 3px arrow from (520, 210) to (630, 210); box 90×34 at (595, 193) with green 2px border, fill `rgba(0,131,0,0.12)`, bold 12px green label "close runs".
- **Annotation (bold 12px green `#008300`, two lines, near x=250, y=255):** "the error still happens —" / "but so does the close".
- **Caption (12px `#444`, bottom right):** "illustrative — a five-line file with one corrupted row".

## Why the Leak Kills a Stranger, Hours Later

**Tags:** `where it's used` (blue), `resource leak` (red), `rule of thumb` (orange)

- **The limit** — the operating system caps this server at 100 files open at the same time
- **The leak** — 2 requests in every 100 hit the poison-line error; naive code leaks one open handle each time
- **The countdown** — 2 leaked handles per 100 requests means the 100-handle limit is hit at request 5,000
- **The symptom** — nothing looks wrong for hours, then every request fails with "too many open files"
- **The fix** — with the close in `finally`/`defer`/`with`, failing requests still close their file: zero leaks

*Example (italic):* The server that ran fine all morning dies at lunch — the crash at request 5,000 was scheduled by the quiet leaks at requests 50, 100, 150, and on.

**Key point:** Missing cleanup rarely fails the request that caused it; it fails a stranger's request thousands of calls later — which is exactly why these keywords exist.

### Visualization (canvas `c3`, 720×300)

Single-panel line chart: open file handles versus requests served, a climbing red leak line hitting the 100-handle limit at request 5,000 against a flat green cleaned-up line.

- **Title (bold 15px, `#1a5276`, top center):** "Leaked Handles Climb to the Limit; Cleaned-Up Code Stays Flat".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 185; x = requests served 0 to 5,000 with 12px `#444` tick labels "0", "1,000", "2,000", "3,000", "4,000", "5,000"; y = open handles 0 to 110 with 12px `#444` labels at 0, 25, 50, 75, 100 and light `#e5e9ef` gridlines.
- **Limit line:** horizontal dashed (dash 6/4) `#6b7280` 2px line at y-value 100; 12px `#6b7280` label above its left end: "OS limit: 100 open files".
- **Leaking server:** red `#e74c3c` 3px line through requests = `[0, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000]`, handles = `[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]`; 12px red label "no cleanup (2 leaks per 100 requests)" alongside the line near x-value 2,600.
- **Guarded server:** green `#008300` 3px line, same request grid, handles = `[2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2]`; 12px green label "close in with/finally/defer" just above the line near x-value 1,200.
- **Crash marker:** red 8px dot where the leak line meets the limit (request 5,000, 100 handles); bold 13px red annotation two lines to its upper left: "dies at request 5,000 —" / "every request fails from here".
- **Caption (12px `#444`, bottom right):** "illustrative — 2 leaked handles per 100 requests".

## It Runs the Goodbye, It Doesn't Catch the Error

**Tags:** `common mistake` (red), `cleanup vs catch` (orange)

- **Not a catcher** — `finally` runs while the error is on its way out; the error keeps travelling up afterwards
- **The catcher is separate** — `except`/`catch` is the keyword that stops an error; `finally` never does
- **Stacked defers** — several `defer`s unwind in reverse order: the last thing opened is the first thing closed
- **`with` scope** — its cleanup fires at the end of the `with` block, not at the end of the whole function
- **The mistake** — writing `finally`, seeing the close run, and assuming the program survived the error

*Example (italic):* After the `finally` closed the file, the line-4 error kept climbing and the program still crashed — the cleanup ran, the rescue never happened.

**Common mistake:** Treating `finally`/`defer`/`with` as error handling. They guarantee the goodbye, not the survival — pair them with `except`/`catch` when you also want to recover.

### Visualization (canvas `c4`, 720×300)

Two-column vertical flow diagram: on the left an error passes upward straight through a `finally` gate and still reaches the caller; on the right an `except` block stops the same error.

- **Title (bold 15px, `#1a5276`, top center):** "Cleanup Lets the Error Through — Catch Stops It".
- **Column headers (bold 13px `#1a5276`, centered):** "finally / defer / with" at x=210, y=55; "except / catch" at x=530, y=55.
- **Left column (centered at x=210):** bottom box 200×38 at y=232, red `#e74c3c` 2px border, fill `rgba(231,76,60,0.10)`, bold 12px red label "error raised at line 4"; red 2px upward arrow to a middle box 200×38 at y=150, green `#008300` 2px border, fill `rgba(0,131,0,0.10)`, bold 12px green label "finally: file closed"; red 2px upward arrow continuing to a top box 200×38 at y=72, red 2px border, bold 12px red label "error reaches the caller".
- **Right column (centered at x=530):** identical bottom box "error raised at line 4" at y=232; red 2px upward arrow to a middle box 200×38 at y=150, blue `#2a78d6` 2px border, fill `rgba(42,120,214,0.10)`, bold 12px blue label "except: error handled"; above it at y=85 only a 12px `#6b7280` label "nothing escapes" with a short `#6b7280` dashed (dash 4/3) stub arrow that stops.
- **Annotation (bold 12px orange `#d95926`, two lines, centered near x=370, y=283):** "finally is the goodbye, except is the rescue —" / "they are different keywords for different jobs".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c` (used only for genuine error states, as here), orange `#e67e22`.
- **Data:** all positions, step labels, and line points are the hardcoded arrays and coordinates above (no randomness); the leak-chart numbers (100-handle limit, 2 leaks per 100 requests, death at request 5,000) must match the section text exactly.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
