# The App Lifecycle

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The App Lifecycle

**Subtitle:** A mobile app can be killed by the operating system any moment it is off screen — good apps save their work as if every switch away were the last moment they will ever run

## Seven Fields, One Phone Call

**Tags:** `core idea` (blue), `expect death` (red), `mobile OS` (orange)

- **The form** — a user is filling a 9-field order form in a food-delivery app; 7 fields are typed in
- **The call** — at minute 2 a phone call arrives; the order app slides off screen into the background
- **The kill** — at minute 3:30 the OS quietly kills the backgrounded app to free memory for the call
- **No warning** — the app gets no "you are dying" signal; its process simply stops existing
- **The return** — at minute 6 the call ends, the user taps back — the app relaunches from scratch
- **The loss** — if the 7 fields lived only in memory, the user faces an empty form

*Example (italic):* The user typed for two minutes, took a four-minute call, and came back to a blank form — the app was dead for more than half that time and never knew it.

**Key point:** The app lifecycle is the OS-controlled sequence foreground → background → killed → relaunched; the app must expect death at any moment it is not on screen.

### Visualization (canvas `c1`, 720×300)

Step timeline of the app's state over the 8-minute episode: foreground, background, killed, then foreground again after relaunch.

- **Title (bold 15px, `#1a5276`, top center):** "One Order Form, 8 Minutes: Foreground → Background → Killed → Relaunched".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes 0 to 8 with 12px `#444` tick labels every 2 minutes; y = three labeled state bands, 12px `#444` labels at x=55 right-aligned: "Foreground" at y=105, "Background" at y=160, "Killed" at y=215; horizontal gridlines `#e5e9ef` at those three y levels.
- **Step line (3px), colored per segment:** green `#008300` at y=105 from minute 0 to 2 (foreground, typing 7 of 9 fields); yellow `#c98500` at y=160 from minute 2 to 3.5 (background during the call); red `#e74c3c` at y=215 from minute 3.5 to 6 (process dead); green `#008300` at y=105 from minute 6 to 8 (relaunched); vertical 2px connectors in the segment's new color at minutes 2, 3.5, 6.
- **Event markers:** vertical dashed `#6b7280` (dash 4/3) lines at minutes 2, 3.5, 6 with 12px `#6b7280` labels near the top: "call arrives 2:00", "OS kills 3:30", "user returns 6:00".
- **Annotation (bold 13px red `#e74c3c`, near minute 4.5, y=90):** "7 typed fields vanish here — and no code was told".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## The State Machine and Its Callbacks

**Tags:** `worked example` (blue), `state machine` (green), `callbacks` (orange)

- **Enter foreground** — launch fires a create/start callback; the app draws its UI and runs freely
- **Go background** — switching away fires a pause/save-state callback; this is the one guaranteed save window
- **Suspended** — soon after (seconds on iOS) the OS freezes the process; no code runs, memory intact
- **Killed** — under memory pressure the OS deletes the process with no callback at all
- **Relaunch** — a fresh process starts; memory variables are gone, only the saved-state bundle and disk remain
- **Hand-check** — 7 fields in memory: after backgrounding all 7 exist; after the kill, 0 in memory, 7 in saved state if the pause callback wrote them

*Example (italic):* The pause callback at 2:00 writes the 7 fields to the saved-state bundle; at the 6:00 relaunch the app restores all 7 and the user only re-types the last 2.

**Key point:** Every transition except the kill announces itself with a callback — so all saving must happen at the background transition, never later.

### Visualization (canvas `c2`, 720×300)

State-machine diagram: rounded boxes for the lifecycle states connected by labeled arrows, with the kill arrow highlighted as the only silent one.

- **Title (bold 15px, `#1a5276`, top center):** "The Lifecycle State Machine: One Arrow Fires No Callback".
- **Top row boxes (each 130×44, 8px radius, 12px `#2c3e50` centered text, y=80):** "Not running" at x=25 fill `rgba(107,114,128,0.12)` border `#6b7280`; "Foreground" at x=205 fill `rgba(0,131,0,0.12)` border 2px `#008300`; "Background" at x=385 fill `rgba(201,133,0,0.12)` border 2px `#c98500`; "Suspended" at x=565 fill `rgba(42,120,214,0.12)` border `#2a78d6`.
- **Bottom box:** "Killed (process gone)" 170×44 at x=420, y=200, fill `rgba(231,76,60,0.12)` border 2px `#e74c3c`.
- **Arrows (2px `#6b7280`, filled triangle heads, 11px `#6b7280` labels):** "launch — create callback" from Not running to Foreground; "switch away — pause callback" from Foreground to Background (upper arrow) and "return — resume callback" back (lower arrow); "seconds later (iOS)" from Background to Suspended; red 3px `#e74c3c` arrow from Suspended down to Killed labeled in bold 12px red "memory pressure — NO callback"; green 2px `#008300` arrow from Killed left-then-up to Foreground labeled "relaunch — cold start".
- **Annotation (bold 13px red `#e74c3c`, near x=80, y=225):** "save at the pause callback — the kill never warns you".
- **Caption (12px `#444`, bottom right):** "callback names generic across platforms".

## Where the Seven Fields Can Survive

**Tags:** `where it's used` (blue), `saved state` (green), `analytics` (orange)

- **Memory** — the 7 fields as plain variables survive backgrounding but die with the process
- **Saved state** — the small per-screen bundle written at pause survives a kill, not a reboot
- **Disk** — a database or file survives kills and reboots both; login tokens belong here
- **Analytics** — session logic tied to process lifetime counts this one 8-minute order as 2 sessions
- **Background limits** — timers, downloads, and sockets freeze soon after backgrounding (seconds on iOS, more gradually on Android), so "finish later" code never runs

*Example (italic):* The same 7 fields survive 1 of the 3 disruptions in memory, 2 of 3 in saved state, and 3 of 3 on disk.

**Key point:** Pick storage by what it must outlive — screen switch, process kill, or reboot — and count analytics sessions by user time, not by process lifetime.

### Visualization (canvas `c3`, 720×300)

Survival matrix: 3 storage layers (rows) against 3 death events (columns), each cell a green check or red cross.

- **Title (bold 15px, `#1a5276`, top center):** "What Survives What: 3 Storage Layers vs 3 Disruptions".
- **Column headers (bold 12px `#1a5276`, centered at y=75):** "backgrounded" at x=305, "process killed" at x=460, "device reboot" at x=615.
- **Row labels (12px `#444`, left at x=20, vertically centered on rows y=110, 165, 220):** "memory variables", "saved-state bundle", "disk / database".
- **Cells:** rounded rects 120×38, 8px radius, centered on the column x positions per row; survives = fill `rgba(0,131,0,0.12)` border 2px `#008300` with bold 14px green check "✓"; lost = fill `rgba(231,76,60,0.12)` border 2px `#e74c3c` with bold 14px red cross "✗".
- **Cell values (hardcoded):** memory `[✓, ✗, ✗]`; saved-state `[✓, ✓, ✗]`; disk `[✓, ✓, ✓]`.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=270):** "only disk survives everything — memory survives almost nothing".
- **Caption (12px `#444`, bottom right):** "matrix schematic; exact bundle size limits vary by platform".

## Backgrounded Is Not Alive

**Tags:** `common mistake` (red), `background kills` (orange)

- **The belief** — "the user only switched away for a minute; my app is still running back there"
- **The reality** — the OS reclaims backgrounded apps whenever memory runs short, without notice
- **The decay** — on a busy phone, of 100 backgrounded apps about 55 still exist after 5 minutes and only 6 after 30
- **No signal** — the kill fires no callback; the app discovers it was dead only at the next launch
- **The test** — background the app, kill its process by hand, relaunch: if the form comes back empty, the bug is real
- **The fix** — write state at every pause callback and treat each backgrounding as possibly final

*Example (italic):* A developer tests by switching away for ten seconds and back — the app survives every time on their idle phone, so the lost-form bug ships.

**Common mistake:** Assuming the app keeps running in the background. Suspension stops all code within seconds on iOS (Android throttles more gradually), and the kill that follows is silent — code that saves "later" saves never.

### Visualization (canvas `c4`, 720×300)

Decay curve: percent of backgrounded apps still in memory over the 30 minutes after switching away.

- **Title (bold 15px, `#1a5276`, top center):** "Backgrounded Apps Still Alive: the 30-Minute Decay".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = minutes since backgrounding 0 to 30, 12px `#444` tick labels at 0/5/10/15/20/30; y = percent alive 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Decay line:** blue `#2a78d6` 3px line with 4px dots through minutes `[0, 1, 2, 5, 10, 15, 20, 30]`, percent alive `[100, 91, 78, 55, 34, 21, 13, 6]`.
- **Half-life marker:** vertical dashed `#6b7280` (dash 4/3) line at minute 5 up to the curve, 12px `#6b7280` label "55% left at 5 min".
- **Shaded zone:** light red fill `rgba(231,76,60,0.08)` under the curve from minute 15 to 30, bold 12px red `#e74c3c` label "most apps are gone" near x=22 min, y=210.
- **Annotation (bold 13px orange `#d95926`, near x=10 min, y=85):** "every point on this curve died without a callback".
- **Caption (12px `#444`, bottom right):** "survival percentages illustrative — varies by device memory and OS".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the timeline events (2:00 call, 3:30 kill, 6:00 return), the 7-of-9 form fields, the survival matrix (memory ✓✗✗, saved-state ✓✓✗, disk ✓✓✓), and the decay curve minutes `[0,1,2,5,10,15,20,30]` / percent `[100,91,78,55,34,21,13,6]` are invented and labeled illustrative; callback names are kept generic across platforms.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
