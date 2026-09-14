# Cameras That Broadcast to Everyone

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Cameras That Broadcast to Everyone

**Subtitle:** A camera reachable from the internet with no real check is not hacked — it is simply published

## Alice's Living Room, Answering Any Request

**Tags:** `core idea` (blue), `authorization` (orange), `defensive` (green)

- **The setup** — Alice mounts an indoor camera to watch the living room and wants it viewable from her phone
- **The shortcut** — the install opens a path from the internet straight to the camera's video stream
- **No check** — the stream answers whoever asks; it never demands a login, a token, or a paired device
- **The "secret"** — the only protection was that nobody knew the address, and an address is not a secret
- **Not a break-in** — nothing was defeated or exploited; the camera did exactly what it was configured to do
- **What leaks** — a camera is also a microphone and a floorplan: routines, voices, faces, and room layout
- **The right name** — this is a broken authorization decision, published by configuration rather than by attack

*Example (italic):* Alice never types a password to see her living room from the office — and neither does anyone else who sends the very same request.

**Key point:** A camera reachable from the internet with no real check is not hacked, it is published — obscurity of the address is not an authorization decision.

### Visualization (canvas `c1`, 720×300)

Flow diagram: the internet reaches an opened inbound path, which reaches a camera whose stream answers any request; three pills show what the stream carries.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "The Path Is Open and the Stream Answers Anyone".
- **Internet box:** rounded box (8px radius) at x=30, y=110, 150×60, fill `rgba(42,120,214,0.12)`, 2px `#2a78d6` border; centered at x=105 with 12px `#2c3e50` "the internet" (y=136) and 11px `#6b7280` "anyone, anywhere" (y=154).
- **Open-path box:** rounded box at x=250, y=110, 170×60, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border; centered x=335 with 12px "inbound path opened" (y=133) and 11px `#6b7280` "so the phone app works" (y=151).
- **Camera box:** rounded box at x=500, y=110, 180×60, fill `rgba(213,81,129,0.12)`, 2px `#d55181` border; centered x=590 with 12px "indoor camera" (y=133) and 11px `#6b7280` "stream answers any request" (y=151).
- **Arrows (3px):** `#2a78d6` from (180,140) to (244,140) with head at x=250; `#d95926` from (420,140) to (494,140) with head at x=500. Label 11px `#6b7280` "request" centered at (212,130); label bold 12px `#e74c3c` "no login asked" centered at (457,128).
- **Payload pills:** three rounded boxes 66×26 at y=200, x=490 / 562 / 634, fill `rgba(74,58,167,0.10)`, 2px `#4a3aa7` border, centered 11px `#4a3aa7` text "picture", "audio", "floorplan" (baseline y=217).
- **Annotation (bold 13px orange `#d95926`, left, x=40, y=250):** "obscurity of the address is not a check".
- **Caption (12px `#444`, bottom right):** "schematic".

## How Long "Nobody Knows the Address" Lasts

**Tags:** `worked example` (blue), `enumeration` (orange)

- **The claim** — "nobody knows my camera's address", treated as though the address were a password
- **The space** — the reachable address space is roughly 4 billion slots, a number a computer finds small
- **The rate** — one commodity scanner probes about 40,000 addresses per second hunting open services
- **The division** — 4,000,000,000 ÷ 40,000 = 100,000 seconds = 27.8 hours, so a full sweep finishes inside 30 h
- **In parallel** — 4 scanners finish in 6.9 hours, 10 in 2.8 hours, 40 in 0.7 hours (about 42 minutes)
- **The conclusion** — obscurity expires on the first sweep after setup, and sweeps run continuously
- **Then it's a query** — search engines that index reachable devices turn discovery into a lookup, not an attack

*Example (italic):* Alice's camera comes online Tuesday morning; a single sweeping scanner reaches its address before Wednesday lunch (rates illustrative).

**Key point:** Address obscurity has a measurable shelf life — one sweep of the space — and the arithmetic puts that at hours, not years.

### Visualization (canvas `c2`, 720×300)

Bar chart: hours to sweep the whole address space, at 1, 4, 10, and 40 parallel scanners.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Time to Sweep 4,000,000,000 Addresses at 40,000/Second".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = hours 0 to 30 (6 px per hour), gridlines `#e5e9ef` at 10 / 20 / 30 with 12px `#444` right-aligned tick labels "10 h", "20 h", "30 h"; x-axis 2px `#999`.
- **Bars (70px wide, centered at x = 150, 300, 450, 600), hardcoded hours `[27.8, 6.9, 2.8, 0.7]` mapped to pixel heights `[167, 41, 17, 4]`:** fills/borders blue `rgba(42,120,214,0.35)`/`#2a78d6`, yellow `rgba(201,133,0,0.35)`/`#c98500`, orange `rgba(217,89,38,0.35)`/`#d95926`, red `rgba(231,76,60,0.30)`/`#e74c3c`.
- **Value labels:** bold 12px in each bar's border color, centered 8px above the bar top: "27.8 h", "6.9 h", "2.8 h", "0.7 h".
- **X labels (12px `#444`, below baseline, y=+18):** "1 scanner", "4 scanners", "10 scanners", "40 scanners"; axis caption 12px `#444` "scanners running in parallel" centered at y=+38.
- **Annotation (bold 13px red `#e74c3c`, left, x=250, y=70):** "obscurity is gone in under a day".
- **Note (12px `#6b7280`, left, x=250, y=92):** "40 scanners: 0.7 h = about 42 minutes".
- **Caption (12px `#444`, bottom right):** "scan rates illustrative".

## The Ladder of What "Protected" Means

**Tags:** `rule of thumb` (blue), `ladder` (green), `where it's used` (orange)

- **Rung 1 — no check** — the stream answers any request at all; 48 of 400 audited cameras, which is 12%
- **Rung 2 — obscurity only** — a hard-to-guess address and nothing more; 92 cameras, 23%, enumerated anyway
- **Rung 3 — factory login** — the shipped username and password are public and documented; 140 cameras, 35%
- **Rung 4 — real credential** — a per-user password the owner chose, ideally with a second factor; 96 cameras, 24%
- **Rung 5 — no inbound path** — the camera dials out to a service, nothing at home listens; 24 cameras, 6%
- **The sum** — 48 + 92 + 140 + 96 + 24 = 400, so 280 of 400 (70%) open without an owner-set credential
- **Durable exposure** — open feeds get aggregated onto public lists, so one discovery outlives the sweep
- **The defense** — rungs 4 and 5 together: no inbound path plus a real login, never a clever address

*Example (italic):* Of 400 reachable cameras, 280 (70%) need no owner-chosen credential to view; the other 120 (30%) do (audit illustrative).

**Key point:** "Protected" is a ladder, not a yes/no — only the top two rungs are authorization; the lower three are obscurity wearing authorization's clothes.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: the five rungs of the ladder with the audited camera counts and derived percentages.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "What 400 Reachable Cameras Actually Required".
- **Rows (bars start at x=300, height 22, top edges at y = 60, 100, 140, 180, 220), each with a right-aligned 12px `#444` label ending at x=290:** "no check at all", "obscure address only", "factory default login", "per-user credential", "no inbound path".
- **Bars from hardcoded counts `[48, 92, 140, 96, 24]` at 2.2857 px per camera → pixel widths `[110, 210, 320, 219, 55]`:** fills/borders red `rgba(231,76,60,0.30)`/`#e74c3c`, orange `rgba(217,89,38,0.35)`/`#d95926`, yellow `rgba(201,133,0,0.35)`/`#c98500`, green `rgba(0,131,0,0.30)`/`#008300`, aqua `rgba(25,158,112,0.30)`/`#199e70`.
- **Value labels:** bold 12px in the bar's border color at bar end + 8px: "48 (12%)", "92 (23%)", "140 (35%)", "96 (24%)", "24 (6%)".
- **No rung-number captions** — the row order carries the ladder; adding "rung N" labels crowds the row.
- **Annotation (bold 13px red `#e74c3c`, left, x=60, y=268):** "280 of 400 (70%) open with no owner-set credential".
- **Caption (12px `#444`, bottom right, y=294):** "audit illustrative".

## "Who Would Find My Camera?"

**Tags:** `common mistake` (red), `untargeted` (orange)

- **The question** — "who would bother looking for my camera?" assumes somebody must choose you first
- **Nobody chose you** — sweeps are untargeted: every address gets probed in turn, yours among them, on schedule
- **Continuous** — sweeps do not stop after one pass; a device is rediscovered each time it comes back online
- **No skill needed** — watching an open feed is just opening a stream, so the audience is not attackers but anyone
- **Wrong denominator** — the population that can watch is not "skilled attackers", it is everyone with a browser
- **Being boring is not a control** — an untargeted sweep has no notion of whether your living room is interesting
- **The fix** — remove the inbound path and require a real login; do not rely on being unremarkable

*Example (italic):* Bob assumes his camera is too dull to interest anyone; the sweep that reaches it never evaluates that question at all.

**Common mistake:** Reasoning from "I am not a target" — this exposure is untargeted and long-lived. The right question is "what does my camera hand to a stranger's request?"

### Visualization (canvas `c4`, 720×300)

Two-panel schematic: a rare targeted attack picking one home, versus a continuous untargeted sweep probing a grid of every address, with a few that answer highlighted.

- **Title (bold 15px, `#1a5276`, top center, y=24):** "Nobody Hunts One Home — Sweeps Touch Every Address".
- **Divider:** 1px `#e5e9ef` vertical line at x=330 from y=45 to y=265.
- **Left panel header (bold 12px `#2c3e50`, x=48, y=60):** "targeted attack (rare)".
- **Left panel:** violet `#4a3aa7` filled circle r=9 at (80,140) with 12px `#6b7280` centered label "one attacker" at (80,168); rounded box (8px) at x=170, y=120, 110×40, fill `rgba(107,114,128,0.10)`, 2px `#6b7280` border, centered 12px `#2c3e50` "one chosen home" at (225,144); 3px `#4a3aa7` arrow from (92,140) to (164,140) with head at x=170; 12px `#6b7280` "needs a reason to pick you" at (48,205), left-aligned.
- **Right panel header (bold 12px `#2c3e50`, x=368, y=60):** "untargeted sweep (continuous)".
- **Right panel grid:** 17 columns × 6 rows of small squares centered on x = 368 + 20·col, y = 95 + 18·row. Default square is 7×7 filled `rgba(107,114,128,0.35)`. The six hardcoded `[row, col]` pairs `[[0,3],[1,11],[2,6],[3,15],[4,1],[5,9]]` are drawn instead as 9×9 squares filled magenta `#d55181`.
- **Right panel labels:** 12px `#6b7280` centered "every address probed, in turn, again and again" at (528,212); bold 12px `#d55181` centered "highlighted: a device that answers" at (528,234).
- **Annotation (bold 13px magenta `#d55181`, left, x=40, y=282):** "no skill needed to watch an open feed".
- **Caption (12px `#444`, bottom right, y=282):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data (no randomness anywhere — hardcoded literal arrays only):**
  - Sweep arithmetic: 4,000,000,000 addresses ÷ 40,000 per second = 100,000 s = 27.777… h, printed as 27.8 h; ÷4 = 25,000 s = 6.94 h → 6.9 h; ÷10 = 10,000 s = 2.78 h → 2.8 h; ÷40 = 2,500 s = 0.694 h → 0.7 h ≈ 42 minutes. Space size and scan rate are illustrative round figures.
  - Ladder audit: counts `[48, 92, 140, 96, 24]` sum to 400 exactly; percentages 12 / 23 / 35 / 24 / 6 are those counts ÷ 400 and sum to 100. Rungs 1–3 total 280 = 70%; rungs 4–5 total 120 = 30%. Audit is illustrative and labeled as such.
  - Text numbers and chart numbers must match to the digit.
- **Safety framing:** defensive and educational only. No procedure for locating or accessing any device, no real vendor / model / device-search-engine names, no IP addresses, hostnames, URLs, or credential strings — generic placeholders and generic descriptions ("an indoor camera", "a device search engine") throughout. People are Alice/Bob.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
