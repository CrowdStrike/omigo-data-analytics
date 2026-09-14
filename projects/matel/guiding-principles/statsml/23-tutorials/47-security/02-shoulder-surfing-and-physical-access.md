# Shoulder Surfing &amp; Physical Access

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Shoulder Surfing &amp; Physical Access

**Subtitle:** The attacks that need no network at all — a screen on a train, a sticky note under a keyboard, an unlocked laptop at lunch

## The Stranger in Seat 12B

**Tags:** `core idea` (blue), `over the shoulder` (orange), `no network needed` (green)

- **The commute** — Alice opens her laptop on the 7:40 train and types her passcode to unlock it
- **The seat behind** — Bob sits one row back and slightly to the side, with a clear view of her keyboard
- **The glance** — a six-character passcode typed at two keys per second is exposed for about three seconds
- **The screen** — she then opens a customer list, so names, emails and account numbers sit in plain view
- **The definition** — reading secrets off someone's screen or keyboard by direct observation is shoulder surfing
- **Why it works** — nothing is decrypted or guessed; the secret is simply *displayed*, and displays are public

*Example (italic):* Bob never touches the network — he reads the passcode as it is typed and photographs the customer list while Alice scrolls (scenario illustrative).

**Key point:** Shoulder surfing is an observation attack: the secret is read, not broken, so every cryptographic control on the laptop is bypassed by a person sitting close enough to see.

### Visualization (canvas `c1`, 720×300)

Side-view schematic of two train rows: Alice's laptop screen in front, Bob behind it, with a dashed sight cone reaching the screen.

- **Title (bold 15px, `#1a5276`, top center):** "One Row Back Is Close Enough to Read the Screen".
- **Seat backs:** two grey rounded boxes (radius 6, fill `rgba(107,114,128,0.10)`, 2px `#6b7280`): Alice's seat at x=90, y=150, 70×110; Bob's seat at x=470, y=150, 70×110.
- **Alice figure:** filled `#2a78d6` circle head radius 12 at (175, 150) and a rounded body box x=161, y=166, 28×70, fill `rgba(42,120,214,0.30)`, 2px `#2a78d6`; 12px `#444` centered label "Alice" at (175, 275).
- **Bob figure:** filled `#d95926` circle head radius 12 at (555, 150) and a rounded body box x=541, y=166, 28×70, fill `rgba(217,89,38,0.28)`, 2px `#d95926`; 12px `#444` centered label "Bob, one row back" at (555, 275).
- **Laptop:** a screen rect x=225, y=112, 130×86, fill `#fff`, 2px `#1a5276`; base rect x=215, y=198, 150×8, fill `rgba(26,82,118,0.30)`, 2px `#1a5276`; five 11px `#6b7280` mock rows of text drawn as 3px-tall grey bars (fill `rgba(107,114,128,0.45)`) at y = 124, 138, 152, 166, 180, each x=234 with widths `[104, 88, 96, 74, 100]`.
- **Screen caption (bold 12px `#1a5276`, centered at (290, 104)):** "customer list".
- **Sight cone:** two dashed (dash 5/4) 1.5px `#d95926` lines from Bob's eye at (543, 148) to the screen's top-right (357, 114) and bottom-right (357, 198); shade the enclosed triangle with fill `rgba(217,89,38,0.10)`.
- **Distance marker:** 1.5px `#6b7280` line from (365, 230) to (535, 230) with 4px end ticks, and 12px `#6b7280` centered label "about 1.5 m" at (450, 246).
- **Annotation (bold 13px `#d95926`, left-aligned at (60, 62)):** "passcode exposed ≈ 3 seconds while typed".
- **Annotation (bold 13px `#4a3aa7`, left-aligned at (60, 82)):** "no network, no malware, no password cracking".
- **Caption (12px `#444`, bottom right):** "scene schematic, illustrative".

## A Walkthrough of 60 Desks

**Tags:** `worked example` (blue), `unlocked screens` (orange), `audit` (green)

- **The audit** — after her commute, Alice's team walks the office floor once and inspects 60 desks
- **One finding each** — every desk is scored by its single worst finding, so the four buckets never overlap
- **Walkway-readable** — 21 of 60 screens are legible from the aisle: 21 ÷ 60 = 35% of desks
- **Unlocked and empty** — 12 desks sit signed-in with nobody there: 12 ÷ 60 = 20%
- **Written down** — 9 desks have credentials on paper, under a keyboard or taped to a monitor: 15%
- **Nothing found** — the remaining 18 desks are clean: 18 ÷ 60 = 30%, and 21+12+9+18 = 60 desks
- **The total** — 42 of 60 desks, or 70%, leak something to anyone simply walking past

*Example (italic):* The 12 unlocked-and-empty desks were all found during the lunch hour, when Alice's own laptop was among them (counts illustrative).

**Key point:** A single walk-past audit turns an abstract worry into a number you can act on — here 70% of desks exposed a secret without a single network packet being sent.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: the 60 audited desks split into four mutually exclusive findings, each bar labeled with its count and percentage.

- **Title (bold 15px, `#1a5276`, top center):** "One Walkthrough, 60 Desks: What Was Visible".
- **Axes:** origin x=60, baseline y=240, plot width 610, plot height 180; y = desks 0 to 24 (so a value maps to `240 - v / 24 * 180`), gridlines `#e5e9ef` at 6/12/18/24 with 12px `#444` right-aligned tick labels at x=54; x-axis 2px `#999` from x=60 to x=670.
- **Bars (76px wide, centered at x = 145, 290, 435, 580) from the hardcoded array `[21, 12, 9, 18]`:** colors in order — walkway-readable fill `rgba(217,89,38,0.35)` border `#d95926`; unlocked-and-empty fill `rgba(213,81,129,0.35)` border `#d55181`; credentials on paper fill `rgba(201,133,0,0.35)` border `#c98500`; nothing found fill `rgba(0,131,0,0.28)` border `#008300`; all borders 2px.
- **Value labels (bold 13px in each bar's border color, centered 10px above the bar top):** "21 · 35%", "12 · 20%", "9 · 15%", "18 · 30%" — percentages computed in JS as `v / 60 * 100`.
- **X labels (12px `#444`, centered under the baseline at y=258, second line at y=274):** "readable from" / "the walkway"; "unlocked," / "nobody there"; "credentials" / "on paper"; "nothing" / "found".
- **Annotation (bold 13px `#1a5276`, left-aligned at (330, 58)):** "42 of 60 desks = 70% leaked something".
- **Caption (12px `#444`, bottom right):** "audit counts illustrative".

## Five Doors That Have No Login

**Tags:** `where it's used` (blue), `tailgating` (orange), `physical vectors` (green)

- **Shoulder surfing** — a passcode or record is read directly off a screen or keyboard by a bystander
- **Written-down credentials** — a sticky note under a keyboard turns a strong secret into a readable object
- **Unattended unlocked screens** — a signed-in laptop left at lunch is a live session anyone can drive
- **Tailgating** — Bob carries a box, Alice holds the badge door open, and an unbadged person is now inside
- **Planted USB devices** — a dropped drive plugged in "to see whose it is" runs code on a trusted machine
- **Common thread** — each converts physical proximity into digital access with no credential ever guessed
- **Where you meet it** — open-plan offices, coworking spaces, trains, cafés, and any shared badge entrance

*Example (italic):* Alice holds the door for a stranger with full hands; twenty minutes later that stranger is unplugging a drive from her unlocked laptop.

**Key point:** Physical access is a credential. Once someone is inside the building and beside a signed-in machine, the identity system has already been satisfied on their behalf.

### Visualization (canvas `c3`, 720×300)

Fan-in flow diagram: five physical vectors on the left converge into one "digital access" box on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Five Physical Routes, One Digital Outcome".
- **Vector boxes:** five rounded boxes (radius 8, 2px border, 12px centered text) at x=30, width 250, height 34, at y = 52, 96, 140, 184, 228 — "shoulder surfing" fill `rgba(42,120,214,0.15)` border `#2a78d6`; "credentials written down" fill `rgba(201,133,0,0.18)` border `#c98500`; "unlocked screen, nobody there" fill `rgba(213,81,129,0.18)` border `#d55181`; "tailgating through a badge door" fill `rgba(217,89,38,0.18)` border `#d95926`; "planted USB device" fill `rgba(74,58,167,0.15)` border `#4a3aa7`.
- **Outcome box:** rounded box (radius 8) at x=470, y=118, 220×70, fill `rgba(231,76,60,0.12)`, 2px `#e74c3c`, bold 13px `#1a5276` line "digital access" centered at (580, 148) and 12px `#2c3e50` line "no password guessed" centered at (580, 168).
- **Arrows:** 2.5px lines in each source box's border color from (280, boxY+17) to (466, 153) with a 9px filled arrowhead at the end, angle from `Math.atan2`.
- **Annotation (bold 13px `#d95926`, centered at (360, 272)):** "proximity is the only prerequisite".
- **Caption (12px `#444`, bottom right):** "vector map, illustrative".

## Encryption Cannot Defend a Screen

**Tags:** `common mistake` (red), `screen lock` (orange)

- **The confusion** — a long unique passphrase and full-disk encryption feel like they cover everything
- **Why they miss** — both defend a secret in transit or at rest; here it is on display and being read
- **Length is irrelevant** — an observed 30-character passphrase is exactly as compromised as a short one
- **Lunch arithmetic** — Alice is away 40 minutes; a 30-second task fits 40 × 60 ÷ 30 = 80 times over
- **Timeout as a control** — a 1-minute lock leaves 2 such windows, 5 minutes leaves 10, 15 minutes leaves 30
- **So configure it** — screen lock timeout, privacy filters, and badge-door discipline are security controls
- **Not paranoia** — the shortest window still leaves room, so the control reduces exposure rather than ending it

*Example (italic):* Alice's 15-minute timeout means her signed-in session sits available for 15 of her 40 lunch minutes — 30 chances at a 30-second task (illustrative).

**Common mistake:** Treating cryptography as the whole of security. Shoulder surfing and unattended sessions are defeated by lock timeouts, screen position, and door discipline — not by stronger keys.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: how many 30-second windows an unattended signed-in laptop offers during a 40-minute lunch, by screen-lock timeout.

- **Title (bold 15px, `#1a5276`, top center):** "A 40-Minute Lunch: 30-Second Windows by Lock Timeout".
- **Rows (bar left edge x=250, top edges y = 70, 118, 166, 214), right-aligned 12px `#444` labels ending at x=240:** "locks after 1 min", "locks after 5 min", "locks after 15 min", "never locks".
- **Bars (22px tall) from the hardcoded exposure minutes `[1, 5, 15, 40]`:** windows computed in JS as `mins * 60 / 30` giving `[2, 10, 30, 80]`; pixel width is `windows / 80 * 400`, giving `[10, 50, 150, 400]`. Fills/borders: green `rgba(0,131,0,0.45)` / `#008300`; yellow `rgba(201,133,0,0.45)` / `#c98500`; orange `rgba(217,89,38,0.45)` / `#d95926`; red `rgba(231,76,60,0.45)` / `#e74c3c`.
- **Value labels (bold 12px in the bar's border color, 8px right of each bar end, vertically centered):** computed text `windows + ' windows'` → "2 windows", "10 windows", "30 windows", "80 windows".
- **Reference note (12px `#6b7280`, left-aligned at (250, 58)):** "one window = 30 s, enough to copy a file or plug in a drive".
- **Annotation (bold 13px `#008300`, left-aligned at (250, 262)):** "shortening the timeout is the whole fix".
- **Caption (12px `#444`, bottom right):** "lunch length illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded arrays above (no randomness). The desk audit is `[21, 12, 9, 18]` over 60 desks — percentages `35 / 20 / 15 / 30` are computed at render time as `v / 60 * 100` and the counts sum to exactly 60; the exposed subtotal is 21+12+9 = 42 of 60 = 70%. The lunch-window chart derives `[2, 10, 30, 80]` from exposure minutes `[1, 5, 15, 40]` as `mins * 60 / 30`. All figures are invented and labeled illustrative; text numbers must match chart numbers exactly.
- **Naming and content rules:** fictional people Alice and Bob only, no real company names; never write out an example password, token, or credential string — refer to "a passcode" or "credentials on paper" only.
- **Framing:** defensive/educational — the page explains observation and proximity attacks so a reader recognizes them and configures lock timeouts, screen position, and door discipline; no operational attack guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
