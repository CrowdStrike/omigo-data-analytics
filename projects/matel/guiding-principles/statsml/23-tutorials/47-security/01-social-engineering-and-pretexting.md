# Social Engineering & Pretexting

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Social Engineering &amp; Pretexting

**Subtitle:** No password is cracked — the person who knows it is simply asked for it, by a caller with a badge-shaped story and a deadline

## The Call Alice Gets on a Tuesday Afternoon

**Tags:** `core idea` (blue), `pretexting` (orange), `the helpdesk call` (green)

- **The call** — Alice's desk phone rings and a calm voice says he is Bob from IT support downstairs
- **The story** — her account is locked out, he is clearing the ticket queue before a 5 p.m. audit
- **The trigger** — while he talks, the attacker starts a login as Alice, so a real code is texted to her
- **The ask** — "the system just sent you a six-digit code; read it back to me so I can unlock you"
- **The result** — Alice reads out the code, the attacker types it in, and the session is his
- **The names** — that invented identity and story is the *pretext*; the whole technique is *social engineering*
- **What broke** — no password was guessed, no software was exploited; a human was persuaded to help

*Example (italic):* The code that arrives on Alice's phone is genuine — it was triggered by the attacker's own login attempt seconds earlier, which is exactly why the call feels legitimate.

**Key point:** Social engineering attacks the person, not the system; pretexting is the specific move of inventing a role and a reason so the request sounds like part of everyone's normal job.

### Visualization (canvas `c1`, 720×300)

Six-box loop diagram: the attacker triggers a real one-time code, the pretext call harvests it, and the attacker logs in.

- **Title (bold 15px, `#1a5276`, top center):** "The Code Is Real — Only the Caller Is Fake".
- **Top row (boxes 52px tall, 8px radius, centered two-line 12px `#2c3e50` text at y+21 and y+38):** box 1 at x=30, y=62, 195 wide, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, text "1. attacker starts a login" / "as Alice"; box 2 at x=270, y=62, 195 wide, same blue style, text "2. system texts a real" / "one-time code to Alice"; box 3 at x=510, y=62, 180 wide, fill `rgba(107,114,128,0.12)`, 2px `#6b7280` border, text "3. Alice's phone" / "buzzes".
- **Bottom row:** box 4 at x=510, y=180, 180 wide, fill `rgba(213,81,129,0.15)`, 2px `#d55181` border, text "4. caller: \"read me" / "the code\""; box 5 at x=270, y=180, 195 wide, fill `rgba(201,133,0,0.15)`, 2px `#c98500` border, text "5. Alice reads the" / "code out loud"; box 6 at x=30, y=180, 195 wide, fill `rgba(231,76,60,0.14)`, 2px `#e74c3c` border, text "6. attacker is inside" / "as Alice".
- **Arrows (3px, 10px heads):** left-to-right along the top row (box 1 → box 2 → box 3), a vertical arrow down at x=600 from y=114 to y=180, then right-to-left along the bottom row (box 4 → box 5 → box 6).
- **Annotation (bold 13px violet `#4a3aa7`, centered at x=300, y=152):** "nothing was cracked — the code was handed over".
- **Caption (12px `#444`, bottom right):** "illustrative scenario".

## What a 500-Employee Pretext Drill Produces

**Tags:** `worked example` (blue), `arithmetic` (orange)

- **The drill** — a company's security team runs an authorized pretext-call exercise on 500 employees
- **Stayed on the call** — 120 of the 500 engaged with the caller instead of hanging up: 120 ÷ 500 = 24.0%
- **Read out the code** — 30 of those 120 gave up the texted code: 30 ÷ 120 = 25.0% of engagers
- **Overall rate** — 30 ÷ 500 = 6.0% of everyone contacted, so 30 accounts would have been opened
- **Declined** — 120 − 30 = 90 stayed on the line but refused, and 500 − 120 = 380 never engaged
- **Reported it** — 12 of the 90 refusers called security afterwards: 12 ÷ 500 = 2.4% raised an alarm
- **The asymmetry** — the drill "passed" for 470 people, yet 30 handed-over codes is 30 real intrusions

*Example (italic):* 380 non-engagers + 90 refusals + 30 code hand-overs = 500 contacted, and the 12 who reported the call are a subset of the 90 refusers (figures illustrative).

**Key point:** Defenders read 94% as a pass; an attacker reads 30 successes. A control that fails 6% of the time on every employee, every month, is not a control.

### Visualization (canvas `c2`, 720×300)

Horizontal funnel: 500 employees contacted narrowing to 30 codes handed over, plus the 12 who reported it.

- **Title (bold 15px, `#1a5276`, top center):** "500 Called, 120 Engaged, 30 Read Out the Code".
- **Rows (bars 20px tall starting at x=250, top edges at y = 70, 118, 166, 220), right-aligned 12px `#444` labels ending at x=240:** "employees called", "stayed on the call", "read out the code", "hung up and reported".
- **Bars from hardcoded counts `[500, 120, 30, 12]`, pixel widths computed as `count / 500 * 420` (giving 420, 100.8, 25.2, 10.08):** fills `rgba(42,120,214,0.30)`, `rgba(201,133,0,0.45)`, `rgba(231,76,60,0.35)`, `rgba(0,131,0,0.45)`, with matching 2px borders `#2a78d6`, `#c98500`, `#e74c3c`, `#008300`.
- **Value labels (bold 12px in the bar's border colour, 8px past each bar's right end):** "500", "120  (24.0%)", "30  (6.0%)", "12  (2.4%)"; percentages computed at render time as `count / 500 * 100`.
- **Separator:** 1px `#e5e9ef` dashed line across x=60 to x=700 at y=205, with a 11px `#6b7280` note at (250, 200) "subset of the 90 who refused".
- **Annotation (bold 13px `#e74c3c`, at x=300, y=196):** "30 codes = 30 accounts opened".
- **Caption (12px `#444`, bottom right):** "drill figures illustrative".

## Five Levers, and Why One Success Is Enough

**Tags:** `where it's used` (blue), `the levers` (orange), `defensive` (green)

- **Authority** — the caller claims a role that normally does make such requests, so refusing feels insubordinate
- **Urgency** — a deadline ("before the 5 p.m. audit") removes the pause in which anyone would verify
- **Plausibility** — the story matches reality: accounts really do lock, and IT really does call about them
- **Reciprocity** — he offers help first, and the pull to help a helper back is strong and pre-verbal
- **Only one hit needed** — at the drill's 6.0% rate, 10 calls give a 46.1% chance of at least one success
- **Fifty calls** — 1 − 0.94^50 = 95.5%, so a patient caller working a directory succeeds nearly always
- **The counter** — hang up and dial the published helpdesk number; never read a code to an inbound caller

*Example (italic):* With p = 0.06 per call, the chance of at least one success is 1 − (1 − 0.06)^n: 6.0% at one call, 26.6% at five, 46.1% at ten, 71.0% at twenty, 95.5% at fifty.

**Key point:** The attacker needs one yes out of hundreds of calls, while the defender needs every employee to say no every time — which is why the fix must be a process, not vigilance.

### Visualization (canvas `c3`, 720×300)

Bar chart: probability of at least one successful pretext call as the number of calls grows, at a 6% per-call rate.

- **Title (bold 15px, `#1a5276`, top center):** "One Call Rarely Works; Fifty Almost Always Do (p = 6% per call)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = probability 0 to 100%, gridlines `#e5e9ef` at 25/50/75/100 with 12px `#444` tick labels "25%", "50%", "75%", "100%"; 2px `#999` x-axis.
- **Bars (62px wide, centred at x = 132, 244, 356, 468, 580) for call counts `[1, 5, 10, 20, 50]`:** heights computed at render time from `p = 1 - Math.pow(0.94, n)` scaled to 180px, giving 6.0%, 26.6%, 46.1%, 71.0%, 95.5%; fills step blue `rgba(42,120,214,0.35)`, aqua `rgba(25,158,112,0.35)`, yellow `rgba(201,133,0,0.40)`, orange `rgba(217,89,38,0.40)`, red `rgba(231,76,60,0.35)` with matching 2px borders `#2a78d6`, `#199e70`, `#c98500`, `#d95926`, `#e74c3c`.
- **Labels:** bold 12px in the bar's border colour above each bar, printed as `p.toFixed(1) + '%'`; 12px `#444` call-count labels "1", "5", "10", "20", "50" below the baseline and an axis caption "calls placed" at (360, baseline + 38).
- **Annotation (bold 13px `#4a3aa7`, left-aligned at x=150, y=58):** "the attacker only has to win once".
- **Caption (12px `#444`, bottom right):** "computed from the drill's 6.0% rate".

## There Is No Patch, and Alice Is Not Foolish

**Tags:** `common mistake` (red), `not a patch` (orange)

- **Not a bug** — every system behaved exactly as designed: the login ran, the code sent, the code worked
- **Nothing to update** — no version bump, no scanner finding, no CVE; patching everything changes nothing
- **Not foolishness** — the pretext supplied authority, urgency, and a reason that made helping the correct move
- **Blame backfires** — punish the person who fell for it and the next victim stays quiet, costing you hours
- **Fix the process** — publish one verification rule: hang up, dial the known number, never read codes aloud
- **Remove the ask** — push-approval or hardware keys leave no readable code for a caller to request
- **Reward reports** — the 12 who reported are the detection system; make reporting fast and consequence-free

*Example (italic):* Two employees can behave identically and only one gets called; treating the outcome as a character test measures who was targeted, not who is careless.

**Common mistake:** Filing social engineering under "vulnerabilities to patch" or "employees to blame". It is a process gap — verify identity out-of-band, remove readable codes, and make reporting the easiest response.

### Visualization (canvas `c4`, 720×300)

Two-panel schematic: a software flaw closes with a patch; a pretext call has nothing to patch, only a process to change.

- **Title (bold 15px, `#1a5276`, top center):** "A Patch Fixes a Flaw; Nothing Patches a Phone Call".
- **Panel headers (bold 13px `#2c3e50`):** "software flaw" at (45, 62); "pretext call" at (395, 62).
- **Left panel boxes (270 wide, 44 tall, 8px radius, centred 12px text):** at (45, 78) fill `rgba(231,76,60,0.14)`, 2px `#e74c3c`, text "vulnerable code, version 1.4"; at (45, 158) fill `rgba(0,131,0,0.14)`, 2px `#008300`, text "patched, version 1.5"; 3px `#6b7280` downward arrow at x=180 from y=122 to y=158.
- **Right panel boxes (270 wide):** at (395, 78) fill `rgba(107,114,128,0.12)`, 2px `#6b7280`, text "no flaw — everything worked"; at (395, 158) 60 tall, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6`, two-line text "hang up, call the published" / "number, never read codes out"; 3px `#6b7280` downward arrow at x=530 from y=122 to y=158.
- **Outcome labels (bold 12px, centred):** "fixed" in `#008300` at (180, 228); "process change, not a patch" in `#2a78d6` at (530, 240).
- **Divider:** 1px `#e5e9ef` vertical line at x=365 from y=50 to y=250.
- **Annotation (bold 13px `#4a3aa7`, centred at x=360, y=278):** "the pretext supplied the authority and the deadline — not the target's carelessness".
- **Caption (12px `#444`, top right, 11px):** "schematic".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no randomness anywhere — all chart data is hardcoded literal arrays. The drill counts (500 contacted, 120 engaged, 30 codes handed over, 90 refused, 380 non-engagers, 12 reported) are invented and labeled illustrative; they must close exactly (380 + 90 + 30 = 500, and 12 ⊂ 90). Percentages (24.0%, 6.0%, 25.0%, 2.4%) and the at-least-one-success curve (6.0%, 26.6%, 46.1%, 71.0%, 95.5% from `1 − 0.94^n` at n = 1, 5, 10, 20, 50) are computed in JS at render time, never hardcoded as label strings. Text numbers must match chart numbers exactly.
- **Naming and content rules:** fictional people only (Alice the employee, Bob the claimed IT caller); no real company names; no credential strings, no example passwords, and no six-digit code value is ever shown — the code is referred to generically.
- **Framing:** defensive/educational throughout — the page explains the persuasion mechanism so readers recognize the call and follow an out-of-band verification process; no operational attack scripts or guidance.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
