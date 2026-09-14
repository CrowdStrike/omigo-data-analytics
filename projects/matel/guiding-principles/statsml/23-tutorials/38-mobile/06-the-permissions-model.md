# The Permissions Model

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Permissions Model

**Subtitle:** Phones make every app ask before touching the camera, mic, or location — and when the app asks decides whether the user says yes

## The Camera Ask That Waits for the Tap

**Tags:** `core idea` (blue), `ask at use` (green), `sandbox` (orange)

- **The app** — a meal-journal app lets people snap a photo of every dish they cook
- **The rival** — a competing app fires five permission prompts the moment it first opens
- **Ask-at-use** — the journal app stays silent until the user actually taps the camera button
- **The moment** — at that tap, "allow camera access?" is the most reasonable question possible
- **The lock** — until the user says yes, the OS keeps the camera sealed off from the app entirely

*Example (italic):* A user browses recipes for two days before tapping the camera; only then does the prompt appear — and she taps Allow without a second thought.

**Key point:** That is the permissions model: apps start with access to nothing sensitive, and each resource is unlocked only by an explicit grant — the OS owns the dialog, but the app picks the moment, and the moment is a product decision.

### Visualization (canvas `c1`, 720×300)

Two-row flow diagram: the install-time wall of prompts (ends in denial) vs the ask-at-use flow (ends in a grant), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Two Apps, Two Moments to Ask for the Camera".
- **Row 1 (y=95), label 12px `#444` at x=20:** "wall at install"; blue `#2a78d6` rounded box at x=160 labeled "first launch" (12px), 3px arrow to a red `#e74c3c` box at x=350 labeled "5 prompts, back to back", 3px arrow to bold 12px red text at x=560 "✗ finger learns to tap Deny".
- **Row 2 (y=205), label:** "ask at use"; blue box at x=160 labeled "first launch — no prompts", 3px arrow to a blue box at x=350 labeled "day 2: taps camera button", 3px arrow to a green `#008300` box at x=545 labeled "one prompt, in context" with bold 12px green "✓ Allow" beneath it.
- **Box style:** 140–170px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px ink `#1a5276`, centered near y=270):** "same dialog, same permission — only the timing differs".

## Three Timings, Three Grant Rates

**Tags:** `worked example` (blue), `one-shot prompt` (red)

- **The test** — 300 new users, 100 per strategy, and all of them eventually need the camera
- **Cold at launch** — the prompt fires before the app has shown any value: 38 of 100 allow
- **Ask at use** — the prompt fires at the camera-button tap: 71 of 100 allow
- **Explainer first** — an in-app screen says why, then the real prompt fires: 86 of 100 allow
- **One shot** — on iOS one denial is final (only Settings undoes it); Android allows about one retry
- **The recovery** — of the 62 launch-strategy deniers, about 4 ever flip it back in Settings

*Example (italic):* Same app, same permission, same dialog — moving the ask from launch to first use lifts grants from 38 to 71 of 100.

**Key point:** The system dialog is effectively a one-shot resource — a pre-prompt explainer lets a hesitant user say "not now" in your UI without burning the real prompt.

### Visualization (canvas `c2`, 720×300)

Vertical bar chart: camera grants per 100 users under the three prompt timings, with a one-shot annotation.

- **Title (bold 15px, `#1a5276`, top center):** "Same Dialog, Three Timings: Camera Grants per 100 Users".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` tick labels.
- **Bars (width 110, centered at x = 170, 360, 550), heights from values `[38, 71, 86]`:**
  - "cold at launch": orange `#d95926`, value 38
  - "at the camera tap": blue `#2a78d6`, value 71
  - "explainer, then ask": green `#008300`, value 86
- **Labels:** bold 13px `#2c3e50` value on top of each bar; 12px `#444` category label under the baseline.
- **Annotation (bold 13px magenta `#d55181`, upper left near x=80, y=70):** "denied at launch: only ~4 of 62 ever undo it in Settings".
- **Caption (12px `#444`, bottom right):** "grant rates illustrative".

## Why the OS Locks the Camera in the First Place

**Tags:** `where it's used` (blue), `privacy by design` (green), `product funnel` (orange)

- **The sandbox** — each app runs in its own sealed box; camera, mic, contacts sit outside it
- **The gate** — a permission grant is the only door through the sandbox wall, one per resource
- **Privacy by design** — the default is no access; the user opts in rather than opting out
- **The funnel** — a denied camera is a broken funnel: the share-a-photo flow simply dead-ends
- **The stakes** — at 1,000 users, 38% vs 86% grant means 380 vs 860 people can use the feature
- **Revocable** — the user can shut the door later, so apps must survive losing a grant mid-life

*Example (italic):* The meal-journal app never sees the contact list or GPS — not because it promises not to look, but because the sandbox has no door it never asked to open.

**Key point:** The prompt is the user-visible tip of the sandbox model — nothing sensitive flows until a human opens a gate, so every gate you ask for has to earn its moment.

### Visualization (canvas `c3`, 720×300)

Sandbox diagram: the app inside a sandbox box, four resources outside it, one granted gate open and three locked.

- **Title (bold 15px, `#1a5276`, top center):** "The Sandbox: Every Resource Sits Behind Its Own Gate".
- **Sandbox:** large rounded rectangle from (250, 80) to (470, 250), 2px ink `#1a5276` border, fill `rgba(26,82,118,0.06)`, 12px ink label "app sandbox" at its top; inner blue `#2a78d6` rounded box (140×40, fill `rgba(42,120,214,0.15)`) centered at (360, 165) labeled "meal-journal app" (12px `#2c3e50`).
- **Resource boxes (110×36, 8px radius, 12px text):** "camera" at (80, 100), "microphone" at (80, 200), "location" at (555, 100), "contacts" at (555, 200).
- **Gates:** camera → app: solid 3px green `#008300` arrow with bold 12px green label "granted"; the other three connect with dashed (dash 5/4) 2px mute `#6b7280` lines, each broken by a small 12px `#6b7280` "locked" label, resource boxes stroked `#6b7280`.
- **Annotation (bold 13px green `#008300`, bottom center near y=285):** "one grant opened one gate — the other three stay shut".
- **Caption (12px `#444`, bottom right):** "schematic".

## The Launch-Day Wall of Prompts

**Tags:** `common mistake` (red), `prompt fatigue` (orange)

- **The mistake** — firing every prompt at first launch, before the app has shown any value
- **Prompt fatigue** — each back-to-back dialog trains the finger to reach Deny a little faster
- **The slide** — per-prompt grants fall as the wall grows: 61, 47, 35, 26, 19 of 100 users
- **Burned forever** — each denial spends a one-shot dialog at the worst possible moment
- **The fix** — rank permissions by need and ask at the feature that uses each, never sooner

*Example (italic):* An app that demands camera, mic, location, contacts, and photos in its first minute gets the fifth prompt granted by only 19 of 100 users.

**Common mistake:** Treating prompts as free and retryable. Each one is a single spendable ask — a wall of five at launch burns all five where the user has the least reason to say yes.

### Visualization (canvas `c4`, 720×300)

Vertical bar chart: grant rate per 100 users by the prompt's position in a five-prompt launch wall.

- **Title (bold 15px, `#1a5276`, top center):** "The Launch Wall: Each Extra Prompt Grants Worse".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; y = 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` tick labels.
- **Bars (width 70, centered at x = 130, 240, 350, 460, 570), heights from values `[61, 47, 35, 26, 19]`:** colors in order blue `#2a78d6`, violet `#4a3aa7`, yellow `#c98500`, orange `#d95926`, red `#e74c3c`; 12px `#444` labels "1st prompt" … "5th prompt" under the baseline, bold 13px `#2c3e50` value on top of each bar.
- **Annotation (bold 13px red `#e74c3c`, upper right near x=430, y=75):** "the 5th ask: 19 of 100 — and denials rarely reverse".
- **Caption (12px `#444`, bottom right):** "per-prompt grant rates illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); every number is invented and labeled illustrative — grant rates per 100 by timing `[38, 71, 86]`, Settings recovery ~4 of 62 deniers, launch-wall per-prompt grants `[61, 47, 35, 26, 19]`, and the 380-vs-860-per-1,000 funnel in the text follows from the 38% and 86% rates; the sandbox diagram is schematic with no data.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
