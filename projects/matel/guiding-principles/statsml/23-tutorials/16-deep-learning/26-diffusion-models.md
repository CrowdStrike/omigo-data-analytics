# Diffusion Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Diffusion Models

**Subtitle:** A diffusion model learns to remove a little noise at a time — run that cleanup on pure static, and a brand-new picture comes out

## A Photo Drowning in Static

**Tags:** `core idea` (blue), `adding noise` (green), `running example` (orange)

- **The photo** — an old TV shows a photo of a dog; every second the screen adds a bit more static
- **Step by step** — after a few seconds the dog is grainy, after many the screen is pure static
- **Nothing lost at once** — each single step barely changes the picture; only the pile-up destroys it
- **The training trick** — show a model a slightly-static photo and ask: which specks are the static?
- **Easy homework** — we added the static ourselves, so we can grade every guess the model makes
- **The skill learned** — a cleaner that can peel one layer of static off any picture it is shown

*Example (italic):* Comparing second 3 to second 4 of the TV, the dog looks nearly identical — spotting that one small dose of static is a learnable task.

**Key point:** Diffusion training is millions of rounds of one simple quiz: here is a picture with a little added static — point out the static.

### Visualization (canvas `c1`, 720×300)

Three side-by-side mini-panels showing the same 1-D brightness slice of the dog photo (16 pixels across) at three moments: clean, half static, pure static.

- **Title (bold 15px, `#1a5276`, top center):** "One Row of the Photo: the Shape Drowns in Static".
- **Panels:** three plot areas of width 200, x origins 40 / 275 / 510, baseline y=245, plot height 170; y = brightness 0–100 (no y ticks, one light `#e5e9ef` gridline at 50); panel titles bold 13px `#1a5276` above each: "clean photo", "half static", "pure static".
- **Panel 1 (clean, blue `#2a78d6` 3px line, fill `rgba(42,120,214,0.15)`):** 16 evenly spaced points, brightness `[12, 15, 20, 30, 48, 66, 80, 86, 86, 80, 66, 48, 30, 20, 15, 12]` — one smooth hill (the dog's bright face).
- **Panel 2 (half static, orange `#d95926` 3px line):** each value is half clean + half static: `[34, 18, 48, 35, 69, 51, 70, 51, 83, 65, 46, 59, 38, 53, 23, 39]` — the hill is still faintly visible.
- **Panel 3 (pure static, mute `#6b7280` 3px line):** the static alone: `[55, 20, 75, 40, 90, 35, 60, 15, 80, 50, 25, 70, 45, 85, 30, 65]` — no hill left.
- **Arrows:** small 2px `#444` arrows between panels at mid-height labeled 12px `#444` "+ static".
- **Annotation (bold 12px orange `#d95926`, above panel 2):** "each step is small — only the pile-up kills the picture".
- **Caption (12px `#444`, bottom right):** "illustrative brightness values, 0–100 scale".

## Following One Pixel, By Hand

**Tags:** `worked example` (blue), `do the arithmetic` (green)

- **One pixel** — take a single pixel from the dog's face: brightness 80 on a 0-to-100 scale
- **The noising rule** — each step keeps half the current value and blends in half a static value
- **Four steps** — with static values 20, 90, 40, 60 the pixel goes 80 → 50 → 70 → 55 → 57.5
- **Check one step** — step 1 is 0.5×80 + 0.5×20 = 50; every later step is the same one-line sum
- **The original fades** — after four halvings only 0.5⁴ = 6.25% of the 80 remains: 5 points of 57.5
- **Undo one step** — if the model guesses the last static was 60, then 2×57.5 − 60 = 55, one step cleaner

*Example (italic):* The model never sees the whole journey — it just learns "at 57.5, the static mixed in was probably about 60", and that single guess rewinds one step.

**Key point:** One noising step is 0.5×pixel + 0.5×static, and one denoising step is 2×pixel − guessed static — the entire engine is arithmetic you can redo by hand. Real models take ~1,000 much gentler steps; we use 4 big ones so it fits on paper.

### Visualization (canvas `c2`, 720×300)

Single-panel line chart tracking the one pixel's brightness across the four noising steps (orange, forward) and back along the same values during denoising (green dashed, reverse arrows).

- **Title (bold 15px, `#1a5276`, top center):** "One Pixel: 80 → 50 → 70 → 55 → 57.5, Then Rewound".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; x = step 0 to 4, tick labels "step 0" … "step 4" (12px `#444`); y = brightness 0–100, light `#e5e9ef` gridlines at 25, 50, 75 with 12px `#444` labels.
- **Forward path (noising):** orange `#d95926` 3px line through hardcoded points (step, brightness) = `[[0, 80], [1, 50], [2, 70], [3, 55], [4, 57.5]]`, 7px orange dots, bold 12px orange value labels above each dot: "80", "50", "70", "55", "57.5"; 12px orange label "adding static →" near the line's start.
- **Static doses:** 11px `#6b7280` labels under the axis between ticks: "static 20", "static 90", "static 40", "static 60".
- **Reverse path (denoising):** green `#008300` 3px dashed (dash 6/4) line through the same points offset 8px above, with left-pointing arrowheads at each segment; bold 12px green label "← model peels static off" near step 3.5, y=95.
- **Annotation (bold 12px violet `#4a3aa7`, near step 3.2, y=210):** two lines: "only 6.25% of the original pixel" / "survives inside 57.5".
- **Caption (12px `#444`, bottom right):** "illustrative — one pixel, 0–100 brightness".

## From Pure Static to Pictures That Never Existed

**Tags:** `where it's used` (blue), `generation` (green), `big picture` (orange)

- **The flip** — to generate, hand the trained cleaner pure static and let it denoise step after step
- **Never existed** — the static was fresh, so the "restored" photo is brand new, not a stored copy
- **Different seeds** — different starting static gets sculpted into a different picture every time
- **Behind the tools** — text-to-image generators run exactly this loop, with text steering each step
- **Beyond photos** — the same recipe generates audio, video frames, and molecules: anything noisable
- **Why it wins** — a thousand small cleanups are far easier to learn than one giant leap to a photo

*Example (italic):* Feed the cleaner a fresh screen of static and it gradually carves out a two-hill skyline no camera ever shot — new static, new picture.

**Key point:** Generation is denoising run from the end: start at pure static, remove a little noise at a time, and a new image condenses out.

### Visualization (canvas `c3`, 720×300)

Three side-by-side mini-panels running the c1 story in reverse on NEW static: pure static, half denoised, finished picture — a two-hill skyline that was never a training photo.

- **Title (bold 15px, `#1a5276`, top center):** "Run Backwards on Fresh Static: a Picture That Never Existed".
- **Panels:** same geometry as c1 — plot width 200, x origins 40 / 275 / 510, baseline y=245, plot height 170, y = brightness 0–100; panel titles bold 13px `#1a5276`: "fresh static", "half denoised", "new picture".
- **Panel 1 (fresh static, mute `#6b7280` 3px line):** 16 points `[70, 30, 85, 25, 60, 45, 90, 20, 55, 75, 35, 65, 15, 80, 40, 50]` — note this is DIFFERENT static than c1's.
- **Panel 2 (half denoised, aqua `#199e70` 3px line):** halfway blend `[43, 28, 65, 48, 71, 60, 73, 28, 40, 55, 45, 70, 49, 75, 43, 38]` — two bumps starting to emerge.
- **Panel 3 (new picture, green `#008300` 3px line, fill `rgba(0,131,0,0.12)`):** `[15, 25, 45, 70, 82, 75, 55, 35, 25, 35, 55, 75, 82, 70, 45, 25]` — a clean two-hill skyline, clearly not c1's one-hill dog.
- **Arrows:** small 2px `#444` arrows between panels at mid-height labeled 12px `#444` "− static".
- **Annotation (bold 12px green `#008300`, above panel 3):** "two hills, not the dog — a brand-new image".
- **Caption (12px `#444`, bottom right):** "illustrative — same denoiser, new starting static".

## Why Not One Big Jump?

**Tags:** `common mistake` (red), `small steps` (orange)

- **Not a filter** — a photo editor's denoise button removes light grain; this rebuilds from 100% static
- **Not one jump** — asked to name the photo behind pure static in one go, the honest answer is "any"
- **Mush average** — a one-jump model hedges across every plausible photo and outputs a gray blur
- **Tiny quizzes** — guessing the one small dose of static added at a single step is a solvable problem
- **Not memory** — the model stores a cleanup skill, not photos; each run composes something new

*Example (italic):* From pure static, a one-jump guesser outputs a flat smear hovering around brightness 50 — the average of every picture it might have been.

**Common mistake:** Thinking the model leaps from static to photo in one shot. It takes hundreds of small steps, and breaking the impossible jump into easy steps is exactly why it works.

### Visualization (canvas `c4`, 720×300)

Two side-by-side panels contrasting the same fresh static resolved two ways: one giant jump (gray mush) versus many small steps (the crisp two-hill picture).

- **Title (bold 15px, `#1a5276`, top center):** "Same Static, Two Strategies: One Jump vs Many Small Steps".
- **Panels:** two plot areas of width 290, x origins 60 and 400, baseline y=245, plot height 170, y = brightness 0–100 (light `#e5e9ef` gridline at 50); panel titles bold 13px: left `#e74c3c` "one big jump", right `#008300` "1,000 small steps".
- **Left panel (mush, red `#e74c3c` 3px line):** 16 points `[48, 49, 50, 52, 53, 54, 54, 54, 53, 52, 51, 50, 49, 48, 48, 47]` — nearly flat around 50; 12px `#e74c3c` label inside: "averages every possible photo".
- **Right panel (crisp, green `#008300` 3px line, fill `rgba(0,131,0,0.12)`):** the same two-hill picture as c3 panel 3: `[15, 25, 45, 70, 82, 75, 55, 35, 25, 35, 55, 75, 82, 70, 45, 25]`; 12px `#008300` label inside: "one committed, crisp picture".
- **Shared origin note:** 12px `#6b7280` label centered under both panels: "both start from the same fresh static as the chart above".
- **Annotation (bold 13px magenta `#d55181`, centered near y=285):** "hedging makes mush — small steps let the model commit".
- **Caption (12px `#444`, bottom right):** "illustrative brightness values".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** every brightness slice and the pixel path are the hardcoded literal arrays above (no `Math.random()`); c2's path values follow exactly from 0.5×pixel + 0.5×static with statics 20, 90, 40, 60 starting at 80; c1 panel 2 and c3 panel 2 are the literal half-blends of their neighbors; keep text numbers and chart numbers identical.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
