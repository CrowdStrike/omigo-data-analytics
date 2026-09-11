# Diffusion Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Diffusion Models

**Subtitle:** Image generators don't paint stroke by stroke — they start from pure static and remove a little noise at a time until a picture is left

## From Static to a Picture

**Tags:** `core idea` (blue), `denoising` (green)

- **Training** — take a real photo, add noise bit by bit until it's pure static; the model learns to undo one step
- **Generating** — start from fresh static and apply that undo step about 50 times; a picture emerges
- **The prompt** — the text steers every denoise step toward "a cat on a bike" instead of anything else
- **One skill** — the model only ever learns "remove a little noise"; repetition does the painting
- **The name** — "diffusion" is the noising direction; generation runs the film backwards

*Example (italic):* Like a sculptor who "removes everything that isn't the statue" — each pass strips away a little static that isn't the image.

**Key point:** A diffusion model learns one small skill — undo a little noise — and an image is that skill applied dozens of times in a row.

### Visualization (canvas `c1`, 720×300)

Five square frames left to right showing the same scene going from pure static to clean, with step labels underneath.

- **Title (bold 15px, `#1a5276`, top center):** "One Image Emerging Over 50 Denoise Steps".
- **Frames:** five 116×116 squares, tops y=60, left edges x = `[36, 173, 310, 447, 584]`, 1.5px `#6b7280` borders, white fill.
- **Scene (drawn in every frame, opacity rising left to right with alpha = `[0.05, 0.25, 0.55, 0.85, 1.0]`):** sky band `#2a78d6` rect (full width, top 40% of frame), sun `#c98500` filled circle radius 12 at (frame x+82, y+26), hill `#008300` filled triangle across the bottom 45%.
- **Noise (density falling left to right, counts = `[420, 300, 170, 60, 0]` dots per frame):** 2×2 px gray `#6b7280` dots at deterministic pseudo-random positions from a `Math.sin(i * 12.9898 + f * 78.233) * 43758.5453` fractional hash (NO `Math.random()`).
- **Step labels (bold 12px `#1a5276`, centered under each frame at y=200):** "step 0", "step 10", "step 25", "step 40", "step 50"; 11px `#6b7280` sublabels at y=216: "pure static", "shapes hinted", "scene visible", "details settle", "clean image".
- **Arrows:** 1.5px `#6b7280` short arrows between frames at mid-height (y=118).
- **Annotation (bold 12px orange `#d95926`, centered at y=252):** "every arrow is the same learned move: remove a little noise".
- **Caption (11px `#444`, bottom right, y=290):** "frames illustrative; real models denoise a number grid, not dots".

## Denoising Three Pixels by Hand

**Tags:** `worked example` (blue), `predict the noise` (orange)

- **A tiny image** — three pixel brightnesses: 80, 40, 10 (0 = black, 100 = white)
- **Add noise** — sprinkle +15, −20, +5 onto the pixels: the noisy image reads 95, 20, 15
- **The training pair** — input: the noisy 95, 20, 15; target: the noise +15, −20, +5 that was added
- **The model's job** — look at a noisy image and predict what noise was sprinkled on it
- **Recover** — subtract the predicted noise: 95−15, 20+20, 15−5 gives back 80, 40, 10

*Example (italic):* Training data is free to make — take any photo, sprinkle known noise, and the right answer is the noise you just sprinkled.

**Key point:** The model never learns "draw a cat" — it learns "spot the noise"; redo the three subtractions and you've run one denoise step.

### Visualization (canvas `c2`, 720×300)

Two panels: the forward "add noise" direction on the left, the learned "predict & subtract" direction on the right, with the three pixel values shown as labeled bars.

- **Title (bold 15px, `#1a5276`, top center):** "Forward: Add Known Noise — Backward: Predict and Subtract It".
- **Left panel:** rounded rect x=30, y=48, 320×200, fill `rgba(107,114,128,0.05)`, 2px `#6b7280` border; bold 13px `#6b7280` header "training: noise it, remember the noise" centered at (190, 68).
  - Three pixel groups (centers x = `[110, 190, 270]`): clean bar (fill `#2a78d6`, width 28, baseline y=200, height = value × 1.0 px for values 80/40/10) and noisy bar beside it (fill `#c98500`, values 95/20/15); 11px value labels above each bar in the bar's color; 11px `#444` labels "px 1", "px 2", "px 3" at y=216.
  - Noise labels (bold 11px `#d55181`, centered between each bar pair at y=90): "+15", "−20", "+5".
  - Legend (11px, y=236, from x=60): blue swatch "clean", yellow swatch "noisy", magenta text "noise added".
- **Right panel:** rounded rect x=380, y=48, 310×200, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; bold 13px `#008300` header "generating: predict noise, subtract" centered at (535, 68).
  - Three rows of 12px `#2c3e50` text, left-aligned at x=400, y=100/122/144: "sees 95 → predicts +15 → 80", "sees 20 → predicts −20 → 40", "sees 15 → predicts +5 → 10".
  - Bold 12px `#008300` line centered at (535, 180): "clean image recovered: 80, 40, 10".
  - 11px `#6b7280` line centered at (535, 202): "real generation repeats this ~50 times".
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "the target is the noise itself — spot it, subtract it, and the image is back".
- **Caption (11px `#444`, bottom right, y=292):** "3-pixel example, numbers exact".

## Where Diffusion Shows Up

**Tags:** `where it's used` (blue), `image generation` (green)

- **Text-to-image** — the well-known image generators are diffusion models under the hood
- **Beyond images** — the same recipe generates video clips, audio, and even molecule shapes
- **Vs LLMs** — an LLM writes left to right, one token at a time; diffusion refines the whole canvas each pass
- **The speed dial** — fewer denoise steps is faster but rougher; more steps is slower but cleaner
- **Why it won** — adding noise is easy, so perfect training pairs can be made from any photo for free

*Example (italic):* Anything you can add noise to, you can train a denoiser for — a video clip, a sound wave, or a protein shape.

**Key point:** LLMs generate sequentially, diffusion refines the whole output in repeated passes — two different routes to "generate".

### Visualization (canvas `c3`, 720×300)

Two lanes contrasting how an LLM and a diffusion model build their output.

- **Title (bold 15px, `#1a5276`, top center):** "Two Ways to Generate: Append vs Refine".
- **LLM lane:** rounded rect x=30, y=52, 660×92, fill `rgba(42,120,214,0.05)`, 2px `#2a78d6` border; bold 13px `#2a78d6` header "LLM: append one token at a time" at x=48, y=74 (left-aligned).
  - Five word boxes (rounded rects 86×28, tops y=96, left edges x = `[48, 152, 256, 360, 464]`, fill `rgba(42,120,214,0.12)`, 1.5px `#2a78d6` border, 12px `#1a5276` centered labels): "The", "cat", "sat", "on", "the"; a sixth dashed-border box at x=568 labeled "mat?" in `#6b7280`; 1.5px `#6b7280` arrows between boxes at mid-height.
- **Diffusion lane:** rounded rect x=30, y=160, 660×92, fill `rgba(0,131,0,0.05)`, 2px `#008300` border; bold 13px `#008300` header "diffusion: refine the whole canvas each pass" at x=48, y=182 (left-aligned).
  - Four canvas boxes (rounded rects 86×48, tops y=194, left edges x = `[48, 214, 380, 546]`, 1.5px `#008300` border): each holds a mini sun-and-hill scene at alphas `[0.15, 0.45, 0.75, 1.0]` plus gray noise dots with counts `[80, 40, 15, 0]` (same deterministic hash as c1); 1.5px `#6b7280` arrows between boxes labeled 11px `#6b7280` "denoise" above.
- **Annotation (bold 12px orange `#d95926`, centered at y=274):** "the LLM's output grows; the diffusion canvas is always full — it just gets cleaner".
- **Caption (11px `#444`, bottom right, y=294):** "lanes simplified".

## Not a Collage of Stored Photos

**Tags:** `common mistake` (red), `text-to-image` (orange)

- **No lookup** — at generation time there is no image database; only learned patterns in the knobs
- **The static matters** — different starting static gives a different picture from the same prompt
- **Same prompt twice** — two images, not one; the dice live in the starting noise
- **Steps ≠ resolution** — the step count sets how refined the denoising is, not the pixel count
- **Memorization caveat** — an image repeated many times in training can be reproduced too closely

*Example (italic):* Ask for "a lighthouse at sunset" twice and you get two different lighthouses — the prompt steered two different patches of static.

**Common mistake:** Thinking the model pastes together stored photos — it denoises fresh static using learned patterns; nothing is looked up or cropped.

### Visualization (canvas `c4`, 720×300)

One prompt fanning out to two different starting noises and two different final images.

- **Title (bold 15px, `#1a5276`, top center):** "Same Prompt, Different Static, Different Picture".
- **Prompt box:** rounded rect x=40, y=118, 170×56, fill `rgba(26,82,118,0.08)`, 2px `#1a5276` border; bold 12px `#1a5276` centered two-line label: "prompt:" / "\"lighthouse at sunset\"".
- **Noise boxes:** two 96×96 squares at (300, 56) and (300, 168), 1.5px `#6b7280` borders, filled with gray noise dots (counts 260 each) from two different deterministic hash offsets; bold 11px `#6b7280` labels "static A" and "static B" centered under each at y+110.
- **Image boxes:** two 96×96 squares at (520, 56) and (520, 168), 1.5px `#008300` borders: top one draws a sun high-left (`#c98500` circle at x+30, y+28) with a tall thin lighthouse rect (`#d55181` 12×48 at x+58, y+38); bottom one draws the sun low-right (circle at x+66, y+40) and the lighthouse left (rect at x+22, y+30); both over a `#2a78d6` sea band on the bottom third; bold 11px `#008300` labels "image A" and "image B" centered under each at y+110.
- **Arrows:** 1.5px `#6b7280` from the prompt box to each noise box, and from each noise box to its image box, arrowheads at the ends; 11px `#6b7280` label "~50 denoise steps" above each noise→image arrow.
- **Annotation (bold 12px orange `#d95926`, centered at y=284):** "the prompt steers, the static decides the rest — no photo was looked up".
- **Caption (11px `#444`, bottom right, y=297):** "sketches illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all noise-dot positions come from a deterministic `Math.sin`-hash (no `Math.random()`); the c2 pixel numbers must read exactly clean 80/40/10, noise +15/−20/+5, noisy 95/20/15 to match the text; frame alphas and dot counts are the arrays above.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
