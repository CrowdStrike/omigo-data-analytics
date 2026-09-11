# Open-Weight Models

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Open-Weight Models

**Subtitle:** Publishing the trained weights lets anyone run the model themselves — which is not the same thing as open source, and not the same thing as free

## What Actually Gets Published — and What Doesn't

**Tags:** `core idea` (blue), `spectrum` (green), `reproducibility` (orange)

- **The artifact** — one large file of numbers plus a config and tokenizer, run offline on your own hardware, no per-token bill
- **Frozen version** — the checkpoint you downloaded cannot be silently updated or deprecated under you
- **The hosted contrast** — an API rents you behaviour you cannot inspect, copy, or pin to a fixed build
- **Also published** — the architecture and inference code, plus the licence terms that bind your use of it
- **Sometimes published** — the training code, the run logs, and the evaluation harness behind the scores
- **Rarely published** — the training data itself, and the recipe of mixtures, filters, and ordering
- **Openness is a spectrum** — "open weight" means weights only; open source would need recipe and data too
- **What you lose** — reproducibility: you can run the checkpoint exactly, you cannot rebuild it

*Example (italic):* A large open-weight release can be running locally tonight and still offer no path to rebuilding it, because the data and the mixture were never published.

**Key point:** Open weights turn a model from a service you call into an artifact you hold — but weights alone let you run it, not reproduce it, which is why this is not open source.

### Visualization (canvas `c1`, 720×300)

Checklist of the eight artifacts a release could ship, marked by how often each actually appears, with the run-it / rebuild-it split bracketed.

- **Title (bold 15px, `#1a5276`, top center):** "What a Typical Open-Weight Release Includes".
- **Eight rows**, baselines y = 56 + 23·i for i=0..7:
  - **Marker:** filled circle radius 8 centred at (48, y−4) in the row colour at 15% alpha with a 1.5px solid border; bold 11px symbol in the row colour, centred: "✓" for included, "~" for partial, "✗" for withheld.
  - **Name:** bold 12px row colour, left-aligned at x=66.
  - **Status:** 11px `#2c3e50`, left-aligned at x=290.
  - Rows, in order — (marker, name, status, colour):
    1. ✓ "trained weights" / "published — the whole point of the release" / green `#008300`
    2. ✓ "architecture + inference code" / "published — needed to load the weights" / green `#008300`
    3. ✓ "tokenizer + config" / "published — vocabulary and layer shapes" / green `#008300`
    4. ✓ "licence terms" / "published — and binding, restrictions included" / green `#008300`
    5. ~ "evaluation harness" / "sometimes — often just the score table" / yellow `#c98500`
    6. ~ "training code + logs" / "rarely — run script, hyperparameters, curves" / orange `#d95926`
    7. ✗ "data recipe and mixture" / "rarely — the filters, ratios, and ordering" / magenta `#d55181`
    8. ✗ "the training data itself" / "almost never — licensing and sheer size" / magenta `#d55181`
- **Brackets:** 1.5px vertical lines at x=628 with 6px end ticks pointing right — first from y=46 to y=131 in green `#008300`, second from y=138 to y=223 in magenta `#d55181`.
  - Bold 11px green at x=640: "enough" (y=82), "to RUN it" (y=98).
  - Bold 11px magenta at x=640: "needed" (y=174), "to REBUILD" (y=190).
- **Annotation (bold 12px orange `#d95926`, centered at y=252):** "you can run it exactly — you cannot rebuild it".
- **Caption (11px `#444`, bottom right, y=290):** "typical release; individual releases vary".

## The Licence Still Binds You

**Tags:** `common mistake` (red), `licence terms` (orange)

- **Not automatically OSI** — many open-weight licences fail the open-source definition on use restrictions
- **Acceptable-use policy** — named categories of use are forbidden outright, and the ban travels with the weights
- **Commercial gate** — some licences require a separate agreement above a stated user or revenue threshold
- **Attribution and naming** — derived models may have to carry a required name prefix and a notice file
- **Output restrictions** — some terms can forbid using the outputs to train or improve a competing model
- **No warranty, no indemnity** — weights ship as-is, while a paid API often does carry an indemnity clause
- **Redistribution** — passing weights to someone else usually means passing the same licence along too

*Example (italic):* A public checkpoint fine-tuned into a product can carry a licence that requires a name prefix and forbids training a rival model on its outputs.

**Common mistake:** Reading "downloaded from a public page" as "unrestricted". A public download is still a licence you accepted, and it is usually not the permissive one you assumed.

### Visualization (canvas `c2`, 720×300)

Map of six restriction families that commonly appear in open-weight licences, as a 3×2 grid of cards.

- **Title (bold 15px, `#1a5276`, top center):** "Free to Download, Still Not Unrestricted".
- **Six cards:** rounded rects 200×76, r=6, at x = `[40, 260, 480]` and y = `[56, 148]`; 2px border in the card colour, fill the same colour at 6% alpha; bold 12px coloured header centred at (x+100, y+22); two 11px `#2c3e50` centred lines at y+42 and y+60:
  - **(40, 56) orange `#d95926`** — "acceptable use" / "named uses forbidden" / "outright, no exceptions"
  - **(260, 56) blue `#2a78d6`** — "commercial gate" / "above a stated threshold," / "ask for a separate licence"
  - **(480, 56) aqua `#199e70`** — "naming + notice" / "derived models must carry" / "a prefix and a notice file"
  - **(40, 148) violet `#4a3aa7`** — "output limits" / "may not train a competing" / "model on the outputs"
  - **(260, 148) magenta `#d55181`** — "as-is, no warranty" / "no indemnity — a paid API" / "often does provide one"
  - **(480, 148) yellow `#c98500`** — "redistribution" / "pass the weights on and" / "the licence goes with them"
- **Annotation (bold 12px orange `#d95926`, centered at y=254):** "read it before you ship, not after".
- **Caption (11px `#444`, bottom right, y=290):** "simplified; not legal advice".

## What Running It Yourself Actually Costs

**Tags:** `worked example` (blue), `rule of thumb` (green), `total cost` (orange)

- **Weights are free, serving is not** — the licence fee is zero and the GPU bill starts on day one
- **VRAM arithmetic** — parameters × bytes each: 8B params at 16-bit (2 bytes) = 16 GB of weights
- **Overhead on top** — KV cache and activations add roughly 25%, so about 20 GB — a 24 GB card
- **It scales fast** — 70B params at 16-bit is 140 GB of weights: several GPUs, not one
- **Quantization is the lever** — 8-bit halves that 70B to 70 GB, 4-bit quarters it to 35 GB, at some quality cost
- **Idle GPUs bill like busy ones** — low or spiky traffic favours per-token pricing over a reserved box
- **You now own operations** — patching, security, uptime, and re-running your evals on every upgrade
- **Break-even is utilisation** — the crossover is arithmetic about volume, not a question of ideology

*Example (italic):* Illustrative: at $2,400/month for a reserved GPU box and $3.00 per million tokens on an API, the two cost the same at 800 million tokens a month — 2,400 ÷ 3 = 800.

**Key point:** Self-hosting wins on steady high volume and on control; an API wins on low or bursty traffic. Compute your own crossover from your own token counts before choosing a side.

### Visualization (canvas `c3`, 720×300)

Two-line monthly-cost chart — flat self-hosted GPU cost vs linear per-token API cost — with the break-even volume computed in JS from the plotted series and printed.

- **Title (bold 15px, `#1a5276`, top center):** "Flat GPU Bill vs Per-Token Bill (illustrative)".
- **Data (hardcoded, no randomness):** `vols = [0, 200, 400, 600, 800, 1000, 1200]` in millions of tokens per month; `flat = 2400` dollars for every volume; `apiRate = 3.0` dollars per million tokens, so `api[i] = vols[i] * 3.0` → `[0, 600, 1200, 1800, 2400, 3000, 3600]`.
- **Axes:** origin x=80, baseline y=230, plot right x=660, plot top y=60; y scale $0–$4,000 (so 0.0425 px per dollar), x scale 0–1,200 M (0.483333 px per million).
  - Gridlines 1px `#e5e9ef` at $1k/$2k/$3k/$4k (y = 187.5, 145, 102.5, 60); 11px `#6b7280` right-aligned labels at x=72: "$1k", "$2k", "$3k", "$4k".
  - Axis lines 1px `#999`; x ticks at 0/300/600/900/1200 M (x = 80, 225, 370, 515, 660) with 11px `#444` centred labels at y=247: "0", "300M", "600M", "900M", "1.2B".
  - X-axis title 12px `#444` centred at (370, 264): "tokens served per month".
- **Self-hosted line:** 2.5px `#008300` horizontal at y=128 from x=80 to x=660; bold 12px `#008300` left-aligned label at (110, 118): "self-hosted: flat $2,400/mo".
- **API line:** 2.5px `#2a78d6` polyline through the seven `api` points; bold 12px `#2a78d6` left-aligned label at (505, 86): "API: $3 per 1M tokens".
- **Break-even (computed, never hardcoded):** find the first index where `api − flat` changes sign, linearly interpolate for the zero crossing, convert to pixels. Draw a 1.5px `#d95926` dashed (5/4) vertical from the baseline to the crossing, a 5px `#d95926` filled dot at the crossing, and a bold 11px `#d95926` centred label 16px above it reading the formatted computed volume (e.g. "800M/mo") — value formatted with `v >= 1000 ? (v/1000).toFixed(1)+'B' : Math.round(v)+'M'`.
- **Zone labels (11px `#6b7280`):** "API cheaper here" centred at (255, 208); "self-hosting cheaper here" centred at (560, 208).
- **Annotation (bold 12px orange `#d95926`, centered at y=281):** text built from the computed crossing — "below the crossing the API wins; above it the box does".
- **Caption (11px `#444`, bottom right, y=296):** "illustrative; hardware and prices vary".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), matching `48-copyright-complications` exactly. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then three `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%). Three canvases: `c1` artifact checklist, `c2` licence-restriction map, `c3` cost break-even chart.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Bullet budget:** max 8 bullets per section — 9 only if a section genuinely needs it, never more. Current counts: 8 / 7 / 8. When a section runs over, merge two adjacent bullets that share a subject into one bold-label + phrase line (may run 90–115 chars, still one line at normal width); never delete a fact and never split down to short stubs.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundRect` helper.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`.
- **Data integrity:** no `Math.random()` anywhere — every figure is a hardcoded literal. The c3 break-even is computed at render time by sign-change interpolation over the two plotted series and printed from that computation; it must never be written as a literal string. The VRAM arithmetic in section 3 closes exactly: 8e9 × 2 bytes = 16 GB, +25% ≈ 20 GB; 70e9 × 2 = 140 GB, × 1 byte (8-bit) = 70 GB, × 0.5 bytes (4-bit) = 35 GB. The cost example closes: $2,400 ÷ $3 per million = 800 million tokens.
- **Content discipline:** strictly vendor-neutral — no model families, labs, or product names anywhere on the page; use "a large open-weight release", "a research lab", "Vendor A". No named-actor scenarios. Cost and licence figures labelled illustrative; licence canvas carries "not legal advice". Neutral educational tone; the page teaches the concept, not a tool choice.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
