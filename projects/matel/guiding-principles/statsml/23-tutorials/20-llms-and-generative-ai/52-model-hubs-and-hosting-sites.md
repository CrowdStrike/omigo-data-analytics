# Model Hubs & Hosting Sites

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Model Hubs &amp; Hosting Sites

**Subtitle:** A public hub turns a trained model into a downloadable dependency addressed by name and revision — and the listing publishes far more than weights, because a model can be perfectly downloadable and still unrunnable for you

## A Package Registry, But the Packages Are Models

**Tags:** `core idea` (blue), `dependency management` (green)

- **The mental model** — a hub is to trained models what a package registry is to code libraries
- **One entry** — a versioned repo of weights, config, tokenizer, and model card; datasets use the same shape
- **The address** — `namespace/model-name` plus a revision; a client library resolves it and caches it locally
- **Big-file storage** — content-addressed blobs so multi-gigabyte weights dedupe and resume mid-download
- **Versioning** — git-style history lets you pin one exact revision instead of tracking a moving branch
- **Beyond files** — leaderboards, hosted demo apps, task and licence filters, and endpoints you can call remotely
- **The consequence** — picking a model becomes dependency management, not just a modelling choice
- **The one real difference** — a library is kilobytes of readable text; a model is gigabytes of opaque numbers

*Example (illustrative):* One model name in a config file resolves through the client library, caches about 16 GB on the first run, and loads from disk on every run after.

**Key point:** Once a model is addressed by name and revision, everything you already know about managing dependencies applies to it — including the failure modes.

### Visualization (canvas `c1`, 720×300)

Side-by-side anatomy of a library dependency and a model dependency, with the shared resolve→download→cache→load flow underneath.

- **Title (bold 15px, `#1a5276`, top center):** "Same Shape, Different Payload".
- **Two panels:** rounded rects 320×150, radius 6, at x = `[25, 375]`, y=44. Left border 2px `#2a78d6`, fill `rgba(42,120,214,0.06)`; right border 2px `#199e70`, fill `rgba(25,158,112,0.06)`.
  - **Headers (bold 13px, centered at panel center x, y=66):** left `#2a78d6` "library dependency"; right `#199e70` "model dependency".
  - **Id line (bold 12px `#1a5276`, left-aligned at x = panel x + 16, y=92):** left "package-name @ 2.4.1"; right "namespace/model-name @ rev".
  - **Content lines (11px `#2c3e50`, left-aligned at x = panel x + 16, y = 114/132/150/168):**
    - left: "code files", "manifest + metadata", "README / docs", "resolved by a package manager"
    - right: "weight files (often multi-GB)", "config + tokenizer files", "model card (the README)", "resolved by a client library"
- **Flow strip:** four rounded rects 140×38, radius 6, at x = `[30, 205, 380, 555]`, y=212; border 1.5px `#4a3aa7`, fill `rgba(74,58,167,0.07)`; bold 12px `#4a3aa7` centered labels at y=236: "resolve name", "download files", "cache locally", "load in memory".
- **Arrows:** 1.5px `#6b7280` between consecutive flow boxes at y=231, from box right edge to next box left edge minus 6, with arrowheads.
- **Annotation (bold 12px orange `#d95926`, centered at y=272):** "one flow for both — the model just weighs a million times more".
- **Caption (11px `#444`, bottom right, y=292):** "cache size illustrative".

## What a Listing Actually Publishes

**Tags:** `worked example` (blue), `listing fields` (green), `common mistake` (red)

- **Weights and size** — parameter values across several multi-gigabyte shards, plus a headline count like 8B
- **Architecture and config** — the model family plus layer and dimension settings, which decide what code can load it
- **Weight file format** — the serialization container; tensor-only formats load safely, pickle-style ones do not
- **Precision variant** — the same model republished at different bytes per parameter, so one listing can carry several
- **Hardware requirement** — 8B params × 2 bytes = 16.0 GB of weights, and ~20% for KV cache and activations makes 19.2 GB
- **Tokenizer and context length** — the vocabulary files, plus the largest window the weights were trained for
- **Licence and gating** — what you may do with it, and whether access needs acceptance or approval
- **Provenance and evals** — base model or fine-tune of what, and scores the uploader reported rather than measured

*Example (illustrative):* At 16-bit the 8B model needs 19.2 GB and will not fit a 16 GB card; the same listing's 4-bit variant needs 8 × 0.5 = 4.0 GB, or 4.8 GB with the same overhead, and fits comfortably.

**Key point:** The weights are only one field. The architecture and config decide whether your stack can load the model at all, and parameter count times precision decides whether your hardware can hold it — a model can be perfectly downloadable and still unrunnable for you.

### Visualization (canvas `c2`, 720×300)

Annotated mock listing on the left with ten published fields, and a right-hand panel computing the memory requirement from the parameter count and precision at render time.

- **Title (bold 15px, `#1a5276`, top center):** "One Listing, Ten Fields — Two of Them Decide If It Runs".
- **Listing card:** rounded rect x=25, y=44, 400×198, radius 6, fill `rgba(26,82,118,0.05)`, 2px `#1a5276` border.
- **Ten field rows** at y = `[62, 80, 98, 116, 134, 152, 170, 188, 206, 224]`. Each row draws a bold 11px colored label left-aligned at x=40, then the 11px `#2c3e50` value immediately after it (position taken from `ctx.measureText` on the label, no fixed value column):
  - `#2a78d6` "weights: " — "4 shard files, 16.1 GB total"
  - `#2a78d6` "parameters: " — "8B"
  - `#4a3aa7` "architecture: " — "decoder-only, 32 layers, dim 4096"
  - `#d95926` "weight format: " — "tensor-only (safe) vs pickle-style"
  - `#199e70` "precision variants: " — "16-bit · 8-bit · 4-bit"
  - `#199e70` "tokenizer: " — "vocab files, 128k tokens"
  - `#199e70` "context length: " — "8,192 tokens"
  - `#c98500` "licence / access: " — "research-only · gated"
  - `#d55181` "provenance: " — "fine-tune of other-namespace/base-model"
  - `#6b7280` "eval: " — "71.2 public benchmark (self-reported)"
- **Memory panel:** rounded rect x=445, y=44, 250×198, radius 6, fill `rgba(42,120,214,0.06)`, 2px `#2a78d6`; bold 12px `#2a78d6` centered header "will your hardware hold it?" at y=66.
  - **Computed lines (11px `#2c3e50`, left-aligned at x=458).** JS computes `w16 = 8 * 2`, `t16 = w16 * 1.2`, `w4 = 8 * 0.5`, `t4 = w4 * 1.2` and prints one decimal each; nothing is hardcoded as text.
    - y=92: "8B × 2 bytes = 16.0 GB weights"
    - y=108: "+20% KV cache & activations"
    - y=124 (bold 12px `#e74c3c`): "= 19.2 GB → 16 GB card fails"
    - y=152: "4-bit variant:"
    - y=168: "8B × 0.5 bytes = 4.0 GB weights"
    - y=184 (bold 12px `#008300`): "= 4.8 GB → fits comfortably"
  - **Divider:** 1px `#e5e9ef` horizontal line from x=458 to x=682 at y=138.
  - **Footer (bold 11px `#6b7280`, centered, y=228):** "same weights, different bytes per value".
- **Annotation (bold 12px orange `#d95926`, centered at y=268):** "downloadable is not runnable — config decides if it loads, size decides if it fits".
- **Caption (11px `#444`, bottom right, y=290):** "Illustrative Example — invented listing; memory arithmetic computed at render time".

## Reading It Before You Trust It

**Tags:** `common mistake` (red), `supply chain` (orange)

- **Publisher** — a known lab, a company account, or an anonymous handle sets how hard you verify
- **Popularity is not quality** — downloads measure visibility and age, not accuracy on your particular task
- **Card completeness** — a card with no training data or evals stated is an unknown, not a neutral
- **Weights are not inert** — some formats rebuild objects on load, so loading can run code; tensor-only ones cannot
- **Hidden behaviour** — a fine-tune can act unlike its base model, and the card need not disclose it
- **Name and revision traps** — typosquatted names sit one character away, and a branch can change under you
- **No source to read** — you cannot diff weights the way you diff code, so test behaviour instead
- **Mitigations** — pin the revision, verify the publisher, prefer tensor-only, scan, and eval before shipping

*Example (illustrative):* A team sorted by downloads, pinned nothing, and fetched a branch; a quiet re-upload changed the model under a build that had passed review two weeks earlier.

**Common mistake:** Sorting by downloads and taking the top row. A model is a dependency you cannot read, so the controls are the supply-chain ones — pin the revision, trust the publisher, prefer safe formats, and evaluate before you ship.

### Visualization (canvas `c3`, 720×300)

Two load paths from a downloaded repo, plus the two name-level traps as a bottom strip.

- **Title (bold 15px, `#1a5276`, top center):** "Two Load Paths, Two Name Traps".
- **Repo box:** rounded rect x=25, y=95, 130×56, radius 6, fill `rgba(26,82,118,0.08)`, 2px `#1a5276`; bold 12px `#1a5276` centered "the downloaded" (y=119) / "model repo" (y=137).
- **Upper path (unsafe):** rounded rect x=210, y=52, 210×64, radius 6, 2px `#d95926`, fill `rgba(217,89,38,0.07)`; bold 12px `#d95926` centered "legacy pickled weights" at y=72; 11px `#2c3e50` centered "loading deserializes objects" (y=92), "→ code can execute" (y=108).
- **Upper outcome:** rounded rect x=450, y=52, 150×64, radius 6, 2px `#e74c3c`, fill `rgba(231,76,60,0.07)`; bold 12px `#e74c3c` centered "treat as untrusted" at y=78; 11px `#2c3e50` centered "scan · isolate · avoid" at y=98.
- **Lower path (safe):** rounded rect x=210, y=136, 210×64, radius 6, 2px `#008300`, fill `rgba(0,131,0,0.07)`; bold 12px `#008300` centered "tensor-only weights" at y=156; 11px `#2c3e50` centered "numbers only — nothing" (y=176), "to reconstruct or run" (y=192).
- **Lower outcome:** rounded rect x=450, y=136, 150×64, radius 6, 2px `#199e70`, fill `rgba(25,158,112,0.07)`; bold 12px `#199e70` centered "safe to load" at y=162; 11px `#2c3e50` centered "still evaluate" (y=180), "its behaviour" (y=196).
- **Arrows (1.5px `#6b7280`, arrowheads):** repo right edge (155,123) to (204,84) and to (204,168); (420,84) to (444,84); (420,168) to (444,168).
- **Bottom strip:** two rounded rects 290×44, radius 6, at x = `[25, 330]`, y=214; left 1.5px `#4a3aa7` fill `rgba(74,58,167,0.07)`, right 1.5px `#c98500` fill `rgba(201,133,0,0.07)`.
  - left: bold 11px `#4a3aa7` centered "typosquatted name" (y=232), 11px `#2c3e50` centered "one character off a popular repo" (y=249)
  - right: bold 11px `#c98500` centered "unpinned revision" (y=232), 11px `#2c3e50` centered "tomorrow's fetch is a different model" (y=249)
- **Annotation (bold 12px orange `#d95926`, centered at y=276):** "pin the revision · verify the publisher · prefer tensor-only · scan · eval before shipping".
- **Caption (11px `#444`, bottom right, y=294):** "defensive overview; no exploit detail".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`), copied structurally from `20-llms-and-generative-ai/48-copyright-complications.html`. Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then **three** `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Canvas list:** exactly three canvases, ids `c1`, `c2`, `c3`, in section order — dependency anatomy, annotated listing plus computed memory panel, load paths plus name traps.
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label). Each section carries exactly 8 bullets; adjacent facts sharing a subject are merged into one line rather than dropped, and every bullet fits one line at the 50/50 split.
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links, no cross-page links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Shared `roundRect` and `arrowHead` helpers. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red `#e74c3c` used only for the fails-to-fit line and the untrusted-load outcome.
- **Data:** no generated data and no `Math.random()` anywhere. The mock listing figures (8B parameters, 32 layers, dim 4096, 4 shards / 16.1 GB, 128k vocab, 8,192 context, 71.2 benchmark, research-only gated licence) are invented and labeled illustrative. Every memory number in `c2` is computed in JS from `params = 8` and `bytesPerParam = 2` or `0.5` with a 1.2 overhead factor — 16.0, 19.2, 4.0, and 4.8 GB are printed from that arithmetic, never typed as strings, and the same four numbers appear in the section's bullets and example.
- **Angle discipline:** this page covers a hub as *infrastructure* — the repo model, what a listing publishes, and the trust problem. It must not reuse the download-instead-of-train framing, the months-vs-minutes chart, or the sentiment-classifier example of `54-data-technologies/47-huggingface`.
- **Naming discipline:** hubs are referred to generically ("a public model hub"); no invented brand names; repos are always `namespace/model-name`; no named actors in examples.
- **Security discipline:** risk categories and mitigations only — format-level deserialization risk, typosquatting, unpinned revisions, undisclosed fine-tune behaviour. No exploit steps, no payloads, no attack tooling.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
