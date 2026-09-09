# safetensors / GGUF

**Page type:** detail page (single card-section with two-column layout table: text left 45%, canvas right 55%)
**HTML title tag:** safetensors / GGUF

**Subtitle:** Model weight storage formats for ML.

## Intro callout

**Core trade-off:** Safety (no arbitrary code execution) and speed (memory-mappable) — but the ecosystem is fragmented between formats and tooling is still evolving.

## How It Works

Formats built specifically for neural network weights. **safetensors** (Hugging Face) replaces Python's pickle for model distribution; **GGUF** (llama.cpp) targets local inference with quantization built in. Both fix the same security disaster: pickle has no boundary between data and code — loading a model file can execute anything.

- **Schema:** Header describes every tensor: name, dtype, shape, byte offsets
- **Layout:** Small header + raw tensor bytes; memory-mappable
- **GGUF extra:** Model metadata + quantization levels (Q4/Q5/Q8) in one format
- **Best for:** Distributing and loading weights you didn't create yourself

**How loading works:** The parser reads the header (JSON in safetensors), learns that `layer1.weight` is F32 with shape [768,768] at bytes 0–2359296, and `mmap`s the data region. Nothing is executed — there is no `__reduce__`, no eval, just offsets into raw numbers. Memory-mapping means the OS pages tensors in on demand: a 70B model "loads" instantly, and individual layers can be read without touching the rest of the file. GGUF adds quantization: the same model ships at F16 (140 GB), Q8 (70 GB), or Q4 (40 GB, fits consumer GPUs).

> **The lesson:** pickle conflates data with code — loading means executing arbitrary Python. safetensors enforces a strict data-only contract: header describes layout, body is raw numbers, nothing more.

*Example: Hugging Face Hub (safetensors is the default), llama.cpp / Ollama / LM Studio (GGUF), training checkpoints.*

### Visualization (canvas `c1`, 720×300)

Diagram of the safetensors file layout, a pickle-vs-safetensors comparison, and a GGUF quantization size bar.

- **Title (bold 14px `#1a5276`, top center):** "safetensors — Header Describes, Body Is Only Numbers"
- **File layout strip:** horizontal strip at x=60, y=50, height 54, total width 600.
  - Header segment: 150px wide, solid `#1a5276` fill, white text: bold 11px "JSON header" and 9px "names·dtypes·shapes·offsets".
  - Tensor segments (label, width px, fill), each with white 2px stroke border and `#1a5276` 10px monospace label centered:
    - `layer1.weight`, 170, `rgba(39,174,96,0.5)`
    - `layer1.bias`, 60, `rgba(39,174,96,0.35)`
    - `layer2.weight`, 170, `rgba(26,82,118,0.4)`
    - `…`, 50, `rgba(26,82,118,0.2)`
- **Offset pointer arrow:** orange `#e67e22` (1.5px) elbow line from under the header (x=fx+110) down and across to under `layer1.weight` (x=fx+235), with a small orange arrowhead pointing up, and a 10px label: `"layer1.weight": offsets [0, 2359296] — mmap, load on demand, nothing executed`
- **vs pickle panel** (left-aligned text rows below the strip):
  - Red `#e74c3c` bold 12px label "pickle:" followed by 11px monospace: `load() → can run os.system("…") — data and code share the format`
  - Green `#27ae60` bold 12px label "safetensors:" followed by 11px monospace: `load() → reads offsets, maps bytes. Pure data.`
- **GGUF quantization bar:** bold 12px `#1a5276` label "GGUF — same 70B model, pick your size:", then a row of decreasing-width bars (label, width px, fill), each 24px tall with bold white 10px centered label, separated by 8px gaps:
  - `F16 140GB`, 240, `rgba(26,82,118,0.6)`
  - `Q8 70GB`, 120, `rgba(39,174,96,0.55)`
  - `Q4 40GB`, 70, `rgba(230,126,34,0.6)`
  - `Q2 25GB`, 44, `rgba(231,76,60,0.55)`
  - Gray `#666` 10px note to the right of the bars: "quality ↔ size trade-off in one format"

## Regeneration instructions

- **Template/layout:** file-formats detail page. h1 with 2px `#2980b9` bottom border, `.subtitle` paragraph, one `.intro-callout` (background `#f8f9fa`, left border `3px solid #2980b9`, padding 10px 14px, 0.93rem), then one `.card-section` with an h2 ("How It Works", 1.3rem `#1a5276`, 2px `#2980b9` bottom border) containing a `table.layout` with a single row: `td.text-col` (45%) and `td.viz-col` (55%, holds the canvas).
- **Text-column structure:** lead paragraph, `<ul>` of labeled bullets, a follow-on paragraph (margin-top 10px, 0.9rem), a `.key-point` box (background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem), and a `.example` italic gray (`#555`) 0.9rem line.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `ul` 0.92rem with 20px left margin; `code` on `#f0f4f8` background, 2px 6px padding, 3px radius; canvas `width:100%`, `1px solid #e0e0e0` border, 4px radius.
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `rgba(26,82,118,…)` blue fills.
- **Canvas:** intrinsic size 720×300; use `window.devicePixelRatio` scaling (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper.
- In regenerated HTML, any card links use `.html` extensions. No nav bar, no back/home links.
