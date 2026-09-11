# Mixture of Experts

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Mixture of Experts

**Subtitle:** A mixture-of-experts model keeps many specialist sub-networks on staff but sends each word to only two of them — so a trillion-parameter model computes like a model a sixth of its size

## The Clinic with Sixteen Specialists

**Tags:** `core idea` (blue), `routing` (green), `specialists` (orange)

- **The clinic** — a big clinic employs 16 specialist doctors: heart, skin, bone, allergy, and so on
- **The front desk** — a nurse reads each patient's symptom card and sends them to exactly 2 doctors
- **One visit, two doctors** — a rash patient sees the skin and allergy specialists; the other 14 stay idle
- **The model version** — an MoE layer holds 16 expert sub-networks; a tiny router picks 2 per word
- **Per-word choice** — the router decides fresh for every word, so different words wake different experts

*Example (italic):* The word "eczema" flowing through the model is the rash patient — the router lights up 2 of the 16 experts and the visit costs 2 doctor-hours, not 16.

**Key point:** A mixture of experts stores many specialists but a learned router activates only a few per word — full staff on payroll, small bill per visit.

### Visualization (canvas `c1`, 720×300)

Left-to-right routing diagram: one patient card flows through the front-desk router into a 4×4 grid of 16 specialist boxes, of which exactly 2 are lit.

- **Title (bold 15px, `#1a5276`, top center):** "One Visit: the Front Desk Picks 2 of 16 Specialists".
- **Patient box:** rounded rect x=30–140, y=132–172, fill `rgba(42,120,214,0.15)`, 2px blue `#2a78d6` border; bold 12px blue label centered: "patient: 'rash'".
- **Router box:** rounded rect x=195–295, y=132–172, fill `rgba(217,89,38,0.12)`, 2px orange `#d95926` border; bold 12px orange label centered: "front desk (router)"; 2px `#6b7280` arrow from patient box to router box.
- **Expert grid:** 16 boxes 70×34, columns at x = `[370, 452, 534, 616]`, rows at y = `[62, 108, 154, 200]`; 12px labels from the hardcoded array `["heart", "skin", "lung", "bone", "eye", "gut", "nerve", "blood", "kidney", "allergy", "ear", "joint", "hormone", "liver", "sleep", "infection"]` filled left-to-right, top-to-bottom.
- **Chosen experts:** "skin" and "allergy" boxes fill `rgba(0,131,0,0.20)` with 2px green `#008300` border and bold labels; 2px green lines from the router box to each; the other 14 boxes fill `#f8f9fa` with 1px `#e5e9ef` border and `#6b7280` labels.
- **Annotation (bold 12px green `#008300`, near x=195, y=225, two lines):** "2 of 16 doctors see the patient —" / "the other 14 stay idle".
- **Caption (12px `#444`, bottom right):** "illustrative — one word, one routing decision".

## Counting the Knobs: 1,000B Stored, 160B Used

**Tags:** `worked example` (blue), `active vs total` (green)

- **The staff list** — 16 experts × 60B parameters each = 960B, plus 40B of shared parts everyone uses
- **Total stored** — 960B + 40B = 1,000B: a one-trillion-parameter model on paper
- **One word's bill** — the word touches the 40B shared parts plus its 2 chosen experts: 40 + 2×60 = 160B
- **The fraction** — 160B of 1,000B means only 16% of the model does any work on a given word
- **No new math** — the saving is pure skipping: the 14 unchosen experts are simply never run

*Example (italic):* Stored = 40 + 16×60 = 1,000B; used per word = 40 + 2×60 = 160B — two lines of arithmetic anyone can redo by hand.

**Key point:** Active parameters, not total parameters, set the compute bill: this trillion-parameter model computes each word with 160B — about 1/6 of its size.

### Visualization (canvas `c2`, 720×300)

Two horizontal stacked bars on a shared parameter axis: the full stored model on top, the slice one word actually uses below, with the idle experts visibly skipped.

- **Title (bold 15px, `#1a5276`, top center):** "Stored vs Used per Word: 1,000B on Disk, 160B at Work".
- **Axis:** horizontal 2px `#999` line at y=245 from x=110 to x=670 (width 560), scale 0 to 1,000B; tick labels "0", "250B", "500B", "750B", "1,000B" (12px `#444`) below.
- **Row 1 (bar center y=110, height 34), 12px `#444` label at x=20 ("stored"):** blue `#2a78d6` segment for shared 0–40B, then 16 expert segments of 60B each (40–1,000B) filled `rgba(42,120,214,0.35)` with 1px white dividers every 60B; bold 12px blue total label "1,000B" just right of the bar end.
- **Row 2 (bar center y=185, height 34), label ("used per word"):** blue segment for shared 0–40B, then 2 green `#008300` segments of 60B each (40–160B) with 1px white dividers; bar ends at 160B; bold 12px green total label "160B" right of the bar end; from 160B to 1,000B a dashed 1px `#6b7280` outline (dash 4/3) with 11px `#6b7280` label centered inside: "14 idle experts — skipped".
- **Annotation (bold 13px orange `#d95926`, near x=400, y=65):** "only 16% of the knobs work on any one word".
- **Caption (12px `#444`, bottom right):** "illustrative — 16 experts of 60B plus 40B shared".

## Why Trillion-Parameter Models Don't Cost Trillions

**Tags:** `where it's used` (blue), `scaling` (green), `cost` (orange)

- **The dense way** — in an ordinary (dense) model every knob touches every word, so cost grows with size
- **The MoE way** — grow the staff, not the visit: add experts and the per-word bill barely moves
- **The headline trick** — this is how frontier labs ship trillion-parameter models at mid-size prices
- **More knowledge, same speed** — extra experts store more specialties without slowing each word down
- **Where you meet it** — many frontier LLMs today are MoE under the hood, priced by active size

*Example (italic):* At 1,000B total, the dense bill is 1,000B of work per word while the 2-of-16 MoE bill is 160B — same shelf of knowledge, about 1/6 the compute.

**Key point:** MoE breaks the link between how much a model knows (total parameters) and what each word costs (active parameters) — that is why trillion-parameter models don't cost trillions.

### Visualization (canvas `c3`, 720×300)

Two-line chart: compute per word versus total model size, a dense model climbing linearly and the 2-of-16 MoE staying nearly flat, with the gap at 1,000B called out.

- **Title (bold 15px, `#1a5276`, top center):** "Cost per Word as the Model Grows: Dense vs 2-of-16 MoE".
- **Axes:** origin x=90, baseline y=245, plot width 560, plot height 185; x = total parameters 0 to 1,000B with 12px `#444` tick labels "0", "250B", "500B", "750B", "1,000B"; y = compute per word 0 to 1,000B with light `#e5e9ef` gridlines and 12px `#444` labels at 250, 500, 750, 1,000.
- **Shared x points:** `[100, 250, 500, 750, 1000]`.
- **Dense line:** magenta `#d55181` 3px line, cost values `[100, 250, 500, 750, 1000]` (cost = size); 12px magenta label "dense — every knob, every word" near x=560, above the line.
- **MoE line:** green `#008300` 3px line, cost values `[48, 66, 98, 129, 160]` (cost = 40 shared + 2/16 of the experts); fill under `rgba(0,131,0,0.10)`; 12px green label "MoE — 2 of 16 experts" near x=560, below the line.
- **Gap marker:** vertical dashed `#6b7280` (dash 4/3) line at x=1,000B between the two line endpoints; bold 12px `#6b7280` labels "1,000B" at the dense end and "160B" at the MoE end (staggered to avoid overlap).
- **Annotation (bold 13px green `#008300`, near x=330, y=95, two lines):** "same trillion knobs of knowledge —" / "about 1/6 the bill per word".
- **Caption (12px `#444`, bottom right):** "illustrative — cost in active parameters per word".

## Big Model, Small Bill — But the Building Must Fit Everyone

**Tags:** `common mistake` (red), `memory vs compute` (orange)

- **The claim** — "it only uses 160B, so it's really a 160B model" — half right, and wrong where it hurts
- **Compute** — per word, yes: the MoE and a dense 160B model do a similar amount of arithmetic
- **Memory** — every expert must sit loaded and ready, because the next word may call any of them
- **The clinic again** — each visit bills 2 doctor-hours, but the building still needs 16 offices
- **Not an ensemble** — the 16 experts are parts of one model trained together, not 16 separate models voting

*Example (italic):* The 2-of-16 MoE pays per word like a 160B model but needs the memory of a 1,000B one — cheap to run, expensive to house.

**Common mistake:** Reading "trillion parameters" as the running cost, or "160B active" as the memory need. Compute follows active parameters (160B); memory and storage follow total parameters (1,000B).

### Visualization (canvas `c4`, 720×300)

Grouped bar chart comparing a dense 160B model with the 1,000B MoE on three yardsticks — compute per word, knowledge stored, memory needed — showing where they match and where they split.

- **Title (bold 15px, `#1a5276`, top center):** "Dense 160B vs MoE 1,000B: Alike on Cost, Apart on Memory".
- **Axes:** origin x=90, baseline y=240, plot width 560, plot height 175; y = billions of parameters 0 to 1,000 with light `#e5e9ef` gridlines and 12px `#444` labels at 250, 500, 750, 1,000.
- **Groups:** three pairs of bars 52px wide with 14px within-pair gap, group centers at x = `[210, 390, 570]`, group labels below the baseline (bold 12px `#444`): "compute per word", "knowledge stored", "memory needed".
- **Bar values (hardcoded):** dense 160B model = `[160, 160, 160]` in blue `#2a78d6`; MoE 1,000B model = `[160, 1000, 1000]` in green `#008300`; bold 12px value labels above every bar ("160", "160", "160", "1,000", "1,000").
- **Legend (12px, top left inside plot):** blue swatch "dense 160B", green swatch "MoE 1,000B (160B active)".
- **Tie marker:** dashed `#6b7280` (dash 4/3) bracket over the first pair with 11px `#6b7280` label "same bill per word".
- **Annotation (bold 13px magenta `#d55181`, near x=390, y=70, two lines):** "pays like 160B, knows like 1,000B —" / "but must fit in memory like 1,000B".
- **Caption (12px `#444`, bottom right):** "illustrative — parameters in billions".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all bar values, line points, box positions, and labels are the hardcoded arrays above (no randomness); the running arithmetic is 16 experts × 60B + 40B shared = 1,000B stored and 40B + 2×60B = 160B active, and every number shown in a chart must match the text.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
