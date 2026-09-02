# Shape Classification Hierarchy

**Page type:** other (single-page long doc: h1, subtitle, h2 sections with full-width canvases and data tables; no card grid, no obj-table)
**HTML title tag:** Shape Classification Hierarchy

**Subtitle:** CNN classifies shape purely from the histogram image. Valley and spike are algorithmic post-checks.

## Full Decision Flow

### Visualization (canvas `flow`, width 100% of page × 580)

Flowchart of rounded boxes (radius 8, stroke `#666` width 1.5) connected by gray arrows (stroke/fill `#666`, triangular 8px heads; optional gray `#888` label at arrow midpoint). Box text: first line bold 12px, subsequent lines 17px, centered; positions are fractions of canvas width W.

- **Input box** at (0.38W, 15), 0.24W×35: "Raw Data" — fill `#e8e8e8`, text `#333`. Arrow down from (0.5W, 50) to (0.5W, 70).
- **Render box** at (0.3W, 70), 0.4W×35: "Histogram + Density + Bin Counts" — fill `#e8f0f8`, text `#1a5276`.
- **Three fan-out arrows** with labels: (0.37W,105)→(0.2W,140) labeled "image"; (0.5W,105)→(0.5W,140) labeled "density"; (0.63W,105)→(0.8W,140) labeled "bins".
- **Level 1 boxes** (y=145, each 0.24W×40): "CNN\nSigmoid heads" at 0.08W — fill `#d6eaf8`, text `#1a5276`; "Peak Counting\n(algorithmic)" at 0.38W — fill `#d5f5e3`, text `#1e8449`; "Point-Mass Check\n(algorithmic)" at 0.68W — fill `#fadbd8`, text `#922b21`.
- **Arrows down** from each (y 185→215), then **result boxes** (y=215, 0.24W×35): "MOUNTAIN" (`#d6eaf8`/`#1a5276`), "VALLEY" (`#d5f5e3`/`#1e8449`), "SPIKE" (`#fadbd8`/`#922b21`).
- **Mountain branch:** arrow (0.2W, 250→290); box at (0.05W, 290), 0.3W×40: "CNN Shape Output\n(bell, right_skew, heavy_tail...)" — `#d6eaf8`/`#1a5276`.
- **Valley branch:** arrow (0.5W, 250→280); box at (0.38W, 280), 0.24W×40: "Separation + Depth" — fill `#fef9e7`, text `#7d6608`; arrow (0.5W, 320→350); three leaf boxes (each 0.075W wide, 40 tall, 3px apart, starting x=0.38W, y=350): "bimodal", "multi\nmodal", "u_shaped" — all `#d5f5e3`/`#1e8449`.
- **Spike branch:** arrow (0.8W, 250→280); box at (0.68W, 280), 0.24W×40: "Location + Monotonicity" — `#fef9e7`/`#7d6608`; arrow (0.8W, 320→350); four leaf boxes (each 0.058W wide, 40 tall, 2px apart, starting x=0.68W, y=350): "spike", "zero\n_infl", "desc", "asc" — all `#fadbd8`/`#922b21`.
- **Convergence arrows** from (0.2W, 390), (0.5W, 390), (0.8W, 390) all to (0.5W, 430); **final box** at (0.3W, 430), 0.4W×40: "STATISTICAL ACTION\nSub-class → which test to use" — fill `#d4efdf`, text `#1e8449`.
- **Summary text** centered: at y=505 in `#555` 17px: "CNN only for mountain (visual judgment needed). Valley + Spike are simple threshold checks."; at y=530 in `#1a5276` bold 11px: "CNN classifies shape purely from histogram image. Valley + Spike are algorithmic post-checks."

## Ames Housing Dataset

CNN (generative) shape classification — real results from running the model on actual data. (Paragraph styled 0.9em, `#555`.)

### Visualization (canvas `validation`, width 100% × 680)

3×3 grid of mini histogram panels (16px gaps, 10px top padding). Each panel: background `#fafcfe`, border `#e0e0e0`; centered feature name in bold 17px `#1a5276` and shape label in 17px of the feature color; 12-bin histogram (bars in feature color at alpha 0.4, bar width fills panel minus 20px), a connecting line through bar tops (feature color, width 2), upper and lower SE band polygons filled in feature color at alpha 0.12 (per-bin SE = √count/√n × chartH × 2.5, upper clamped to chart height, lower to 0), and a light `#ccc` baseline axis.

Panels (name / shape label / 12 bin counts / color):

| Feature | Shape label | Data | Color |
|---------|-------------|------|-------|
| SalePrice | right_skew (0.975) | 30,25,18,14,10,8,6,4,3,2,1,1 | `#e67e22` |
| Gr Liv Area | right_skew (1.000) | 28,22,18,14,10,8,6,5,4,3,2,1 | `#e67e22` |
| Lot Area | heavy_tail (0.956) | 5,12,25,38,30,15,8,4,3,2,2,3 | `#e74c3c` |
| Garage Area | bell (0.943) | 2,5,12,22,35,42,38,28,18,10,5,2 | `#27ae60` |
| Lot Frontage | heavy_tail (0.965) | 4,10,20,32,28,16,8,4,3,2,2,4 | `#e74c3c` |
| Year Built | ascending (1.000) | 3,4,5,7,9,12,16,20,26,32,38,42 | `#2980b9` |
| Total Bsmt SF | heavy_tail (0.997) | 8,18,30,38,28,15,8,4,3,2,1,1 | `#e74c3c` |
| 1st Flr SF | right_skew (0.996) | 25,20,16,14,12,10,8,6,5,4,3,2 | `#e67e22` |
| Year Remod/Add | u_shaped (1.000) | 28,8,5,4,3,3,4,5,8,15,25,38 | `#8e44ad` |

### Ames results table

| Feature | Top-1 Shape (Score) | Top-2 Shape (Score) | Pipeline Action |
|---------|---------------------|---------------------|-----------------|
| SalePrice | right_skew (0.975) | bimodal (0.919) | Check sub-populations (top-2 bimodal is high) |
| Gr Liv Area | right_skew (1.000) | bell (0.487) | Profile tail separately from body |
| Lot Area | heavy_tail (0.956) | bimodal (0.341) | Detect outlier islands; split at gap |
| Garage Area | bell (0.943) | heavy_tail (0.811) | Proceed to range significance testing |
| Lot Frontage | heavy_tail (0.965) | bell (0.914) | Core is bell-like; isolate tail region |
| Year Built | ascending (1.000) | u_shaped (0.996) | Temporal feature — bucket by era |
| Total Bsmt SF | heavy_tail (0.997) | bell (0.530) | Detect zero-mass (no basement); split |
| 1st Flr SF | right_skew (0.996) | bimodal (0.987) | Split into sub-populations first |
| Year Remod/Add | u_shaped (1.000) | ascending (0.610) | Two clusters: never-remodeled vs recent |

## Adult Census Dataset (Kaggle)

### Visualization (canvas `validation2`, width 100% × 460)

3×2 grid of mini histogram panels, identical rendering to the `validation` canvas above.

| Feature | Shape label | Data | Color |
|---------|-------------|------|-------|
| Age | right_skew (1.000) | 8,14,20,25,22,18,15,12,10,8,5,3 | `#e67e22` |
| fnlwgt | right_skew (0.995) | 32,25,18,12,8,5,4,3,2,1,1,1 | `#e67e22` |
| Education_Num | spike (0.923) | 2,3,5,8,12,38,15,8,5,3,2,1 | `#2980b9` |
| Capital_Gain | zero_inflated (1.000) | 48,3,2,1,1,0,0,0,0,0,0,2 | `#2980b9` |
| Capital_Loss | zero_inflated (1.000) | 46,2,1,1,0,0,0,0,0,0,0,1 | `#2980b9` |
| Hours_per_week | spike (0.998) | 3,4,5,6,8,42,8,4,3,2,1,1 | `#2980b9` |

### Adult Census results table

| Feature | Top-1 Shape (Score) | Top-2 Shape (Score) | Pipeline Action |
|---------|---------------------|---------------------|-----------------|
| Age | right_skew (1.000) | bell (0.135) | Profile tail; bucket by decade |
| fnlwgt | right_skew (0.995) | bell (0.982) | Body is bell-like; isolate tail |
| Education_Num | spike (0.923) | heavy_tail (0.910) | Discrete ordinal; bucket by level |
| Capital_Gain | zero_inflated (1.000) | multimodal (0.417) | Split zero vs non-zero; profile each |
| Capital_Loss | zero_inflated (1.000) | multimodal (0.121) | Split zero vs non-zero; profile each |
| Hours_per_week | spike (0.998) | heavy_tail (0.928) | Spike at 40h; bucket around spike |

## Regeneration instructions

- **Layout:** single-page long doc in document order: h1, subtitle, "Full Decision Flow" h2 + `flow` canvas, "Ames Housing Dataset" h2 + intro paragraph + `validation` canvas + results table, "Adult Census Dataset (Kaggle)" h2 + `validation2` canvas + results table.
- **Canvas setup:** each canvas declares only a `height` attribute (580 / 680 / 460); width is taken from the rendered bounding rect (CSS `width: 100%`). Backing store is multiplied by `window.devicePixelRatio`, CSS size fixed, `ctx.scale` back to logical coordinates — shared `setupCanvas(id)` helper. Canvas style: display block, margin 15px 0, background `#f8f9fa`, border `1px solid #e0e0e0`, radius 8px.
- **Page style:** body -apple-system/system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276`, 2px solid `#2980b9` bottom border, padding-bottom 8px; paragraphs `#333` 0.92em; subtitle `#666` 1.05em. Unused-on-page helper classes also present: `.pattern` monospace 1.1em with letter classes R `#e74c3c`, B `#2980b9`, L `#8e44ad`; `code` background `#e8f0f8` color `#1a5276`.
- **Tables:** border-collapse, width 100%, 0.85em; th/td padding 8px 10px, border `1px solid #ddd`, left-aligned; th background `#f0f4f8` color `#1a5276` bold; even rows `#fafafa`. Tables use `<thead>`/`<tbody>`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`; extras used here: `#2980b9`, `#8e44ad`, box fills `#d6eaf8`/`#d5f5e3`/`#fadbd8`/`#fef9e7`/`#d4efdf`/`#e8f0f8`/`#e8e8e8`, dark text variants `#1e8449`/`#922b21`/`#7d6608`.
- No nav bar, no back/home links.
