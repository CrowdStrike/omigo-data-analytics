# Sampling Methods

**Page type:** detail page (tutorial page: h2 card-sections, each a two-column layout table — text left 50%, canvas right 50%)
**HTML title tag:** Sampling Methods

**Subtitle:** Different ways to pick who gets measured — and why the picking method matters more than the count

## Three Ways to Poll the Town About the New Park

**Tags:** `core idea` (blue), `running example` (green)

- **The question** — a town of 10,000 must decide: build the new park or not?
- **Random** — pull 300 names from the full resident list; everyone has an equal chance
- **Stratified** — split the list by neighborhood first, then draw randomly inside each
- **Convenience** — stand at one park gate and ask whoever happens to walk by
- **The catch** — the gate crowd picked itself; park lovers show up at parks

*Example:* Same town, same 300 answers collected — three picking methods, three different verdicts.

**Key point:** a sample is only as good as the chance each person had of being in it — the method decides that, not the count.

### Visualization (canvas `c1`, 720×300)

Three dot-grid panels showing who gets picked under each method.

- **Title (bold 16px, `#1a5276`, top center):** "Who Gets Picked: blue = north side, orange = south side"
- **Panels (each a 6×8 dot grid, spacing 32px×22px, starting y=68):** "Random" at x0=30 (title green `#008300`), "Stratified" at x0=265 (title blue `#2a78d6`), "Convenience" at x0=500 (title red `#e74c3c`); bold 13px panel titles at y=52.
- **Dots:** indices 0–47 per panel; the first 29 (~60%, top rows) are north-siders (blue `#2a78d6`), the rest south-siders (orange `#d95926`). Unpicked dots radius 3.5 in translucent color (`rgba(42,120,214,0.25)` north, `rgba(217,89,38,0.25)` south); picked dots radius 7 in full color with a 1px `#1a5276` stroke.
- **Selection logic (deterministic pseudo-random via a `sin`-hash `frac()` helper — no `Math.random()`):** Random panel picks ~25% spread across all dots; Stratified picks a proportional pattern (fixed columns, alternating rows, from both north and south); Convenience picks mostly north dots in the top 4 rows plus a single south dot (matching the 90/10 gate mix).
- **Annotations:** bold 13px red centered under the Convenience panel: "gate crowd: south side almost never asked"; muted 12px bottom-left: "big dot = picked for the poll".

## The Gate Poll Goes 9 Points Wrong: the Numbers

**Tags:** `worked example` (green), `by hand` (blue)

- **The town** — 6,000 north-siders (70% favor the park), 4,000 south-siders (40% favor)
- **The truth** — 0.6 × 70 + 0.4 × 40 = 58% favor
- **Stratified 300** — 180 north + 120 south, weighted the same way → 58%
- **Gate poll 300** — the north gate draws 90% north-siders: 0.9 × 70 + 0.1 × 40 = 67%
- **The damage** — 9 points off, from picking the wrong mix, not from too few people

*Example:* Redo it yourself: swap the 90/10 gate mix for the true 60/40 mix and 67% falls back to 58%.

**Key point:** bias is a wrong mix. Stratifying locks the sample's mix to the population's mix, so the weighted answer lands on the truth.

### Visualization (canvas `c2`, 720×300)

Bar chart: "% favor the park" for the truth and the three methods.

- **Title (bold 15px, `#1a5276`, top center):** '"% Favor the Park" by Method (truth = 58%)'
- **Data:** labels `['truth (all 10,000)', 'random 300', 'stratified 300', 'gate poll 300']` with values `[58, 58, 58, 67]` and bar colors `[#1a5276 (ink), #008300 (green), #2a78d6 (blue), #e74c3c (red)]`; y max 80.
- **Axes:** padding top 56, bottom 56, left 70, right 30; horizontal baseline in `#999`; bars 110px wide, evenly spaced; alpha 0.9 for the truth bar, 0.75 for the rest.
- **Truth reference line:** dashed muted-gray (dash 5/4, 1.5px) horizontal line at 58%.
- **Labels:** bold 13px value labels ("58%", "67%") above bars; 12px method labels below.
- **Error whisker:** green vertical whisker on the "random 300" bar spanning ±2.9 points around 58% with 6px end caps (the ±3-point wobble at n=300).
- **Annotations:** bold 13px red, two lines above the gate-poll bar: "+9 pts: wrong mix," / "not bad luck"; muted 12px near top-left of plot: "random wobbles ±3; the gate misses by 9 every time".

## Drawing Names From the Hat: Put It Back or Not?

**Tags:** `with replacement` (blue), `without replacement` (blue), `bootstrap link` (orange)

- **The hat** — 5 names: Ana, Ben, Chi, Dee, Eli; we draw 3 for the poll
- **Without replacement** — a drawn name stays out: the hat shrinks 5 → 4 → 3, no repeats
- **With replacement** — each name goes back in: the hat stays at 5, Ben can appear twice
- **Check it** — P(Ana is drawn): without = 3/5 = 60%; with = 1 − (4/5)³ = 48.8%
- **Who uses which** — surveys and train/test splits go without; bootstrap and bagging go with
- **Bootstrap fact** — resampling a big list with replacement leaves ~37% of items out each time

*Example:* One with-replacement run: Ben, Dee, Ben — a repeat that a survey would never allow.

**Key point:** put the name back and every draw faces the same hat — that independence is exactly what the bootstrap needs and what surveys avoid.

### Visualization (canvas `c3`, 720×300)

Split panel: three successive draws from the 5-name hat, without replacement (left) vs with replacement (right), divided by a vertical dashed `#bdc3c7` line (dash 4/3) at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Drawing 3 From {Ana, Ben, Chi, Dee, Eli}"
- **Hat rows:** each row is 5 name chips (14px-radius circles, 54px apart) with bold 11px white names; present names filled `rgba(42,120,214,0.75)`, the currently picked name orange `#d95926`, removed names hollow with `#c0c6cc` stroke and gray text; a muted "hat: N" count label to the right of each row.
- **Left (header bold 13px green `#008300` at (185,56)): "WITHOUT replacement — hat shrinks"** — rows at y=92/152/212 starting x=48: draw 1 all present, Chi picked ("hat: 5"); draw 2 Chi missing, Ana picked ("hat: 4"); draw 3 Chi+Ana missing, Eli picked ("hat: 3"). Captions: bold 12px green "draws: Chi, Ana, Eli — no repeats possible"; muted 12px "surveys, train/test splits".
- **Right (header bold 13px violet `#4a3aa7` at (540,56)): "WITH replacement — hat stays full"** — rows at y=92/152/212 starting x=408, all five present each time ("hat: 5" ×3); picks Ben, Dee, Ben. Captions: bold 12px orange "draws: Ben, Dee, Ben — Ben repeats!"; muted 12px "bootstrap, bagging".

## More Data Doesn't Fix a Biased Gate

**Tags:** `common mistake` (red), `famous failure` (orange)

- **The instinct** — "our sample is huge, so it must be right" — size cures noise, not bias
- **1936** — Literary Digest polled 2.4 million (car and phone owners): Landon wins easily
- **Reality** — Roosevelt won in a landslide; Gallup's ~50,000 quota sample called it
- **Noise** — random wobble, shrinks like 1 ÷ √n as the sample grows
- **Bias** — a wrong mix baked in by the method; it stays put at any n

*Example:* Polling 3,000 at the gate still says 67% — a bigger biased sample is just more confidently wrong.

**Common mistake:** judging a sample by its size. Ask first "who could never end up in this sample?" — that question exposes the gate.

### Visualization (canvas `c4`, 720×300)

Two-line chart: polling error vs sample size — noise melts, bias stays flat.

- **Title (bold 15px, `#1a5276`, top center):** "Polling Error vs Sample Size"
- **Data:** x labels `['100', '400', '2,500', '10,000', '2.4 million']` (equally spaced); random-sample error `[5.0, 2.5, 1.0, 0.5, 0.03]` points; biased-gate error `[9.3, 9.1, 9.0, 9.0, 9.0]` points; y max 11.
- **Axes:** padding top 52, bottom 56, left 70, right 200; L-shaped axis in `#999`; x-axis title "people polled"; rotated y-axis title "error (pts)" — muted gray 12px.
- **Series:** random line green `#008300` width 3; convenience line red `#e74c3c` width 3.
- **Marker:** filled red 6px-radius dot at the last bias point, with bold 12px right-aligned two-line label: "Literary Digest 1936:" / "2.4M answers, still wrong".
- **Annotations (bold 13px):** red "biased gate: stuck at ~9 pts" near the left of the bias line; green "random: error melts as n grows" below the random line.
- **Legend (right side, 12px):** green swatch "random sample"; red swatch "convenience (gate)".

## Regeneration instructions

- **Template:** tutorial detail page (tutorials style, see `tutorials/CLAUDE.md`). h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks. Each section: `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with `.text-col` (50%) and `.viz-col` (50%) holding one 720×300 canvas.
- **Text cell structure:** `.tags` row of colored pills, `<ul>` of one-line bullets each opening with `<b>bold term</b>` (colored `#1a5276`), one italic `.example` paragraph, one `.key-point` callout (background `#f8f9fa`, left border `3px solid #1a5276`); the last section's callout is prefixed "Common mistake:" instead of "Key point:".
- **Tag pill styles:** 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; bullets 0.92rem; canvases `width:100%` with `1px solid #e0e0e0` border, 4px radius. HTML entities used in text: `&times;` (×), `&rarr;` (→), `&minus;` (−), `&sup3;` (³), `&divide;` (÷), `&radic;` (√).
- **Chart palette (JS object P):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Project palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- **Canvas:** intrinsic 720×300; sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) via a shared `setup(id)` helper (`ctx.scale` back to logical coordinates). Data hardcoded; the c1 dot selection uses a deterministic `Math.sin`-hash helper `frac(i, k)`, never `Math.random()`.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
