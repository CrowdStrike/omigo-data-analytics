# Kernel Density Estimation

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Kernel Density Estimation

**Subtitle:** Instead of dropping values into bins, put a small smooth bump on every data point and add the bumps up — you get a histogram's shape with no arbitrary bin edges

## Twelve Coffee Waits, One Smooth Curve

**Tags:** `core idea` (blue), `smooth density` (green), `no bins` (orange)

- **The shop** — a coffee shop times 12 customer waits (minutes): 2.0, 2.3, 2.5, 2.8, 3.1, 3.4, 6.0, 6.3, 6.5, 6.9, 7.2, 9.8
- **The trick** — draw a small bell-shaped bump centered on each wait, then add all 12 bumps up
- **The result** — the summed curve shows two humps: morning regulars near 2.7 min, lunch rush near 6.5 min
- **No edges** — nothing was binned; the curve's shape comes only from where the points sit
- **Reading it** — curve height means "data is dense here"; the valley near 4.7 min means almost no waits there

*Example (italic):* The two-hump curve told the owner she runs two different shops — a fast morning counter and a slow lunch line — from just 12 timings.

**Key point:** A kernel density estimate (KDE) is a histogram without arbitrary bins: one smooth bump per data point, summed, so dense regions rise and empty regions dip.

### Visualization (canvas `c1`, 720×300)

Single-panel KDE curve over a dot rug: the 12 wait times as dots on the baseline with the summed Gaussian-kernel curve (bandwidth h=0.5) above them.

- **Title (bold 15px, `#1a5276`, top center):** "12 Wait Times: Dots on the Floor, One Smooth Curve Above".
- **Data:** waits `[2.0, 2.3, 2.5, 2.8, 3.1, 3.4, 6.0, 6.3, 6.5, 6.9, 7.2, 9.8]` (minutes).
- **Axes:** origin x=60, plot width 610, baseline y=245, chart height 185; x scale 0–12 min with ticks and 12px `#444` labels at 0, 2, 4, 6, 8, 10, 12; y scale density 0–0.32 (unlabeled axis line only); x-axis caption 12px `#444` "wait time (minutes)".
- **Curve:** density(x) = (1/(12·0.5)) · Σ over the 12 waits of exp(−((x−w)/0.5)²/2)/√(2π), sampled at x = 0, 0.05, 0.10, ... 12 (deterministic formula, no randomness); blue `#2a78d6` 3px line, fill under curve `rgba(42,120,214,0.15)`.
- **Rug:** the 12 waits as 5px green `#008300` dots sitting on the baseline.
- **Annotations:** blue bold 13px "morning regulars" above the first hump (peak ≈ 0.27 at x≈2.7); orange `#d95926` bold 13px "lunch rush" above the second hump (peak ≈ 0.24 at x≈6.5); mute `#6b7280` bold 12px "one straggler" with a short arrow to the 9.8 dot.
- **Caption (12px `#444`, bottom right):** "Gaussian kernel, bandwidth h = 0.5 min (illustrative)".

## Stacking Twelve Little Bumps

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **One bump each** — each wait gets a bell curve of width h = 0.5 min; alone it peaks at 0.399/(12·0.5) ≈ 0.066
- **Read a point** — the density at x = 2.5 is just the sum of every bump's height at 2.5
- **Near points count** — waits 2.0, 2.3, 2.5, 2.8, 3.1, 3.4 contribute 0.242, 0.368, 0.399, 0.333, 0.194, 0.079
- **Far points don't** — the lunch-rush waits are 7+ bandwidths away, so they contribute ≈ 0
- **Add and scale** — the kernel sum is 1.615; divide by n·h = 12·0.5 = 6 to get density 0.27

*Example (italic):* At x = 2.5 you can redo it by hand: 0.242 + 0.368 + 0.399 + 0.333 + 0.194 + 0.079 = 1.615, then 1.615 / 6 = 0.27.

**Key point:** KDE at any x is arithmetic you can do by hand: evaluate each point's bump at x, add them, divide by n·h — no bins, no counting rules.

### Visualization (canvas `c2`, 720×300)

The 12 individual scaled kernels as thin bumps with their thick summed curve on top, and a dashed vertical probe at x = 2.5 marking density 0.27.

- **Title (bold 15px, `#1a5276`, top center):** "Each Point Gets a Bump; the KDE Is Their Sum".
- **Data:** same waits `[2.0, 2.3, 2.5, 2.8, 3.1, 3.4, 6.0, 6.3, 6.5, 6.9, 7.2, 9.8]`; kernels g_w(x) = exp(−((x−w)/0.5)²/2)/(√(2π)·12·0.5), each peaking at ≈ 0.066; sum curve = Σ g_w(x); sample x = 0 to 12 step 0.05.
- **Axes:** origin x=60, plot width 610, baseline y=245, chart height 185; x scale 0–12 with labels at 0, 2, 4, 6, 8, 10, 12 (12px `#444`); y scale 0–0.32.
- **Individual bumps:** 12 thin 1.5px aqua `#199e70` lines at 60% opacity, one per wait.
- **Sum curve:** blue `#2a78d6` 3px line on top.
- **Probe:** dashed magenta `#d55181` (dash 4/3) vertical line at x=2.5 from baseline up to the sum curve, with a 5px magenta dot at the intersection (y for density 0.27); magenta bold 13px label beside it, two lines: "at x = 2.5:" / "sum 1.615 ÷ 6 = 0.27".
- **Legend (12px, top right):** aqua "one bump per point (peak ≈ 0.066)", blue "their sum = the KDE".
- **Caption (12px `#444`, bottom right):** "Gaussian kernel, h = 0.5; far points add ≈ 0".

## The Histogram That Changes Its Story

**Tags:** `where it's used` (blue), `bin trouble` (orange), `failure mode` (red)

- **Same 12 waits** — bin them two ways and the picture disagrees with itself
- **2-min bins** — edges at 0, 2, 4, 6, 8, 10 give counts 0, 6, 0, 5, 1: two towers and a clear gap
- **4-min bins** — edges at 0, 4, 8, 12 give counts 6, 5, 1: one sliding lump, gap erased
- **Nothing changed** — only the analyst's bin choice moved; the data never did
- **KDE's answer** — the bump-sum curve shows the two-hump shape without asking anyone to pick edges

*Example (italic):* One analyst reported "two customer types", another "waits just taper off" — same 12 numbers, different bin widths.

**Key point:** Histogram shape depends on bin width and bin origin — two arbitrary choices. KDE replaces both with one smooth, edge-free curve, which is why density plots are the default first look at a distribution.

### Visualization (canvas `c3`, 720×300)

Dual-panel histogram of the same 12 waits: 2-min bins (left) show the valley, 4-min bins (right) erase it; dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Same 12 Waits, Two Bin Choices, Two Different Stories".
- **Left panel (2-min bins):** counts `[0, 6, 0, 5, 1]` for edges 0, 2, 4, 6, 8, 10; axis origin x=55, width 285, baseline y=240, chart height 170, y scale 0–7; bars fill `rgba(42,120,214,0.45)` with 1px `#2a78d6` borders; count labels bold 12px blue above each nonzero bar; edge labels 11px `#444` below; green `#008300` bold 12px annotation "gap visible: two groups"; caption 12px `#444` "bin width 2 min".
- **Right panel (4-min bins):** counts `[6, 5, 1]` for edges 0, 4, 8, 12; axis origin x=400, width 285, same baseline/height/scale; bars fill `rgba(217,89,38,0.45)` with 1px `#d95926` borders; count labels bold 12px orange; edge labels 11px `#444`; magenta `#d55181` bold 12px annotation, two lines: "gap erased:" / "looks like one taper"; caption "bin width 4 min".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Bandwidth Is the New Knob

**Tags:** `common mistake` (red), `bandwidth` (orange), `rule of thumb` (green)

- **Not free** — KDE kills bin edges but adds one choice: the bump width h, called the bandwidth
- **Too narrow** — h = 0.2 makes nearly every wait its own spike; noise reads as structure
- **Too wide** — h = 2.0 melts everything into one blob; the real two-group story vanishes
- **About right** — h = 0.5 shows the two humps and the valley without inventing extra wiggles
- **Sanity check** — try a few bandwidths; trust only the shapes that survive all reasonable choices

*Example (italic):* With h = 0.2 the owner "found" seven customer types; with h = 2.0 she found one; only the two-hump story held up across bandwidths.

**Common mistake:** Believing KDE is assumption-free. Swapping bin width for bandwidth trades one knob for another — a too-small h manufactures clusters and a too-large h hides them, so always look at more than one.

### Visualization (canvas `c4`, 720×300)

Three KDE curves for the same 12 waits at bandwidths 0.2, 0.5, and 2.0, overlaid above the dot rug.

- **Title (bold 15px, `#1a5276`, top center):** "One Dataset, Three Bandwidths: Spiky, Right, Blurred".
- **Data:** same waits `[2.0, 2.3, 2.5, 2.8, 3.1, 3.4, 6.0, 6.3, 6.5, 6.9, 7.2, 9.8]`; for each h in {0.2, 0.5, 2.0}, density(x) = (1/(12·h)) · Σ exp(−((x−w)/h)²/2)/√(2π), sampled at x = 0 to 12 step 0.05.
- **Axes:** origin x=60, plot width 610, baseline y=245, chart height 185; x scale 0–12 with labels at 0, 2, 4, 6, 8, 10, 12 (12px `#444`); y scale 0–0.40.
- **Curves:** magenta `#d55181` 2px line for h = 0.2 (spiky, tallest peak ≈ 0.35); green `#008300` 3px line for h = 0.5 (two humps); orange `#d95926` 2px line for h = 2.0 (one low blob, peak ≈ 0.11).
- **Rug:** the 12 waits as 4px `#6b7280` dots on the baseline.
- **Legend (bold 12px, top right, color-matched):** magenta "h = 0.2 — noise becomes spikes", green "h = 0.5 — two real groups", orange "h = 2.0 — story melted away".
- **Takeaway (bold 13px `#1a5276`, bottom center):** "only the two-hump shape survives every reasonable bandwidth".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Determinism:** all KDE curves are computed from the hardcoded 12-value array with the exact Gaussian formula above — no `Math.random()` anywhere.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
