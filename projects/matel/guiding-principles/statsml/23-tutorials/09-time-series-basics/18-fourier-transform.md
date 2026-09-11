# Fourier Transform

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Fourier Transform

**Subtitle:** Any wiggly signal is a sum of simple waves — the Fourier transform reads off the recipe: which wave speeds are in the mix, and how big each one is

## One Messy Line, Two Hidden Waves

**Tags:** `core idea` (blue), `sum of waves` (green), `frequency view` (orange)

- **The café** — a café logs customers every hour for one week: 168 numbers that look like chaos
- **Hidden waves** — the line is exactly a flat base of 40, a daily wave of ±25, a weekly wave of ±10
- **Fourier's claim** — any signal, however messy, is a sum of simple waves at different speeds
- **The transform** — it lists each hidden wave's speed and size, turning 168 wiggles into a recipe
- **Two views** — the time view shows when things happened; the frequency view shows what rhythms exist

*Example (italic):* At hour 6 the daily wave peaks: 40 + 25 + 2 ≈ 67 customers — the first lunchtime rush of the week.

**Key point:** The Fourier transform rewrites a signal as a sum of waves and reports the size of each. Same data, new ruler: instead of "value at each hour" you get "size at each speed".

### Visualization (canvas `c1`, 720×300)

Dual-panel line chart: the messy combined café signal (left) and its three separated ingredients on the same axes (right), split by a vertical dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "One Week of Café Visitors = Baseline + Daily Wave + Weekly Wave".
- **Data:** t = 0..167 hours; combined signal v(t) = 40 + 25·sin(2π·t/24) + 10·sin(2π·t/168).
- **Left panel (combined):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–80; blue `#2a78d6` 2.5px line of v(t); day labels "Mon".."Sun" 12px `#444` below baseline at every 24 h; blue bold 12px annotation "168 hourly readings"; caption 12px `#444` "the raw hourly log".
- **Right panel (ingredients):** axis origin x=400, width 280, same baseline/height, y scale 0–80; dashed ink `#1a5276` 2px flat line at 40 labeled "baseline 40" (12px); green `#008300` 2.5px line 40 + 25·sin(2π·t/24) labeled "daily ±25"; orange `#d95926` 2.5px line 40 + 10·sin(2π·t/168) labeled "weekly ±10"; green bold 13px annotation "add these three → the left chart".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Scoring Every Candidate Wave

**Tags:** `worked example` (blue), `rule of thumb` (green)

- **The test** — to check for a rhythm, multiply the signal by a test wave at that speed, then average
- **A match** — the 7-cycles-per-week test wave scores 12.5, exactly half the daily wave's size of 25
- **A miss** — a test wave at 5 cycles per week scores 0: positive and negative products cancel out
- **The spectrum** — double each speed's score (speed 0: just the average): spikes 25 (daily), 10 (weekly)
- **Read it** — 7 cycles per week means a 24-hour period; a faster wave means a shorter period

*Example (italic):* Sweeping test waves from 0 to 14 cycles per week, only speeds 0, 1, and 7 score nonzero — the whole week fits in a three-line recipe.

**Key point:** The Fourier transform is this multiply-and-average test run at every speed at once. The spectrum is just the list of scores — a spike marks a real rhythm.

### Visualization (canvas `c2`, 720×300)

Stem plot of the café signal's frequency spectrum: wave size versus speed in cycles per week, with three labeled spikes.

- **Title (bold 15px, `#1a5276`, top center):** "The Frequency Recipe: Wave Size at Each Speed".
- **Data:** speeds 0–14 cycles/week with sizes `[40, 10, 0, 0, 0, 0, 0, 25, 0, 0, 0, 0, 0, 0, 0]`.
- **Axes:** origin x=70, width 580, baseline y=240, chart height 180, y scale 0–45; speed labels "0".."14" 12px `#444` below baseline, axis caption "cycles per week"; y-axis label "wave size" 12px.
- **Stems:** nonzero stems 5px wide with 6px dots on top — speed 0 in ink `#1a5276`, speed 1 in orange `#d95926`, speed 7 in green `#008300`; zero speeds drawn as 2px mute `#6b7280` ticks on the baseline; bold 13px value labels "40", "10", "25" above their stems in the stem's color.
- **Annotations:** green bold 13px near speed 7: "daily rhythm: 7 cycles/wk = 24-h period"; orange bold 12px near speed 1: "weekly rhythm"; ink 12px near speed 0: "baseline (average level = 40)".
- **Caption (12px `#444`, bottom):** "multiply-and-average test: a match scores size ÷ 2 (daily: 12.5), a miss scores 0".

## Three Numbers Instead of 168

**Tags:** `where it's used` (blue), `denoising` (orange), `compression` (green)

- **Real data** — add sensor noise and the hourly readings now jump around by up to ±10 each hour
- **Noise spreads** — noise smears small sizes across every speed; real rhythms stay as tall spikes
- **Denoise** — keep the three tall spikes (40, 25, 10), zero everything else, rebuild: a clean curve
- **Compress** — 168 noisy readings shrink to 3 sizes plus their speeds; MP3 and JPEG use a close cousin
- **Seasonality** — spike hunting is how libraries auto-detect daily and weekly cycles in your data

*Example (italic):* The dashboard built from the 3-number rebuild shows the café's true rhythm; the raw feed shows every jitter.

**Key point:** In the frequency view, "clean the signal" becomes "keep some spikes, drop others". Filtering, compression, and seasonality detection are all versions of that one move.

### Visualization (canvas `c3`, 720×300)

Dual-panel before/after: the noisy hourly signal (left) versus the smooth curve rebuilt from only the three spectrum spikes (right), split by a dashed divider at x=360.

- **Title (bold 15px, `#1a5276`, top center):** "Denoising: Keep the 3 Tall Spikes, Drop the Rest".
- **Data:** t = 0..167; noisy(t) = 40 + 25·sin(2π·t/24) + 10·sin(2π·t/168) + 6·sin(2.9·t) + 4·sin(1.3·t + 0.7) (deterministic jitter, coefficients hardcoded); rebuilt(t) = 40 + 25·sin(2π·t/24) + 10·sin(2π·t/168).
- **Left panel (noisy):** axis origin x=55, width 280, baseline y=245, chart height 185, y scale 0–90; blue `#2a78d6` 2px line of noisy(t); day labels "Mon".."Sun" 12px `#444`; magenta `#d55181` bold 12px annotation "raw: jitters up to ±10"; caption 12px `#444` "168 noisy hourly readings (illustrative)".
- **Right panel (rebuilt):** axis origin x=400, width 280, same baseline/height and y scale; green `#008300` 3px line of rebuilt(t); same day labels; green bold 13px annotation "rebuilt from 3 numbers: 40, 25, 10"; caption "same axes, noise speeds zeroed".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## The Recipe Forgets the Clock

**Tags:** `common mistake` (red), `timing` (orange)

- **Two cafés** — one is busy only the first half of the week, the other only the second half
- **Same spectrum** — both show one spike near 7 cycles per week; the transform cannot tell them apart
- **No clock** — the spectrum says which rhythms exist overall, never when they happened
- **The fix** — slice the signal into windows and transform each slice (a spectrogram) to recover timing
- **Not causes** — a spike at 7 per week says "a daily pattern exists", not why it exists

*Example (italic):* A café busy only Mon–Wed and one busy only Fri–Sun can hand you the very same spectrum.

**Common mistake:** Reading a spectrum as a timeline. Identical spectra can hide opposite timings — when "when" matters, use windowed transforms (spectrograms), not one global recipe.

### Visualization (canvas `c4`, 720×300)

Three-part chart: two stacked mini time panels on the left (signals A and B) feeding one shared spectrum panel on the right, split by a dashed divider at x=380.

- **Title (bold 15px, `#1a5276`, top center):** "Two Different Weeks, One Spectrum".
- **Data (time panels):** t = 0..167; signal A = 40 + 25·sin(2π·t/24) for t < 84, flat 40 for t ≥ 84; signal B = flat 40 for t < 84, 40 + 25·sin(2π·t/24) for t ≥ 84.
- **Panel A (top left):** axis origin x=55, width 290, baseline y=135, chart height 85, y scale 0–80; blue `#2a78d6` 2px line; bold 12px blue label "A: busy first half".
- **Panel B (bottom left):** axis origin x=55, width 290, baseline y=250, chart height 85, same y scale; violet `#4a3aa7` 2px line; bold 12px violet label "B: busy second half"; day labels "Mon".."Sun" 11px `#444` below this baseline only.
- **Right panel (shared spectrum, baseline removed):** axis origin x=420, width 250, baseline y=235, chart height 165, y scale 0–15; speeds 0–14 with sizes `[0, 0, 0, 0, 0, 2, 4, 12.5, 4, 2, 0, 0, 0, 0, 0]`; magenta `#d55181` 5px stems with 5px dots; speed labels every 2 (11px `#444`), axis caption "cycles per week"; magenta bold 13px annotation "A and B give this same spectrum (illustrative)"; caption 12px `#444` "timing is gone — the transform has no clock".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=380 from y=38 to h-12.

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
