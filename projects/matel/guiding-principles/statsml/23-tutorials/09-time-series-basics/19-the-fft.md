# The FFT

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The FFT

**Subtitle:** The Fast Fourier Transform is the same Fourier transform computed cleverly — halving the work over and over cuts n² multiplies to n log n, turning hours into a blink

## One Second of Guitar Sound, One Big Bill

**Tags:** `core idea` (blue), `running example` (green), `n² cost` (orange)

- **The app** — a phone tuner records one second of a guitar's A string: 8,192 sound samples
- **The question** — which frequencies are in the clip? Fourier answers by testing each one
- **The bill** — testing 8,192 frequencies against 8,192 samples costs 8,192² = 67,108,864 multiplies
- **The shortcut** — the FFT gets the exact same answer in 8,192 × 13 = 106,496 multiplies
- **Why 13** — 13 = log2(8,192): the clip can be halved 13 times, and that halving is the whole trick
- **The name** — FFT = Fast Fourier Transform: not a new transform, a fast route to the same one

*Example (italic):* On an early-90s chip doing 1 million multiplies a second, the naive sum takes 67 seconds; the FFT takes a tenth of a second.

**Key point:** The FFT computes the same Fourier transform while cutting n² work down to n·log2(n) — 630× fewer multiplies for one second of audio.

### Visualization (canvas `c1`, 720×300)

Dual panel split by a vertical dashed divider at x=360: the guitar waveform (left) and a log-scale bar pair comparing the two operation counts (right).

- **Title (bold 15px, `#1a5276`, top center):** "One Second of Guitar A (110 Hz): the Question and the Bill".
- **Left panel (waveform):** midline y=160, x from 55 over width 280; 160 points, i = 0..159, t = i/4000 s (first 40 ms), y = 1.0·sin(2π·110·t) + 0.5·sin(2π·220·t), amplitude scaled to 70px; blue `#2a78d6` 2px line; ink `#1a5276` bold 12px annotation "which frequencies are inside?" at top of panel; caption 12px `#444` below baseline "first 40 ms of the 8,192-sample clip".
- **Right panel (op-count bars):** axis origin x=400, width 280, baseline y=245, chart height 185; y maps log10(ops) over range 0–8; two 70px-wide bars centered at x=470 and x=610; naive bar fill `rgba(217,89,38,0.55)` (orange) height for log10(67,108,864)=7.83, labeled "naive: 67,108,864" bold 12px orange `#d95926` above; FFT bar fill `rgba(0,131,0,0.4)` height for log10(106,496)=5.03, labeled "FFT: 106,496" bold 12px green `#008300` above; green bold 13px annotation "630× fewer multiplies, same answer"; caption 12px `#444` "multiplies needed (log scale)".
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-12.

## Halving the Clip Until It Is Trivial

**Tags:** `worked example` (blue), `divide and conquer` (green)

- **Tiny clip** — shrink to 8 samples so the trick fits on paper: naive cost is 8 × 8 = 64 multiplies
- **Split** — separate even positions (0,2,4,6) from odd (1,3,5,7): two 4-sample problems
- **Reuse** — the two half-size answers merge into the full answer with just 8 extra operations
- **Repeat** — halve again down to 2-sample problems: 3 rounds of halving, since log2(8) = 3
- **Count** — 3 rounds × 8 operations = 24 total, versus 64 naive — even 8 points already win

*Example (italic):* For the 8,192-sample guitar clip the same halving runs 13 rounds of 8,192 operations each — exactly the 106,496 from the tuner.

**Key point:** Divide, solve the halves, merge in n steps: depth log2(n) times width n is where n·log(n) comes from.

### Visualization (canvas `c2`, 720×300)

Butterfly diagram for an 8-point FFT: four node columns connected by three rounds of pairwise merges, each round colored differently.

- **Title (bold 15px, `#1a5276`, top center):** "8-Point FFT: Halve, Solve, Merge — 3 Rounds of 8 Operations".
- **Nodes:** 8 rows at y = 60, 90, 120, 150, 180, 210, 240, 270; four columns of 5px ink `#1a5276` dots at x = 110, 270, 430, 590; input labels "s0".."s7" 12px `#444` left of column 1.
- **Column headers (bold 12px, `#1a5276`):** "8 samples" (x=110), "pairs merged" (x=270), "quads merged" (x=430), "full spectrum" (x=590), all at y=44.
- **Round 1 (blue `#2a78d6`, 2px):** between columns 1→2, cross-connect row pairs (0,1), (2,3), (4,5), (6,7): each node links to both nodes of its pair.
- **Round 2 (aqua `#199e70`, 2px):** between columns 2→3, cross-connect span-2 pairs (0,2), (1,3), (4,6), (5,7).
- **Round 3 (violet `#4a3aa7`, 2px):** between columns 3→4, cross-connect span-4 pairs (0,4), (1,5), (2,6), (3,7).
- **Round captions (11px, matching each round's color):** "8 ops" centered under each of the three line bundles at y=288.
- **Takeaway (bold 13px green `#008300`, bottom right):** "3 × 8 = 24 multiplies (naive: 64)".

## From Hours to a Blink

**Tags:** `where it's used` (blue), `scaling` (orange), `rule of thumb` (green)

- **Scaling law** — double the data: the naive cost grows ×4, the FFT cost barely more than ×2
- **A million samples** — naive needs 1.1 trillion multiplies; the FFT needs 21 million — 52,000× less
- **In seconds** — at 100 million multiplies per second that is 3 hours versus 0.2 seconds
- **Everywhere** — MP3, JPEG, MRI scans, and Wi-Fi run FFTs thousands of times every second
- **History** — Cooley and Tukey published it in 1965, turning spectra from a luxury into a default

*Example (italic):* An MRI reconstructs each image slice with FFTs; at n² cost a routine scan would keep the patient in the tube all day (illustrative).

**Key point:** n·log(n) versus n² is the difference between real-time and overnight — the FFT is what made frequency analysis a routine everyday tool.

### Visualization (canvas `c3`, 720×300)

Log-log growth chart: multiplies needed versus clip length for the naive n² method and the FFT, with the gap at one million samples bracketed.

- **Title (bold 15px, `#1a5276`, top center):** "Multiplies Needed as the Clip Grows: n² vs n·log2(n)".
- **Axes:** origin x=70, baseline y=245, plot width 560, height 190; x positions evenly spaced for n = 2^10..2^20 with tick labels "1k", "4k", "16k", "64k", "256k", "1M" (12px `#444`); y maps log10(ops) over range 3–13 with tick labels "10³", "10⁶", "10⁹", "10¹²" (11px `#666`) and light grid lines `#e5e9ef`.
- **Naive curve (orange `#d95926`, 3px, 4px dots):** log10 values `[6.02, 7.22, 8.43, 9.63, 10.84, 12.04]` at the six ticks (n² for n = 1,024; 4,096; 16,384; 65,536; 262,144; 1,048,576).
- **FFT curve (green `#008300`, 3px, 4px dots):** log10 values `[4.01, 4.69, 5.36, 6.02, 6.67, 7.32]` (n·log2(n) = 10,240; 49,152; 229,376; 1,048,576; 4,718,592; 20,971,520).
- **Annotations:** orange bold 12px near top right "n²: 1.1 trillion (~3 hours)"; green bold 12px near its right endpoint "n·log n: 21 million (~0.2 s)"; magenta `#d55181` bold 13px vertical bracket between the two right endpoints labeled "52,000×".
- **Caption (12px `#444`, bottom):** "times assume 100 million multiplies per second".

## Fast, Not Different

**Tags:** `common mistake` (red), `naming` (orange)

- **Same answer** — the FFT outputs exactly the DFT's numbers; only the arithmetic route changes
- **Not approximate** — no accuracy is traded for speed; differences are only float rounding
- **Loose talk** — people say "the FFT of the signal" meaning the spectrum; the FFT is the algorithm
- **Powers of two** — the classic version wants n = 2^k like 8,192; modern libraries handle any n
- **Padding** — zero-padding a clip up to the next power of two is a convenience, not a requirement

*Example (italic):* Both methods put the guitar's peak at 110 Hz with magnitude 1.00 and its overtone at 220 Hz with 0.50 — the printouts match digit for digit.

**Common mistake:** Thinking "FFT" names a different or approximate transform. It is the same Fourier transform computed cleverly — use it anywhere you would compute a DFT.

### Visualization (canvas `c4`, 720×300)

Twin spectra split by a vertical dashed divider at x=360: the identical guitar spectrum computed the slow way (left) and by FFT (right).

- **Title (bold 15px, `#1a5276`, top center):** "The Same Spectrum, Computed Both Ways".
- **Data (both panels, identical):** 8 frequency bins labeled "0", "55", "110", "165", "220", "275", "330", "385" (Hz, 11px `#444` below bars); magnitudes `[0.02, 0.05, 1.00, 0.06, 0.50, 0.04, 0.02, 0.01]`.
- **Left panel:** axis origin x=55, width 280, baseline y=235, chart height 165, y scale 0–1.1; bars fill `rgba(42,120,214,0.45)`; header bold 12px orange `#d95926` "slow DFT — 67,108,864 multiplies" at top of panel.
- **Right panel:** axis origin x=400, width 280, same baseline/height/scale; identical bars, fill `rgba(0,131,0,0.4)`; header bold 12px green `#008300` "FFT — 106,496 multiplies".
- **Peak labels (both panels, bold 12px ink `#1a5276`):** "1.00" above the 110 Hz bar, "0.50" above the 220 Hz bar.
- **Divider:** dashed `#bdc3c7` (dash 4/3) vertical line at x=360 from y=38 to h-40.
- **Takeaway (bold 13px magenta `#d55181`, bottom center):** "identical spectra, digit for digit — only the work differs".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper (here it hardcodes 720×300) sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
