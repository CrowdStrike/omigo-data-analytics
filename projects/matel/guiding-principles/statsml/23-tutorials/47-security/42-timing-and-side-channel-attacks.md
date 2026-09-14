# Timing & Side-Channel Attacks

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Timing & Side-Channel Attacks

**Subtitle:** A system can be mathematically secure and still leak its secrets through how long it takes, how much power it draws, or which cache lines it touches — the answer leaks through the stopwatch

## The Token Check That Answers Too Quickly

**Tags:** `core idea` (blue), `timing leak` (red), `defensive` (green)

- **The server** — an API checks a submitted token against the real one, `kxmwqzpt`, byte by byte
- **The shortcut** — the comparison returns early at the first mismatched character
- **The leak** — a guess with 3 correct leading characters runs longer than one with 0 correct
- **The clock** — each matched byte adds ~0.4 µs on top of a ~1.0 µs base (numbers illustrative)
- **The lesson** — the return value says only yes/no, but the response TIME says how close you got

*Example (italic):* Guess `aaaaaaaa` is rejected in 1.0 µs; guess `kxmaaaaa` is also rejected — but in 2.2 µs, telling the attacker three characters are right.

**Key point:** A side channel is any observable behavior — time, power, cache state, even sound — that reveals more than the official answer; here the rejection is identical but the timing is not.

### Visualization (canvas `c1`, 720×300)

Staircase line chart: server response time vs number of correct leading characters in the guess, for an early-exit comparison.

- **Title (bold 15px, `#1a5276`, top center):** "Early-Exit Compare: Response Time Climbs With Every Correct Leading Byte".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = correct prefix length 0 to 8 with 12px `#444` tick labels at every integer; y = response time 0 to 5 µs, gridlines `#e5e9ef` at 1/2/3/4, 12px `#444` labels "1µs"–"4µs".
- **Staircase line:** blue `#2a78d6` 3px step line through prefix lengths `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, times (µs) `[1.0, 1.4, 1.8, 2.2, 2.6, 3.0, 3.4, 3.8, 4.2]`, 4px radius blue dots at each point.
- **Marker:** vertical dashed `#6b7280` (dash 4/3) line at prefix length 3, 12px `#6b7280` label "guess kxmaaaaa → 2.2 µs" beside it.
- **Annotation (bold 13px red `#e74c3c`, near x=6, y=90):** "each matched byte adds 0.4 µs — the clock answers a question the API never would".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## Stealing the Token One Character at a Time

**Tags:** `worked example` (blue), `linear search` (orange)

- **The setup** — the attacker already knows the first two characters are `kx` and probes position 3
- **The probe** — send 26 guesses `kxaaaaaa` … `kxzaaaaa`, timing each rejection 1,000 times
- **The tell** — 25 medians cluster near 1.8 µs; the guess `kxmaaaaa` alone takes 2.2 µs, so position 3 is `m`
- **The collapse** — blind search over 8 lowercase letters is 26⁸ ≈ 209 billion guesses
- **Hand-check** — with timing, it is at most 26 tries per position × 8 positions = 208 guesses total

*Example (italic):* An impossible 209-billion-guess search becomes 208 guesses (~208,000 timed requests) — the side channel turns exponential search into linear search.

**Key point:** Timing lets the attacker confirm each character independently, so the guesses ADD (26×8) instead of MULTIPLYING (26⁸) — that is the entire catastrophe.

### Visualization (canvas `c2`, 720×300)

Bar chart of the 26 probe guesses for position 3: median rejection time per candidate letter, with one bar standing above the rest.

- **Title (bold 15px, `#1a5276`, top center):** "Probing Position 3: One Letter Answers Slower Than the Other 25".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = candidate letters a–z, 11px `#444` labels under every bar (bar width 16px, gap 7px); y = median time 0 to 2.5 µs, gridlines `#e5e9ef` at 0.5/1.0/1.5/2.0.
- **Bars:** letters a–z with median times (µs) `[1.79, 1.82, 1.78, 1.81, 1.80, 1.77, 1.83, 1.79, 1.81, 1.78, 1.80, 1.82, 2.20, 1.79, 1.77, 1.81, 1.80, 1.78, 1.83, 1.79, 1.82, 1.80, 1.77, 1.81, 1.79, 1.80]`; all bars fill `rgba(42,120,214,0.35)` except index 12 (`m`) solid red `#e74c3c`.
- **Callout:** bold 12px red `#e74c3c` label "m — 2.2 µs" above the tall bar, thin red arrow to it.
- **Annotation (bold 13px violet `#4a3aa7`, upper left near y=70):** "26×8 = 208 timed guesses replace 26⁸ ≈ 209 billion".
- **Caption (12px `#444`, bottom right):** "median of 1,000 timings per letter, illustrative".

## The Fix, and the Bigger Family of Leaks

**Tags:** `where it's used` (blue), `constant time` (green), `cache timing` (orange)

- **The fix** — constant-time comparison: always compare every byte, OR the differences, answer at the end
- **The result** — every rejection takes the same 4.2 µs, so the stopwatch learns nothing
- **Cache timing** — shared CPU caches let one process infer which memory another process just touched
- **Spectre/Meltdown** — this publicly documented class showed speculative execution leaks across boundaries
- **Crypto libraries** — they obsess over constant-time code because keys leak through branches and lookups
- **Other channels** — power draw, electromagnetic emission, and even sound have all carried secrets

*Example (italic):* After the server switches to a constant-time compare, `aaaaaaaa` and `kxmaaaaa` are both rejected in exactly 4.2 µs — the 208-guess attack is dead.

**Key point:** The defense is to make observable behavior independent of the secret — a security boundary must account for what an observer can MEASURE, not just what the API returns.

### Visualization (canvas `c3`, 720×300)

Two-line comparison on shared axes: the leaky early-exit staircase vs the flat constant-time line, across correct-prefix lengths.

- **Title (bold 15px, `#1a5276`, top center):** "Constant-Time Compare: The Flat Line Has Nothing to Measure".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = correct prefix length 0 to 8, 12px `#444` integer tick labels; y = response time 0 to 5 µs, gridlines `#e5e9ef` at 1/2/3/4.
- **Early-exit line:** red `#e74c3c` 3px step line through prefix lengths `[0, 1, 2, 3, 4, 5, 6, 7, 8]`, times (µs) `[1.0, 1.4, 1.8, 2.2, 2.6, 3.0, 3.4, 3.8, 4.2]`, 12px red label "early exit (leaks)" near its middle.
- **Constant-time line:** green `#008300` 3px flat line at 4.2 µs across the full x range, 12px green label "constant time — always all 8 bytes" above it at x≈2.
- **Annotation (bold 13px green `#008300`, near x=5, y=110):** "same time for every guess → timing reveals nothing".
- **Caption (12px `#444`, bottom right):** "timings illustrative; flat line pays full cost on purpose".

## "The Network Is Too Noisy to Time That"

**Tags:** `common mistake` (red), `averaging` (orange)

- **The hope** — a 0.4 µs signal drowns under ±5 µs of network jitter, so the leak seems unusable
- **The math** — averaging n samples shrinks random noise by √n while the signal stays put
- **Hand-check** — 100 samples: ±0.5 µs left; 400 samples: ±0.25 µs — already below the 0.4 µs signal
- **The patience** — at 10,000 samples per guess the noise floor is ±0.05 µs, eight times under the signal
- **The corollary** — adding RANDOM delay does not help either; it is just more noise to average away

*Example (italic):* At 400 repeated timings per guess the residual noise (±0.25 µs) drops below the 0.4 µs per-byte signal, and the character-by-character attack works over the network.

**Common mistake:** Trusting noise as a defense. Noise averages out; a systematic timing difference does not — only removing the difference (constant-time code) closes the channel.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: residual noise after averaging n samples, against a fixed vertical line marking the 0.4 µs signal.

- **Title (bold 15px, `#1a5276`, top center):** "Averaging Beats Jitter: Noise Shrinks by √n, the Signal Does Not".
- **Axis:** vertical 2px `#999` baseline at x=230, bars extend right, scale 80px per µs (max width 400); left-aligned 12px `#444` row labels at x=20.
- **Rows (top to bottom at y = 60, 105, 150, 195, 240):**
  - "n = 1 — noise ±5.0 µs": red `#e74c3c` bar width 400
  - "n = 100 — noise ±0.5 µs": red `#e74c3c` bar width 40
  - "n = 400 — noise ±0.25 µs": green `#008300` bar width 20
  - "n = 2,500 — noise ±0.10 µs": green bar width 8
  - "n = 10,000 — noise ±0.05 µs": green bar width 4
- **Signal marker:** vertical dashed `#4a3aa7` (dash 4/3) line at x=262 (0.4 µs), bold 12px violet label "signal: 0.4 µs per byte" at its top.
- **Bar style:** 14px tall, red bars fill `rgba(231,76,60,0.35)`, green bars solid, 11px `#444` value labels at bar ends.
- **Annotation (bold 13px magenta `#d55181`, right side near y=250):** "past n=400 the noise is under the signal — patience defeats jitter".
- **Caption (12px `#444`, bottom right):** "noise = 5 µs / √n, illustrative jitter".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); microsecond timings are invented and labeled illustrative; the search-space figures (26⁸ ≈ 209 billion, 26×8 = 208) and the √n noise reductions (5.0 / 0.5 / 0.25 / 0.10 / 0.05 µs at n = 1 / 100 / 400 / 2,500 / 10,000) are exact arithmetic. Framing is defensive/educational: the attack is shown only to motivate constant-time code; Spectre/Meltdown mentioned at concept level as publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
