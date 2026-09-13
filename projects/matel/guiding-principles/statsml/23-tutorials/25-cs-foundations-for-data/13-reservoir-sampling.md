# Reservoir Sampling

**Page type:** detail page (tutorial card-sections, two-column layout: text left 50%, canvas right 50%, one table row per section)
**HTML title tag:** Reservoir Sampling

**Subtitle:** Keep a fair 1,000-row sample from a stream that never announces its end — one pass, fixed memory, no total needed

## A Fair 1,000 From a Stream of Unknown Length

**Tags:** core idea (blue), running example (green)

- **The problem** — events stream past all day; you can store 1,000, and nobody knows the final count
- **Fill first** — keep the first 1,000 events as they arrive; the tank (the "reservoir") is full
- **Event number i** — keep it with probability 1000/i; if kept, it evicts one random resident
- **So** — event 2,000 gets in with chance 50%; event 100,000 with chance 1%
- **The magic** — at every moment, all events seen so far are equally likely to be in the tank

*Example:* After 50,000 events, each one — first or last — sits in the sample with probability exactly 2%.

**Key point:** One pass, 1,000 slots, no idea how long the stream is — and the sample is a fair random draw at any stopping point.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the reservoir sampling mechanism: stream → decision diamond → reservoir, with a discard branch and an eviction arrow.

- **Title (bold 15px, `#1a5276`, top center):** "One Decision per Arriving Event — Memory Never Grows"
- **Incoming stream (left):** six 26×26px squares in blue `#2a78d6` with increasing opacity (0.25 to 0.85), labeled below in 12px `#444`: "stream: event i arrives"; blue arrow pointing right to the decision diamond.
- **Decision diamond (center):** diamond shape at ~x=345, fill `rgba(201,133,0,0.15)`, yellow `#c98500` stroke width 2; two bold 13px yellow text lines inside: "keep it?" / "prob = 1000/i".
- **Keep branch:** green `#008300` arrow to the reservoir, bold 12px green label "keep".
- **Discard branch:** gray `#6b7280` arrow downward, 12px gray label "discard (most events)".
- **Reservoir (right):** 190×120px box, fill `rgba(25,158,112,0.10)`, aqua `#199e70` stroke width 2, containing a 10×5 grid of small aqua dots (radius 4, `rgba(25,158,112,0.65)`); bold 13px aqua label below: "reservoir: exactly 1,000 rows".
- **Eviction:** magenta `#d55181` arrow down out of the reservoir with bold 12px magenta label: "if kept: one random resident evicted".
- **Takeaway (bottom left, bold 13px orange `#d95926`):** "event 2,000: 50% in · event 100,000: 1% in — yet the final sample is fair"

## Tank of 3, Stream of 10 — Check It by Hand

**Tags:** worked example (green)

- **Shrink it** — a reservoir of 3 and a stream of 10 events, small enough to verify on paper
- **Events 1-3** — walk straight in; the tank holds {1, 2, 3}
- **Event 4** — kept with probability 3/4; if kept, it replaces one of the three at random
- **Event 7** — kept with probability 3/7; event 10 with probability 3/10
- **The result** — every event 1 through 10 ends in the tank with probability exactly 3/10
- **Check event 1** — free entry, then must dodge 7 evictions; the odds multiply out to 3/10

*Example:* Event 10 enters with chance 3/10 and never faces an eviction after — also exactly 3/10.

**Key point:** Early events enter free but risk eviction; late events enter rarely but stay safe — every path lands on 3/10.

### Visualization (canvas `c2`, 720×300)

Combined bar + line chart: flat final-inclusion bars with a declining entry-probability line, for k=3, n=10.

- **Title (bold 15px, `#1a5276`, top center):** "Reservoir of 3, Stream of 10: Where Each Event Ends Up"
- **Axes:** x = event number 1–10 (labels under each bar, caption "event number"); y from 0% to 100% with gridlines at 0/25/50/75/100% (light `#e5e9ef`, gray labels); gray `#999` L-shaped axis frame. Padding: top 52, bottom 56, left 70, right 185.
- **Bars:** 10 bars (30px wide) all at final inclusion probability 30%, fill `rgba(74,58,167,0.55)` (violet).
- **Line (orange `#d95926`, width 2.5, dots radius 4):** P(kept on arrival) per event: `[1, 1, 1, 0.75, 0.60, 0.50, 0.429, 0.375, 0.333, 0.30]`; bold 12px orange fraction labels at events 4, 7 and 10: "3/4", "3/7", "3/10".
- **Annotation:** on a white background strip near the 30% level, bold 13px violet `#4a3aa7` centered text: "every event ends up in the tank with exactly 30%".
- **Legend (right side, 12px `#222`):** violet swatch "P(in final sample)"; orange swatch "P(kept on arrival)".

## Spot-Checking an Endless Event Feed

**Tags:** where it's used (blue), common mistake (red)

- **The job** — eyeball 1,000 raw events a day from a feed logging millions, to catch bad data early
- **"First 1,000" fails** — all from just after midnight: batch jobs and one time zone dominate
- **"Last 1,000" fails** — only the final minutes: this morning's outage is invisible
- **Reservoir wins** — 1,000 rows spread fairly over the whole day, in one pass, in fixed memory
- **Also used for** — log sampling in stream processors, fair training subsets, quick profiling

*Example:* A midnight-only sample can miss the daytime traffic mix entirely — the fair sample sees every hour.

**Key point:** Convenience samples (first N, last N) inherit the stream's time patterns; the reservoir gives every event the same chance.

### Visualization (canvas `c3`, 720×300)

Hourly traffic bar chart with fair-sample dots and a callout on hour 0.

- **Title (bold 15px, `#1a5276`, top center):** "One Day of Events by Hour — and the Slice \"First 1,000\" Sees"
- **Data:** 24 hourly volumes in thousands: `[30, 20, 15, 12, 10, 12, 20, 40, 70, 90, 100, 105, 110, 108, 100, 95, 90, 85, 80, 70, 60, 50, 45, 38]`.
- **Axes:** x = hour of day 0–23 (labels every 3 hours, caption "hour of day (illustrative volumes)"); y 0–120k with gridlines at 0/30k/60k/90k/120k; gray `#999` axis frame. Padding: top 52, bottom 56, left 70, right 30.
- **Bars:** 20px wide; hour 0 highlighted magenta `rgba(213,81,129,0.75)`, all others blue `rgba(42,120,214,0.40)`.
- **Fair-sample dots:** one green `#008300` dot (radius 3.5) floating 10px above each bar top — the reservoir sample reaches every hour of the day.
- **Callout:** dashed magenta leader line (dash 4/3, width 1.5) from the hour-0 bar to bold 13px magenta text: "\"first 1,000\" all come from this sliver"; below it, bold 13px green two-line text: "reservoir sample: spread over all 24 hours —" / "every hour represented (green dots)".

## But Don't Late Events Get a Worse Deal?

**Tags:** common mistake (red), watch out (orange)

- **The worry** — event 50,000 gets only a 2% entry chance while event 1 walked in free: unfair?
- **Two forces** — getting IN (harder over time) and STAYING in (safer the later you arrive)
- **Early events** — entered free, then risked eviction at each of the 49,000 later arrivals
- **Late events** — enter with slim odds but face almost no eviction rounds afterwards
- **Multiply** — the two forces cancel exactly: in-and-stayed = 1000/50,000 = 2% for everyone

*Example:* Event 25,000: enters with 4%, survives the rest with 50% — 4% × 50% = 2%, same as everyone.

**Common mistake:** Judging fairness by the entry ticket. The declining entry odds are precisely what makes the final sample fair.

### Visualization (canvas `c4`, 720×300)

Three-curve line chart: entry odds fall, survival odds rise, their product stays flat at 2%.

- **Title (bold 15px, `#1a5276`, top center):** "Two Forces That Cancel (k = 1,000, stream of 50,000)"
- **Axes:** x = event number i from 1,000 to 50,000 (tick labels 1,000 / 10,000 / 25,000 / 40,000 / 50,000, caption "event number i"); y from 0% to 100% with gridlines at 0/25/50/75/100%; gray `#999` axis frame. Padding: top 52, bottom 56, left 70, right 190.
- **Curves:**
  - P(gets in) = 1000/i — orange `#d95926`, width 3, hyperbolic decline.
  - P(survives) = i/50,000 — aqua `#199e70`, width 3, straight rise from 2% to 100%.
  - Product — violet `#4a3aa7`, width 4, flat horizontal line at 2%.
- **Marker at i=25,000:** dashed gray vertical guide from the baseline up to 50%; orange dot (radius 5) at (25,000, 4%) labeled bold 12px orange "gets in: 4%"; aqua dot at (25,000, 50%) labeled bold 12px aqua "survives: 50%".
- **Annotation (bold 13px violet, above the flat product line):** "4% × 50% = 2% — the product is flat for everyone"
- **Legend (right side, 12px `#222`):** orange swatch "P(gets in) = 1000/i"; aqua swatch "P(survives) = i/50,000"; violet swatch "product = 2%".

## Regeneration instructions

- **Template:** tutorials topic-page layout (social-graph reference style): `<h1>` (no index number), `.subtitle`, then four `.card-section` blocks each with an `<h2>` and a `table.layout` with one `<tr>`: left `<td class="text-col">` (50%) holding `.tags` pills, a `<ul>` of one-line bullets with `<b>` lead terms, an italic `.example` line, and a `.key-point` callout; right `<td class="viz-col">` (50%) holding one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border; section h2 1.3rem `#1a5276` with 2px `#2980b9` bottom border; `.subtitle` `#666` 0.95rem. Bullets 0.92rem, `li b` in `#1a5276`; inline `code` in ui-monospace on `#f4f6f8`. `.example` italic `#555` 0.9rem. `.key-point` background `#f8f9fa`, left border 3px solid `#e74c3c`, padding 8px 12px, 0.9rem.
- **Tag pills:** `.tag` inline-block, 0.72rem bold, padding 2px 10px, radius 10px; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Canvas:** intrinsic 720×300, CSS `width:100%`, 1px `#e0e0e0` border, radius 4px; scaled with `window.devicePixelRatio` via a shared `setup(id)` helper (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); a shared `arrowLine(ctx, x1, y1, x2, y2, color)` helper draws arrows with filled triangular heads.
- **Chart palette (`P` object):** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- No nav bar, no back/home links, no cross-page links. In regenerated HTML any card links would use `.html` extensions.
