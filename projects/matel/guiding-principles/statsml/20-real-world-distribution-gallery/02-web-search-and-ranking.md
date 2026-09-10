# Web Search / Ranking — Distribution Patterns

**Page type:** detail page (three-column obj-table layout: text left ~38%, primary distribution canvas middle ~31%, insight canvas right ~31%; one table per section)
**HTML title tag:** Web Search / Ranking — Distribution Patterns

## What Everyone Searches For — Query Frequency (Zipf's Law)

**Pitfall label (color `#795548`):** FEW WINNERS, ENDLESS LONG TAIL

Think of search queries like pop songs: a tiny handful of hits ("weather", "news") get played millions of times a day, while a vast long tail of obscure queries each get searched once or twice — ever. The popular ones are easy to optimize because you have tons of data. The rare ones? You're basically flying blind. And you can't use the same strategy for both.

- In this simulated data, the top 2% of queries carry roughly 40% of all volume — like a few songs dominating every playlist
- The long tail is full of one-off queries where the engine has almost nothing to learn from
- Head and tail need different handling — hand-tuning works for the hits, not for the one-offs
- If you plot this on a chart, it looks like a cliff followed by an endless flat desert

### Visualization (canvas `canvas-zipf`, 380×320)

Histogram of simulated query volumes drawn with the shared `drawHistogram` helper (bars + Gaussian-smoothed density line with 95% SE band).

- **Data generation:** seeded RNG mulberry32(101); 5000 samples of a power law via inverse CDF `u^(-1/1.2) - 1` (alpha=1.2, shifted to start near 0), capped at 200.
- **Bins/axes:** 50 bins, x from 0 to 200, x labels integer-rounded at 6 ticks; y labels 0 to max count at 5 ticks. X-axis label "Query Volume (binned)", y-axis label (rotated) "Distinct Queries".
- **Title (bold 13px, `#1a5276`):** "Query Frequency — Power Law (Zipf)".
- **Colors:** bar fill `rgba(41,128,185,0.35)`, bar stroke `#2980b9` (0.5px); density line `#2c3e50` 2px; SE band fill `rgba(41,128,185,0.18)`; axes `#333`; tick labels `#555` 11px; axis titles `#1a5276` 12px; white plot background.
- **Layout:** padding top 35, right 20, bottom 45, left 55.

### Visualization (canvas `canvas1b`, 400×340)

Horizontal bar chart on a log scale: illustrative e-commerce query examples from generic head to specific tail.

- **Title (bold 12px, `#1a5276`):** "Query Traffic: Generic Head vs Specific Tail".
- **Data (query, volume/day, type):** `"iphone"` 5,200,000 head; `"shoes"` 3,800,000 head; `"gpu"` 2,100,000 head; `"clothes"` 1,900,000 head; `"nike air max"` 420,000 mid; `"iphone 15 pro"` 180,000 mid; `"red running shoes 10.5"` 2,400 tail; `"iphone 11 pro 256GB used"` 340 tail; `"silver gray free shipping"` 12 tail.
- **Bar width:** proportional to `log10(vol) / log10(max vol)`; bars fill by type: head `rgba(41,128,185,0.75)`, mid `rgba(230,126,34,0.7)`, tail `rgba(231,76,60,0.7)`; strokes are the same colors at full opacity.
- **Labels:** query text right-aligned left of each bar (`#333` 10px); volume label right of each bar in bar color bold 9px, formatted "5.2M/day", "420K/day", "12/day" etc.
- **Brackets (right edge, rotated labels):** blue `#2980b9` bracket spanning the top 4 bars labeled "HEAD"; red `#e74c3c` bracket spanning the bottom 3 bars labeled "TAIL".
- **Bottom insight (`#555` 10px, centered):** "Generic = starting query (millions) → Specific = rare (single digits)".
- **Log scale note (`#999` 9px):** "(illustrative volumes, log scale — bars span ~6 orders of magnitude)".
- **Layout:** padding top 35, right 15, bottom 45, left 150; white background.

## Clicks Concentrate at the Top — Click-Through Rate (Exponential Decay)

**Pitfall label (color `#2980b9`):** STEEP DROP-OFF BY POSITION

Position #1 gets ~30% of all clicks, position #2 gets ~17%, and so on — dropping by nearly half each step. One explanation: users lean on the ranking itself as a relevance signal, defaulting to the top result rather than weighing each option on its merits.

- Clicks drop by nearly half with each position (#1≈30%, #2≈17%, #3≈9%...)
- Consistent with users treating position as a shortcut for relevance
- This shape IS the entire ad revenue model — higher spot = more expensive
- Similar decay curves are widely reported across search engines and marketplaces

### Visualization (canvas `canvas-ctr`, 380×320)

Bar chart via shared `drawBarChart` helper: CTR by search result position.

- **Data generation:** seeded RNG mulberry32(202); positions #1–#10 with `CTR = 30 * 0.55^(i-1) + noise((rng-0.5)*0.5)`, floored at 0.5.
- **Labels:** "#1" … "#10". Y-axis format "N%" (0 decimals), 5 ticks.
- **Title (bold 13px, `#1a5276`):** "Click-Through Rate by Position — Exponential Decay".
- **Axis labels:** x "Search Result Position", y (rotated) "CTR (%)".
- **Colors:** bar fill `rgba(41,128,185,0.35)`, stroke `#2980b9`; axes `#333`; tick labels `#555`; axis titles `#1a5276`; white background.
- **Layout:** bars 70% of slot width with 30% gap; padding top 35, right 20, bottom 45, left 55.

### Visualization (canvas `canvas2b`, 400×340)

Cumulative CTR area chart with a top-3 threshold callout.

- **Title (bold 12px, `#1a5276`):** "Cumulative CTR — Top 3 Capture Most Clicks".
- **Data:** cumulative percentage of total CTR from the same 10 position values as `canvas-ctr` (running sum / total × 100), one point per position centered in its slot.
- **Series:** filled area under curve `rgba(39,174,96,0.3)`; line `#27ae60` 3px; 4px dots at each point — red `#e74c3c` for positions 1–3, green `#27ae60` for 4–10.
- **Threshold:** horizontal dashed (5/4) red `#e74c3c` 2px line at the cumulative value of position 3; right-aligned bold red label above it: "NN% from top 3 alone!" (NN = rounded cumulative % at position 3, ~70%); small red downward arrow pointing at position 3.
- **Axes:** x labels "#1"…"#10" plus title "Position" (`#1a5276`); y labels 0%, 25%, 50%, 75%, 100% (`#555`); axes `#333`; padding top 40, right 25, bottom 50, left 55; white background.

## People Either Bounce or Stay — Dwell Time (Bimodal)

**Pitfall label (color `#27ae60`):** TWO CAMPS, NO MIDDLE GROUND

When someone clicks a search result, one of two things happens in this data: they leave within a few seconds (consistent with a misclick or an instant "wrong page"), OR they stay 30-120 seconds actually reading. The roughly 8-20 second range is essentially empty — like a restaurant where people either walk in and immediately leave, or sit down for a full meal. "Average time on page" blends two completely different behaviors into a number that describes neither.

- Two clusters: "Nope, wrong page" (2-5 seconds) and "This is exactly what I needed" (30-120 seconds)
- The ~8-20 second zone is basically empty — no "moderate interest" shows up
- Averaging these two groups gives you a meaningless number — like averaging the temperature of fire and ice
- Separate the groups before using time-on-page as a quality signal

### Visualization (canvas `canvas-dwell`, 380×320)

Histogram (shared `drawHistogram` helper) of bimodal dwell times.

- **Data generation:** seeded RNG mulberry32(303); 4000 samples: 35% bounce cluster normal(mean 3.5s, sd 1.2, floor 0.5); 65% engaged cluster normal(mean 65s, sd 25, clamped to [20, 150]).
- **Bins/axes:** 50 bins, x from 0 to 150 with labels like "0s"…"150s"; x-axis label "Dwell Time (seconds)", y-axis label "Count".
- **Title:** "Dwell Time — Bimodal (Bounce vs Engaged)".
- **Colors:** bar fill `rgba(230,126,34,0.35)`, stroke `#2980b9`; density line `#2c3e50` with SE band `rgba(41,128,185,0.18)`; standard helper layout and axis colors as above.

### Visualization (canvas `canvas3b`, 400×340)

ECDF step-function chart of the same dwell-time data, highlighting two jumps and an empty middle.

- **Title (bold 12px, `#1a5276`):** "ECDF — Two Jumps Reveal Two Populations".
- **Series:** ECDF of sorted dwell data (subsampled ~300 points), line `#e67e22` 2.5px; x from 0 to 150s.
- **Dead zone:** shaded band `rgba(231,76,60,0.12)` from x=8s to x=20s spanning full plot height, labeled in bold red 10px, two stacked lines centered in the band: "DEAD" / "ZONE".
- **Annotations:** bold orange (`#e67e22`) 11px label "35% bounced" with a short orange arrow pointing to the first plateau (~35% level); bold green (`#27ae60`) label "65% engaged" with a green arrow pointing to the second rise.
- **Axes:** x ticks 0s–150s in 30s steps with title "Dwell Time" (`#1a5276`); y ticks 0%, 25%, 50%, 75%, 100% with rotated title "Cumulative %"; axes `#333`, tick labels `#555`; padding top 40, right 25, bottom 50, left 55; white background.

## Each Search is a Coin Flip of "Found It?" — Queries Per Session (Geometric)

**Pitfall label (color `#e74c3c`):** SAME QUIT RATE EVERY TIME

About 40% of search sessions end after just one query. Of those who try again, another 40% leave after query #2. And so on. It's like a coin flip after each search: "Did I find what I needed?" The drop-off rate is constant — each query has the same chance of being your last. Here's the paradox for search engines: better search means people find answers faster, which means fewer queries, which means less ad revenue. Oops.

- The "give-up rate" is the same after every single search — like flipping the same coin each time
- Steeper drop-off actually means the search engine is BETTER (people find answers faster)
- Better search = fewer queries = fewer ad impressions = less money (the search engine paradox)
- A constant rate is consistent with each query acting independently — your 5th search has about the same chance of being your last as your 1st

### Visualization (canvas `canvas-queries`, 380×320)

Bar chart (shared `drawBarChart` helper) of the number of queries per session.

- **Data generation:** seeded RNG mulberry32(404); 3000 sessions simulated with stop probability p=0.4 per query (geometric, capped at 12); counts plotted for 1–10 queries.
- **Labels:** "1"…"10"; y format integer-rounded counts.
- **Title:** "Queries Per Session — Geometric Distribution".
- **Axis labels:** x "Queries Per Session", y "Sessions".
- **Colors:** bar fill `rgba(41,128,185,0.35)`, stroke `#2980b9`; standard helper layout and colors.

### Visualization (canvas `canvas4b`, 400×340)

Survival-curve chart: fraction of sessions still searching after q queries.

- **Title (bold 12px, `#1a5276`):** "Survival Curve — Constant Drop-off Rate".
- **Data:** survival points computed from the bar-chart counts: point at q=0 with s=1.0, then s = remaining sessions / total after each of queries 1–10; x scaled 0–10.
- **Series:** fill under curve `rgba(142,68,173,0.2)`; line `#8e44ad` 3px; 5px purple `#8e44ad` dots with 1.5px white strokes.
- **Annotation:** red `#e74c3c` bracket with arrowhead between the survival points at q=2 and q=3, with bold red 11px two-line label "~40% drop" / "each step".
- **Bold insight (centered, `#8e44ad` bold 11px):** "Constant hazard rate = memoryless process".
- **Axes:** x ticks 0,2,4,6,8,10 with title "Queries Issued" (`#1a5276`); y ticks 0%–100% in 25% steps; axes `#333`, tick labels `#555`; padding top 40, right 25, bottom 50, left 55; white background.

## Quick Retry vs. New Topic — Time Between Searches (Mixture)

**Pitfall label (color `#8e44ad`):** TWO BEHAVIORS BLENDED TOGETHER

Look at the gap between one search and the next. You'll see two distinct patterns mixed together: a sharp spike at 3-8 seconds (consistent with someone quickly rewording the same question), and a long, slow tail stretching to minutes (consistent with finishing one task and moving on to a new topic). In this data a boundary around 12 seconds cleanly separates the two — one way to tell "still looking for the same thing" apart from "moved on".

- Quick retry in 3-8 seconds — reads like "That didn't work, let me rephrase"
- Long gap of 30+ seconds — reads like "Done with that, now something else entirely"
- The boundary between the two suggests where one search task ends and another begins
- The share of fast retries is a plausible proxy for how often search is failing people

### Visualization (canvas `canvas-reformat`, 380×320)

Histogram (shared `drawHistogram` helper) of gaps between consecutive queries.

- **Data generation:** seeded RNG mulberry32(505); 4000 samples: 45% fast-reformulation spike normal(mean 5s, sd 1.5, floor 1); 55% new-intent tail `15 + Exponential(rate 0.03)`, capped at 180.
- **Bins/axes:** 50 bins, x from 0 to 180 labeled "0s"…"180s"; x-axis label "Gap Between Queries (seconds)", y-axis label "Count".
- **Title:** "Reformulation Gap — Mixture (Spike + Exponential Tail)".
- **Colors:** bar fill `rgba(142,68,173,0.35)`, stroke `#2980b9`; density line `#2c3e50` with SE band; standard helper layout.

### Visualization (canvas `canvas5b`, 400×340)

Jittered dot-strip plot separating the two populations around a 12-second classification boundary.

- **Title (bold 12px, `#1a5276`):** "Before/After — Two Populations Separated".
- **Boundary:** vertical dashed (4/3) dark `#333` 2px line at x=12s (x scale 0–180s), labeled below in bold 10px `#333`: "12s boundary", with a small upward arrow.
- **Background zones:** left of boundary `rgba(231,76,60,0.08)`; right of boundary `rgba(39,174,96,0.08)`.
- **Dots:** data split at 12s; gaps ≤12s drawn as red `rgba(231,76,60,0.7)` 3px dots jittered around an upper strip (25% plot height); gaps >12s as green `rgba(39,174,96,0.7)` 3px dots jittered around a lower strip (65% plot height); each strip subsampled to ~150 dots.
- **Zone labels (bold 11px, centered):** red "FAILURE" / "(same query)" over the left zone; green "NEW INTENT" / "(different topic)" over the right zone.
- **Counts annotation (bold 13px, near bottom of plot):** red "NN%" in left zone and green "NN%" in right zone (computed shares, roughly 45%/55%).
- **Bottom insight (`#8e44ad` bold 10px, centered):** "Spike mass ≈ share of quick retries".
- **Axes:** x ticks at 0, 30, 60, 90, 120, 150, 180 (labeled with "s") plus title "Time Between Queries" (`#1a5276`); padding top 45, right 20, bottom 55, left 45; white background.

## Most People Click Once, But Researchers Go Deep — Click Depth (Geometric with Tail)

**Pitfall label (color `#e67e22`):** FAST DROP-OFF WITH A RESEARCH BUMP

More than half of all search sessions end after a single click — consistent with someone typing a brand name, getting the website, done. Each additional click is about half as likely as the previous one. BUT about 11% of sessions dig 5+ pages deep — consistent with comparison shopping or research. They're a completely different population from the one-click crowd, and optimizing for "average clicks" mashes the two together and helps neither.

- 55% of sessions: one click and done — like typing a URL directly
- Each extra click halves in likelihood — most people stop quickly
- About 11% of sessions go 5+ clicks deep — a distinct research/comparison mode
- In the illustration on the right, deeper sessions carry more revenue per click — deep engagement is a different (and often more valuable) mode

### Visualization (canvas `canvas6`, 380×320)

Histogram (shared `drawHistogram` helper) of clicks per search session.

- **Data generation:** seeded RNG mulberry32(606); 2000 sessions: 55% single click (depth 1); 37% geometric(0.5) starting at depth 2 (capped 12); 8% research tail uniform integer in [5, 12].
- **Bins/axes:** 12 bins, x from 0 to 12 with integer labels; x-axis label "Clicks", y-axis label "Count".
- **Title:** "Click Depth per Search Session".
- **Colors:** bar fill `rgba(41,128,185,0.5)`, stroke `#2980b9`; density line `#2c3e50` with SE band; standard helper layout.

### Visualization (canvas `canvas6b`, 400×340)

Bar chart of illustrative revenue per click by click depth, with a light-to-dark blue gradient.

- **Title (bold 12px, `#1a5276`):** "Revenue per Click by Depth (Illustrative)".
- **Data:** depths ["1", "2", "3", "4", "5+"] with values `[0.05, 0.12, 0.25, 0.40, 0.80]` dollars; y scale max $0.90.
- **Bar fills (light to dark):** `rgba(41,128,185,0.3)`, `rgba(41,128,185,0.45)`, `rgba(41,128,185,0.6)`, `rgba(41,128,185,0.75)`, `rgba(41,128,185,0.9)`; strokes `#2980b9`.
- **Value labels:** bold 12px `#1a5276` above each bar: "$0.05", "$0.12", "$0.25", "$0.40", "$0.80".
- **Axes:** x labels "Depth 1"…"Depth 5+" with title "Click Depth" (`#1a5276`); y labels "$0.00"–"$0.90" at 5 ticks; axes `#333`, tick labels `#555`; padding top 40, right 20, bottom 55, left 55; white background.
- **Annotation (red `#e74c3c` bold 10px, two centered lines at bottom):** "Shallow = navigational (low value)" / "Deep = research (high value)".

## When Search Comes Up Empty — Zero-Result Queries (Failure Spikes)

**Pitfall label (color `#16a085`):** SEARCH DEAD ENDS

Sometimes you search for something and get literally zero results — a complete dead end. In this data it doesn't happen evenly across query types. Brand searches almost never fail, while searches for unindexed entities or badly misspelled queries fail 20-50% of the time. Failure rates cluster at the two extremes, so fixing zero-result rates means targeting the specific problem categories, not making broad improvements.

- Navigation and brand searches: ~1-2% come up empty (well-covered)
- Product searches: ~7% fail (gaps in the catalog)
- Entity (people/place) searches: ~30% fail (the engine simply doesn't know about them)
- Badly misspelled queries: 35%+ come up empty (spell-check can only fix small typos)
- Zero-result rate by category is one lens on how much headroom the engine has left

### Visualization (canvas `canvas7`, 380×320)

Histogram (shared `drawHistogram` helper) of zero-result rates across query categories.

- **Data generation:** seeded RNG mulberry32(707); 2000 samples: 70% low-failure categories Beta(1, 40)×100 (sampled via sums of exponentials); 30% high-failure categories Beta(5, 10)×100.
- **Bins/axes:** 30 bins, x from 0 to 60 labeled "0%"…"60%"; x-axis label "Zero-Result Rate (%)", y-axis label "Count".
- **Title:** "Zero-Result Rate by Query Category".
- **Colors:** bar fill `rgba(231,76,60,0.5)`, stroke `#2980b9`; density line `#2c3e50` with SE band; standard helper layout.

### Visualization (canvas `canvas7b`, 400×340)

Bar chart of zero-result rate by query intent type with a green-to-red color gradient.

- **Title (bold 12px, `#1a5276`):** "Zero-Result Rate by Query Intent Type (Illustrative)".
- **Data:** labels ["Nav", "Brand", "Product", "Long-tail", "Entity", "Misspelled"] with values `[1, 2, 7, 18, 32, 38]` percent; y scale max 45%.
- **Bar fills (green→red):** `rgba(39,174,96,0.8)`, `rgba(46,204,113,0.7)`, `rgba(230,126,34,0.5)`, `rgba(230,126,34,0.7)`, `rgba(231,76,60,0.7)`, `rgba(231,76,60,0.9)`; strokes same colors at full opacity.
- **Value labels:** bold 11px `#333` above each bar: "1%", "2%", "7%", "18%", "32%", "38%".
- **Axes:** x labels are the intent types (10px) with title "Query Intent Type" (`#1a5276`); y labels 0%–45% at 5 ticks with rotated title "Zero-Result Rate"; axes `#333`, tick labels `#555`; padding top 40, right 20, bottom 55, left 55; white background.
- **Bottom annotation (red `#e74c3c` bold 10px, centered):** "Entity + misspelled queries carry most of the failure mass".

## Regeneration instructions

- **Layout:** one `<table class="obj-table">` per section, each with a single `<tr>` of three `<td>`s: text cell (38% width) holding `<span class="pitfall-label">`, `<h3>`, `<p>`, `<ul>`; middle cell (31%, centered) holding the primary distribution canvas (width=380, height=320); right cell (31%, centered) holding the insight canvas (width=400, height=340).
- **Page CSS:** body system sans-serif (-apple-system stack), margin 20px, background `#f9f9f9`, text `#333`; h1 `#1a5276` centered; table cells `1px solid #2980b9` border, 12px padding; h3 `#1a5276` 1.0em weight 700; p 14px line-height 1.6; li 14px line-height 1.5; `.pitfall-label` inline-block bold 0.72em uppercase letter-spacing 0.5px; `canvas { width: 100%; height: auto; }`.
- **Pitfall label colors:** assigned by document order from the cycling array `["#795548", "#2980b9", "#27ae60", "#e74c3c", "#8e44ad", "#e67e22", "#16a085", "#d35400", "#c0392b", "#1abc9c"]` via a small script that sets `style.color` on each `.pitfall-label`.
- **Charts:** all canvases set `max-width` to the intrinsic width, size the backing store to the displayed width (`getBoundingClientRect().width`, falling back to the intrinsic width) × `window.devicePixelRatio`, and `ctx.scale` by that combined factor. Simulated data uses a seeded mulberry32 RNG (seeds 101, 202, 303, 404, 505, 606, 707 per section) plus Box-Muller `randn()` and inverse-CDF `randExp(lambda)` helpers. Shared `drawHistogram` (bars + Gaussian-kernel smoothed density line `#2c3e50` with 95% SE band `rgba(41,128,185,0.18)`, sigma 1.5, effective N clamped to [30, 200]) and `drawBarChart` helpers handle titles, axes, and tick labels.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(41,128,185,0.35)`.
- No nav bar, no back/home links. In regenerated HTML any card links would use `.html` extensions.
