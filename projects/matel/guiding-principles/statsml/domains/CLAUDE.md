# Domain Pitfalls

Industry-specific data traps across finance, healthcare, e-commerce, cybersecurity, education, and more.

## Pending TODO

- **19 pages in this folder still generate chart data with `Math.random()`**, so their figures change
  on every load and any statistic printed beside them is unverifiable. Deferred until the domains
  content is reduced — some of these pages may shrink or merge, and fixing charts that are about to
  be cut is wasted work. Full list, reference implementations, and the two-part fix (seeded `lcg()`
  **plus** render-time computed labels, applied to the `.md` sibling as well) are in the Global TODO
  in the parent `CLAUDE.md`.

  Reference pages in this folder already converted: `150-schema-compliance-not-data-compliance`,
  `034-crypto`, `081-sports-analytics`.

- **68 pages have an `.obj-title` that is a verbatim copy of the `<h2>` above it — 505 sections.**
  The slot was designed to hold a *refined restatement* of the pitfall (sharper, more concrete
  phrasing of the same idea), so as a copy it carries zero information. Deferred with the same
  reasoning as the `Math.random()` item: these pages need content revision anyway, and rewriting
  505 subtitles on sections that may later shrink or merge is wasted work. Fold this into the
  content pass rather than running it as a standalone sweep.

  Every affected page is in this folder; no other folder has the defect. Full list with per-page
  section counts: run the detector below.

  **Detector** (compares each `<h2>` to the first `.obj-title` in the table under it):

  ```
  python3 - <<'PY'
  import re,glob
  norm=lambda s:re.sub(r'\s+',' ',re.sub(r'<[^>]+>','',s).replace('&amp;','&')).strip().lower().rstrip(':.')
  for f in sorted(glob.glob('domains/*.html')):
      t=open(f).read(); d=0
      for h,b in re.findall(r'<h2[^>]*>(.*?)</h2>(.*?)(?=<h2|\Z)',t,re.S):
          m=re.search(r'class="obj-title"[^>]*>(.*?)</div>',b,re.S)
          if m and norm(h)==norm(m.group(1)): d+=1
      if d: print(d,f)
  PY
  ```

  **The `.md` siblings are in three different states** — check which before editing, the fix differs:

  | State | Pages | What the `.md` has after `## Heading` | Fix |
  |---|---|---|---|
  | `no-subtitle-line` | 36 | bullets start immediately; no subtitle at all | *insert* a new bold subtitle line |
  | `plain-bold` | 17 | `**Duplicate Of Heading**` | replace that line's text |
  | `labeled` | 15 | `**Obj-title:** Duplicate Of Heading` | replace the text after the label |

  In the 36 `no-subtitle-line` cases the `.md` is clean and the html generation step invented the
  duplicate — so those specs never encoded the defect, but 35 md files *do* describe the obj-title
  as "repeating the pitfall name" in their **Regeneration instructions**; that wording is the root
  cause and must be corrected to "a refined restatement of the pitfall name, never a copy of it" or
  the next regeneration reintroduces the copy.

  **Which md convention to standardise on is an open editorial call** — `plain-bold` (used by the
  good reference pages) vs `labeled` `**Obj-title:**` (more explicit about which slot it fills).
  Decide once and apply, rather than per-page.

  Rules for the subtitle text, derived from the pages already done:
  - Never reuse the heading's words as the whole subtitle; it must add specificity the heading lacks.
  - Prefer a concrete number or contrast **already present in that section's own bullets/example** —
    do not invent, estimate, or recompute a figure to fill the slot.
  - One line, ~45-75 chars, Title Case, no trailing period, must not wrap at normal page width.
  - Entities (`&mdash;` `&times;` `&rarr;`) in the html, real characters in the `.md`.

  Reference pages already correct — read one for voice calibration: `001-finance`, `034-crypto`,
  `139-sampling-curse`, `033-stock-markets`, `150-schema-compliance-not-data-compliance`.
  Done in this pass: `001-finance`, `002-healthcare`, `003-ecommerce`, `004-cybersecurity`,
  `005-education`, `006-advertising`.

  Two loose ends found while surveying, worth handling in the same pass:
  - **25 sections in 5 pages are near-duplicates** rather than exact — the subtitle adds a word or
    two but no new information: `033-stock-markets`, `049-protein-discovery`,
    `050-research-publications`, `051-rental-apartments`, `052-commercial-realestate`. The detector
    above does not catch these.
  - **`folk-wisdom/21`-`25` have the defect in the `.md` only** — their html does not, which means
    those five html pages are stale relative to their specs. Confirm the direction of that drift
    before regenerating either side.

- **Chart-vs-prose numeric contradictions — content fix needed on the pages below.** Each item was
  recomputed in `python3` from the page's own data arrays or pixel geometry, so the "actual" column
  is derived, not estimated. Every fix must be applied to the `.md` sibling too — the audit confirmed
  the `.md` files mirror these defects faithfully. **The audit is partial:** only `025-028`, `030`,
  `065-067`, `038`, and `101` have been swept. The remaining ~110 pages are unaudited.

  Where two repairs are valid (correct the prose to match the chart, or the chart to match the prose),
  the choice is an editorial call about what the section is teaching — decide per page, don't default.

  **High — the headline number of a section is wrong:**

  | Page | Claimed | Computed |
  |---|---|---|
  | `028-observability` | p99 marker on the `1K-2K` bucket labeled `p99 = 2000ms`; "10,000 users/day at 1M req" | Cumulative from plotted counts `[5,15,35,30,8,3,2,1,.5,.3,.2]` reaches 99.0% at the end of `100-500` → the histogram's own **p99 = 500ms**. 2000ms is the p99.8 → **2,000** users/day |
  | `028-observability` | One quantity, three values: obj-title "10M time series", bullet chain "5 → 500 → 5 billion", chart "25 Billion" | 5×100×10M = 5e9 (bullet, no status tag); 5×100×5×10M = 2.5e10 (chart, with tag). 10M is tag *cardinality*, not a series count. Plotted heights 0.14/0.45/0.60 also read off the stated log axis as **25 / 32,000 / 1,000,000** |
  | `065-compute-utilization` | "4 Cores Provisioned, **2.5 Delivered**" | Reality bar is user 30 / sys 20 / **steal 25** / idle 25 → 4×(1−0.25) = **3.0 cores**. 2.5 requires 37.5% steal |
  | `065-compute-utilization` | "Avg 5ms, **p99 200ms** — one GC pause every 30s"; "too rare to move the mean" | Baseline `3+rand()*6` over 631 samples → mean **6.00ms**; with pauses **7.29ms**, so the pause *does* move the mean. 200ms/30s = 0.667% duty → **p99 = 8.96ms**; 200ms sits at **p99.4** |
  | `066-gpu-cluster` | Legend + bullet "A100 **30% slower**" | `a100ComputeEnd=0.9` vs `h100ComputeEnd=0.6` → **50% slower**. 30% would be 0.78 |
  | `067-caching` | Bar = 99.4% correct + 0.1% wrong + 0.5% miss | `correctW=616.28`, `staleW=12` (floor) → `missW = 620−616.28−12 = **−8.28**`: the miss rect draws with **negative width**, spilling past the bar edge, label landing outside it. The 0.1% segment also draws 4× wider than the 0.5% one |
  | `101-power-generation` | Bullet "about 3 hours" and `'3-hour ramp!'` annotation | `duckData` min 5 at hour 12 → max 38 at hour 18 = **6-hour** ramp |
  | `038-news-media` | `'~90% value lost'` drawn at t=6h | Curve is `exp(-3.5*t)` with t in days; `exp(-3.5×0.25)=0.417` → **58%** lost. 90% is at **15.8h** |

  **Medium — secondary figure wrong, or chart contradicts its own bullet:**

  | Page | Claimed | Computed |
  |---|---|---|
  | `030-autonomous` | Routine bar labeled "99.9%" | Tail `0.15·e^(−0.2i)` integrates to 46.4% of plotted area — chart contradicts its label by ~460× |
  | `030-autonomous` | Prose "60% real-world performance" | Chart fill and label are **62%** (`barW*0.62`); same split in the `.md` |
  | `030-autonomous` | Bar labeled "60% Confidence"; annotation "95% → 60% drop" | Bar draws at `0.63`. Worse: the chart's story contradicts its own bullet that confidence *stays high* when predictions are wrong |
  | `028-observability` | Title "100 Traces, 1% Kept" | 10×5 = **50**-dot grid; head-based keeps 1 → 2%, tail-based keeps 10 → **20%** |
  | `028-observability` | Bullet "Service B … 0.1% of volume" | Volume array `[2,5,90,1.5,1.5]` with the CRITICAL star on the **1.5%** bar; no 0.1% bar exists |
  | `025-cloud-infrastructure` | Axis "30 minutes", legend "per-second" | 180 points; per-second = 3 minutes. Six 30-point 5-min buckets require **10-second** sampling |
  | `025-cloud-infrastructure` | Title "Same **$12K spike**" | $12K is the March *total*; the spike over the Jan-Feb baseline is **$5.3K** |
  | `026-multi-cloud-platforms` | Bullet "renamed services **3x**" | Chart draws 3 eras and **2** break markers = 2 renames |
  | `065-compute-utilization` | Prose "throttles at **1.9**" | Band shaded 1.8→2.0, dot test `cores > 1.8`. At 1.8, 9.35% of samples are throttle events; at 1.9, 1.27% |
  | `066-gpu-cluster` | "Settles at 85%" | Drawn at `plotH*0.35` = y 83.5, which reads **68.1%** off the chart's own 0%/100% anchors |
  | `066-gpu-cluster` | "100x spike!" on a linear 1-100ms axis | Baseline draws at **5.95ms**, plateau at **104.95ms** → **17.6×** |
  | `067-caching` | Prose "sized for 5%" (→20× overload) | Capacity line drawn at **20%** → **5×**. Chart gives the backend 4× headroom the prose says it lacks |
  | `067-caching` | Green "Sweet Spot" band at i=25-45 | With miss = `0.9e^(−i/15)`, stale = `0.85(1−e^(−i/25))`: sum minimizes at **i=21**, minimax crossover at **i=14** — both outside the band, which extends into monotonically worsening territory |

  **Low — rounding, cosmetic, or a dimensional slip:**

  - `028-observability`: grid labeled "100 daily alerts" draws 60; "5% over 3 months" is **7%** at the plotted month-3 position.
  - `025-cloud-infrastructure`: the three attempt bars total **8.5h**, not the labeled 8h; "success" marker sits at 95% progress.
  - `026-multi-cloud-platforms`: equal-pixel ticks labeled 0/7/14/21/**30**/37 — a 9-day gap drawn in a 7-day slot.
  - `027-identity`: asserts `O(n × m)` for what is graph reachability — O(n+e) per principal, O(n·e) for a full review. Depth `m` is not a multiplicative factor.
  - `030-autonomous`: `Cost: ∞` for false negatives is degenerate — any P(object) > 0 forces braking, threshold → 0, vehicle inoperable. Use a large finite ratio. Also uses `lag/30*2.6` where 60 mph = **88.0 ft/s** gives 4.40 ft, not 4.33.
  - `065-compute-utilization`: "consuming about 1.5 cores" vs a ramp with mean **1.35** ending at 1.94; "~8% of timeline" vs 40/555 = **7.21%**.
  - `067-caching`: "100% traffic" apex draws at **95.5%**; one bullet gives impact as "TTL × users affected" (user-seconds) while the chart footer says "TTL × request rate" — only the chart version is dimensionally valid.
