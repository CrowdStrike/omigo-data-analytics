# SEC Filings & Regulatory Data Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: bullets + example callout left ~40%, canvas right ~60%)
**HTML title tag:** SEC Filings Data Pitfalls

**Subtitle:** SEC filings look pristine but arrive stale, get retroactively amended, and hide material information behind inconsistent tags, boilerplate, and buried footnotes.

## Filing Delay (60 Days Post-Quarter)

- **Statutory deadlines:** 10-K reports are due 60 days after fiscal year-end for large accelerated filers
- **Quarterly lag:** 10-Q reports are due 40 days after quarter-end, so every filing arrives weeks late
- **Markets move first:** press releases and earnings calls land 3-4 weeks after the period ends
- **Stale on arrival:** SEC filing data reaches you after prices have absorbed the same numbers
- **Exponential decay:** by the time statements hit EDGAR, 90-95% of the informational edge is gone
- **Filing-date signals:** models trade on 60+ day old data against traders who acted 5-6 weeks earlier
- **Vendor delay:** parsing, validation, and distribution add another 3-5 days of staleness

**Example:** A company with fiscal year ending December 31st must file its 10-K by March 1st (60 days). However, it held an earnings call on January 25th. By the time the 10-K appears in your data pipeline (March 5th), the stock has already moved 12% based on the earnings announcement. Any "alpha" from the detailed financial statements was priced in 38 days ago. Your quantitative model is analyzing information that the market fully digested over a month prior.

### Visualization (canvas `canvas1`, 720×240)

Timeline with milestone markers plus an exponential information-value decay curve.

- **Title (bold 17px `#1a5276`):** "Information Value Decay: Quarter End to Filing Date".
- **Timeline:** horizontal `#2c3e50` 3px line at y=100 from x=80 to x=680; 8px circle markers with white outlines, two-line 14px `#2c3e50` labels below, and bold 12px monospace day counts above in the marker color:
  - "Q4 Ends (Dec 31)" at day 0 — `#1a5276`, "+0d"
  - "Earnings Call (Jan 25)" at day 25/65 — `#e67e22`, "+25d"
  - "10-K Filed (Mar 1)" at day 60/65 — `#e74c3c`, "+60d"
  - "Data in API (Mar 5)" at day 65 — `#e74c3c`, "+65d"
- **Decay curve:** green `#27ae60` 3px exponential curve below the timeline, value = 100·0.05^(t) from 100% down to 5% across the same span; labeled "Information Value" (14px green) with y labels "100%" and "5%" (bold 13px `#2c3e50`).
- **Shaded zone:** `rgba(231,76,60,0.15)` rectangle from the earnings-call position to the end (the already-priced-in region).
- **Bottom annotation (centered, bold 15px red):** "By filing date, 95% of information value is gone".

## Amendment/Restatement Overwrites History

- **Amendments overwrite:** 10-K/A and 10-Q/A filings replace the original versions inside EDGAR
- **Original lost:** retrieving the pre-amendment figures through standard APIs is near impossible
- **Restatement size:** revenue gets revised 10-50% retroactively, sometimes years after the fact
- **Broken trends:** historical trend analysis built on "current" data snapshots is invalidated
- **Survivorship bias:** backtests on latest-version filings see a sanitized history that never existed
- **Errors vanish:** without point-in-time archives, near-bankrupt companies look stable in hindsight
- **Fraud models fail:** corrections erase the very signals that would have predicted malfeasance

**Example:** In 2019, Company XYZ originally reported $2.3B in revenue for Q3 2018. In March 2020, after an accounting review, they filed a 10-K/A restating Q3 2018 revenue to $1.6B (a 30% reduction). Today's EDGAR API returns only the amended $1.6B figure. A backtested model trained on "historical" data never sees the original $2.3B error, creating an artificial picture of smooth growth. The model cannot learn to detect the revenue recognition problems because the problematic data has been erased from the official record.

### Visualization (canvas `canvas2`, 720×240)

Two stacked line charts comparing amended database history vs original filings.

- **Title (bold 17px `#1a5276`):** "Historical Data: Database View vs. Reality".
- **X axis:** quarters Q1 2018 - Q1 2019, labeled 12px `#2c3e50` at bottom.
- **Top panel — header bold 14px `#2c3e50`: "What the database shows today (amended):"** — smooth green `#27ae60` 3px line through `[450, 480, 520, 550, 580]` ($M), 5px green dots, 12px monospace value labels "$450M" … "$580M" above points.
- **Bottom panel — header: "What actually happened (original filings):"** — dashed red `#e74c3c` 3px line (dash 5/5) through original values `[450, 480, 720, 550, 580]`; solid green 2px line through corrected `[450, 480, 520, 550, 580]`.
- **Restatement highlight at Q3 2018:** `rgba(231,76,60,0.2)` shaded band between the $720M (red dot, labels "$720M" / "(original)" in red monospace) and $520M points (green dot, labels "$520M" / "(amended)"); red downward arrow between them labeled bold 13px red: "Restatement" / "-28% revenue".

## XBRL Tagging Inconsistency

- **Taxonomy size:** the US GAAP taxonomy offers companies 14,000+ XBRL tags to choose from
- **Labeling variance:** identical accounting concepts get tagged differently at every company
- **Revenue alone:** 50+ valid tag options exist, from plain Revenues to SalesRevenueNet and beyond
- **Longest variants:** RevenueFromContractWithCustomerExcludingAssessedTax and RevenueFromContractWithCustomerIncludingAssessedTax
- **Custom extensions:** companies invent proprietary tags whenever standard taxonomy doesn't fit
- **Comparison blocked:** cross-company work is near impossible without extensive mapping tables
- **Coverage split:** "Revenues" returns 40% of companies, "SalesRevenueNet" a different 35%, barely overlapping
- **Expert reconciliation:** mapping needs accounting knowledge plus annual taxonomy maintenance

**Example:** Building a peer comparison of five tech companies, you query for "Revenue" in their XBRL filings. Company A uses "Revenues", Company B uses "SalesRevenueNet", Company C uses "RevenueFromContractWithCustomerExcludingAssessedTax", Company D uses a custom extension "TotalRevenueIncludingSubscriptionAndServices", and Company E uses "SalesRevenueGoodsNet". A naive query for any single tag returns at most 20% of your sample. You need a mapping table with 50+ revenue variants, and even then you're uncertain if all definitions are truly comparable (e.g., does one include assessed taxes while another excludes them?).

### Visualization (canvas `canvas3`, 720×240)

Tag-per-company row diagram plus a coverage bar chart for tag queries.

- **Title (bold 17px `#1a5276`):** "XBRL Revenue Tag Variations Across Companies".
- **Company rows (13px `#2c3e50` name at left, colored box sized to tag length with 11px white monospace tag text):**
  - Company A: `Revenues` — `#1a5276`
  - Company B: `SalesRevenueNet` — `#2980b9`
  - Company C: `RevenueFromContract...ExcludingTax` — `#27ae60`
  - Company D: `TotalRevenue (custom extension)` — `#e67e22`
  - Company E: `SalesRevenueGoodsNet` — `#e74c3c`
- **Coverage chart — header bold 14px `#1a5276`: "Query Results: Companies Found by Tag Choice"** — horizontal 6px bars scaled to 300px max, right-aligned 12px monospace tag labels, bold 12px percentage in bar color at bar end:
  - `Revenues` 42% (`#1a5276`); `SalesRevenueNet` 38% (`#2980b9`); `RevenueFrom...` 22% (`#27ae60`); `Custom extensions` 18% (`#e67e22`); `Combined (all)` 85% (`#27ae60`).
- **Bottom annotation (centered, bold 13px red):** "Single tag queries miss 60-80% of companies".

## Boilerplate Drowns Signal

- **Filing length:** a 10-K averages 180-220 pages, of which only 5-10% is materially new
- **Mandated padding:** the rest is legally required boilerplate, repeated near-verbatim annually
- **Risk factors:** 95% copy-paste, listing "we face competition" and "cybersecurity threats"
- **Polluted sentiment:** static legal language dominates word frequency in NLP sentiment models
- **Filler vocabulary:** "uncertainty", "risk", "may", "could" appear hundreds of times per filing
- **Density decay:** requirements expand and filings lengthen while new disclosure stays constant
- **Zero alpha:** full-text models predict boilerplate patterns, not material business changes

**Example:** A 10-K filing contains 200 pages. Pages 1-80 are boilerplate: standard business descriptions, regulatory environment, competition landscape that hasn't changed in 5 years. Pages 81-150 contain financial statements and MD&A, 70% of which is repeated from prior quarters. Pages 151-190 are risk factors, 95% identical to last year. The actual new, material information — a change in revenue recognition policy, new legal exposure, strategic pivot — appears in 8-10 pages scattered throughout. An NLP sentiment model trained on the full 200 pages is fitting 95% noise, learning to classify legal boilerplate rather than business reality.

### Visualization (canvas `canvas4`, 720×240)

Horizontal stacked content-breakdown bar, a small noise/signal pie, and an information-density mini bar chart.

- **Title (bold 17px `#1a5276`):** "10-K Filing Content Breakdown (200 pages typical)".
- **Stacked bar (550×80px at x=100 y=70, `#2c3e50` 2px border; segments labeled with name, bold percentage, and "(Npg)" page count in white — or `#2c3e50` when segment <10%):**
  - "Boilerplate / Legal" 40% (80pg) — `#95a5a6`
  - "Repeated from Prior Year" 30% (60pg) — `#e67e22`
  - "Standard Updates" 20% (40pg) — `#f39c12`
  - "Materially New Info" 7% (14pg) — `#27ae60`
  - "Actual Signal" 3% (6pg) — `#2ecc71` (too narrow for an inside label)
- **Pie — header bold 14px `#1a5276`: "NLP Model Trained on Full Text:"** — 25px-radius pie: 95% gray `#95a5a6` slice, 5% green `#27ae60` slice; labels bold 13px: "95% Noise/Boilerplate" (gray), "5% Signal" (green).
- **Density chart — header bold 14px `#1a5276`: "Information Density by Section:"** — six mini bars (values scaled to max 60): "Bus. Desc." 8%, "Risk" 5%, "MD&A" 25%, "Financials" 45%, "Notes" 55%, "Controls" 3%; bar colors: >40 green `#27ae60`, >20 orange `#e67e22`, else gray `#95a5a6`; 9px labels below and bold 10px values above.

## Form 4 Insider Trading Timing Manipulation

- **Two-day rule:** insiders must report stock transactions on Form 4 within 2 business days
- **Weak enforcement:** that deadline is loosely policed and frequently missed without penalty
- **10b5-1 plans:** sales can be pre-programmed 3-6 months ahead of the actual transaction
- **Indistinguishable:** filing date alone cannot separate planned sales from informed trading
- **Blackout clustering:** trading is barred around earnings, so post-announcement sells look like signals
- **Unflagged plans:** 60-80% of insider sales are pre-planned, and filings don't consistently mark them
- **Reporting lag:** the reported date can sit weeks after the actual transaction date
- **Late corrections:** amendments appear months later, making real-time analysis unreliable

**Example:** An insider at TechCorp sells $5M of stock on February 3rd, filing Form 4 on February 5th. This appears alarming until you discover it's part of a 10b5-1 plan established 6 months earlier. Meanwhile, three other insiders sell a combined $12M on January 26th (immediately after the earnings blackout period ends), which looks like coordinated selling but is simply the first allowable trading window. A naive model flagging "insider sell volume spike" would trigger on both patterns, despite one being scheduled sales and the other being regulatory timing artifacts, neither providing useful information about company prospects.

### Visualization (canvas `canvas5`, 720×240)

Stacked monthly bar chart of insider sell volume with shaded blackout periods.

- **Title (bold 17px `#1a5276`):** "Insider Sell Volume Throughout Year (Timing Artifacts)".
- **Layout:** plot at x=70 y=60, 620×120; `#2c3e50` 2px L-axes; y-axis label "$M" (11px); month labels Jan-Dec (12px `#2c3e50`); max volume 24.
- **Blackout shading:** `rgba(52,73,94,0.15)` vertical bands each labeled "Blackout" (10px `#2c3e50`) spanning month positions 0.5-1.5, 3.5-4.5, 6.5-7.5, 9.5-10.5 (around Q4/Q1/Q2/Q3 earnings).
- **Data (planned gray `#95a5a6` bottom / discretionary red `#e74c3c` stacked top), Jan→Dec:** planned `[4, 18, 5, 3, 12, 4, 3, 15, 5, 4, 13, 4]`, discretionary `[1, 5, 2, 1, 4, 1, 1, 6, 2, 1, 5, 1]` — spikes in Feb/May/Aug/Nov right after blackouts.
- **Legend:** red swatch "Discretionary sales (potential signal)"; gray swatch "10b5-1 planned sales (no signal)" (13px `#2c3e50`).
- **Bottom annotation (centered, bold 14px orange `#e67e22`):** "90% of \"alarming\" spikes are pre-planned trades clustered at blackout boundaries".

## Footnote Burial Strategy

- **Deliberate burial:** critical disclosures sit in footnotes on pages 80-150, where few readers look
- **Footnote-only items:** litigation exposure, contingent liabilities, off-balance-sheet entities
- **Obscuring language:** the dense legalese there is written to obscure rather than clarify
- **Footnote 3:** revenue recognition changes hide there, not in MD&A, so year-over-year analysis misleads
- **Footnote 18:** billions in related-party transactions get one paragraph, far too thin to assess
- **Unassessable:** arms-length pricing and conflict of interest cannot be judged from that detail
- **Parser scope:** automated tools read the balance sheet, income statement, and cash flow statement
- **Footnotes skipped:** they ignore or poorly parse footnotes, missing material information

**Example:** Your automated parser extracts clean financial statements showing revenue growth of 15% YoY. The primary tables are pristine. However, buried in Footnote 11 on page 112, a single paragraph discloses an $800M contingent liability from ongoing litigation that has a "reasonably possible" chance of resulting in loss. Footnote 18 on page 127 mentions a $2.1B related-party transaction with the CEO's family trust. Footnote 3 notes a change in revenue recognition timing that adds 4% to current year revenue but isn't comparable to prior periods. None of this appears in your model's feature set because your parser didn't read beyond the primary financial statements. Your model sees clean 15% growth; the reality is 11% comparable growth plus $2.9B in hidden risk exposure.

### Visualization (canvas `canvas6`, 720×240)

Iceberg diagram: small visible tip (primary statements) above the waterline, large submerged mass (footnotes) below.

- **Title (bold 17px `#1a5276`):** "The Footnote Iceberg: What Automated Parsers Miss".
- **Water:** light blue `#e8f4f8` fill below the waterline at y=90.
- **Above-water tip:** small trapezoid (180 wide, 45 tall) at x=250, fill `#ecf0f1`, `#95a5a6` 2px stroke; label bold 13px `#1a5276`: "Primary Financial" / "Statements".
- **Below-water mass:** larger trapezoid (widening to 280 wide, 120 tall), fill `#bdc3c7`, `#7f8c8d` 2px stroke; label bold 14px `#1a5276` below: "Footnotes" and 11px "(Material disclosures buried on pages 80-150)".
- **Footnote list inside the submerged mass (12px `#2c3e50` text, 5px severity dot at left — red `#e74c3c` = material, orange `#e67e22` = concerning):**
  - Footnote 3: Revenue recognition change (orange)
  - Footnote 7: Segment reclassification (orange)
  - Footnote 11: $800M contingent liability (red)
  - Footnote 14: Lease obligation $450M (orange)
  - Footnote 18: Related-party $2.1B (red)
  - Footnote 21: Stock-based comp change (orange)
- **Parser annotation (right, red arrow pointing up at the tip, bold 12px red, three lines):** "Most automated" / "tools parse" / "only this".
- **Risk summary (bottom right):** heading bold 13px `#1a5276` "Hidden Exposure:"; red 12px lines "$800M contingent liability", "$2.1B related-party transaction", "$450M off-balance sheet lease"; bold 14px red "Total: $3.35B unmodeled risk".
- **Legend:** red dot "Material"; orange dot "Concerning" (11px `#2c3e50`).

## Regeneration instructions

- **Layout:** one `<h2>` per pitfall (1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px), each followed by its own single-row `.obj-table`: left `<td>` (40%) with `.obj-title`, `<ul>` bullets, and an `.example` callout (background `#f0f4f8`, left border `3px solid #2980b9`, padding 10px 14px, 0.9em, bold "Example:" lead in `#1a5276`); right `<td>` (60%, centered) with the canvas. Even table rows background `#fafcfe`.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px. A `.philosophy` callout style is defined but unused. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="240"` per chart; scripts on this page draw at intrinsic size without devicePixelRatio scaling (each IIFE paints a white background first); when regenerating, canvases should use `window.devicePixelRatio` scaling per project convention.
- **Palette:** primary blue `#1a5276`, accent blue `#2980b9`, green `#27ae60`/`#2ecc71`, red `#e74c3c`, orange `#e67e22`/`#f39c12`, grays `#95a5a6`/`#bdc3c7`/`#7f8c8d`/`#2c3e50`, water blue `#e8f4f8`.
- **Links:** in regenerated HTML, any card links use `.html` extensions (this page has none).
