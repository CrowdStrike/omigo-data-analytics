# Chi-squared test

**Page type:** detail page (card-sections: Overview two-column layout table with text 45% / canvas 55%, full-width Real-World Examples canvas + callout boxes, Quick Decision table)
**HTML title tag:** Chi-squared test — Statistical Tests Reference

**Subtitle:** Tests whether observed frequencies differ from expected frequencies

Note: the source HTML has a top back link "← Statistical Tests Reference" to `../12-statistical-tests.html`; per project convention (no back/home links) it is not part of the regenerated page.

## Overview

**What it measures**

Whether observed frequencies differ from expected frequencies (goodness-of-fit or independence in contingency tables).

**Key assumptions**

- Expected count ≥ 5 in each cell (rule of thumb)
- Observations are independent
- Categories are mutually exclusive and exhaustive
- Large sample size relative to number of cells

**What breaks when violated**

- Sparse cells (expected < 5): chi-squared statistic becomes inflated, giving false positives
- Many cells with small counts: the chi-squared distribution approximation fails entirely
- Dependent observations (repeated measures): vastly overstates significance

**Failure box (monospace, red):**

Rare disease classification: 4 categories, n=38.
Expected counts: [15.2, 12.4, 7.6, 2.8]
Cell 4 has expected=2.8 < 5. Chi-sq says p=0.02.
Fisher's exact says p=0.09. False positive from sparse cell.

**Alt-note box (green):**

**Use instead:** Fisher's exact test when any expected cell < 5. Collapse rare categories. For 2x2 tables with small n, always prefer Fisher's exact. G-test (likelihood ratio) as a more robust alternative for larger tables.

### Visualization (canvas `c3`, 960×460)

Contingency-table heatmap of expected counts with sparse cells highlighted in red.

- **Title (bold 13px `#1a5276`, top center):** "Contingency Table: Expected Counts (sparse cells in red)"
- **Table data (2 rows × 4 columns):** row "Disease+": `[15.2, 8.4, 6.1, 2.8]`; row "Disease-": `[12.8, 7.6, 5.9, 3.2]`. Column labels: "Type A", "Type B", "Type C", "Type D" (17px `#1a5276`, centered above columns); row labels right-aligned left of rows.
- **Cell rendering:** cells 120×65px starting at (140, 55), 4px inner gap. Non-sparse cells (expected ≥ 5): fill `rgba(41,128,185, 0.1 + min(1, value/16)×0.35)` (blue intensity by value), 1px `#bbb` border, value text 17px `#1a5276`. Sparse cells (expected < 5, i.e. 2.8 and 3.2 in Type D): fill `#fdedec`, 3.5px `#e74c3c` border, value bold 16px red with a second red line "< 5!" beneath.
- **Verdict captions (17px, bottom center):** red `#e74c3c` "Chi-sq: p=0.02 (FALSE POSITIVE)"; green `#27ae60` "Fisher exact: p=0.09 (correct: not significant)".

## Real-World Examples

### Visualization (canvas `c3r`, 960×300)

Horizontal bar chart of IDS detection by attack type, with the sparse zero-day row highlighted, plus two verdict boxes.

- **Title (bold 16px `#1a5276`, top center):** "🛡️ IDS Evaluation: Sparse Cell (Zero-Day n=12) Inflates Chi-Squared"
- **Rows (bar height 36px, 10px gap, starting at (150, 38); bar width scaled to n/2500 of 300px max):**
  - DDoS — n=2400, detected 2280, rate 95%, expected 2208
  - Phishing — n=1800, detected 1620, rate 90%, expected 1656
  - SQLi — n=340, detected 310, rate 91%, expected 312.8
  - Zero-Day — n=12, detected 7, rate 58%, expected 3.1 (sparse)
- **Bar rendering:** total bar fill `rgba(41,128,185,0.15)` (sparse row: `rgba(231,76,60,0.15)`); detected portion overlaid in `rgba(39,174,96,0.5)` (sparse row: `rgba(231,76,60,0.5)`). Row label 15px `#1a5276` right-aligned with "n=…" 13px `#888` beneath. Rate label bold 15px right of bar — green `#27ae60`, red `#e74c3c` for the sparse row. Sparse row also gets a 2.5px red outline around the bar and a 13px red note "Expected cell = 3.1 < 5!".
- **Verdict boxes (bottom center, two 220×36px boxes side by side):** left — fill `#fdedec`, 1.5px `#e74c3c` border, bold 15px red "Chi-squared: p=0.03" with 13px "(driven by sparse zero-day cell)"; right — fill `#eafaf1`, 1.5px `#27ae60` border, bold 15px green "Fisher exact: p=0.11" with 13px "(not significant — correct)".

**Real-world box 1 (🛡️ Security: Intrusion detection system evaluation):**

A SOC team evaluates their IDS across 4 attack types: DDoS (n=2400), phishing (n=1800), SQL injection (n=340), and zero-day exploits (n=12). They cross-tabulate detection success × attack type. The zero-day cell has expected count of 3.1. Chi-squared reports the IDS performs "significantly differently" across types (p=0.03). Fisher's exact: p=0.11. The "significance" was manufactured by the sparse zero-day cell — with 12 total events, you can't reliably estimate detection rates.

**Real-world box 2 (🧬 Genomics: Rare variant association):**

A GWAS study tests association between a rare SNP (minor allele frequency 1.2%) and disease status. In a cohort of 500, the 2×3 genotype table has expected counts of [3.6, 0.04, 0.0] for homozygous-minor and heterozygous-minor cells. Chi-squared gives a "significant" p=0.01, but it's entirely driven by cells with expected values near zero. Fisher's exact test returns p=0.31. Publishing the chi-squared result would be a false positive entering the literature.

## Quick Decision

| Data Situation | If Assumptions Met | If Violated | Universal Fallback |
|---|---|---|---|
| Categorical independence | Chi-squared test | Expected cell < 5: inflated statistic | Fisher's exact test |

## Regeneration instructions

- **Layout:** h1 + `.subtitle`, then three `.card-section` blocks each with an h2 (1.3rem `#1a5276`, 2px `#2980b9` bottom border): "Overview" (a `table.layout` row: `td.text-col` 45% with `.obj-title` headings, bullets, `.failure`, `.alt-note`; `td.viz-col` 55% with canvas `c3`), "Real-World Examples" (full-width canvas `c3r` followed by two `.real-world` boxes), "Quick Decision" (`.decision-table`).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with 2px solid `#2980b9` bottom border; `.subtitle` `#666` 0.95rem; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; `code` on `#e8f0f8`; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius.
- **Callout boxes:** `.failure` — background `#fdedec`, left border 3px `#e74c3c`, monospace ('SF Mono'/'Fira Code'), color `#922`, 0.85rem. `.alt-note` — background `#eafaf1`, left border 3px `#27ae60`, color `#1a5276`, 0.85rem. `.real-world` — background `#fef9e7`, left border 4px `#e67e22`, 0.88rem; `.domain` heading weight 600 `#7d6608`; `strong` inside `#e67e22`.
- **Decision table:** `.decision-table` — th background `#1a5276` white text; td 1px `#e0e0e0` border; even rows `#fafcfe`; column 3 text `#e74c3c`, column 4 text `#27ae60` weight 500.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`.
- **Canvas:** intrinsic `width`/`height` attributes as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper. No nav bar, no back/home links.
