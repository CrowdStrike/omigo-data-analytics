# Abandoned Custom Features in Pipeline

**Page type:** detail page (two-column obj-table layout: text left 40%, canvas right 60%, one row)
**HTML title tag:** Abandoned Custom Features — Common Bad Practices

**Subtitle:** Negligent Accumulation — Added tracking or a feature for a short-term research project. Project ended. Feature never launched. The custom instrumentation still runs — consuming compute, adding latency, polluting the schema. Nobody removes it because nobody owns it anymore.

## Section 1 (single row, three titled blocks + closing paragraphs)

### The Practice

- Researcher adds a custom event ("track scroll depth on page X for experiment Y"). Experiment ends. Event keeps firing. Adds 2ms latency per page load. Generates 50GB/day of data nobody reads. Costs $400/month in storage.
- Engineer adds a feature to the feature store for a model that was never deployed. Feature computation runs daily (3 hours of Spark). Nobody uses the output. But removing it requires understanding what depends on it — and nobody wants to risk breaking something.
- After 2 years: 30% of pipeline compute goes to features nobody uses. Schema has 200 columns, 80 are from abandoned projects. New hires spend days trying to understand what `exp_q3_2024_scroll_v2` means.

### Why It's a Data Science Problem

- **Schema pollution:** Every abandoned feature is a column in your feature store that confuses the next person. They might accidentally USE it in a model — training on stale, unmaintained, potentially broken data.
- **Compute waste:** Feature computation isn't free. 30 abandoned features × 10 minutes each = 5 hours of daily compute for nothing. That's cluster capacity not available for actual work.
- **Latency creep:** Each instrumentation point adds latency to the user-facing product. 20 abandoned tracking events × 2ms = 40ms added to page load. For nothing. But nobody knows which 20 are safe to remove.
- **Data quality confusion:** When investigating a data quality issue, you encounter these ghost features. "Is this column still maintained? Does anything depend on it? Why does it have NULLs after March 2024?" Hours of investigation for something that's just dead code in data form.

### The Ownership Problem

- Person who added it left the company. No documentation on why it exists or what depends on it.
- "I'll clean it up after the project" → project ends → person moves to next project → cleanup never happens.
- No automated system tracks: "this feature was added on date X for project Y. Project Y ended on date Z. Feature should be reviewed for removal."
- Fear of removal: "what if something breaks?" → everything stays forever → pipeline becomes a graveyard of dead features that nobody dares touch.

**The compounding cost:** Each abandoned feature is cheap individually ($30/month compute, one schema column). But they accumulate. After 3 years of 10 projects/year, each leaving 3-5 abandoned features: 30-50 zombie features. Combined cost: $1,500/month compute + 5 hours daily pipeline time + schema that nobody can understand + latency creep that degrades user experience.

**The fix:** Feature TTL (time-to-live). Every feature/event added to the pipeline gets a mandatory expiry date. If not renewed (conscious decision to keep it), it auto-disables. Forces regular review of what's still needed.

### Visualization (canvas `c1`, 720×400)

Stacked area chart: pipeline accumulating dead features over 36 months.

- **Background:** full-canvas light gray `#f9f9f9`.
- **Title (bold 13px, `#1a5276`, top center):** "Pipeline accumulates dead features over time".
- **Margins:** left 60, right 30, top 50, bottom 60. Axes: L-shaped `#333` lines width 1.5 (y-axis left, x-axis bottom).
- **X axis:** 36 monthly steps; labels "Year 1", "Year 2", "Year 3" (10px `#333`) centered at 17%, 50%, 83% of plot width, 14px below axis.
- **Data generation:** start with active=10, dead=0; every 3 months (m>0 and m%3==0): active += 3, dead += 2, active -= 1. Push one point per month for 36 months. Y scale max = 60 features.
- **Dead-features area (bottom):** filled `rgba(231,76,60,0.3)` from baseline up to dead[m]/60 of plot height, with red `#e74c3c` line (width 2) along its top.
- **Active-features area (stacked on top):** filled `rgba(39,174,96,0.3)` between dead[m] and dead[m]+active[m], with green `#27ae60` line (width 2) along its top.
- **Labels (bold 11px, left-aligned):** green `#27ae60` "Active features (in use)" at 60% plot width, 30px below plot top; red `#e74c3c` "Dead features (abandoned, still running)" at 40% plot width, 65% down the plot height.
- **Bottom annotations (centered):** bold 12px red `#e74c3c` "Year 3: ~40% of pipeline compute is dead features" at h-30; italic 12px `#555` "Nobody removes them because nobody knows what depends on them." at h-12.

## Regeneration instructions

- **Layout:** standard detail-page `.obj-table`: full-width table with `border-collapse: collapse`, one `<tr>` with left `<td>` (40%) holding three `.obj-title` blocks (second and third with `margin-top:14px`) each followed by a `<ul>`, then two closing `<p>` paragraphs; right `<td>` (60%, centered) holds the single canvas.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.6em `#1a5276`; `.subtitle` `#666` 1.0em; `p` `#333` 0.95em; `ul` 0.9em `#333`, margin 8px 0 8px 20px; `li` margin 6px 0; `strong` `#1a5276`; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em, weight 600, `#1a5276`, margin-bottom 8px. No nav bar, no back/home links.
- **Canvas:** intrinsic `width="720" height="400"`; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setup(id)` helper; `canvas { display: block; margin: 0 auto; }`. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, area fills `rgba(231,76,60,0.3)` and `rgba(39,174,96,0.3)`, gray text `#666`/`#555`/`#333`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
