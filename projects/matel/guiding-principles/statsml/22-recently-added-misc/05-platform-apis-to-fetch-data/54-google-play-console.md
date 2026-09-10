# Google Play Console

**Page type:** detail page (h1 + subtitle + verified badge, one "Overview" two-column obj-table row: text left 45%, code + canvas right 55%, then an "Official API References" list)
**HTML title tag:** Google Play Console — Platform APIs

**Subtitle:** Google's reporting APIs for your Android app: installs, ratings, crash rates, and subscription events — totals only, plus per-purchase detail for your own customers.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Installs, ratings, and reviews, broken down by country, device, and app version
- Crash and freeze rates per release, with the user counts behind each rate
- Subscription and refund status for your own customers, with push notifications on changes
- Bulk CSV report downloads for historical backfill

### Key point (callout, red left border)

**Small slices vanish.** Cells with too few users are withheld, so a crash spike on one rare phone model — the finding you most want — is exactly the row most likely to be missing. Missing does not mean zero, and averaging only the visible rows understates the problem.

### Watch out for

- The newest days are always incomplete; comparing today against last week manufactures a fake decline that "recovers" on its own
- Crash rates only cover devices that opted into reporting — a subset, not your whole install base
- The same subscriber gets a new purchase token on upgrades and resubscribes; counting tokens double-counts people
- There is no per-user install/uninstall timeline — that level of detail simply isn't offered

### Code block (right column, `pre`)

Lead-in (small gray paragraph, bold intro): **Vitals query** — one reported cell, one withheld cell

```
{ "deviceModel": "google/oriole",
  "userPerceivedAnrRate": 0.0043,
  "distinctUsers": 41208 }

{ "deviceModel": "oem_x/rare_sku",
  "metrics": [] }
//  ^ empty: too few users to report.
//    NOT a zero rate. The rare-device
//    regression you are hunting hides here.
//
// Freshness metadata tells you the latest
// COMPLETE day — truncate the series there.
```

Canvas lead-in (small gray paragraph, bold): **ANR and crash rate across staged releases — and why pooling method matters**

### Visualization (canvas `vitalsCanvas`, responsive width × 380)

Multi-line chart: three rate series over 8 releases, with a shaded elevated-rate zone, a "rollout halted" annotation, and a legend.

- **Title (bold 13px, `#1a5276`, top center):** "Android vitals across releases"
- **Subtitle (italic 10px, `#888`):** "illustrative values — shape of the argument, not measured rates" (JS comment: "Illustrative release series.")
- **X values (releases, labeled "v" + version):** 4.09, 4.10, 4.11, 4.12, 4.13, 4.14, 4.15, 4.16
- **Series data (%):**
  - ANR rate (denominator-pooled), red `#e74c3c`, solid: `[0.42, 0.39, 0.55, 0.81, 0.74, 0.48, 0.36, 0.31]`
  - Crash rate (denominator-pooled), orange `#e67e22`, solid: `[0.61, 0.58, 0.63, 0.69, 0.66, 0.57, 0.49, 0.44]`
  - ANR: unweighted mean of visible cells (JS comment: "naive unweighted mean over surviving (unsuppressed) device cells"), purple `#8e44ad`, dashed (`5,4`): `[0.31, 0.29, 0.40, 0.58, 0.53, 0.35, 0.27, 0.23]`
- **Marks:** line width 2, dots radius 3.5 in series color at every point
- **Axes:** y linear 0.0% to 1.0%, gridlines every 0.2% (`#eee`), right-aligned labels `#666` formatted "0.0%"…"1.0%"; solid `#999` left axis and bottom axis; padding top 62, right 24, bottom 78, left 56
- **Threshold band:** area above 0.47% filled `rgba(231,76,60,0.07)` with a dashed (`4,4`) red `#e74c3c` line at 0.47%; italic 9px red label above-left of the line: "elevated-rate zone (illustrative)" (JS comment: "bad-behaviour threshold band (illustrative Play guidance zone)")
- **Annotation:** dotted (`2,3`) gray `#999` vertical line at release index 3 (v4.12) from just above the ANR point to the top of the plot; italic 9px `#666` centered label at the top: "rollout halted"
- **Legend (bottom, 10px, swatch = 18px line segment in series color, dashed for the third):** "ANR rate (denominator-pooled)" `#e74c3c`; "Crash rate (denominator-pooled)" `#e67e22`; "ANR: unweighted mean of visible cells" `#8e44ad` dashed; label text `#2c3e50`
- **Caption (italic 10px purple `#8e44ad`, bottom center):** "the dashed series understates every release: suppressed small-device cells are the worst ones"
- Redraws on window resize; height fixed at 380 CSS px

## Official API References

- [Google Play Developer API](https://developers.google.com/android-publisher) — purchases, subscriptions, reviews, release management
- [Play Developer Reporting API](https://developers.google.com/play/developer/reporting) — Android vitals metric sets, freshness metadata, anomalies

## Regeneration instructions

- **Layout:** single detail page. `h1`, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with `<ul>` bullets and one `.key-point` div between them; right `<td>` (55%) holds a small gray lead-in `<p>` (0.85em, `#555`), a `<pre>` code block, a second lead-in `<p>`, and the `<canvas>`. Then `h2` "Official API References" with a `<ul>` of external links. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.section-label` bold `#1a5276` block; `li`/`p` 0.93em.
- **Canvas:** `<canvas id="vitalsCanvas" height="380">`, CSS `width: 100%`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); redraw on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, band fill `rgba(231,76,60,0.07)`, gray text `#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions.
