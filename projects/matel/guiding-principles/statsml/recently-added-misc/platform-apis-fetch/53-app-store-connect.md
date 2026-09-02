# App Store Connect

**Page type:** detail page (h1 + subtitle + verified badge, one "Overview" two-column obj-table row: text left 45%, code + canvas right 55%, then an "Official API References" list)
**HTML title tag:** App Store Connect — Platform APIs

**Subtitle:** Apple's reporting APIs for your own app: downloads, sales, subscriptions, and crashes — delivered as totals, never as individual users.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Daily downloads, sales, and store-listing views, broken down by country and device
- Subscription status, renewals, and refunds for your own customers, with push notifications on changes
- Crash counts per app version
- Finance reports of what Apple actually pays out

### Key point (callout, red left border)

**Small numbers are hidden.** If a country or slice has too few users, Apple withholds the row entirely for privacy — the data is missing precisely because it is small. Averages computed over only the visible rows are silently biased toward the big markets.

### Watch out for

- Recent days keep filling in after the fact, so the newest day always looks like a dip — exclude it or you'll chart a fake decline
- Units, active devices, and installs count different things; adding them double-counts
- Payout figures never exactly match sales figures — commission, tax, and a different calendar. The gap is expected, not a bug
- There is no per-user journey: impressions can't be joined to the installs they caused

### Code block (right column, `pre`)

Lead-in (small gray paragraph, bold intro): **Analytics report rows** — same day, two territories

```
{ "date": "2026-08-21", "territory": "US",
  "impressions": 184203,
  "product_page_views": 41577,
  "first_time_downloads": 6188 }

{ "date": "2026-08-21", "territory": "LU",
  "impressions": null,
  "product_page_views": null,
  "first_time_downloads": null }
//  ^ withheld: below the privacy threshold.
//    Missing BECAUSE small. Drop rows like
//    this and small markets vanish from
//    every rate you compute.
```

Canvas lead-in (small gray paragraph, bold): **Threshold suppression: what a naive mean does to a long-tailed market mix**

### Visualization (canvas `thresholdCanvas`, responsive width × 380)

Combo chart: log-scale volume bars per territory (left axis) with a conversion-rate line (right axis), a dashed suppression-floor line, and two computed mean labels.

- **Title (bold 13px, `#1a5276`, top center):** "Suppressed rows are missing because they are small"
- **Subtitle (italic 10px, `#888`):** "illustrative territory mix; suppression floor drawn at an arbitrary level" (JS comment: "Illustrative: territories sorted by volume, each with a conversion rate that happens to be higher in small markets.")
- **Data (territory, downloads, conversion rate):** US 6188 / 0.149; GB 2410 / 0.141; DE 1980 / 0.138; JP 1520 / 0.133; FR 1180 / 0.131; CA 860 / 0.128; AU 610 / 0.126; BR 430 / 0.121; IN 300 / 0.118; MX 180 / 0.112; SE 96 / 0.185; NO 61 / 0.192; DK 44 / 0.201; FI 28 / 0.208; LU 14 / 0.221; IS 7 / 0.234
- **Suppression threshold:** THRESH = 100 downloads (JS comment: "illustrative suppression floor"); territories below it (SE, NO, DK, FI, LU, IS) are "suppressed"
- **Left axis (volume, log10):** gridlines at 1, 10, 100, 1000, 10000 (`#eee`, labels `#666`); rotated bold 11px `#1a5276` axis label on the left: "downloads (log)"
- **Bars:** width 56% of group width; visible territories fill `rgba(26,82,118,0.35)` with stroke `#1a5276`; suppressed territories fill `rgba(231,76,60,0.30)` with stroke `#e74c3c`; territory names 9px below the plot, colored `#e74c3c` if suppressed else `#2c3e50`
- **Floor line:** dashed (`5,4`) red `#e74c3c` width 1.5 horizontal line at the log-scale position of 100; italic 10px red label above-left: "privacy threshold — below this, row withheld"
- **Right axis (conversion rate):** linear 10% to 25%, green `#27ae60` labels at 10%, 15%, 20%, 25%; rotated bold 11px green axis label on the right: "conversion rate"
- **Rate line:** solid green `#27ae60` width 2 through the visible (unsuppressed) territories; dashed (`4,3`) purple `#8e44ad` segment connecting from the last visible point through the suppressed territories; points radius 3.5 filled green when visible, purple when suppressed
- **Mean labels (bold 10px, top-left of plot):** green "mean over visible rows: X%" and purple "mean over all rows: Y%", both computed from the data at draw time (unweighted means over the rate column: visible ≈ 13.0%, all ≈ 15.5%)
- **Layout:** padding top 60, right 52, bottom 68, left 52; white background
- **Caption (italic 10px `#666`, bottom center):** "the withheld tail carries a different rate — dropping it biases the estimate, it does not just shrink n"
- Redraws on window resize; height fixed at 380 CSS px

## Official API References

- [App Store Connect API — Apple Developer Documentation](https://developer.apple.com/documentation/appstoreconnectapi) — sales/finance reports and analytics report requests
- [App Store Server API — Apple Developer Documentation](https://developer.apple.com/documentation/appstoreserverapi) — transaction history, subscription statuses, refund lookup

## Regeneration instructions

- **Layout:** single detail page. `h1`, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with `<ul>` bullets and one `.key-point` div between them; right `<td>` (55%) holds a small gray lead-in `<p>` (0.85em, `#555`), a `<pre>` code block, a second lead-in `<p>`, and the `<canvas>`. Then `h2` "Official API References" with a `<ul>` of external links. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.section-label` bold `#1a5276` block; `li`/`p` 0.93em.
- **Canvas:** `<canvas id="thresholdCanvas" height="380">`, CSS `width: 100%`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); redraw on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, suppressed fill `rgba(231,76,60,0.30)`, gray text `#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions.
