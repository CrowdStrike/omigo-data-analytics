# Android Usage Stats

**Page type:** detail page (h1 + subtitle + verified badge, one "Overview" two-column obj-table row: text left 45%, code + canvas right 55%, then an "Official API References" list)
**HTML title tag:** Android Usage Stats — Platform APIs

**Subtitle:** An Android API that lets an app see how much time the phone's owner spends in each app — but only after the user flips a special settings switch by hand.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- How long each app was on screen, per day, week, or month
- A timeline of when apps were opened and closed
- When the screen turned on and off, and when the phone was unlocked
- App launch counts, worked out by counting the app-open events yourself

### Key point (callout, red left border)

**There is no permission pop-up.** The only way in is to send the user to a special Settings screen and hope they flip the toggle. If they don't, the API quietly returns an empty list instead of an error — so "no data" and "no access" look identical unless you record the permission state alongside the data.

### Watch out for

- People who grant this access are a self-selected, unusually engaged slice — an average computed from them is not an average of everyone
- Screen time is not attention: an app left open but idle still racks up minutes
- Fine-grained history is only kept for days; long history exists only in coarse buckets — you can't have both
- You see which app was used and for how long — never what was done inside it

### Code block (right column, `pre`)

Lead-in (small gray paragraph, bold intro): **Kotlin** — the essentials

```
// No permission dialog exists — only a deep
// link to Settings, which the user may ignore.
ctx.startActivity(
    Intent(Settings.ACTION_USAGE_ACCESS_SETTINGS))

// Screen time per app, last 7 days
val perApp = usm.queryUsageStats(
        UsageStatsManager.INTERVAL_DAILY, begin, end)
    .groupBy { it.packageName }
    .mapValues { (_, rows) ->
        rows.sumOf { it.totalTimeInForeground } }

// An empty result is ambiguous:
//   genuinely no usage  OR  access revoked.
// Record the permission state with the data.
```

Canvas lead-in (small gray paragraph, bold): **Usage-access grant funnel: who ends up in your sample**

### Visualization (canvas `grantFunnel`, responsive width × 380)

Funnel bar chart: 5 stages of the usage-access grant funnel, each bar drawn over a faint full-height "ghost" of the total population, with per-stage drop-off labels.

- **Title (bold 13px, `#1a5276`, top center):** "The Settings toggle is the sampling frame"
- **Subtitle (italic 10px, `#888`):** "illustrative shape — measure your own rate and report it"
- **Data (stage label, value %, bar color) — JS comment: "Illustrative funnel shape, not measured figures.":**
  - Installs — 100% — `#1a5276`
  - Sees prompt — 78% — `#2980b9`
  - Opens Settings — 52% — `#e67e22`
  - Toggles on — 34% — `#27ae60`
  - Still on at d30 — 23% — `#8e44ad`
- **Axes:** y from 0% to 100%, gridlines every 20% (`#eee`), right-aligned labels `#666`; gray baseline `#999` at the bottom of the plot; padding top 58, right 22, bottom 74, left 52
- **Bars:** width 50% of group width; ghost of the full population behind each bar = full-height rectangle `rgba(26,82,118,0.08)`; value label bold 12px `#2c3e50` above each bar (e.g. "100%"); stage label 10px `#2c3e50` below the axis
- **Drop-off labels:** below each stage label from stage 2 on, italic 9px red `#e74c3c`: "-22pp", "-26pp", "-18pp", "-11pp"
- **Annotation:** dashed (`4,4`) purple `#8e44ad` horizontal line from the left axis to the last bar at the 23% level; italic 10px purple text above it, left-aligned: "everything you measure comes from this slice"
- **Caption (italic 10px `#666`, bottom center):** "drop-off is not random: granters are the more engaged, more permissive users"
- Redraws on window resize; height fixed at 380 CSS px

## Official API References

- [UsageStatsManager — Android API reference](https://developer.android.com/reference/android/app/usage/UsageStatsManager) — queryUsageStats, queryEvents, interval constants
- [UsageEvents.Event — Android API reference](https://developer.android.com/reference/android/app/usage/UsageEvents.Event) — event types for deriving launches and sessions

## Regeneration instructions

- **Layout:** single detail page. `h1`, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with `<ul>` bullets and one `.key-point` div between them; right `<td>` (55%) holds a small gray lead-in `<p>` (0.85em, `#555`), a `<pre>` code block, a second lead-in `<p>`, and the `<canvas>`. Then `h2` "Official API References" with a `<ul>` of external links. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.section-label` bold `#1a5276` block; `li`/`p` 0.93em.
- **Canvas:** `<canvas id="grantFunnel" height="380">`, CSS `width: 100%`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); redraw on window resize.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, ghost fill `rgba(26,82,118,0.08)`, gray text `#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions.
