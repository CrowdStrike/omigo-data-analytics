# Smart Scales & Blood Pressure Cuffs

**Page type:** detail page (two-column obj-table layout: text left 45%, payload + canvas right 55%, single Overview row, followed by an API-references list)
**HTML title tag:** Smart Scales &amp; Blood Pressure Cuffs — Platform APIs

**Subtitle:** Lets you read weight, body-composition and blood-pressure readings from connected home devices, mainly through the Withings cloud API.

**Verified badge:** Last verified: August 2026

## Overview

**What you can get** (section label, left column)

- Weight readings, timestamped, from smart scales
- Body-composition estimates: fat, muscle, water and bone
- Blood pressure and pulse from connected cuffs
- Which device took each reading, and whether it was measured or typed in by hand
- Webhook notifications when new readings arrive

**Key-point callout (red left border):**

**A shared household scale guesses who stepped on it.** It matches each reading to the nearest stored profile by weight, and the guess fails most exactly when two people's weights converge — which may be the very change you are studying. Readings can also be reassigned or deleted by users later, so history is not fixed.

**Watch out for** (section label, left column)

- Every value comes with a power-of-ten exponent (quantity = value × 10^unit) — ignore it and 72.5 kg reads as 72500, and the exponent can differ between records of the same type
- Hand-typed entries sit in the same column as device readings; a flag (`attrib`) separates the two measurement processes
- Body-fat and muscle numbers are estimates from an electrical signal — they swing with hydration and firmware updates; only weight is a direct measurement
- Readings happen when the user chooses, so the data reflects the moments people decided to measure — not their typical state

**Payload note (right column, inline-styled 0.85em `#555`):** **Two "weight" records** — one measured by the scale, one typed in by the user, with different exponents

Code block (`pre`), verbatim:

```
{ "measuregrps": [
    { "attrib": 0,      // measured by the device
      "measures": [
        {"value": 72500, "type": 1, "unit": -3},
                        // 72.500 kg  weight
        {"value": 18740, "type": 6, "unit": -3}
                        // 18.740 %   fat ratio
      ] },
    { "attrib": 2,      // typed in by the user
      "measures": [
        {"value": 730, "type": 1, "unit": -1}
                        // 73.0 kg    weight
      ] }
] }
```

**Chart caption (above canvas):** **Two people, one scale, 60 days.** Readings get filed under the wrong profile exactly where the two weights converge — hollow red markers sit on the profile a reading was filed under.

### Visualization (canvas `misattributionChart`, responsive width × 380)

Two-series line chart over 60 days: true weight trajectories of two household members, a shaded convergence band, and hollow red markers showing readings filed under the wrong profile inside the band.

- **Data model (deterministic, computed):**
  - User A true series: `78 - 6.4*(t/45)` for t ≤ 45, then `71.6 + 1.4*((t-45)/15)`, plus wobble `0.22*sin(1.10t) + 0.12*cos(0.37t)` (weight-loss trend then slight regain).
  - User B true series: `71.0 + 0.34*sin(0.80t) + 0.16*cos(1.70t)` (stable around 71 kg).
  - Misassigned readings (all inside the convergence window): day 38 owner A filed as B; day 43 owner B filed as A; day 47 owner A filed as B; day 51 owner B filed as A; day 54 owner A filed as B.
  - Convergence band: days 35–55.
- **Layout:** height 380; margins left 62, right 18, top 74, bottom 52. x maps day 0–60; y maps weight 64–84 kg.
- **Title (top left):** bold 13px `#1a5276` "Shared household scale: profile assignment fails where the two weights converge"; 11px `#555` sub-line "Label noise is a function of the separation between profiles — the same quantity under study".
- **Convergence band:** filled `rgba(26,82,118,0.35)` from day 35 to 55, full plot height; centered blue `#1a5276` 10px two-line label near the top: "profiles within scale's" / "discrimination margin".
- **Gridlines:** `#e8e8e8` horizontal at y = 64, 68, 72, 76, 80, 84 with `#555` 11px right-aligned tick labels.
- **Axes:** `#2c3e50` width 1.2 L-shape; x tick marks every 10 days labeled "day 0" … "day 60" in `#555`; below them `#888` centered "60 consecutive days"; rotated y-axis title "weight (kg)" in `#555`.
- **Series lines:** width 2, User A `#1a5276`, User B `#27ae60`, one point per integer day.
- **Separation annotation (orange `#e67e22`):** dashed (`[4,3]`) vertical line at day 8 spanning between the two curves; 10px left-aligned label at day 9, midway: "separation large → assignment reliable".
- **Misassigned readings:** for each swap — dashed (`[3,3]`) red `#e74c3c` vertical connector between the owner's true value and the filed-under profile's value at that day; a small 2.5px-radius filled dot on the true owner curve (colored `#1a5276` if owner A, `#27ae60` if owner B); a hollow marker (4.5px radius circle, white fill, `#e74c3c` stroke width 1.8) on the filed-under curve.
- **Legend (top, y=56, 11px `#555` labels):** line swatch `#1a5276` "User A (true series)"; line swatch `#27ae60` "User B (true series)"; hollow red-stroked white circle swatch "reading filed under the wrong profile".
- Redraws on window resize.

## Official API References

- [Withings Developer Portal](https://developer.withings.com/) — app registration, OAuth 2.0 setup and developer guides
- [Withings API Reference](https://developer.withings.com/api-reference) — measure (getmeas), sleep, heart, user/getdevice and notify endpoints with type codes

## Regeneration instructions

- **Layout:** platform-APIs detail page. h1, `.subtitle` paragraph, `.verified` badge span, then `h2` "Overview" with a single-row `.obj-table` (left `<td>` 45%: section labels + bullet lists + one `.key-point` callout; right `<td>` 55%: payload note + `<pre>` JSON + chart note + `<canvas>`), then `h2` "Official API References" with a link list.
- **Page CSS:** body system sans-serif (-apple-system stack), line-height 1.6, text `#2c3e50`, white background, padding 30px 40px. h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.verified` inline badge — background `#eaf2f8`, border `1px solid #2980b9`, text `#1a5276`, padding 2px 10px, radius 4px, 0.8em. h2 1.3em `#1a5276` with `border-bottom: 2px solid #2980b9`. `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em. `.key-point` background `#f8f9fa`, `border-left: 3px solid #e74c3c`, padding 10px 14px, 0.93em. `.section-label` bold `#1a5276` block. Payload/chart notes are inline-styled 0.85em `#555` paragraphs. `li`/`p` 0.93em; links `#1a5276`; `code` background `#f4f4f4`. No nav bar, no back/home links.
- **Canvas:** `<canvas id="misattributionChart" height="380">`, CSS `display:block; width:100%`; drawing code reads `getBoundingClientRect().width`, sets backing store to `rect.width * dpr` / `380 * dpr` using `window.devicePixelRatio`, fixes CSS height to 380px, `ctx.scale` back to logical coordinates, and re-renders on `resize`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, band fill `rgba(26,82,118,0.35)`; grid `#e8e8e8`; text `#555`/`#2c3e50`/`#888`.
- In regenerated HTML, any card/page links use `.html` extensions.
