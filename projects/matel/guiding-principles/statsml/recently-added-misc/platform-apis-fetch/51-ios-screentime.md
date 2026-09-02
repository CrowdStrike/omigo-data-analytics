# iOS Screen Time

**Page type:** detail page (h1 + subtitle + verified badge, one "Overview" two-column obj-table row: text left 45%, code + canvas right 55%, then an "Official API References" list)
**HTML title tag:** iOS Screen Time — Platform APIs

**Subtitle:** Apple's frameworks for building screen-time and parental-control apps — deliberately designed so that app-usage data cannot be collected.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- Let the user pick which apps or categories to limit, through a system picker
- Get notified when usage of those apps crosses a time threshold you set
- Block ("shield") apps and websites on a schedule
- Show usage summaries to the user — on their device only

### Key point (callout, red left border)

**Identity and transport are separated on purpose.** Your code receives opaque tokens instead of app names, and the only component allowed to see usage durations has no network access — so numbers can be drawn on screen but never sent to a server. Any dataset that claims to contain iOS per-app usage was collected some other way, with its own severe selection bias.

### Watch out for

- You never learn which app used the time — only that a threshold you set was crossed
- You cannot list installed apps or see any usage from before the user authorized you
- The required entitlement must be requested from Apple and justified; it is not self-serve
- Users can revoke authorization at any time, and you get no server-side signal that they did

### Code block (right column, `pre`)

Lead-in (small gray paragraph, bold intro): **The whole design in a few lines of Swift** — tokens without names, thresholds without breakdowns, no way to phone home

```
// The user picks; you receive opaque tokens.
@State var selection = FamilyActivitySelection()
// selection.applicationTokens : Set<ApplicationToken>
// token.bundleIdentifier -> does not exist

let event = DeviceActivityEvent(
    applications: selection.applicationTokens,
    threshold: DateComponents(minute: 30)
)
// When it fires you learn "30 minutes elapsed" --
// not which app, and the extension that could
// see the breakdown has no network access.
```

Canvas lead-in (small gray paragraph, bold): **What the API exposes, by field**

### Visualization (canvas `visibilityCanvas`, responsive width × 380)

Dot-matrix availability grid: 8 field rows × 3 availability columns, one large filled dot per row marking which column applies, small gray dots (`#e0e0e0`, radius 3.5) in the other cells.

- **Title (bold 13px, `#1a5276`, top center):** "Screen Time API: resolution available to your code"
- **Subtitle (italic 10px, `#888`):** "the blocked rows are design decisions, not gaps"
- **Columns (headers colored, 10px):** "Readable value" `#27ae60`, "Token / render only" `#e67e22`, "Not available" `#e74c3c`
- **Rows (right-aligned labels, 11px `#2c3e50`) and their active column (0/1/2):**
  - Total activity duration → 0 (Readable value)
  - Per-category duration → 1 (Token / render only)
  - Per-app duration → 1
  - App name / icon (for display) → 1
  - App bundle identifier → 2 (Not available)
  - List of installed apps → 2
  - Usage before authorization → 2
  - Send usage to your server → 2
- **Marks:** active cell = filled circle radius 8 in the column color with white 1.5px stroke; inactive cells = radius 3.5 circle `#e0e0e0`
- **Layout:** padding top 62, right 18, bottom 42, left min(210, 42% of width); even-index rows shaded `rgba(26,82,118,0.04)`; vertical column separators `#ddd`; outer grid frame `#999`; white background
- **Caption (italic 10px `#666`, bottom center):** "durations are real; identity and transport are what is withheld"
- Redraws on window resize; height fixed at 380 CSS px

## Official API References

- [FamilyControls](https://developer.apple.com/documentation/familycontrols) — authorization, FamilyActivityPicker, and the opaque token types
- [DeviceActivity](https://developer.apple.com/documentation/deviceactivity) — schedules, threshold events, DeviceActivityMonitor and DeviceActivityReport

## Regeneration instructions

- **Layout:** single detail page. `h1`, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with `<ul>` bullets and one `.key-point` div between them; right `<td>` (55%) holds a small gray lead-in `<p>` (0.85em, `#555`), a `<pre>` code block, a second lead-in `<p>`, and the `<canvas>`. Then `h2` "Official API References" with a `<ul>` of external links. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.section-label` bold `#1a5276` block; `li`/`p` 0.93em; `a` and `code` styled (`code` background `#f4f4f4`).
- **Canvas:** `<canvas id="visibilityCanvas" height="380">`, CSS `width: 100%`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); redraw on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, bar/row fill `rgba(26,82,118,0.04-0.35)`, gray text `#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions.
