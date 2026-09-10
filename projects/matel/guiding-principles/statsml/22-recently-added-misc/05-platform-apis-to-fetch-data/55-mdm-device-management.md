# MDM / Device Management

**Page type:** detail page (h1 + subtitle + verified badge, one "Overview" two-column obj-table row: text left 45%, code + canvas right 55%, then an "Official API References" list)
**HTML title tag:** MDM / Device Management — Platform APIs

**Subtitle:** Corporate device-management APIs (Apple MDM, Android Management, Intune, Jamf) that let an employer see what is installed on managed phones and laptops — with visibility set by who owns the hardware.

**Verified badge:** Last verified: August 2026

## Overview

### What you can get

- A per-device inventory: installed apps, OS version, hardware details
- Compliance state — passcode set, patched, encrypted
- Remote actions: lock, wipe, install apps; locate a lost corporate device
- Alerts when devices enroll, check in, or fall out of compliance

### Key point (callout, red left border)

**Who owns the device decides what you see.** Company-owned devices report everything, including personal apps and hardware serial numbers. Employee-owned (BYOD) devices report only the work side, by design. Pooling the two into one statistic compares two different measurement contracts, not two groups of users.

### Watch out for

- "Installed" is not "used" — none of these APIs report how long an app was used
- Everything is a snapshot from the last check-in: a silent device looks unchanged, not unreachable — always keep the check-in time with the data
- Users can un-enroll personal devices at any time, and they tend to drop out for reasons related to what the data would have shown
- No message, browsing, or continuous-location content — MDM configures devices, it does not spy on them

### Code block (right column, `pre`)

Lead-in (small gray paragraph, bold intro): **Apple MDM** — installed-app inventory (excerpt)

```
{
  "Status": "Acknowledged",
  "InstalledApplicationList": [
    { "Identifier": "com.example.crm",
      "Name": "Field CRM",
      "ShortVersion": "7.4.1" },
    { "Identifier": "com.thirdparty.game",
      "Name": "Puzzle Quest" }
      // ^ personal app: visible ONLY because
      //   this device is corporate-owned.
      //   BYOD enrolment returns managed
      //   apps only.
  ]
}
// Nothing here is a usage duration.
```

Canvas lead-in (small gray paragraph, bold): **Field visibility by ownership model**

### Visualization (canvas `ownershipCanvas`, responsive width × 380)

Availability matrix: 10 field rows × 3 ownership-model columns; each cell is a small rectangle rendered in one of three states — "full" (solid green with white text), "conditional" (translucent orange with orange border and dark-orange text), "none" (dashed red outline with red text).

- **Title (bold 13px, `#1a5276`, top center):** "Who owns the hardware determines the schema"
- **Subtitle (italic 10px, `#888`):** "do not pool these populations into one statistic"
- **Columns (headers 10px, two lines each, colored):** "Supervised / corporate-owned" `#1a5276`; "BYOD work profile / user enrolment" `#e67e22`; "App-protection only (MAM)" `#8e44ad`
- **Rows (right-aligned labels 10.5px `#2c3e50`) with cell values (2 = full, 1 = partial/conditional, 0 = not available) per column in order:**
  - Managed app inventory — 2, 2, 2
  - Personal app inventory — 2, 0, 0
  - Hardware serial / IMEI — 2, 0, 0
  - OS build & patch level — 2, 2, 1
  - Compliance state — 2, 2, 2
  - Location (lost mode) — 1, 0, 0
  - Remote full wipe — 2, 0, 0
  - Selective / work-data wipe — 2, 2, 2
  - App usage duration — 1, 0, 0
  - User can remove management — 0, 2, 2
- **Cell rendering:** rect width min(60% of column width, 78px), height 50% of row height; full = fill `#27ae60`, bold 9px white centered text "full"; conditional = fill `rgba(230,126,34,0.35)`, stroke `#e67e22` width 1, bold 9px `#b9600e` text "conditional"; none = dashed (`3,3`) stroke `#e74c3c` width 1, bold 9px `#e74c3c` text "none"
- **Layout:** padding top 76, right 16, bottom 40, left min(190, 38% of width); even-index rows shaded `rgba(26,82,118,0.04)`; vertical column separators `#ddd`; outer grid frame `#999`; white background
- **Caption (italic 10px `#666`, bottom center):** ""app usage duration" is conditional at best, and only on fully managed Android"
- Redraws on window resize; height fixed at 380 CSS px

## Official API References

- [Device Management — Apple Developer Documentation](https://developer.apple.com/documentation/devicemanagement) — MDM protocol commands, profiles, and Declarative Device Management
- [Android Management API](https://developers.google.com/android/management) — enterprises, devices, policies, enrollment tokens

## Regeneration instructions

- **Layout:** single detail page. `h1`, `.subtitle` paragraph, `.verified` badge span, `h2` "Overview", one `.obj-table` with a single `<tr>`: left `<td>` (45%) holds `.section-label` headings ("What you can get", "Watch out for") with `<ul>` bullets and one `.key-point` div between them; right `<td>` (55%) holds a small gray lead-in `<p>` (0.85em, `#555`), a `<pre>` code block, a second lead-in `<p>`, and the `<canvas>`. Then `h2` "Official API References" with a `<ul>` of external links. No nav bar, no back/home links.
- **Page CSS:** body -apple-system/system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; h2 1.3em `#1a5276` with 2px `#2980b9` bottom border; `.verified` badge — background `#eaf2f8`, border `1px solid #2980b9`, color `#1a5276`, padding 2px 10px, radius 4px, 0.8em; `pre` background `#f4f4f4`, padding 14px, radius 6px, 0.82em; `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 10px 14px, 0.93em; `.section-label` bold `#1a5276` block; `li`/`p` 0.93em.
- **Canvas:** `<canvas id="ownershipCanvas" height="380">`, CSS `width: 100%`; sized from `getBoundingClientRect()` and scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates); redraw on window resize.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (dark-orange text `#b9600e`), purple `#8e44ad`, row shading `rgba(26,82,118,0.04)`, gray text `#666`/`#888`.
- In regenerated HTML, any card links use `.html` extensions.
