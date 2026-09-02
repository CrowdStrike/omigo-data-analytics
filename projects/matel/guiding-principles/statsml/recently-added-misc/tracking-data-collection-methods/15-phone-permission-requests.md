# Tracking Data: Phone Permission Requests

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Phone Permission Requests

**Subtitle:** The popup asking for camera or location access. One tap sets a durable grant, and the OS then answers the app's reads without asking again.

## Section 1: What is it?

An OS gate in front of sensors and personal stores.

- **Behind the gate:** location, photos, contacts, microphone
- **The OS shows the prompt**, not the app
- **One answer, durable grant** — later reads succeed with no new prompt
- **Changed only in settings**, not by the app

**Grant and reads are separate things:** consent is recorded once, as a single value; the reads it authorises are an open-ended series. Nothing in the grant record says how many reads followed, or which served the feature the prompt described.

### Visualization (canvas `c1`, 720×320)

Diagram: a permission popup on the left, an arrow to the series of reads that one grant authorises. Hue encodes event kind: blue `#2a78d6` = the grant itself (one user action), green `#008300` = foreground read, orange `#d95926` = background read, mute `#6b7280` = standing grant state (not an event).

- **Popup:** rounded rect (radius 10) at x=30, y=40, 200×160, fill `#f8f9fa`, blue stroke 2px. Text: bold 15px blue, two lines: "\"WeatherApp\" wants" / "to use your location"; 13px muted: "This will be used to show" / "weather for your area." Buttons: blue filled "Allow" (70×28, radius 5) with white bold label, grid-gray "Deny" with muted label. A 15px-radius blue circle at 30% alpha over the Allow button as a tap indicator, with 13px blue caption "one tap" below.
- **Arrow:** solid blue 3px horizontal line from (232,120) to (310,120) with a solid arrowhead at x=322.
- **Read list:** bold 16px ink heading centered at (510,32): "One grant, then a series of reads:". Six rows (starting y=48, 28px pitch), each with a 3×14 hue tab at x=332 and 14px text in the same hue prefixed "→ ":
  1. "Foreground read — user opened the map" (green)
  2. "Foreground read — user opened the map" (green)
  3. "Background read — app refreshed" (orange)
  4. "Background read — app refreshed" (orange)
  5. "Background read — app refreshed" (orange)
  6. "Grant still set; no further prompt" (mute)
- **Footer band** (y=252, 68px tall, ink tint alpha 0.05): legend squares — "the grant — one user action" (blue, x=40), "foreground read" (green, x=262), "background read" (orange, x=420), "standing grant state" (mute, x=578). Centered 14px `#2c3e50` line at y=300: "Every read carries the same consent flag, whichever kind it was."

## Section 2: What does it collect?

- **Location fixes**, coarse or precise depending on the grant — precise typically resolves to a few metres, the reduced tier deliberately to roughly the kilometre scale
- **Contacts entries**, and on some platforms call log metadata
- **Photos** — the whole library or only selected items, plus embedded capture location
- **Camera frames and microphone audio**, while the respective grant is active
- **Step counts** and other health store records
- **Calendar events** and reminders

**Yes or no hides a precision field:** coarse and precise location are different grants. Two rows both flagged permitted can differ by orders of magnitude in what they resolve — the coordinates print to four decimals while the accuracy field says the fix covers a large radius.

**Filter on accuracy, not the flag:** filtering on the permission flag instead of the accuracy field silently mixes the two.

### Visualization (canvas `c2`, 720×320)

3×2 card grid: what each grant scopes, and what it does not resolve. Each of the six permission cards takes its own SERIES hue in order (blue `#2a78d6`, green `#008300`, violet `#4a3aa7`, orange `#d95926`, aqua `#199e70`, magenta `#d55181`); the "does not resolve" line stays muted `#6b7280` in every card.

- **Title (bold 16px, ink, centered, y=20):** "Each grant is scoped to a data class, not to a feature".
- **Cards:** 210×85 rounded rects (radius 6), 3 columns, 15px horizontal / 12px vertical gaps, starting at (35,38); fill hue tint alpha 0.10, 1px hue stroke, 3px solid hue bar along the top edge. Inside each: card name bold 16px in hue; scope 14px `#2c3e50`; "not" line italic 13px muted.
  1. **Location** — "Coordinates + accuracy" — "Not a visit or an intent"
  2. **Contacts** — "Entries about other people" — "They were not asked"
  3. **Camera** — "Frames while in use" — "Not who is in frame"
  4. **Microphone** — "Audio while granted" — "Not who is speaking"
  5. **Photos** — "Library or selected items" — "Capture place, not present place"
  6. **Calendar** — "Event titles and times" — "Not whether you attended"
- **Footer band** (y=250, 70px, ink tint alpha 0.05, centered): 14px `#2c3e50` "Each card names the data class the grant covers," then italic 14px muted "and below it, what the same data does not establish."

### Payload (below canvas c2)

Payload note (italic, above the block): "Sample payload — illustrative structure, not real captured data."

```
{
  // ── the grant, as the OS records it ──
  "permission":    "location",
  "scope":         "always",       // vs "while_in_use", "once"
  "precision":     "reduced",      // coarse and precise are separate grants
  "granted_at":    "2026-03-04T09:12:55Z",

  // ── inferred / plausible: a fix produced under that grant ──
  "lat":           30.2672,
  "lon":           -97.7431,
  "accuracy_m":     1400,          // metres, radius — not a point
  "altitude_m":     null,          // sensor did not report
  "source":        "wifi",         // gps | wifi | cell
  "app_state":     "background",
  "ts":            "2026-08-22T02:41:07Z"
}
```

## Section 3: Why is it collected?

**Stated purpose** (label pill)

- **The feature needs it** — no routing without a position, no capture without the camera. For most permissions on most apps this is the whole story
- **The prompt exists** so the read cannot happen silently

**Additional consequence** (label pill)

- The grant is scoped to the **data class**, not the feature that justified it — granted at "always," a position can be read **on a schedule**
- **Contacts read as a file** describes people who were never asked: the entries are about them, the grant came from the phone's owner

**A coarse switch over a fine-grained series:** every read under one grant carries the same consent flag, so the flag cannot tell a foreground read the user started from a background read they never saw. A gap in the series is a stretch when the app was not scheduled to read, which looks the same as a stretch of staying still.

### Visualization (canvas `c3`, 720×320)

Log-scale bar chart: reads accumulating under one grant while the consent count stays at 1. Counts follow from one stated interval — a background fix every 15 minutes — so the bars are arithmetic, not asserted data.

- **Title (bold 14px, ink, centered, y=24):** "Reads accumulating under one grant". **Subtitle (12px, muted, y=42):** "a background fix every 15 minutes, and no further prompt".
- **Bars** (blue `#2a78d6` at alpha 0.32 fill, 1px blue stroke; 66px wide, centered at x=202 + i×122; baseline y=218; height = log10(v+1)/log10(2881) × 132; value bold 13px blue above each bar, label 12px `#2c3e50` below the baseline):
  1. "First hour" — 4 (label "4")
  2. "One day" — 96 (label "96")
  3. "One week" — 672 (label "672")
  4. "One month" — 2,880 (label "2,880")
- **Consent line:** dashed (6/4) orange `#d95926` 2px horizontal line at the log-scale height of 1, spanning x=60 to w−30, labeled above-left in bold 12px orange: "taps on \"Allow\": stays at 1".
- **Axis note (11px, muted, centered, baseline+38):** "log scale — otherwise the first bar and the line are the same height".
- **Captions (centered):** italic 12px `#2c3e50` at h−26: "Every one of those reads carries the same consent flag as the first." Italic 11px muted at h−9: "Illustrative — counts follow from the 15-minute interval, not from a measured app."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` + bullets + `.key-point` callouts, right `<td>` (55%, centered) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre block below the canvas, left-aligned).
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; `li` 0.93em with `li b` in `#1a5276` weight 600. No nav bar, no back/home links.
- **Callouts:** `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em; leading `<strong>` in `#1a5276`.
- **Label pills:** `.lbl` inline-block uppercase 0.7em weight 700, letter-spacing 0.05em, padding 2px 7px, radius 3px; `.lbl-purpose` background `#eaf2fb` color `#1a5276`; `.lbl-effect` background `#fdf0e6` color `#a8501c`.
- **Payload block:** `.payload` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px, ui-monospace 0.78em, `white-space: pre`, line-height 1.45; `.payload-note` 0.82em `#666` italic.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Include a rounded-rect path helper and a `tint(hex, alpha)` helper for translucent fills.
- **Palette (tracking pages):** categorical tokens blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276` (headings/axes only), text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Project-wide palette reference: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
