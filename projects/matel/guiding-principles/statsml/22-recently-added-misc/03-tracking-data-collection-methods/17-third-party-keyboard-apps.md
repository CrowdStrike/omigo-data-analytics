# Tracking Data: Third-Party Keyboard Apps

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Third-Party Keyboard Apps

**Subtitle:** A keyboard process receives the keystrokes typed into the field that has focus. Whether any of it leaves the device is a separate question from whether it arrives.

## Section 1: What is it?

**Lede:** A replacement keyboard is a separate process that receives the keystrokes.

- **Replaces the built-in keyboard** across the whole phone
- **Receives keystrokes** typed into any field in any app, in order to predict the next word
- **Leaving the device is separate** — prediction can run locally
- **Secure fields:** platforms mark password and other sensitive fields so a keyboard can be excluded from them

### Visualization (canvas `c1`, 720×320)

Flow diagram: where keystrokes go — arrival at the keyboard process vs leaving the device. Hue encodes three stages: blue `#2a78d6` = typed input at the field, green `#008300` = on-device keyboard process (always reached), orange `#d95926` = transmission past the device boundary (conditional); violet `#4a3aa7` marks the boundary itself (structure, not a stage).

- **Header strip (full width, y 0–26):** light blue wash (blue at alpha 0.10); three legend entries with 9×9 color squares, bold 13px: "1 · typed at the field" (blue, x=22), "2 · keyboard process, on device" (green, x=210), "3 · past the boundary" (orange, x=480).
- **Phone outline (left):** rounded rect at (60,44) 150×172 r12, blue stroke width 2. Inside: keyboard block filled blue at alpha 0.35 at (70,140) 130×66 with three centered blue 15px text rows "Q W E R T Y" / "A S D F G H" / "Z X C V B N"; label "focused text field" in text color `#2c3e50` 14px at (135,72).
- **Device boundary:** vertical dashed violet line (dash 5/4, width 1.5) from (400,46) to (400,232); bold 12px violet centered label "DEVICE BOUNDARY" at (400,244).
- **Arrow (always happens):** green line width 2 from (215,130) to (275,120), keystrokes → keyboard process.
- **Keyboard process box (stage 2):** rect (278,88) 108×66, green fill alpha 0.12, green stroke 1.5; bold 14px green centered "keyboard" / "process", 12px mute `#6b7280` "on-device model"; below the box, bold 12px green "ALWAYS ARRIVES HERE" at (332,174).
- **Conditional arrow (stage 3):** dashed orange line (dash 5/3, width 2) from (390,120) to (470,120); bold 12px orange label "ONLY IF SYNC IS ON" at (432,110).
- **Off-device box:** rect (478,88) 190×66, orange fill alpha 0.10, dashed orange stroke 1.5 (dash 4/3); bold 14px orange "keyboard vendor", 12px mute "depends on the implementation" / "and the setting".
- **Footer band (y 268–320):** left half washed green alpha 0.08, right half washed orange alpha 0.08. Bold 13px centered: "ARRIVAL" (green, at w/4) and "TRANSMISSION" (orange, at 3w/4); 13px text-color captions below: "happens whenever the keyboard has focus" (left) and "a separate event, and not implied by arrival" (right).

## Section 2: What does it collect?

- **Characters** typed into the focused field
- **Inter-keystroke timing** — typically at millisecond resolution; the keyboard is the input method itself, so nothing coarsens its timestamps the way a browser does for a web page
- **Backspaces** and re-typing within the field
- **Host app identifier** and the field's declared type
- **Terms** added to the local dictionary
- **Swipe paths** and which autocorrect suggestion was accepted

**Callout:** **`is_secure_field` is set by the app** being typed into, not by the keyboard. The boundary around passwords is enforced by whoever built that screen, and a field left unflagged is indistinguishable from ordinary text.

**Callout:** **`network_sync: false`:** arrival in the keyboard process is not the same event as transmission. Prediction can run entirely on-device.

### Visualization (canvas `c2`, 720×320)

Diagram: which fields reach the keyboard, and which the app can withhold. Hue encodes field type; the secure field drops to mute gray and its block marker is orange; aqua marks the derived artefact.

- **Title (bold 14px ink `#1a5276`, centered, y=18):** "The boundary is set by the field, not by the keyboard".
- **Field rows (boxes at x=45, 175×30, one per row):** "Message body" (y 52, blue `#2a78d6`, reaches), "Search box" (y 92, green `#008300`, reaches), "Email compose" (y 132, violet `#4a3aa7`, reaches), "Password field" (y 172, mute `#6b7280`, withheld). Reaching fields: fill of own hue at alpha 0.10, solid stroke; the withheld field: `#f8f9fa` fill, dashed stroke (4/3). Field name in its own hue, 15px.
- **Arrows:** dashed line (4/3) from each field at (225, y) toward the keyboard-process hub at (370,112), in the field's own hue; width 1.5 for reaching fields, 1 for the withheld one.
- **Blocked marker (secure field only):** orange `#d95926` X (12px cross, line width 2) at x=300 about halfway along its arrow, with 12px orange label "is_secure_field: true" to its right.
- **Hub:** circle center (410,112) radius 40, ink fill alpha 0.10, ink stroke width 2; bold 14px ink centered "keyboard" / "process".
- **Derived output:** aqua `#199e70` line width 2 from (455,112) to (540,112) into a box at (545,82) 130×60 (aqua fill alpha 0.10, aqua stroke 1.5) labeled 14px aqua "local dictionary" / "+ prediction model", with italic 12px mute "derived, not typed" below at (610,158).
- **Footer legend band (y 250–284, ink wash alpha 0.05):** 9×9 swatches at x = 45, 195, 330, 480 for the four fields, 12px labels; the password entry reads "Password field — withheld".
- **Caption (italic 13px mute, centered, y=306):** "A field left unflagged is indistinguishable from ordinary text."

**Payload note (right column, below canvas):** Sample payload — illustrative structure, not real captured data.

```
// What reaches the keyboard process. Whether any of it
// leaves the device is a separate question.
{
  // ── available to the keyboard from the platform ──
  "field_type":       "text",   // password / email / number are distinct types
  "is_secure_field":  false,    // set by the app; true suppresses the keyboard
  "host_app":         "com.example.messaging",
  "locale":           "en-US",
  "keystrokes": [
    { "k": "h", "dt_ms": 0   },
    { "k": "e", "dt_ms": 118 },
    { "k": "<bksp>", "dt_ms": 402 }
  ],

  // ── inferred / plausible, computed over the stream ──
  "wpm":              47,
  "correction_rate":  0.09,
  "learned_terms":    ["…"],    // added to the local dictionary
  "network_sync":     false     // prediction served from on-device model
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Prediction and correction** — suggesting the next word, fixing a mistyped one, learning names the dictionary lacks
- All of it needs **the characters as typed** and the context around them

**Label (effect pill):** Additional consequence

- **Timing works without content** — practised versus hesitant typing, present even when the text is discarded
- **Host app identifier** says which app the typing happened in, context the text alone does not carry

**Callout:** **The learned-terms list is a residual:** a word the shipped dictionary already holds is never added, so the list describes the gap between one person and one model, not how that person writes. Reading it as vocabulary mistakes the leftovers for the whole — and none of it establishes that anything left the phone.

### Visualization (canvas `c3`, 720×320)

Paired bar chart: words typed vs words added to the learned list, by how common the word is. Illustrative counts.

- **Title (bold 14px ink, centered, y=24):** "Words typed, and the few that reach the learned list". Subtitle (12px mute, y=42): "a word the shipped dictionary already holds is never added, however often it is typed".
- **Data (6 bins, typed / learned):** "the, and, to" 240/0; "ordinary words" 180/0; "work jargon" 44/6; "place names" 21/11; "friends' names" 14/13; "nicknames, slang" 9/9.
- **Layout:** baseline y=214, max bar height 132 scaled to max=240, first bin center x=78, step 100, bar width 46; typed bar to the left of center (blue `#2a78d6` fill at alpha 0.30, blue stroke), learned bar to the right (orange `#d95926` fill at alpha 0.55, orange stroke); minimum drawn height 1.5. Baseline is a grid-gray `#e5e9ef` line from x=30 to w-30.
- **Value labels (bold 12px):** typed count in blue above each typed bar; learned count above each learned bar in orange (mute gray when 0). Bin label in text color 12px below baseline (+18).
- **X-axis caption (12px mute, y baseline+38):** "common  ←  how common the word is  →  rare".
- **Legend (12px, y≈258–268):** blue-tinted swatch "words typed" at x=150; orange-tinted swatch "added to the learned list" at x=300.
- **Captions (centered):** italic 12px text color at h-26: "The orange bars are what a vocabulary read sees — the residual, not the writing."; italic 11px mute at h-9: "Illustrative counts — the shape of the split, not measured typing."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede` paragraph, bullets with bolded lead terms (`li b` in `#1a5276`), `.lbl` purpose/effect pills, and `.key-point` callouts; right `<td>` (55%, `text-align: center`) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption plus `.payload` `<pre>` block (both left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em. `.key-point` and `.payload`: background `#f8f9fa`, left border `3px solid #1a5276`; `.payload` ui-monospace 0.78em pre, `.payload-note` 0.82em italic `#666`. `.lbl` pills: uppercase 0.7em bold, `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (720×320); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts use a shared rounded-rect path helper and a `tint(hex, alpha)` helper producing rgba fills from palette tokens.
- **Palette:** this page's charts use the tracking categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Site-wide accents remain #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
