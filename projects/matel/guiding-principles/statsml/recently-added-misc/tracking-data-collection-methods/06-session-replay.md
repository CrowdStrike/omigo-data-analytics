# Tracking Data: Session Replay / Screen Recording

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, three rows)
**HTML title tag:** Tracking Data: Session Replay / Screen Recording

**Subtitle:** No video is captured. A script records the initial DOM, then a log of mutations and input events; a player re-applies that log in a sandboxed frame to reconstruct an approximation of what the page looked like.

## What is it?

A mutation log, not footage — the name misleads.

- **One full DOM snapshot** at load
- **Then a timestamped change log** via `MutationObserver` and input listeners: nodes, attributes, text, scroll, pointer, input values
- **Only the log is uploaded** — no video
- **Playback** re-applies mutations into a sandboxed iframe, rebuilding the page
- **rrweb** is the widely used open-source implementation; commercial tools follow the same model
- **Masking is documented:** password fields by default, others via `maskAllInputs`, per-selector rules and CSS classes

**Key point callout:** **Smaller than video, and structured:** a log reconstructs at any resolution — which is why the approach won — but it is also text that can be queried and joined. That is the larger consequence.

### Visualization (canvas `c1`, 720×320)

Schematic illustration: a browser window with a pulsing REC dot and a reconstructed pointer path.

- **Browser frame:** rounded rect (80,20) 560×200 radius 8, fill `#f5f5f5`, stroke `#6b7280` width 1.5; toolbar `#e8e8e8` (top corners rounded 8) height 28; traffic lights radius 5 at x offsets 16/32/48, colored `#d55181`, `#d95926`, `#008300`; white URL bar rounded rect with 12px `#6b7280` text "https://shopping-site.com/checkout".
- **Recording dot:** magenta `#d55181` circle radius 6 near the toolbar's right edge with a pulse ring (radius 10, 0.4 alpha) and white bold 13px "REC" label.
- **Cursor path:** 15 points from (140,80) to (560,165) — `[140,80],[180,95],[220,90],[260,110],[300,105],[340,130],[360,125],[380,140],[420,135],[440,150],[460,145],[490,160],[520,155],[540,170],[560,165]` — connected by a `rgba(42,120,214,0.35)` line width 2, with 2.5px dots whose `rgba(26,82,118, α)` opacity ramps from 0.2 to 1.0 along the path; blue filled cursor triangle at the end.
- **Page placeholders:** gray `#e5e9ef` blocks inside the frame at (+20,+60) 120×12, (+20,+80) 180×8, (+20,+95) 150×8, (+20,+140) 100×30, (+140,+140) 100×30.
- **Caption (bold 13px `#d55181`, bottom center):** "Pointer path reconstructed from event samples".

## What does it collect?

- **Full DOM snapshot** at load, including rendered text
- **DOM mutations** thereafter, each with a timestamp
- **Pointer positions**, clicks and scroll offsets
- **Sampling:** pointer moves are typically throttled and batched — a replayed cursor path is interpolation between timestamped samples, not a continuous trace (rrweb exposes per-channel `sampling` options)
- **Input values**, subject to the masking configuration — usually each intermediate value, so text typed and then deleted can appear (rrweb's `sampling.input: 'last'` keeps only final values)
- **Viewport size**, user agent and page URL
- **Derived signals** such as rage clicks and dead clicks

**Key point callout:** **The two `source: 5` events are the story:** both are ordinary text inputs. One was declared sensitive, one was not, and the recorder treated them accordingly.

**Key point callout:** **Masking coverage is a property of the deployment,** not of the tool — it is a configuration list, and the list must be maintained as the form changes.

**Key point callout:** **Structured and queryable:** a value captured this way can be searched for and joined later, which footage never could.

### Visualization (canvas `c2`, 720×320)

Heatmap over a page mockup: attention/click hotspots with a cold-to-hot gradient legend and click markers.

- **Page mockup:** rect (110,15) 500×210, fill `#fafafa`, stroke `#e5e9ef`; gray `#e8e8e8` elements — header (+10,+10) 480×20, sidebar (+10,+40) 80×150, three text lines at (+100,+40/+55/+70); blue `#2a78d6` rounded button (+200,+150) 120×35 radius 6 with white 13px label "BUY NOW".
- **Hotspots (radial gradients fading to transparent; center x/y relative to mockup origin, radius, intensity):** buy button (+260,+167) r=50 intensity 1.0 (hottest — magenta core `#d55181` blending to orange `rgba(230,126,34,…)`); header (+300,+20) r=30 0.7; content (+200,+55) r=35 0.5; (+400,+100) r=25 0.4; sidebar (+150,+130) r=20 0.3 (cooler spots use orange core blending to blue `rgba(26,82,118,…)`); alpha = intensity × 0.5.
- **Legend (left of mockup):** 12px `#6b7280` labels "Cold" and "Hot" beside a 12×40 vertical gradient bar from `rgba(42,120,214,0.3)` through `rgba(217,89,38,0.5)` to translucent magenta.
- **Click markers:** magenta `#d55181` stroked circles radius 6 (width 1.5) — four clustered on the BUY NOW button, one at the header hotspot, one in the content area.

**Payload note (below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, verbatim):**

```
// rrweb event stream.
// ── documented / standard ──
// type 2 = FullSnapshot, 3 = IncrementalSnapshot, 4 = Meta.
// Within type 3, data.source: 0 Mutation, 1 MouseMove,
// 2 MouseInteraction, 3 Scroll, 5 Input. timestamp = epoch ms.
[
  { "type": 4, "timestamp": 1755828843000,
    "data": { "href": "https://shop.example/checkout",
              "width": 1440, "height": 900 } },

  { "type": 2, "timestamp": 1755828843118,
    "data": { "node": { "id": 1, "childNodes": […] } } },

  { "type": 3, "timestamp": 1755828847512,
    "data": { "source": 1, "positions": [
      { "x": 612, "y": 388, "id": 214, "timeOffset": -412 },
      { "x": 651, "y": 404, "id": 231, "timeOffset": -98 } ] } },

  // masked field: maskInputOptions / maskAllInputs
  { "type": 3, "timestamp": 1755828851004,
    "data": { "source": 5, "id": 287, "text": "***" } },

  // NOT in the mask list — recorded verbatim
  { "type": 3, "timestamp": 1755828853440,
    "data": { "source": 5, "id": 291,
              "text": "penicillin allergy" } },

  // ── inferred / plausible ── vendor-side, not rrweb
  { "type": 5, "timestamp": 1755828855180,
    "data": { "tag": "rage_click",
              "payload": { "id": 304, "count": 4 } } }
]
```

## Why is it collected?

**Label (purpose pill):** Stated purpose

- **Localising failures** aggregates cannot — a funnel says a step loses users, not that a validation error renders below the fold
- **Reproducing client-side bugs** that never reached a server log

**Label (effect pill):** Additional consequence

- **Conversion optimisation** — finding the point of hesitation
- The log holds **rendered text**, so it can carry account details or form values, and a retention window sized for bug triage is a different question from that one

**Key point callout:** **Selection effect in which sessions get watched:** they are surfaced by a filter — errored, abandoned, rage-clicked. Generalising from them overstates whatever the filter selected on. A replay localises a mechanism; sizing it needs the aggregate the filter was applied to.

### Visualization (canvas `c3`, 720×320)

Area-proportional block + comparison bars: one week of sessions vs the slice a team watches (trigger-rule selection effect).

- **Title (bold 14px `#1a5276`, center):** "One week of sessions, and the slice a team watches"; subtitle (12px `#6b7280`): "recording is population-wide; the corpus is whatever the trigger rule surfaced".
- **Data:** trigger fired: 900 sessions, 340 hit the checkout problem (watched); no trigger: 24,100 sessions, 1,900 hit it. Derived: 25,000 total; watched-slice hit rate 38%; overall hit rate 9%; only 15% of problem sessions left a trigger.
- **Blocks:** large rect at (44,64) width canvas−300 × 122, fill `rgba(42,120,214,0.14)` stroke `#2a78d6`, representing all sessions; left slice (width proportional to 900/25,000, min 22px) fill `rgba(217,89,38,0.42)` stroke `#d95926` width 2. Labels: bold 13px blue "25,000 sessions recorded" and 12px `#2c3e50` "every visit produces a mutation log" right of the slice; below the slice, orange bold "900" and "watched".
- **Comparison bars (right column, labeled "Share hitting the" / "checkout problem" in bold 13px ink):** "in the watched slice" 38% orange; "across all sessions" 9% blue. Bars scaled to 40% = 190px, 16px tall, fill at 0.40 alpha with solid stroke; bold 12px value labels at bar ends; 12px `#2c3e50` captions below each.
- **Annotation (12px `#2c3e50`, left-aligned, two lines at y=224/242):** "Only 15% of the sessions that hit the problem left a trigger behind, so the watched slice" / "names the failure but overstates how common it is."
- **Captions (bottom center):** italic 12px `#2c3e50` "A replay tells you what broke. Only the aggregate it was filtered from tells you how often."; italic 11px `#6b7280` "Illustrative counts — the shape, not a measured deployment."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading + `.lede` + bullets/`.key-point` callouts, right `<td>` (55%, `text-align:center`) holds the canvas (and, in the "What does it collect?" row, the `.payload-note` + `.payload` pre below the canvas, left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; li 0.93em with `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `<strong>` in `#1a5276`; `.lede` 0.95em; `.lbl` pills 0.7em bold uppercase — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` background `#f8f9fa`, left border `3px solid #1a5276`, monospace 0.78em, `white-space: pre`; `.payload-note` 0.82em `#666` italic. No nav bar, no back/home links.
- **Palette:** charts declare a tokens object `P = { blue:#2a78d6, green:#008300, magenta:#d55181, yellow:#c98500, aqua:#199e70, violet:#4a3aa7, orange:#d95926, ink:#1a5276, text:#2c3e50, mute:#6b7280, grid:#e5e9ef }`; red is deliberately excluded from the series rotation, reserved for alarm states. Site palette anchors: #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; a shared `setupCanvas(id)` helper reads the element's own attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helpers: `tint(hex,a)` translucent fill and `rr()` rounded-rect path (this page's `rr()` also accepts a `[tl,tr,br,bl]` radius array for the toolbar's top-only rounding).
- No `Math.random()` in charts — all data arrays are hardcoded literals; invented numbers are labeled "illustrative".
