# Tracking Data: Cross-Device Tracking

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Tracking Data: Cross-Device Tracking

**Subtitle:** Device and browser identifiers are clustered into one node. Some edges come from a shared login; the rest are statistical inferences from shared network and timing, and carry an error rate.

## What is it?

An identity graph that collapses many identifiers into one cluster.

- **Nodes:** a mobile advertising ID, a cookie, a hashed email, a fingerprint
- **Edges** assert two identifiers belong to one person
- **Deterministic edge** — an observation: the same account signed in on both devices
- **Probabilistic edge** — a model output: same IP at compatible times, scored by a classifier
- **Clustering** collapses connected identifiers into one cluster with its own id

**A model, used as a key:** entity resolution has precision and recall like any classifier. Downstream metrics treat its output as a fact, which converts a model's error rate into measurement error in reach, frequency, and unique users.

### Visualization (canvas `c1`, 720×320)

Network (hub-and-spoke) diagram: six devices connected by dashed edges to a central profile node.

- **Title (bold 16px, centered, blue `#2a78d6`, y=18):** "All your devices → one identity graph".
- **Center node:** magenta `#d55181` circle (radius 30) at (360, ~170), white bold 14px two-line label "YOUR" / "PROFILE".
- **Edges:** dashed lines (dash 4/3, width 2) in translucent blue `rgba(42,120,214,0.3)` from center to each device on an 85px-radius ring.
- **Devices (drawn as small blue `#2a78d6` icon shapes with white screens, 14px blue labels):** Phone (top, −90°), Laptop (−30°), Tablet (+30°), Smart TV (+90°), Work PC (150°), Speaker (−150°). Icons are simple filled-rect/circle glyphs per device type.
- **Link labels (orange `#d95926`, 13px):** "same login" (upper left), "same WiFi" (upper right), "same IP" (lower right).

## What does it collect?

- **Device and browser identifiers**, one node each
- **Hashed account identifiers** where a login was observed
- **IP addresses**, and how long each identifier was seen on them
- **Timestamps** — used as a co-occurrence feature, not just activity
- **Link type and confidence score** per edge

**Weakest edge sets the cluster:** the last two edges score 0.71 and 0.58. A cluster is only as sound as its weakest accepted edge.

**The threshold is a business choice:** lower it to raise match rates and more households merge; raise it and reach fragments back into device-level counts. It is rarely reported alongside the metrics it produces.

### Visualization (canvas `c2`, 720×320)

Horizontal segmented bar chart: data collected per device type, four color-coded segments per device row.

- **Title (bold 16px, centered, blue `#2a78d6`, y=18):** "Data collected from each device type".
- **Rows (device name right-aligned in bold blue 15px at x=105, rows start y=38, 48px apart, segments 16px tall starting at x=120, widths proportional to value at 0.7 alpha):**
  - Phone: Location 95, Apps 90, Contacts 70, Usage time 85
  - Laptop: Browsing 95, Accounts 80, Files 40, Usage time 75
  - Tablet: Content 85, Apps 70, Location 60, Usage time 65
  - Smart TV: Viewing 95, Voice 50, Network 80, Usage time 90
- **Segment colors (in order):** blue `#2a78d6`, green `#008300`, orange `#d95926`, magenta `#d55181`.
- **Legend (at x=480, y=55, 22px spacing, 12px swatches):** "Location/Browsing", "Apps/Accounts", "Contacts/Content", "Usage time".
- **Bottom note (magenta 14px, centered):** "Joined: one timeline across every screen, assembled from four separate records".

**Payload note (italic gray, below canvas):** Sample payload — illustrative structure, not real captured data.

**Payload block (monospace, preformatted, verbatim):**

```
// ── inferred / plausible — internal cluster schema.
//    Graph vendors do not publish these structures;
//    field names here are illustrative.
{
  "cluster_id": "hh_4f8c21…",
  "as_of":      "2026-08-22",
  "members": [
    { "id": "idfa:7C9A…",    "type": "mobile_ad_id" },
    { "id": "cookie:e1b7…",  "type": "web_cookie"   },
    { "id": "fp:a7f2…",      "type": "fingerprint"  },
    { "id": "ctv:5D31…",     "type": "tv_device_id" }
  ],
  "edges": [
    { "a": "idfa:7C9A…", "b": "cookie:e1b7…",
      "link_type":  "deterministic",
      "signal":     "shared_login_hash",
      "confidence": 1.00 },

    { "a": "cookie:e1b7…", "b": "ctv:5D31…",
      "link_type":  "probabilistic",
      "signals":    ["shared_ip_28d", "temporal_cooccur"],
      "confidence": 0.71 },

    { "a": "fp:a7f2…", "b": "cookie:e1b7…",
      "link_type":  "probabilistic",
      "signals":    ["fp_rejoin_after_clear"],
      "confidence": 0.58 }
  ],
  "cluster_confidence": 0.66   // weakest-edge bound
}
```

## Why is it collected?

**Stated purpose** (label pill, blue)

- **Deduplication and frequency control** — without a cluster, one person counts once per device and no cap on repeat exposure is possible
- **Attribution** — research on a phone, purchase on a laptop: an unlinked model credits the wrong touchpoint

**Additional consequence** (label pill, orange)

- The cluster is also a **join key** — data attached to any member becomes attached to all of them, and a probabilistic edge is enough to carry it across

**A population figure counts linkability:** clusters form only where the vendor's partners observe the identifiers, and the strongest edges need a login it happened to see. So an audience size overstates when linking fails and understates when two people share an address — and nothing downstream separates a correct merge from a wrong one.

### Visualization (canvas `c3`, 720×320)

Bar chart: reported audience size for the same 1,000 people under five linking outcomes. Illustrative counts — 1,000 people × 3 devices = 3,000 identifiers.

- **Title (bold 13px, ink `#1a5276`, centered, y=24):** "Audience size the graph reports, for the same 1,000 people".
- **Subtitle (12px, muted `#6b7280`, centered, y=42):** "each person carries three devices, so there are 3,000 identifiers either way".
- **Scale:** y maps 0–3,200 onto 148px above baseline y=226; bars 66px wide, first bar center x=128, step 118; light gray baseline.
- **Reference line:** dashed violet `#4a3aa7` horizontal line (dash 6/4, width 1.5) at the 1,000 level, labeled bold 12px violet "1,000 people".
- **Bars (label / sublabel → value):** "nothing linked" / "3 clusters each" → 3,000; "some linked" / "no login observed" → 2,100; "most linked" / "a login on two" → 1,400; "all linked" / "a login on each" → 1,010; "over-merged" / "flatmates share IP" → 780. Bars above 1,000 are blue `#2a78d6` (fill `rgba(42,120,214,0.30)`, 1px stroke); the under-count bar (780) is orange `#d95926` (fill `rgba(217,89,38,0.45)`). Bold 12px value labels above bars in the bar hue; bold 12px labels and muted 12px sublabels below the baseline.
- **X-axis caption (muted 12px, centered):** "how much of each person the vendor could link  →".
- **Caption (italic 12px, `#2c3e50`, centered):** "The figure overshoots when linking fails and undershoots when it links two people."
- **Footnote (italic 11px, muted, bottom center):** "Illustrative — the direction of each error, not a measured graph."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width table, `border-collapse: collapse`, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `.lede` paragraph, bullets and `.key-point` callouts; right `<td>` (55%, centered) holds the canvas, plus (row 2 only) the `.payload-note` caption and `.payload` pre block, both left-aligned.
- **Page style:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with bold lead terms `li b` in `#1a5276` weight 600; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em, leading `strong` in `#1a5276`; `.lbl` uppercase pill labels 0.7em bold — `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`; `.payload` monospace 0.78em, background `#f8f9fa`, left border `3px solid #1a5276`, `white-space: pre`; `.payload-note` 0.82em italic `#666`. No nav bar, no back/home links.
- **Canvas:** each canvas declares intrinsic `width`/`height` attributes (720×320); a shared `setupCanvas(id)` helper reads those attributes, sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Helper: `rr(ctx, x, y, w, h, r)` rounded-rect path. All chart data is hardcoded literal arrays — no random values.
- **Palette:** declared once as tokens `P` — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately excluded from the series rotation (reserved for genuine alarm states). Project-wide palette anchors: `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange.
- In regenerated HTML, any card links use `.html` extensions.
