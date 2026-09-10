# Tracking Data: Aggregation Granularity as a Proxy

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas/payload right 55%, one row per section)
**HTML title tag:** Tracking Data: Aggregation Granularity as a Proxy

**Subtitle:** This is not a collection mechanism. It is a choice of how coarsely to aggregate what was already collected — and the coarsest unit that still clears a non-PII bar is often a close proxy for a person.

## Section 1: What is it?

**Lede:** Not a collection mechanism — a dial on how coarsely to aggregate.

- **A choice, not a property** of the data: which unit to aggregate to
- **Coarser is the standard route** to calling data non-personal — a country is unarguable, a postcode defensible
- **A residence still clears the bar** — a household is not a named person — but holds very few people. The label changed; the targeting value barely did
- **Some signals only support this level:** a home IP is shared by everyone on the WiFi, a television has several viewers, a loyalty account is usually issued per household
- **Others are deliberately coarsened** to it, and the data does not record which route it took

**Callout:** **Where the dial settles:** usually at the finest granularity that still clears the non-PII bar — coarse enough to be defensible, fine enough to stay useful. The result approximates a person without being classified as one.

### Visualization (canvas `c1`, 720×300)

Flow diagram: devices collapsing into one household cluster via a shared public IP.

- **Device boxes (left column, x center 90, rounded rects 110×32 r5, blue `#2a78d6` fill at alpha 0.35 with blue stroke 1.5, bold 15px blue labels):** "Phone A" (y 60), "Laptop" (y 130), "Phone B" (y 200), "Smart TV" (y 262); dashed blue connector lines (dash 4/3) from each device to the router node.
- **Router box:** solid orange `#d95926` rounded rect (270,125) 120×60 r6 centered at (330,155); white bold 16px "One public IP" and 14px "203.0.113.42".
- **Arrow:** blue line width 2 with a solid blue arrowhead from the router (x 395) to the cluster box (x ~488).
- **Cluster box:** rounded rect (495,103) 175×104 r8, blue fill alpha 0.15, blue stroke width 2; bold 16px blue "household cluster", 14px blue "hh_8f21c4…", bold 14px magenta `#d55181` "4 people → 1 identity", 14px mute `#6b7280` "individuals not separable".
- **Caption (15px mute, centered, h−12):** "The finest unit the shared identifier supports is the residence".

## Section 2: What does it collect?

- **Shared public IP**, common to every device behind one router
- **Connected TV identifier**, shared by all viewers of that set
- **Loyalty or subscription account**, typically one per household
- **Delivery address**, used to join otherwise separate records
- **Co-occurrence patterns** — which devices appear on the same network at the same times

**Callout:** **The consequential field is `scope`:** an interest observed on one device but attached to the cluster produces the familiar experience — one person searches, another sees the ad. No audio capture is required, and none is implied.

### Visualization (canvas `c2`, 720×300)

Fan-out diagram: a cluster-scoped signal spreading to all household members.

- **Step 1 box:** solid orange `#d95926` rounded rect (30,40) 165×70 r6; white bold 15px "Phone A searches", 14px "\"plumber near me\"" and "09:14".
- **Arrow:** blue `#2a78d6` line width 2 with solid arrowhead from (198,75) to (270,75).
- **Cluster store box:** rounded rect (275,35) 175×80 r6, blue fill alpha 0.2, blue stroke width 2; bold 15px blue "stored on cluster", 14px blue "topic: plumbing", bold 14px magenta `#d55181` "scope: household".
- **Fan-out targets (three rounded rects 320×34 r5 at x=300, dashed connectors dash 4/3 from the store at (362,120)):**
  1. y 175 — "Phone A → plumber ad" with right-aligned note "(the searcher)" — blue styling (fill alpha 0.35, blue stroke/text)
  2. y 220 — "Phone B → plumber ad" with "(never searched)" — magenta styling (magenta tint alpha 0.18, magenta stroke/text)
  3. y 265 — "Smart TV → plumber ad" with "(never searched)" — magenta styling
  Labels bold 15px; notes 14px.
- **Caption (15px mute, centered, h−8):** "Cluster-scoped signals reach members who produced no signal".

**Payload note (right column, below canvas):** Sample payload — illustrative structure, not real captured data.

```
// A household cluster as an ad platform might hold it.
{
  "cluster_id": "hh_8f21c4…",
  "cluster_type": "household",

  // ── documented as a targeting capability ──
  // IP-based and address-based household
  // targeting are openly sold products.
  "resolved_by": "shared_public_ip",
  "postal_code": "78704",
  "members": [
    { "device_id": "d_a19…", "type": "mobile"  },
    { "device_id": "d_b77…", "type": "desktop" },
    { "device_id": "d_c02…", "type": "ctv"     }
  ],

  // ── inferred / plausible ──
  // Internal scoring schemas are not published.
  "member_count_est": 3,
  "link_confidence": 0.71,
  "interest_signals": [
    { "topic": "home_services/plumbing",
      "observed_on": "d_a19…",
      "ts": "2026-08-22T09:14:00Z",
      "scope": "cluster" }   // ← applied to all members
  ]
}
```

## Section 3: Why is it collected?

**Label (purpose pill):** Stated purpose

- **Reach the decision-making unit** rather than one device
- **Frequency capping** across a home — a normal media-planning requirement

**Label (effect pill):** Additional consequence

- Individual behaviour becomes **partially visible to co-residents** through the ads they are shown
- A **side effect of the aggregation level**, not a separate mechanism

**Callout:** **Same decision, both outcomes:** coarse identity is sometimes chosen for privacy reasons — a household is less identifying than a person — and it produces this leakage as a direct result.

**Callout:** **How many people a record could refer to:** a group of thousands protects its members; a residence gives a group the size of the household, usually single digits. In a two-adult home that narrows any individual claim to one of two — often less, because members differ in age and interests enough to break the tie.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: precision vs identifiability tradeoff across five aggregation levels. Schematic values.

- **Chart area:** x=85, y=40, width 545, height 190; blue `#2a78d6` L-axes, line width 1.5.
- **Data (per level, identifiability / co-resident leakage as fractions of chart height):** City 0.10/0.05; Postcode 0.28/0.16; Household 0.55/0.82; Device 0.80/0.30; Person 0.97/0.08.
- **Bars:** width 42, two per level (identifiability left, leakage right), evenly spaced (gap = chartW/5). Identifiability bars: blue fill `rgba(42,120,214,0.35)` with blue stroke. Leakage bars: orange `#d95926` at globalAlpha 0.4, except Household which is magenta `#d55181` at globalAlpha 0.6 with magenta stroke.
- **X labels (13px, centered under each group):** City, Postcode, Household, Device, Person — Household bold and magenta, others blue.
- **Legend (top-left inside chart):** blue swatch `rgba(42,120,214,0.5)` with 14px blue label "How identifying the level is"; magenta-tinted swatch (alpha 0.5) with 14px magenta label "Leakage to co-residents".
- **Caption (15px mute, centered, h−10):** "Schematic. Household is least identifying yet leaks most between people."

## Regeneration instructions

- **Layout:** tracking-page `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` heading, optional `.lede` paragraph, bullets with bolded lead terms (`li b` in `#1a5276`), `.lbl` purpose/effect pills, and `.key-point` callouts; right `<td>` (55%, `text-align: center`) holds the canvas, and in the "What does it collect?" row also the `.payload-note` caption plus `.payload` `<pre>` block (both left-aligned).
- **Page CSS:** body system sans-serif, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cell borders `1px solid #2980b9`, padding 16px; `.obj-title` bold 1.1em `#1a5276`; li 0.93em. `.key-point` and `.payload`: background `#f8f9fa`, left border `3px solid #1a5276`; `.payload` ui-monospace 0.78em pre, `.payload-note` 0.82em italic `#666`. `.lbl` pills: uppercase 0.7em bold, `.lbl-purpose` background `#eaf2fb` color `#1a5276`, `.lbl-effect` background `#fdf0e6` color `#a8501c`. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart (720×300); scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id)` helper that reads the element's own width/height attributes. Charts share a rounded-rect path helper (`rr`) and a `tint(hex, alpha)` helper producing rgba fills from palette tokens.
- **Palette:** this page's charts use the tracking categorical palette — blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`; ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Red is deliberately not in the rotation (reserved for alarm states). Site-wide accents remain #1a5276 primary blue, #27ae60 green, #e74c3c red, #e67e22 orange.
- In regenerated HTML, any card links use `.html` extensions (this page has none).
