# Bluetooth Exposure Notification — A Join Without a Database

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one h2 + table per aspect, plus philosophy callouts and a summary table)
**HTML title tag:** Bluetooth Exposure Notification — A Join Without a Database

**Subtitle:** During COVID, phones answered the question "was I recently near someone who has now tested positive?" — with no location data, no names, and no server that ever learns who met whom.

## Callout (philosophy box, top)

**The question:** Contact tracing needs a dataset that has never existed — who stood near whom, for how long, going back two weeks. Interviews recover a fraction of it and can't name the stranger on the bus. How do you build that table without building a surveillance system?

**The answer:** You don't build the table. The Apple/Google Exposure Notification system (public spec, 2020) leaves every observation on the phone that made it and ships the *query* — the diagnosis keys — to every handset instead. The join runs everywhere except the server. That inversion, and the two measurement problems it drags in, make this one of the most interesting deployed data systems of the decade.

## 1. The Mechanism: Ship the Query, Not the Data

**Obj-title:** The Inverted Join

Every participating phone broadcasts a random identifier over Bluetooth Low Energy and logs the identifiers it hears nearby, with signal strength and time. The identifier is derived from a daily key and rotates roughly every 15 minutes, so a listener cannot follow one phone across the day.

Math-box:

**When Alice tests positive** (with her consent):
Her phone uploads its recent daily keys — `~14 days`, a few kilobytes.
Every phone downloads the published key list, re-derives the rotating IDs those keys would have produced, and matches them against its **own local log**.

Bob's phone finds a match from last Tuesday, 22 minutes, strong signal → Bob gets a notification. The server never sees Bob, the encounter, or any location.

- **The privacy inversion:** the query moves to the data, not the data to the query
- **What the server holds:** a key list from positive tests — no identities, no sightings, no graph
- **The cost of the inversion:** no table exists to re-query later

### Visualization (canvas `canvas1`, 720×360)

Three-panel flow diagram: Exchange → Upload → Match, showing where the data lives at each stage.

- **Layout:** three panels of width 213 at x = 20, 253, 486, each y=48, height 250, background `#f8fafb`, border `1px solid #e0e0e0`, radius 8. Bold 13px `#1a5276` panel titles centered above each at y=40: "1. Exchange (weeks of normal life)", "2. Upload (on diagnosis)", "3. Match (on every phone)".
- **Panel 1:** two rounded phone rectangles (34×58, radius 6, `#1a5276` stroke, white fill) at (55,90) and (155,90) labeled "Alice" / "Bob" (12px `#333`, centered below). Double-headed dashed arrow between them (`#e67e22`, width 2, dash 5/4) at mid-height, label above in bold orange 11px: "rotating ID" and below "changes ~every 15 min". Under each phone a small log box (70×40, `#eef2f7` fill, `#1a5276` 1px border) labeled "local log" (bold 11px `#1a5276`) with 10px `#666` line "IDs heard + signal + time". Caption at panel bottom (11px `#666`, centered): "nothing leaves either phone".
- **Panel 2:** Alice's phone at left (300,90); a server box (90×54, radius 6, fill `#f0f4f8`, border `2px solid #2980b9`) at right (395,92) labeled "key server" (bold 12px `#1a5276`). Solid green arrow (`#27ae60`, width 2.5) from phone to server labeled bold green 11px "daily keys (~14 days)" with 10px `#666` line under it "consent required". Caption at panel bottom (11px `#666`, centered, two lines): "the server learns: these keys tested positive" / "not who, not where, not who they met".
- **Panel 3:** server box top center (555,70, same style, 90×44); three phone rectangles (26×44) in a row at y=170, x = 510, 555, 600, middle one labeled "Bob" (11px). Three solid blue arrows (`#2980b9`, width 2) fanning from server bottom to each phone, shared label bold blue 11px centered between: "key list → all phones". Under Bob's phone a bold red 11px starburst label "match: Tue, 22 min, strong signal" and green 11px "→ notification, computed locally".
- **Bottom caption (bold 12px `#1a5276`, centered, y=330):** "The edge Alice–Bob exists in exactly one place: Bob's phone."
- **Title (bold 14px `#1a5276`, top center, y=20):** "The Join Runs Everywhere Except the Server".

## 2. The Measurement Problem: Radio Loss Is Not Distance

**Obj-title:** A Proxy Wearing a Costume

Epidemiological risk was commonly framed as "within 2 meters for 15+ minutes." Bluetooth cannot measure meters. It measures received signal strength, and the gap between transmit power and received power — attenuation — is the only distance proxy available.

Math-box:

**The estimator:** `attenuation ≈ tx_power − rssi`, then thresholds decide "near" vs "far".

But attenuation measures the **radio path**, not the geometric one:
A phone in a back pocket with a body in the way at `1.5 m` can attenuate more than line-of-sight at `8 m`.
Independent measurement studies on trams and buses found metal walls reflect the signal so strongly that attenuation barely tracked seat distance at all.

- **Per-device calibration:** every phone model is a different sensor — offsets published per model
- **Thresholds are policy:** health authorities set the buckets; each country tuned the trade-off
- **The lesson:** the error is structured by pockets, bodies and walls — not random noise

### Visualization (canvas `canvas2`, 720×360)

Scatter plot: attenuation vs true distance, with a decision threshold and two annotated misclassifications.

- **Layout:** origin at (70, 300), plot width 590, plot height 240. Axes `#1a5276`, width 2. X: true distance 0–12 m, ticks at 0/2/4/6/8/10/12 with light `#eee` gridlines; y: attenuation 40–90 dB, ticks 40–90 step 10, gray `#666` labels.
- **Axis labels (13px `#1a5276`):** x: "True distance between people (m)"; y (rotated): "Attenuation (dB) — what the phone sees".
- **2 m guideline:** dashed vertical line (`#bbb`, width 1.5, dash 4/4) at x=2 m, 10px `#999` label near the top: "2 m — the epidemiological line".
- **Threshold:** dashed horizontal line (`#1a5276`, width 1.5, dash 6/4) at 63 dB, bold 11px blue label above its right end: "attenuation threshold — below reads as 'near'".
- **Points (literal array, radius 4.5, labeled illustrative):** correctly classified in green `#27ae60` at (0.6,50), (1.0,55), (1.4,52), (1.8,58), (3.0,64), (4.0,73), (4.5,67), (5.5,70), (6.0,80), (6.5,66), (7.5,72), (8.5,69), (9.0,75), (10.0,71), (11.0,78); misclassified in red `#e74c3c` at (1.5,74), (1.9,70) (near but reads far — missed) and (9.5,56), (10.5,58) (far but reads near — false alarm).
- **Annotations (bold 11px red, two lines each, with 1px leader line to the point):** upper left: "1.5 m, phone in pocket," / "body in the path → missed" pointing at (1.5,74); lower right: "10 m across a tram, metal" / "reflections → false alarm" pointing at (9.5,56).
- **Spread note (11px `#999`, near x=3.2 at 46 dB):** "same distance, very different readings — the scatter is the problem".
- **Title (bold 14px `#1a5276`, top center):** "The Phone Measures the Radio Path, Not the Person's Distance".

## 3. The Coverage Problem: Adoption Enters Squared

**Obj-title:** A Network Sensor's Arithmetic

An encounter is recorded only if **both** phones run the system. With adoption p, a random contact edge is observed with probability ≈ p² — and the notification chain multiplies further conditionals on top.

Math-box:

**Coverage of a random contact (independence assumption, illustrative):**

At `p = 20%` adoption: `0.2 × 0.2 = 4%` of contact edges observed
At `p = 40%`: `16%`
At `p = 60%`: `36%`

And a notification also requires the infected person to get tested, receive the result, and consent to upload — each step a factor `< 1` multiplying the chain.

- **Items vs pairs, again:** installs are items, encounters are pairs — birthday-paradox arithmetic
- **Uneven adoption compounds it:** clustered uptake leaves whole subnetworks below the useful threshold
- **Design consequence:** value is superlinear in adoption — hence built into the OS, not an app

### Visualization (canvas `canvas3`, 720×360)

Curve of edges observed vs adoption, with the intuitive diagonal for contrast and three marked points.

- **Layout:** origin at (70, 300), plot width 580, plot height 240. Axes `#1a5276`, width 2. Both axes 0–100%, ticks every 20% in gray `#666`, light `#eee` gridlines.
- **Axis labels (13px `#1a5276`):** x: "Adoption — share of phones participating"; y (rotated): "Share of contact edges observed".
- **Diagonal:** dashed gray line (`#999`, width 1.5, dash 6/4) from origin to (100,100), 11px gray label along it near x=62: "what intuition expects (p)".
- **Curve:** y = p², red `#e74c3c`, width 2.5, 11px bold red label near x=85 below the curve: "what the pair actually needs (p²)".
- **Gap shading:** region between diagonal and curve filled `rgba(231,76,60,0.08)`.
- **Marked points (filled blue `#1a5276` dots radius 5, dashed drop lines dash 3/3, bold 12px blue labels):** (20,4) labeled "20% → 4%"; (40,16) labeled "40% → 16%"; (60,36) labeled "60% → 36%".
- **Note (11px `#999`, two lines, near x=8 at y-value 78):** "and the notification chain multiplies further:" / "tested × result received × consented to upload".
- **Title (bold 14px `#1a5276`, top center):** "Both Phones Must Participate — Coverage Grows as Adoption Squared".

## 4. The Complete Picture

Summary table (`.summary-table`, header row + 6 rows):

| Data concept | Typical analytics system | Exposure notification |
|---|---|---|
| **Where the data lives** | Central warehouse | Only on the handsets that made each observation |
| **The join** | Server joins tables it holds | Every phone joins published keys against its own log |
| **Identifier** | Stable user ID | Random ID rotating ~every 15 minutes |
| **Distance** | Measured or known | Inferred from radio attenuation + per-model calibration |
| **Coverage** | ≈ install base (p) | ≈ adoption squared (p²) — both endpoints needed |
| **Ad-hoc queries later** | Yes — the table exists | No — nothing the protocol didn't anticipate can ever be computed |

## Callout (philosophy box, bottom)

**One sentence:** Exposure notification solved "who was recently near a diagnosed person?" by inverting the join — data stays scattered, the query travels — and its two hard residuals, radio loss as a distance proxy and p² coverage, are measurement problems, not privacy problems.

## Regeneration instructions

- **Layout:** detail page. h1, `.subtitle`, opening `.philosophy` callout, then per aspect: `<h2>N. Title</h2>` (h2 1.4em `#1a5276`, bottom border `2px solid #2980b9`, padding-bottom 8px) followed by a one-row `.obj-table` — left `<td>` (45%) holds `.obj-title`, paragraph, `.math-box`, bullets; right `<td>` (55%, centered) holds the canvas. Section 4 is a `.summary-table`; page closes with a `.philosophy` callout.
- **Page style:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; subtitle `#666` 1.05em; table cell borders `1px solid #e0e0e0`, padding 20px 24px, vertical-align middle; `.obj-title` 1.05em weight 600 `#1a5276`; `strong` `#1a5276`; ul 0.9em `#333`. No nav bar, no back/home links.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em.
- **Math box:** `.math-box` — background `#f8fafb`, border `1px solid #e0e0e0`, radius 6px, padding 16px 20px, 0.9em; `code` background `#eef2f7`, padding 2px 6px, radius 3px.
- **Summary table:** `.summary-table` — 0.9em, th background `#f0f4f8` `#1a5276` padding 10px 14px left-aligned, td padding 10px 14px, borders `1px solid #e0e0e0`.
- **Canvas:** intrinsic 720×360 each; a shared `setupCanvas(id, w, h)` sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Charts hardcode literal data arrays (no Math.random); scatter points labeled illustrative.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, gray text `#666`/`#999`, accent `#2980b9`.
- **Sourcing:** the mechanism (rotating IDs from daily keys, ~14-day key upload on consent, on-device matching, per-model calibration, authority-set attenuation buckets) follows the published Apple/Google Exposure Notification specification; tram/bus reflection findings are from independent published measurement studies; scatter data and coverage percentages are illustrative.
