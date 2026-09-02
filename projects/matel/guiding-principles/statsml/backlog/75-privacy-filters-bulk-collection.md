# Privacy Filters on Bulk-Collected Data

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one section per h2)
**HTML title tag:** Privacy Filters on Bulk-Collected Data

**Subtitle:** Street-view cars photograph everyone on the street — then a model blurs faces and plates before publishing. The redaction step, not the collection step, is the privacy control.

**Intro callout:** When collection is indiscriminate (a camera drives past everything), the privacy guarantee is delegated to an automated detector applied afterwards. That detector is a classifier with false negatives — so the privacy promise is really a recall number. The same pattern appears far beyond maps.

## 1. The Maps Case

- **Capture is indiscriminate.** The car photographs whoever happens to be on the street — bystanders never consented and cannot be asked. Consent-at-collection is structurally impossible.
- **Redaction is post-hoc and automated.** Face and license-plate detectors run over billions of images; matches are irreversibly blurred before publishing. Manual review only handles reported misses.
- **Two copies exist.** The published imagery is blurred; the question of what happens to the raw capture (retention, internal access, training use) is a separate policy decision the blur does not answer.
- **Collection can exceed the declared scope.** The same street-view fleet once captured Wi-Fi payload data alongside imagery — the sensor collects more than the product needs.

**Key point:** The privacy control moved from "what do we collect" to "what does the detector catch." That is a fundamentally weaker guarantee: it is probabilistic, it fails silently, and its failure rate is a model metric nobody outside the pipeline can audit.

### Visualization (canvas `c1`, 720×340)

Pipeline diagram: four stage boxes with a raw-archive branch.

- **Title (bold 14px, `#1a5276`, top center):** "The pipeline: privacy is enforced at step 3, not step 1".
- **Stage boxes** (76px tall at y=70, fill = stage color at 12% alpha, 2px stroke, bold 13px title, 11px `#555` sublines):
  - "1. CAPTURE" at (25,70) 150 wide, blue `#1a5276` — "camera drives past" / "everything and everyone".
  - "2. DETECT" at (215,70) 150 wide, orange `#e67e22` — "face + plate model" / "scans every frame".
  - "3. REDACT" at (405,70) 150 wide, red `#e74c3c` — "irreversible blur" / "on every match".
  - "4. PUBLISH" at (595,70) 105 wide, green `#27ae60` — "blurred imagery" / "goes public".
- **Gray `#999` arrows** between consecutive stages at y=108.
- **Raw archive branch:** purple `#8e44ad` box "RAW ARCHIVE" at (215,210), 150×66 — sublines "unblurred originals —" / "governed separately"; purple arrow from (100,146) to (240,212); purple 11px left-aligned text at (385,235)/(385,250): "retention? internal access?" / "training use? breach surface?".
- **Bottom text:** bold 12px red `#e74c3c` centered: "Whatever the detector misses at step 2 ships at step 4 — silently". Then 12px `#999`: "Consent at collection is impossible for bystanders; the filter is the only control they get".

## 2. Redaction Is a Classifier — With Asymmetric Costs

- **False negative = published PII.** A missed face is a privacy breach that ships to the whole internet. Cost is high, borne by the bystander, and discovered only if someone reports it.
- **False positive = lost utility.** An over-eager blur hits a storefront sign, a mural, a house number. Cost is low and borne by the product.
- **So the operating point must sit at high recall** — accept over-blurring to minimize misses. The threshold choice *is* the privacy policy, expressed as a number on a PR curve.
- **Detector misses are not uniform.** Face detectors historically perform worse on darker skin, occlusions, children, unusual angles — so the residual privacy risk is demographically skewed. The filter protects some groups better than others.
- **Blur is not deletion.** Gaussian blur can be partially inverted or defeated by super-resolution; and a blurred face leaves gait, clothing, body, companions, and location intact — often enough to identify someone who knows the context.

### Visualization (canvas `c2`, 720×340)

Two error-cost boxes above a precision-recall threshold axis with an operating-point marker.

- **Title (bold 14px, `#1a5276`, top center):** "The two errors are not symmetric — so the threshold must favor recall".
- **Error boxes** (290×96 at y=55, 12%-alpha fill, 2px stroke, bold 13px title, 11px `#555` sublines):
  - "FALSE NEGATIVE — missed face" at x=45, red `#e74c3c` — "published to the internet" / "harm falls on the bystander" / "discovered only if reported".
  - "FALSE POSITIVE — over-blur" at x=385, orange `#e67e22` — "a storefront sign gets blurred" / "harm falls on the product" / "visible, cheap, fixable".
- **Threshold axis:** horizontal 2px `#2c3e50` line from x=80 to x=640 at y=235. Left end labels (11px `#555`): "high precision" / "(blur only sure faces)". Right end labels: "high recall" / "(blur anything face-like)".
- **Operating point:** green `#27ae60` filled circle (r=7) at 82% along the axis; bold 12px label above: "operating point"; 11px `#555` below that: "accept over-blurring to minimize misses".
- **Bottom text:** bold 12px `#1a5276` centered: "The threshold choice IS the privacy policy — a number on a precision-recall curve". Then 12px `#999`: "And detector misses skew by demographics, pose, and occlusion — residual risk is not uniform".

## 3. The Same Pattern in Other Domains

Table (ex-table style: header row `#1a5276` background, white text):

| Domain | Bulk capture | Automated redaction |
|--------|--------------|---------------------|
| Street-view maps | 360° imagery of public streets | Face and plate blur |
| Autonomous vehicles | Camera/lidar training footage | Face/plate anonymization before labeling or sharing |
| Medical imaging | CT/MRI volumes, scan headers | DICOM de-identification; "defacing" head scans so the face cannot be reconstructed in 3D |
| Call centers | Full call recordings | Auto-pause or DTMF masking while card numbers are spoken (PCI DSS) |
| Session replay | Every user interaction on a page | Input fields masked client-side before upload |
| Crash / telemetry logs | Stack traces, URLs, payloads | PII scrubbers regex/model-matching emails, tokens, names |
| Smart doorbells, dashcams | Continuous video of shared spaces | Privacy zones masking the neighbor's windows |
| Document release (FOIA, courts) | Full records | Automated + manual redaction of names, SSNs |
| Speech datasets | Recorded utterances | Voice conversion / speaker de-identification |

**Key point:** The invariant: **capture broadly → detect a PII class → irreversibly transform → release**. Every row inherits the same failure modes — detector misses, reversible transforms, and a raw copy whose governance is a separate question.

### Visualization (canvas `c3`, 720×340)

Four-stage pattern boxes above a monospace mapping table of domains.

- **Title (bold 14px, `#1a5276`, top center):** "One pattern, many domains — swap the sensor and the PII class".
- **Stage boxes** (60px tall at y=50, 12%-alpha fill, 2px stroke, bold 13px title, 11px `#555` sublines):
  - "CAPTURE BROADLY" at (20,50) 155 wide, blue `#1a5276` — "sensor records" / "more than needed".
  - "DETECT PII CLASS" at (205,50) 155 wide, orange `#e67e22` — "model or rule finds" / "the sensitive part".
  - "TRANSFORM" at (390,50) 155 wide, red `#e74c3c` — "blur, mask, deface," / "bleep, scrub".
  - "RELEASE" at (575,50) 125 wide, green `#27ae60` — "publish, share," / "or retain".
- **Gray `#999` arrows** between consecutive boxes at y=80.
- **Monospace table** (12px Menlo, left-aligned at x=70, starting y=150, 20px row pitch): bold header line "sensor            PII class            transform", then `#555` rows:
  - `street imagery    faces, plates        blur`
  - `head CT / MRI     facial surface       deface the volume`
  - `call recording    card number spoken   pause / bleep`
  - `session replay    form inputs          mask client-side`
  - `crash logs        emails, tokens       scrub / hash`
  - `speech corpus     speaker identity     voice conversion`
- **Bottom caption (12px `#999`, centered):** "Every row inherits the same three risks: detector misses, reversible transforms, an unredacted raw copy".

## 4. Open Questions for the Data Team

- **What is the privacy SLA?** "We blur faces" means "our detector's recall is X% on our eval set." What is X, on whose eval set, and does the eval set look like the deployment population?
- **Where in the pipeline does redaction run?** At the sensor (raw PII never stored), at ingestion, or at publish? Everything upstream of the filter is an unredacted archive with its own access-control and breach surface.
- **Is the transform actually irreversible?** Blur and pixelation are attackable; black-box masking and cropping are not. The choice is a security decision dressed as an aesthetic one.
- **Who audits the misses?** Report-and-fix means discovery is delegated to the people harmed. Is there a sampled human audit of published output, with the miss rate tracked as a first-class metric?
- **Does redaction scope match identifiability?** Faces and plates are the *detectable* identifiers, not the only ones. Gait, tattoos, a parked car at a home address — the filter defines "identifying" as "what my detector finds."

**Key point:** Related backlog threads: anonymization spectrum (56), statistical de-anonymization (57), declared scope vs incidental collection (68). This item is the sensor-data counterpart — anonymization where the PII is embedded in pixels and audio, not in columns.

### Visualization (canvas `c4`, 720×340)

Stick-figure diagram of remaining identifiers plus a transform-strength column.

- **Title (bold 14px, `#1a5276`, top center):** "\"Identifying\" = what the detector finds — identifiability is bigger than that".
- **Figure (centered around 180,175):** stick figure in 2px `#2c3e50` strokes (torso, arms, legs); head is a circle (r=22) filled `rgba(231,76,60,0.25)` with bold 11px red `#e74c3c` label "blurred" inside.
- **Remaining identifier cues** (12px orange `#e67e22` bullets to the right of the figure, at x offset +40, y offsets −5/+25/+55/+85 from center): "gait and build", "clothing, tattoos, bags", "companions nearby", "exact time and place".
- **Right column** — bold 13px `#1a5276` heading "Transform strength" at (460,78), then three entries (bold 12px colored title + 11px `#555` subline, 44px pitch):
  - "blur / pixelate", red `#e74c3c` — "attackable — super-resolution, deconvolution".
  - "black-box mask", orange `#e67e22` — "contents gone; box location still leaks".
  - "crop / drop frame", green `#27ae60` — "strongest — nothing to attack".
- **Bottom text:** bold 12px red `#e74c3c` centered: "A neighbor recognizes the blurred figure in front of their own house instantly". Then 12px `#999`: "The filter removes the machine-detectable identifier, not identifiability itself".

## Regeneration instructions

- **Template/layout:** backlog detail page, kusto-style 2-column layout. Each section is a `.lang-section` with an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout`: left `td.text-col` 45% (bullets, ex-table, key-point callouts), right `td.viz-col` 55% (one canvas).
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro`: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point`: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `ul` 0.92rem. `.ex-table`: full width, 0.88em; th background `#1a5276` white text; td `1px solid #ddd`; even rows `#f8f9fa`.
- **Canvas:** intrinsic 720×340 each, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates). Shared `arrow()` and `box()` helpers draw arrows with triangular heads and boxes (12%-alpha fill, 2px stroke, bold title, gray sublines).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, grays `#555`/`#666`/`#999`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
