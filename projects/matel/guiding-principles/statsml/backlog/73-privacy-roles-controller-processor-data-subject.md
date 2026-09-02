# Controller, Processor, Data Subject — Who's Who

**Page type:** detail page (backlog-style two-column layout: text left 45%, canvas right 55%, one section per h2)
**HTML title tag:** Controller, Processor, Data Subject — Who's Who

**Subtitle:** Privacy law assigns every party in a data flow one of three roles

**Intro callout:** The definitions are short; applying them to a real pipeline is where everyone gets it wrong. The litmus test: who decides the *purposes and means* of processing?

## 1. The Three Roles

- **Data subject** — the identifiable person the data is about. Holds the rights: access, correction, deletion, portability, objection.
- **Controller** — the entity that decides *why* the data is processed and *how* (purposes and means). Carries most obligations: lawful basis, notices, responding to rights requests, breach notification.
- **Processor** — the entity that processes data *on the controller's instructions* for the controller's purposes. Cannot use the data for its own ends; bound by a data processing agreement.

**Key point:** The role is assigned per **data flow**, not per company. The same organization is a controller for one flow and a processor for another — which is exactly why examples across domains are needed.

### Visualization (canvas `c1`, 720×340)

Diagram: three role boxes with labeled relationship arrows.

- **Title (bold 14px, `#1a5276`, top center):** "Three roles, three relationships".
- **Role boxes** (170×90, fill = role color at 12% alpha, 2px stroke in role color, bold 14px title, 11px gray `#555` sublines):
  - DATA SUBJECT at (40,130), green `#27ae60`, sublines: "the person" / "holds the rights".
  - CONTROLLER at (280,45), blue `#1a5276`, sublines: "decides purposes" / "and means".
  - PROCESSOR at (510,130), orange `#e67e22`, sublines: "acts on the" / "controller's instructions".
- **Arrows** (2px line with filled arrowhead, 11px gray `#666` label at midpoint):
  - Subject → Controller (155,130)→(300,105), green `#27ae60`, label "provides data, exercises rights".
  - Controller → Processor (380,135)→(560,160), blue `#1a5276`, label "instructs via contract (DPA)".
  - Processor → Subject (545,220)→(200,235), gray `#999`, unlabeled; caption below (centered at 370,262, 11px `#666`): "no direct relationship — the processor answers to the controller, not the subject".
- **Bottom text:** bold 12px `#1a5276` centered: "Litmus test: who decided WHY this data is processed? That entity is the controller." Then 12px `#999`: "Rights requests, notices, and liability all attach to the controller first".

## 2. Who's Who Across Domains

Table (ex-table style: header row `#1a5276` background, white text):

| Scenario | Subject | Controller | Processor |
|----------|---------|------------|-----------|
| Employer runs payroll on a payroll SaaS | Employee | Employer | Payroll SaaS |
| Online store uses a cloud email service for order confirmations | Shopper | Store | Email service |
| Hospital stores patient records in a cloud EHR | Patient | Hospital | EHR vendor |
| School uses an ed-tech platform for grading | Student | School | Ed-tech platform |
| App developer embeds a crash-reporting SDK | App user | App developer | SDK vendor* |
| Bank hires an agency for a marketing campaign | Customer | Bank | Agency |
| Social platform decides its own feed ranking | Platform user | Platform itself | Its cloud host |

**Key point:** *Only while the SDK vendor uses crash data solely for the developer. The moment it pools crash data across apps to improve its own product, it becomes a controller for that use.

### Visualization (canvas `c2`, 720×340)

Diagram: worked payroll example with role boxes plus a domain-mapping list.

- **Title (bold 14px, `#1a5276`, top center):** "Worked example: payroll — the same pattern repeats in every domain".
- **Role boxes** (same style as c1):
  - Employee at (30,60), 150×84, green `#27ae60`, sublines: "subject" / "salary, bank details".
  - Employer at (250,60), 160×84, blue `#1a5276`, sublines: "controller" / "decides: pay staff".
  - Payroll SaaS at (480,60), 160×84, orange `#e67e22`, sublines: "processor" / "computes as told".
  - Cloud host at (100,215), 160×74, purple `#8e44ad`, sublines: "sub-processor" / "stores as told".
- **Arrows:** Employee→Employer green labeled "data"; Employer→Payroll SaaS blue labeled "DPA"; Payroll SaaS→Cloud host orange (unlabeled), with two 11px `#666` left-aligned lines at (350,200) and (350,214): "sub-processing," / "controller-authorized".
- **Domain mapping (right side):** bold 12px `#2c3e50` heading at (430,195): "Swap the labels, keep the structure:". Then 12px Menlo monospace `#555` rows, 20px spacing, starting at (430,217):
  - `patient   → hospital  → EHR vendor`
  - `shopper   → store     → email service`
  - `student   → school    → ed-tech platform`
  - `customer  → bank      → marketing agency`
- **Bottom caption (12px `#999`, centered):** "The role structure is invariant across domains — only the entity names change".

## 3. The Tricky Cases

- **Role switching:** a cloud analytics vendor is a processor for client event data, but a controller for its own employee records and its own product telemetry.
- **Joint controllers:** two entities decide purposes together — a brand and a platform co-running a fan page, or two hospitals running a shared study. Both carry controller obligations.
- **Sub-processors:** the payroll SaaS hosts on a cloud provider — the cloud provider is a sub-processor, needing the controller's authorization down the chain.
- **Processor creep:** a processor that starts using client data for its own purposes (training models, benchmarking, cross-client analytics) silently becomes a controller for that use — with obligations nobody set up.
- **Ad tech:** a publisher embedding a tracking pixel is often a *joint* controller with the ad network, not a mere bystander — courts have ruled embedding alone makes you responsible for the collection.

### Visualization (canvas `c3`, 720×340)

Diagram: one central company box with three data flows, each labeled with a different role.

- **Title (bold 14px, `#1a5276`, top center):** "One company, different hats per data flow".
- **Central box:** "Analytics SaaS" at (275,115), 170×100, dark `#2c3e50`, sublines: "one legal entity," / "three data flows".
- **Flow 1 (left top):** bold 13px orange `#e67e22` label "PROCESSOR" at (120,80); 11px `#555` sublines "client's event data," / "client's purposes"; orange arrow (175,100)→(285,130).
- **Flow 2 (left bottom):** bold 13px blue `#1a5276` label "CONTROLLER" at (120,240); sublines "its own HR records," / "its own purposes"; blue arrow (180,245)→(285,200).
- **Flow 3 (right top):** bold 13px red `#e74c3c` label "CONTROLLER (creep)" at (590,80); sublines "pooling client data to" / "improve its own product"; red arrow (440,130)→(530,100).
- **Bottom text:** bold 12px red `#e74c3c` centered: "The third flow is where teams slip: new purpose, new role, obligations nobody set up". Then 12px `#999`: "Ask the litmus question per flow, not per company: who decided the purpose of THIS processing?"

## 4. Why the Distinction Matters for Data Teams

- **What you may do with the data depends on the role.** A processor analyzing client data for its own product insight has left its lane — that is a new purpose requiring a new legal basis.
- **Rights requests route to the controller.** A deletion request to the processor gets forwarded, not fulfilled; pipelines must support deletion propagation controller → processor → sub-processor.
- **Aggregation changes the answer.** "We only compute anonymous aggregates across clients" is a controller decision about purpose — someone decided that, and it wasn't the client.

**Key point:** The recurring failure: teams reason "we're just the vendor, the client owns the data" while running cross-client models on that data. Ownership language hides the role question. Ask instead: for this specific use, who decided the purpose? That entity is the controller — with everything that follows.

### Visualization (canvas `c4`, 720×340)

Decision-flow diagram: three question boxes on the left, each with a "yes" arrow to a role box on the right, "no" arrows cascading down.

- **Title (bold 14px, `#1a5276`, top center):** "Which role am I? — decide per data flow".
- **Question boxes** (230×46 at x=40, white fill, `#2980b9` 1.5px stroke, 12px `#2c3e50` centered two-line text):
  - At y=50: "Is the data about you\n(an identifiable person)?"
  - At y=140: "Did you decide the purpose\nor the essential means?"
  - At y=230: "Do you process it only on\nsomeone else's instructions?"
- **Answer boxes** (200×46 at x=430, role color at 12% alpha fill, 2px role-color stroke, bold 12px role-color text):
  - "DATA SUBJECT" at y=50, green `#27ae60`.
  - "CONTROLLER\n(joint, if decided together)" at y=140, blue `#1a5276`.
  - "PROCESSOR" at y=230, orange `#e67e22`.
- **Arrows:** three horizontal "yes" arrows (270→430 at y 73/163/253) in green/blue/orange with gray "yes" labels; two vertical gray `#999` "no" arrows down the left column (155,96)→(155,140) and (155,186)→(155,230).
- **Bottom captions (12px `#999`, centered):** "\"No\" to all three for a given flow usually means you are a third-party recipient — a separate role with its own rules" and "Answering \"yes\" to the middle question while calling yourself a processor is the most common misclassification".

## Regeneration instructions

- **Template/layout:** backlog detail page, kusto-style 2-column layout. Each section is a `.lang-section` with an `h2` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) and a `table.layout` (border-collapse, 12px cell padding): left `td.text-col` 45% with bullets/tables/key-points, right `td.viz-col` 55% with one canvas.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6. h1 2rem `#1a5276` with 2px `#2980b9` bottom border. `.subtitle` `#666` 0.95rem. `.intro` callout: background `#f0f4f8`, left border `3px solid #2980b9`, padding 8px 12px, 0.9rem. `.key-point` callout: background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem. `ul` 0.92rem. `.ex-table`: full width, 0.88em; th background `#1a5276` white text padding 6px 8px; td 6px 8px padding, `1px solid #ddd` border; even rows `#f8f9fa`.
- **Canvas:** intrinsic 720×340, CSS `width: 100%`, `1px solid #e0e0e0` border, 4px radius; scaled by `window.devicePixelRatio` via a shared `setupCanvas(id)` helper (`ctx.scale` back to logical coordinates). Shared `arrow()` and `roleBox()` helpers draw arrows with triangular heads and role boxes (12%-alpha fill, 2px stroke, bold title, gray sublines).
- **Palette:** `#1a5276` primary blue, `#27ae60` green, `#e74c3c` red, `#e67e22` orange, `#8e44ad` purple, grays `#555`/`#666`/`#999`.
- No nav bar, no back/home links. In regenerated HTML, any card links use `.html` extensions.
