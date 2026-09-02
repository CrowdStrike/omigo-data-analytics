# Structured Signed Tokens

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Structured Signed Tokens

**Subtitle:** The JWT-family design — carry the claims inside the artifact and sign them, so any server holding the verification key can trust the token without ever phoning home.

**Intro callout (blue-left-border box):** **Inversion** (blue lead word): the database row itself is signed into the token, so verification becomes local math — both the design's power and its hardest problem.

## 1. What is it?

The JWT-family token carries its own proof.

- **Self-contained:** subject, expiry, and scopes ride inside the token *(blue)*
- **Signed whole:** the issuer signs the entire package *(blue)*
- **Local check:** any key-holding server verifies integrity itself *(green)*
- **No lookup:** claims are trusted without a database round-trip *(green)*
- **Two promises:** signing and encrypting guarantee different things *(blue)*
- **Signed variant:** tamper-evident but readable by anyone who sees it *(red)*
- **Encrypted variant:** sealed; only decryption-key holders can read *(blue)*

**Key point (red-left-border box):** **Root of trust** (red lead word): the verification key replaces the database — portable across services, awkward for pre-expiry takeback.

### Visualization (canvas `c1`, 720×300)

Two-lane comparison diagram: where the trust comes from in each design.

- **Title (bold 13px `#1a5276`, top center):** "Lookup vs signature: where the trust comes from".
- **Lane 1 (opaque):** label bold 12px `#1a5276` left-aligned at (40, 62): "Opaque token"; boxes 34px tall at y=76 (fill = color at 0.12 alpha, stroke = color width 2, label bold 11px in box color centered):
  | Label | color | x | width |
  |---|---|---|---|
  | Client | #1a5276 | 40 | 110 |
  | Server | #2980b9 | 230 | 110 |
  | Database row | #e67e22 | 420 | 150 |
  `#bbb` width-1.5 arrows with filled heads from (150,93) to (222,93) and from (340,93) to (412,93). Note 11px `#666` left-aligned at (40, 136): "one lookup per request; revoke = delete the row".
- **Lane 2 (signed):** label bold 12px `#27ae60` left-aligned at (40, 172): "Signed token"; boxes 34px tall at y=186 (same style):
  | Label | color | x | width |
  |---|---|---|---|
  | Client | #1a5276 | 40 | 110 |
  | Any server with the verification key | #27ae60 | 230 | 340 |
  `#bbb` arrow from (150,203) to (222,203). Note 11px `#666` left-aligned at (40, 246): "no lookup; trust is local math, valid until expiry".
- **Bottom line (bold 12px `#e67e22`, centered, y=282):** "Signing makes claims tamper-evident but readable; encrypting seals them from view."

## 2. Anatomy of a signed token

- **Header:** declares the algorithm and token type *(blue)*
- **Claims:** the actual facts the token asserts *(blue)*
- **Signature:** computed over the header and claims together *(blue)*
- **Tamper-evident:** change one claim character, verification fails *(green)*
- **Standard claims:** subject, issuer, audience, issue time, expiry *(blue)*
- **Custom claims:** scopes, roles, and tenant identifiers as needed *(blue)*
- **Bake-in:** anything downstream needs is added at issue time *(blue)*
- **Rides along:** every added claim travels on every request *(red)*
- **Bloat habit:** "just put it in the token" inflates overhead *(red)*

**Key point:** **Verifier decides** (red lead word): the signature covers the header too, so the verifier must enforce its own algorithm, never obey the token's.

### Visualization (canvas `c2`, 720×300)

Schematic three-part anatomy diagram — labeled boxes only, no literal token text.

- **Title (bold 13px `#1a5276`, top center):** "Anatomy of a signed token (schematic)".
- **Parts** — three adjacent boxes on one row, y=90, height 90 (fill = part color at 0.12 alpha, stroke = part color width 2; label bold 13px in part color centered at y=116; inner lines 10px `#666` centered, first at y=136, 16px apart):
  | Label | color | x | width | inner lines |
  |---|---|---|---|---|
  | header | #8e44ad | 50 | 150 | algorithm; token type |
  | claims | #1a5276 | 220 | 300 | subject · expiry · scopes; issuer · audience; custom claims |
  | signature | #27ae60 | 540 | 130 | over header + claims |
- **Annotation below claims:** `#bbb` width-1.5 line from (370, 214) up to (370, 188), with a small filled arrowhead at the top (370, 184) pointing up into the box; two centered 11px `#2c3e50` lines at (370, 232) and (370, 246): "every added claim rides along on every request —" / "size grows with claims".
- **Caption (bottom center, 11px `#999`, y=285):** "Schematic labels only — no literal token text is shown."

## 3. Pros / cons

- **Stateless:** local cryptography validates each token *(green)*
- **No round-trip:** the per-request database hit disappears *(green)*
- **No dependency:** auth stops relying on database availability *(green)*
- **Portable:** every key-holding service trusts one token *(green)*
- **Right fit:** exactly the shape service-to-service auth needs *(green)*
- **Size:** the token is large and grows with every claim *(red)*
- **Leak surface:** signed-only claims are readable to any viewer *(red)*
- **Exposure paths:** logs, URLs, and proxies all see every claim *(red)*
- **Algorithm confusion:** a threat-model-level verifier failure *(red)*
- **The trap:** obeying the header instead of a fixed expectation *(red)*
- **The steer:** verification weaker than the issuer intended *(red)*
- **Revocation gap:** valid until expiry, no matter what happens *(red)*
- **Lever 1:** short lifetimes shrink the damage window *(blue)*
- **Lever 2:** a denylist consulted on every request *(blue)*
- **The irony:** the denylist quietly reinstates the lookup *(red)*

### Visualization (canvas `c3`, 720×300)

Two-timeline diagram: the revocation gap of a signed token vs an opaque token.

- **Title (bold 13px `#1a5276`, top center):** "The revocation gap after a leak (illustrative)".
- **Signed lane:** label bold 12px `#1a5276` left-aligned at (60, 66): "Signed token"; axis line `#999` width 1.5 from (60, 110) to (660, 110); tick marks 5px above/below the axis at x=120, x=320, x=620; tick labels 11px centered at y=130: "issued" (`#27ae60`, x=120), "leak discovered" (`#e74c3c`, x=320), "expiry" (`#666`, x=620); shaded rect (320, 96, 300, 28) fill `#e74c3c` at 0.12 alpha; bold 11px `#e74c3c` centered at (470, 88): "accepted everywhere until expiry".
- **Opaque lane:** label bold 12px `#1a5276` left-aligned at (60, 180): "Opaque token"; axis solid `#999` width 1.5 from (60, 225) to (340, 225), then dashed (`setLineDash([5,4])`) `#ccc` from (340, 225) to (660, 225); ticks at x=120 and x=320 with labels 11px at y=245: "issued" (`#27ae60`), "leak discovered" (`#e74c3c`); bold width-2.5 `#27ae60` tick from (340, 215) to (340, 235); bold 11px `#27ae60` left-aligned at (352, 205): "row deleted → dead instantly".
- **Caption (bottom center, 11px `#999`, y=285):** "A denylist can close the gap, but it reinstates the per-request lookup the design set out to avoid."

## 4. Prevalence: current vs historical

- **Dominant in:** service-to-service auth and OAuth ecosystems *(blue)*
- **Why:** one issuer trusted by many independent verifiers *(blue)*
- **Opaque wins:** developer API keys and personal access tokens *(blue)*
- **Long-lived:** such credentials prize central control *(orange)*
- **The trade:** instant revocation beats saving a lookup *(blue)*
- **Early era:** stateless-everything looked like the endgame *(orange)*
- **Push-back:** the revocation problem drove practice to hybrids *(orange)*
- **Hybrid shape:** a short-lived signed access token *(blue)*
- **Paired with:** a long-lived, revocable refresh token *(blue)*

**Key point:** **Quiet admission** (orange lead word): neither pure design suffices — signed buys minutes of lookup-free checks, opaque keeps the issuer's kill switch.

### Visualization (canvas `c4`, 720×300)

Stacked horizontal bars: signed vs opaque share by context.

- **Title (bold 13px `#1a5276`, top center):** "Where each design dominates today (illustrative)".
- **Legend (y=40):** 12×12 swatch at x=210 fill `#1a5276` at 0.6 alpha with 1px `#1a5276` stroke, label 11px `#2c3e50` left-aligned at x=228: "signed (self-contained)"; 12×12 swatch at x=400 fill `#27ae60` at 0.6 alpha with 1px `#27ae60` stroke, label at x=418: "opaque (lookup)".
- **Bars:** 22px tall, 12px gap, starting y=60; labels right-aligned 11px `#2c3e50` ending at x=200; full track 400px wide starting at x=212; left segment = signed share (fill `#1a5276` at 0.6 alpha, 1px `#1a5276` stroke), right segment = opaque share (fill `#27ae60` at 0.6 alpha, 1px `#27ae60` stroke); each segment shows its percentage in bold 10px `#2c3e50` centered inside the segment.
- **Data (label, signed %, opaque %):**
  | Context | signed | opaque |
  |---|---|---|
  | Service-to-service auth | 80 | 20 |
  | OAuth access tokens | 70 | 30 |
  | Web sessions | 40 | 60 |
  | Developer API keys | 15 | 85 |
  | Personal access tokens | 10 | 90 |
- **Caption (bottom center, 11px `#999`, two lines at y=268 and y=284):** "The pendulum settled on hybrids: a short-lived signed access token" / "paired with a revocable refresh token."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for the lead sentence/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one line, no text-wrap — a bold colored label naming the concept plus a phrase of roughly ≤55 characters. Never merge facts back into paragraph bullets; split long content into more labeled bullets. Lead paragraphs are at most one short sentence.
- **Bullet HTML markup:** `<li><span class="pt-label" style="color:#e74c3c">Risk:</span> phrase</li>`, with CSS `.pt-label { font-weight: bold; }`. In markdown the same bullet is `- **Label:** phrase` followed by an italic color tag.
- **Label color scheme (by meaning):** `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. The italic *(blue)*/*(green)*/*(red)*/*(orange)* tag after each md bullet records the label color; drop the tag itself when rendering HTML.
- **Callout/key-point style:** `.intro` and `.key-point` boxes open with a bold `pt-label` lead word (colored by the same scheme) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`; box fills use the box color at 0.12 alpha, bar fills use the series color at 0.6 alpha.
- **Content rule:** token anatomy is always schematic labeled boxes — never render realistic or truncated token strings anywhere on the page; failure modes stay at the threat-model level with no exploit detail.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
