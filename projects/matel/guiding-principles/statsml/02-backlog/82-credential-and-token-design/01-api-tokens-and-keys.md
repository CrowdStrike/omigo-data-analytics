# API Tokens & Keys

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** API Tokens & Keys

**Subtitle:** Opaque random tokens as the baseline credential design — a high-entropy string that means nothing by itself and everything as a database key.

**Intro callout (blue-left-border box):** An opaque API token is deliberately meaningless: all of its power lives in a server-side row it merely indexes. That buys instant revocation and central control; it costs a lookup on every request.

## 1. What is it?

The baseline design for API credentials is an opaque random token.

- **Design:** a high-entropy string with no structure or claims.
- **Fact:** the token is only a key into the issuer's database.
- **Fact:** owner, scopes, and expiry live in the server row.
- **Design:** the server stores only a hash, as with passwords.
- **Mechanism:** each request: hash the string, look up the row.

**Key point (red-left-border box):** **Win:** a database leak yields no usable tokens — the plaintext exists only in the client's hands.

### Visualization (canvas `c1`, 720×300)

Three-box request-flow diagram showing that the token is only a database key.

- **Title (bold 13px `#1a5276`, top center):** "Opaque token: the string is only a database key".
- **Boxes** 140×70 at y=100 (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color, centered at box y+30; sub-line 10px `#666` centered at box y+50):
  | Title | sub-line | color | x |
  |---|---|---|---|
  | Client | holds the secret string | #1a5276 | 40 |
  | Server | hashes what it receives | #2980b9 | 290 |
  | Token store | row: owner, scopes, expiry | #27ae60 | 540 |
- **Arrows:** `#bbb` width-1.5 horizontal arrows with filled triangular heads, from (180,135) to (282,135) and from (430,135) to (532,135); above each arrow, centered 11px `#666` labels at y=90: "1. presents token" (x=231) and "2. hash + lookup" (x=481).
- **Bottom line (bold 12px `#e67e22`, centered, y=250):** "The token itself means nothing — every fact about it lives in the database row."
- **Caption (bottom center, 11px `#999`, y=285):** "Revocation = delete the row. The string in the wild instantly stops working."

## 2. Anatomy of a modern token

- **Prefix:** a short opener naming issuer and token class.
- **Win:** scanners spot leaked tokens in public code.
- **Win:** the issuer can auto-revoke what scanners find.
- **Fact:** the prefix serves operations, not security.
- **Core:** the only secret part — a long random string.
- **Design:** entropy = length × alphabet size.
- **Design:** hex packs 4 bits per char; base62/base64url ≈ 6.
- **Design:** bigger alphabet, shorter string, same entropy.
- **Design:** encoding trades URL/copy/log safety vs length.
- **Checksum:** rejects typos and corruption offline.
- **Win:** no API call or failure log burned on a typo.

**Key point:** **Fact:** all security lives in the random core — prefix and checksum hold zero secret material.

### Visualization (canvas `c2`, 720×300)

Schematic labeled-segment diagram of a modern token — labeled boxes only, no literal token text.

- **Title (bold 13px `#1a5276`, top center):** "Anatomy of a modern opaque token (schematic)".
- **Segments** — three adjacent boxes on one row, y=110, height 60 (fill = segment color at 0.12 alpha, stroke = segment color width 2; label bold 13px in segment color centered at y=138; sub-line 10px `#666` centered at y=156):
  | Label | sub-line | color | x | width |
  |---|---|---|---|---|
  | prefix | issuer-identifiable | #e67e22 | 60 | 140 |
  | random core (base62) | 128+ bits of entropy | #1a5276 | 200 | 340 |
  | checksum | offline validity check | #27ae60 | 540 | 120 |
- **Annotation above prefix:** two centered 11px `#2c3e50` lines at (130, 52) and (130, 66): "recognizable in code —" / "found by secret scanners"; `#bbb` width-1.5 line from (130, 74) down to (130, 106) with a small filled arrowhead at the bottom.
- **Annotation below core:** `#bbb` width-1.5 line from (370, 170) down to (370, 210), with a small filled arrowhead at the top (370, 174) pointing up into the segment; two centered 11px `#2c3e50` lines at (370, 228) and (370, 242): "the only secret part —" / "length × alphabet = entropy".
- **Annotation below checksum:** `#bbb` width-1.5 line from (600, 170) down to (600, 210), with a small filled arrowhead at the top (600, 174) pointing up into the segment; two centered 11px `#2c3e50` lines at (600, 228) and (600, 242): "computed from the core —" / "typos rejected offline".
- **Caption (bottom center, 11px `#999`, y=285):** "Schematic only — segment labels stand in for content; no literal token text is shown."

## 3. Pros / cons and prevalence

- **Win:** revoke, rescope, or expire a token via one row edit.
- **Win:** the issuer sees when and how each token is used.
- **Cost:** every call pays a database or cache round-trip.
- **Fact:** signed tokens exist to remove that per-request cost.
- **Prevalence:** dominant for API keys and personal access tokens.
- **Fact:** central control outweighs latency in those uses.
- **History:** early API era — short static keys, no expiry.
- **History:** next came long random tokens hashed at rest.
- **Trend:** prefixed, checksummed, scanner-friendly is standard.

### Visualization (canvas `c3`, 720×300)

Three-era progression diagram with arrows.

- **Title (bold 13px `#1a5276`, top center):** "Three eras of API token design".
- **Boxes** 200×130 at y=60 (fill = era color at 0.12 alpha, stroke = era color width 2; title bold 13px in era color centered at y=84; items 11px `#2c3e50` centered, first at y=108, 20px apart):
  | Title | color | x | items |
  |---|---|---|---|
  | Short static keys | #e74c3c | 30 | low entropy, guessable; no expiry, no scoping; early API era |
  | Long random tokens | #e67e22 | 260 | high entropy; hashed at rest; still anonymous strings |
  | Prefixed + checksummed | #27ae60 | 490 | scanner-friendly prefix; offline checksum; scoped and expiring |
- **Arrows:** `#bbb` width-1.5 arrows with filled heads from (230,125) to (256,125) and from (460,125) to (486,125).
- **Bottom line (bold 12px `#1a5276`, centered, y=235):** "Each step keeps the same core idea — a meaningless random key —"
- **Second bottom line (bold 12px `#1a5276`, centered, y=253):** "and adds operational safety around it."
- **Caption (bottom center, 11px `#999`, y=285):** "The string got longer and more structured, but never started meaning anything."

## 4. Design trade-offs & failure modes

- **Design:** show the plaintext exactly once, at creation.
- **Win:** one-time display lets the issuer store only a hash.
- **Risk:** "view my key again" forces reversible storage.
- **Fact:** the UX choice is really a storage-security choice.
- **Design:** hash tokens at rest exactly like passwords.
- **Risk:** plaintext token tables turn leaks into mass compromise.
- **Design:** narrow scopes cap what a leaked token is worth.
- **Design:** short expiry caps how long a leak stays live.
- **Fact:** these levers are damage control, not prevention.

**Key point:** **Risk:** assume some tokens will land in public code — each layer shrinks that blast radius.

### Visualization (canvas `c4`, 720×300)

Horizontal bar chart: residual value of a leaked token under progressively safer designs.

- **Title (bold 13px `#1a5276`, top center):** "What a leaked token is still worth, by design (illustrative)".
- **Bars:** 24px tall, 12px gap, starting y=50; labels right-aligned 11px `#2c3e50` ending at x=280; track `#f0f0f0` 330px max starting at x=292; bar fill = row color at 0.6 alpha with 1px solid stroke in row color; value in bold 11px `#2c3e50` placed 6px after the bar end.
- **Data (label, value, color):**
  | Design | value | color |
  |---|---|---|
  | Plaintext at rest, no expiry, full scope | 95 | #e74c3c |
  | Hashed at rest, full scope | 75 | #e67e22 |
  | + narrow scopes | 45 | #e67e22 |
  | + short expiry | 25 | #2980b9 |
  | + scanner prefix, auto-revoke | 10 | #27ae60 |
- **Caption (bottom center, 11px `#999`, y=285):** "Each layer assumes the leak already happened — the design question is how much a stolen token is still worth."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = design/fact (labels: Design, Fact, Mechanism, Prefix, Core, Checksum); `#27ae60` green = win/strength (Win); `#e74c3c` red = flaw/risk/cost (Risk, Cost); `#e67e22` orange = context/history/trend (History, Trend, Prevalence). Key-point boxes open with the same colored bold lead word (Win, Fact, Risk) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`, bar fill via row color at 0.6 alpha on `#f0f0f0` tracks.
- **Content rule:** token anatomy is always schematic labeled boxes — never render realistic or truncated token strings anywhere on the page.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
