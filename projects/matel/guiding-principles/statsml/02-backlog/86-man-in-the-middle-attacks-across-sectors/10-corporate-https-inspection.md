# Corporate HTTPS Inspection

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Corporate HTTPS Inspection

**Subtitle:** The one man-in-the-middle that is installed on purpose — the office proxy opens sealed traffic, reads it for threats and leaks, and seals it again before your browser notices.

**Intro callout (blue-left-border box):** This page is the concept-closer: a sanctioned middleman shows exactly what the padlock does and does not promise. The mechanics are identical to an attack; the difference is consent, disclosure, and who controls the device.

## 1. The setup: the padlock's real promise

The padlock promises less than most people think.

- **Fact:** it means "sealed to whoever holds this certificate's key."
- **Fact:** it does not mean private from everyone.
- **Mechanism:** built-in trusted signers (certificate authorities).
- **Mechanism:** each signer can vouch for any site's identity.
- **Fact:** whoever can add to that list can sit in the middle.
- **Scene:** employers manage company laptops and phones.
- **Policy:** device management can install a company entry.
- **Fact:** standard, documented practice in managed fleets.

**Key point (red-left-border box):** **Fact:** the padlock authenticates a key holder — the trust list decides which key holders count.

### Visualization (canvas `c1`, 720×300)

Trust-list diagram: the browser's list of trusted authorities, with one company-added entry highlighted.

- **Title (bold 13px `#1a5276`, top center):** "The browser's trust list decides who sits in the middle".
- **Outer box:** x=140, y=42, 440×192; fill `#1a5276` at 0.05 alpha, stroke `#1a5276` width 2; label "Browser: trusted authorities" bold 13px `#1a5276` centered at y=66.
- **List rows** 380×26 at x=170 (text centered, baseline row-y+17):
  | Row | y | style |
  |---|---|---|
  | Public authority A | 80 | fill `#f0f0f0`, text 11px `#666` |
  | Public authority B | 112 | fill `#f0f0f0`, text 11px `#666` |
  | Public authority C | 144 | fill `#f0f0f0`, text 11px `#666` |
  | Company entry — added by device management | 176 | fill `#e67e22` at 0.12 alpha, stroke `#e67e22` width 2, text bold 11px `#e67e22` |
- **Bottom line (bold 12px `#e67e22`, centered, y=260):** "Whoever edits this list defines who can sit in the middle."
- **Caption (bottom center, 11px `#999`, y=285):** "Adding a company entry is standard, documented practice on managed devices."

## 2. The trick (sanctioned): open, inspect, re-seal

On a managed device, outbound traffic takes a detour.

- **Setup:** internet traffic routes through an inspection proxy.
- **Mechanism:** the proxy holds the company-installed key.
- **Mechanism:** it opens each connection and reads the contents.
- **Defense:** scans for malware and for data leaving the company.
- **Mechanism:** then re-encrypts and forwards to the real site.
- **Fact:** your browser shows a normal padlock throughout.
- **Fact:** that padlock is issued by the proxy, not the site.
- **Mechanism:** click the padlock and read the issuer name.
- **Fact:** on most managed setups, that reveals the proxy.

**Key point:** **Fact:** each pipe is genuinely sealed — the two pipes just meet in the middle instead of running end to end.

### Visualization (canvas `c2`, 720×300)

Pipe diagram: laptop → proxy → website, two separate sealed pipes meeting at an orange open + scan + re-seal box, with certificate issuer labels under each pipe.

- **Title (bold 13px `#1a5276`, top center):** "Open, inspect, re-seal: two sealed pipes meet at the proxy".
- **End boxes** (fill = box color at 0.12 alpha, stroke = box color width 2; title bold 13px in box color at box y+28; sub-line 10px `#666` at box y+48):
  | Title | sub-line | color | x | y | size |
  |---|---|---|---|---|---|
  | Laptop | your browser | #1a5276 | 40 | 95 | 130×70 |
  | Website | the real site | #27ae60 | 550 | 95 | 130×70 |
- **Proxy box:** x=290, y=85, 140×90; fill `#e67e22` at 0.12 alpha, stroke `#e67e22` width 2; title "Inspection proxy" bold 13px `#e67e22` at y=113; sub-lines 10px `#666`: "open + scan + re-seal" at y=133, "holds company key" at y=150.
- **Pipes** — two filled rects, y=118, height 24, label "sealed" 10px in pipe color centered at y=134:
  | span | fill/stroke |
  |---|---|
  | x=170 to 290 | `#1a5276` at 0.15 alpha, stroke `#1a5276` width 1.5 |
  | x=430 to 550 | `#27ae60` at 0.15 alpha, stroke `#27ae60` width 1.5 |
- **Issuer labels** — thin `#bbb` width-1 vertical lines from (230,146) to (230,192) and from (490,146) to (490,192); centered 11px labels at y=207: "issued by: company proxy" at x=230 in `#e67e22`, "issued by: public authority" at x=490 in `#27ae60`.
- **Bottom line (bold 12px `#1a5276`, centered, y=255):** "The padlock is real on both pipes — neither runs end to end."
- **Caption (bottom center, 11px `#999`, y=285):** "Click the padlock and read the certificate issuer to see which pipe you are on."

## 3. What it teaches: reading the padlock honestly

Same mechanics, different meaning — consent is the line.

- **Unsanctioned:** the identical trick without consent is the attack.
- **Defense:** the browser warning exists to catch this exact case.
- **Risk:** unexpected issuer on your own device is a stop sign.
- **Policy:** on work devices, assume inspection if policy says so.
- **Policy:** disclosure usually sits in the acceptable-use policy.
- **Fact:** encryption authenticates endpoints you chose to trust.
- **Fact:** the trust list is the real perimeter.

**Key point:** **Defense:** two checks cover every case — who issued the certificate, and whose device you are on.

### Visualization (canvas `c3`, 720×300)

2×2 decision matrix: issuer expected/unexpected across, device yours/managed down.

- **Title (bold 13px `#1a5276`, top center):** "Reading an issuer name: what to do, by device".
- **Column headers (bold 11px `#2c3e50`, centered, y=54):** "issuer: expected" at x=285, "issuer: unexpected" at x=495.
- **Row labels (11px `#2c3e50`, right-aligned at x=170):** "your device" baseline y=111, "managed device" baseline y=201.
- **Quadrant cells** 210×90 (fill = cell color at 0.12 alpha, stroke = cell color width 2; verdict bold 12px in cell color centered at cell y+40; sub-line 10px `#666` centered at cell y+58):
  | Verdict | sub-line | color | x | y |
  |---|---|---|---|---|
  | Normal | the padlock means what you think | #27ae60 | 180 | 62 |
  | Warning — do not proceed | classic attack signature | #e74c3c | 390 | 62 |
  | Disclosed inspection | see the acceptable-use policy | #e67e22 | 180 | 152 |
  | Ask IT | could be policy, could be trouble | #999 | 390 | 152 |
- **Bottom line (bold 12px `#1a5276`, centered, y=265):** "The trust list is the real perimeter — read the issuer, then decide."
- **Caption (bottom center, 11px `#999`, y=288):** "One habit covers all four squares: check who issued the certificate."

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one non-wrapping line — a bold colored label plus a phrase of roughly 55 characters or fewer; in HTML the label is `<span class="pt-label" style="color:COLOR">Label:</span>` with `.pt-label { font-weight: bold; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence.
- **Label colors by meaning:** `#1a5276` blue = mechanism/fact (Setup, Fact, Mechanism); `#27ae60` green = defense/win (Defense, Win); `#e74c3c` red = risk/loss (Risk, Unsanctioned); `#e67e22` orange = scene/context/history (Scene, Policy, Trend). Key-point boxes open with the same colored bold lead word (Fact, Defense) followed by one short sentence.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; `.pt-label` bold; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width times `window.devicePixelRatio` and scales the context accordingly. All data arrays are literal and hardcoded — no `Math.random` or `Date.now`.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- **Content rule:** tone stays mechanical and neutral; no vendor or product names anywhere; the sanctioned middleman is drawn orange (disclosed), never red (attack); hedges ("most managed setups", "usually") are kept for unsourced claims.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
