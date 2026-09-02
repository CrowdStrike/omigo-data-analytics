# Revocation & Lifetime Semantics

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Revocation & Lifetime Semantics

**Subtitle:** Whether a credential can be killed before its expiry is decided by one design choice — does the server look it up on every request, or trust it on sight.

**Intro callout (blue-left-border box):** **Premise:** Revocability cannot be bolted on later — every fix on this page works around one design split.

## 1. The core split: lookup vs self-contained

- **Opaque design:** server consults a record on every request
- **Instant kill:** delete the record, dead on the very next request
- **Signed design:** signature verified locally, no per-token record
- **No kill switch:** nothing to delete — valid until embedded expiry
- **Standard patch:** denylist of revoked token IDs, checked every request
- **Hidden cost:** restores the per-request lookup you tried to avoid

**Key point (red-left-border box):** **No free lunch:** stateless instant revocation does not exist — per-request state is paid upfront (opaque) or smuggled back (denylist).

### Visualization (canvas `c1`, 720×300)

Two-path flow diagram with a denylist feedback box.

- **Title (bold 13px `#1a5276`, top center, y=20):** "The core split: lookup-verified vs self-contained".
- **Boxes** (height 44, fill = color at 0.12 alpha, stroke = color width 2, label bold 12px in color, centered):
  | Box | x | y | w | color | label |
  |---|---|---|---|---|---|
  | A1 | 30 | 70 | 150 | #1a5276 | Opaque token |
  | A2 | 250 | 70 | 150 | #1a5276 | Server lookup |
  | A3 | 470 | 70 | 220 | #27ae60 | Row deleted → dead now |
  | B1 | 30 | 160 | 150 | #8e44ad | Signed token |
  | B2 | 250 | 160 | 150 | #8e44ad | Signature check |
  | B3 | 470 | 160 | 220 | #e74c3c | Valid until expiry |
- **Arrows:** `#bbb` width 1.5 horizontal arrows with filled triangular heads at row mid-height (y=92 and y=182), from x=180→244 and x=400→464 on each row.
- **Denylist box:** dashed `#e67e22` (dash [5,4]) rect at x=180, y=232, 360×48; two centered 11px `#e67e22` lines at y=251/266: "Denylist fix: works, but restores the" / "per-request lookup you tried to avoid"; dashed `#e67e22` vertical arrow (head up) from (360, 232) to (360, 208).

## 2. What can be revoked, what cannot

| Credential | Revocable? | How | Latency |
|---|---|---|---|
| Opaque API token | yes | delete server record | instant |
| Session ID | yes | delete server session | instant |
| Signed JWT | no | wait for expiry, or denylist | until expiry |
| Refresh token | yes | revoke server-side record | instant |
| Reset link | self-limiting | single use + short TTL | minutes |
| Recovery codes | yes | mark used / regenerate set | instant |
| Certificate | partly | revocation lists / status checks | hours–days |

**Key point:** **Cautionary tale:** certificate lists and status checks propagate too slowly, so short lifetimes do the real work — the conclusion every row reaches.

### Visualization (canvas `c2`, 720×300)

Revocation-latency spectrum bar with credentials plotted along it.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Revocation latency spectrum (illustrative)".
- **Spectrum bar** (y=150 to y=176, fill = color at 0.35 alpha, stroke = color width 1; segment name bold 11px in segment color, centered under bar at y=196):
  | Segment | x range | color |
  |---|---|---|
  | instant | 60–210 | #27ae60 |
  | minutes | 210–360 | #2980b9 |
  | hours–days | 360–510 | #e67e22 |
  | until expiry | 510–660 | #e74c3c |
- **Instant-zone list** (11px `#2c3e50`, centered at x=135): lines at y=60/76/92/108 — "opaque API token", "session ID", "refresh token", "recovery code"; single `#bbb` width-1 leader line from (135, 116) to (135, 148).
- **Single labels** (11px `#2c3e50`, centered, each with a `#bbb` leader line from (x, 100) to (x, 148)): "reset link (single-use + TTL)" at (285, 92); "certificate (list propagation)" at (435, 92); "signed JWT, no denylist" at (585, 92).
- **Caption (11px `#999`, centered, y=245):** "Server-side records die instantly; self-contained tokens only age out."

## 3. The standard compromise

- **Split design:** short self-contained access, long opaque refresh
- **Lookup-free path:** most requests verify with no server lookup
- **Damage cap:** revoking refresh caps loss at access minutes left
- **Refresh rotation:** each use issues a new token, retires the old
- **One live token:** exactly one family member live at any moment
- **Reuse detection:** a retired refresh token should never reappear
- **Red flag:** reuse means attacker or user holds a stale copy
- **Safe response:** revoke the whole family and force re-login

**Key point:** **Expiry is the revocation story:** worst-case revocation latency equals the access-token lifetime — why lifetimes shrank to minutes.

### Visualization (canvas `c3`, 720×320)

Refresh-rotation chain with a reuse-detection lane.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Refresh rotation and reuse detection".
- **Rotation chain** (boxes 120×40 at y=70, fill `#1a5276` at 0.12 alpha, stroke `#1a5276` width 2, bold 11px `#1a5276` centered labels): "refresh #1" at x=40, "refresh #2" at x=260, "refresh #3" at x=480.
- **Chain arrows:** `#27ae60` width-2 arrows with filled heads at y=90, from x=160→254 and x=380→474; 10px `#27ae60` centered label "use → rotate" above each arrow at y=82 (centers x=207 and x=427).
- **Status labels** (10px, centered under each box at y=128): italic `#999` "retired" under boxes 1 and 2 (x=100, x=320); bold `#27ae60` "active" under box 3 (x=540).
- **Copied-earlier link:** dashed `#999` width-1 vertical line (dash [4,3]) from (100, 110) to (100, 190); 10px `#999` left-aligned label "copied earlier" at (110, 150).
- **Attacker lane** (boxes 40 tall at y=190): box "stolen copy of #1" at x=40, 160 wide, stroke `#e74c3c` width 2, fill 0.12 alpha, bold 11px `#e74c3c`; dashed `#e74c3c` width-1.5 arrow (dash [5,4], filled head) at y=210 from x=200→334; box "reuse of retired token" at x=340, 180 wide, same red style; solid `#e74c3c` arrow at y=210 from x=520→544; box "revoke entire family" at x=550, 150 wide, fill `#e74c3c` at 0.25 alpha, stroke `#e74c3c`, bold 11px `#e74c3c`.
- **Caption (11px `#999`, centered, y=285):** "A retired token reappearing means someone holds a stolen copy — kill the whole family."

## 4. TTL design: current vs historical

- **Historical:** API keys and sessions once lived months or years
- **Forgotten chore:** revocation was manual and often skipped
- **Leak exposure:** leaked credentials stayed live long after the leak
- **Current norm:** access-token lifetimes now measured in minutes
- **Load shift:** security moved from revocation machinery to expiry
- **Short-TTL win:** lower worst-case revocation latency
- **Short-TTL cost:** more refresh traffic, more re-auth failures
- **Cost knee:** refresh cost bends sharply below roughly five minutes
- **Why minutes:** that knee made minutes-scale lifetimes the norm

**Key point:** **TTL is a dial:** stolen-token usable time versus honest-client refresh churn — modern practice sits near the short end.

### Visualization (canvas `c4`, 720×320)

Trade-off curves: revocation latency vs refresh traffic as a function of TTL.

- **Title (bold 13px `#1a5276`, top center, y=20):** "TTL trade-off: revocation latency vs refresh traffic (illustrative)".
- **Axes:** origin x=70, baseline y=260, plot 580×190, stroke `#999` width 1.5; x-axis label 11px `#666` centered at (x=360, y=298): "access-token lifetime →"; y-axis label (rotated −90°, 11px `#666`, at x=30, plot mid-height): "relative cost →".
- **X ticks** (10px `#666`, centered at y=278, at plot-width fractions): "1m" 0.05, "5m" 0.2, "15m" 0.4, "1h" 0.6, "6h" 0.8, "24h" 0.95.
- **Sweet-spot band:** rect from fraction 0.15 to 0.45 of plot width, full plot height, `#27ae60` at 0.08 alpha; bold 11px `#27ae60` centered label "common choice" at band center-x, y=84.
- **Curves** (polylines through points as [x fraction, height fraction above baseline], width 2):
  | Curve | color | points |
  |---|---|---|
  | refresh traffic | #2980b9 | (0.05, 0.95) (0.2, 0.40) (0.4, 0.18) (0.6, 0.08) (0.8, 0.03) (0.95, 0.01) |
  | worst-case revocation latency | #e74c3c | (0.05, 0.02) (0.2, 0.08) (0.4, 0.18) (0.6, 0.35) (0.8, 0.65) (0.95, 0.95) |
- **Curve labels** (11px, left-aligned, in curve color): "refresh traffic" `#2980b9` anchored at plot fraction (0.45, 0.16); "worst-case revocation latency" `#e74c3c` anchored at plot fraction (0.58, 0.45).

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/tables/`.key-point`, right `td.viz-col` (55%) for the canvas. Section 2's markdown table renders as a compact HTML table in the text column (0.85rem, `#ddd` bottom borders, bold `#1a5276` header row).
- **Bullet style:** every text-column bullet is exactly one non-wrapping line — a bold colored label naming the concept plus a phrase short enough for the 45% column (roughly ≤55 characters). Never merge facts back into paragraphs; split long content into more labeled bullets. Markdown form `- **Label:** phrase` renders as `<li><span class="pt-label" style="color:COLOR">Label:</span> phrase</li>` with `.pt-label { font-weight: 600; }`.
- **Label colors by meaning:** `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. Per label — §1: Opaque design, Signed design blue; Instant kill green; No kill switch, Hidden cost red; Standard patch orange. §3: Split design, Refresh rotation, One live token, Reuse detection blue; Lookup-free path, Damage cap green; Red flag, Safe response red. §4: Historical, Current norm, Why minutes orange; Forgotten chore, Leak exposure, Short-TTL cost red; Load shift, Cost knee blue; Short-TTL win green.
- **Callout/key-point style:** the `.intro` callout and each `.key-point` box open with the same bold colored lead word (`.pt-label` span — intro `#2980b9` blue, key points `#e74c3c` red) followed by one short sentence. In markdown the lead word is the second bold segment, e.g. `**Key point:** **No free lunch:** sentence`.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×300, `c3` 720×320, `c4` 720×320; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
