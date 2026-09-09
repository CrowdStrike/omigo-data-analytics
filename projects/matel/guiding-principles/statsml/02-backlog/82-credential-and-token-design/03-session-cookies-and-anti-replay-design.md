# Session Cookies & Anti-Replay Design

**Page type:** detail page (backlog kusto-style two-column layout: one `.lang-section` per numbered h2, text left ~45%, canvas right ~55%)
**HTML title tag:** Session Cookies & Anti-Replay Design

**Subtitle:** A session cookie is a bearer credential — whoever presents it *is* the user. The design questions are where the state lives, what the cookie is bound to, and how often it is replaced.

**Intro callout (blue-left-border box):** **Three answers:** where the state lives, what the cookie is bound to, and when the identifier rotates set what a stolen copy is worth.

## 1. Three cookie designs

- **Random ID:** cookie carries only a high-entropy random identifier.
- **Win:** instant revocation and tiny cookies.
- **Cost:** a store lookup per request; shared server state.
- **Signed cookie:** state rides in the cookie under a server signature.
- **Win:** trusted with no lookup; user reads but cannot forge.
- **Encrypted cookie:** sealed state, neither readable nor forgeable.
- **Win:** adds confidentiality of the claims.
- **Cost:** both client-state designs lose instant revocation.

**Key point (red-left-border box):** **No row to delete:** client-state cookies move the state while trust stays in a server key, so no single session can be killed server-side.

### Visualization (canvas `c1`, 720×300)

Comparison grid: three designs × five properties.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Three cookie designs: where the state lives".
- **Column headers** (bold 12px, y=48, centered): "Random ID" `#1a5276` at x=290, "Signed" `#e67e22` at x=440, "Encrypted" `#8e44ad` at x=590.
- **Row labels** (12px `#2c3e50`, left-aligned at x=30) on rows y=80/120/160/200/240; light `#eee` separator line width 1 under each row (y+16, from x=30 to x=660).
- **Cell values** (bold 12px, centered at column x; color per cell):
  | Row label | Random ID | Signed | Encrypted |
  |---|---|---|---|
  | State lives | server `#27ae60` | cookie `#e67e22` | cookie `#e67e22` |
  | User can read state | n/a — only an ID `#666` | yes `#e74c3c` | no `#27ae60` |
  | User can forge state | no `#27ae60` | no `#27ae60` | no `#27ae60` |
  | Lookup per request | yes `#e74c3c` | no `#27ae60` | no `#27ae60` |
  | Instant revocation | yes `#27ae60` | no `#e74c3c` | no `#e74c3c` |
- **Caption (11px `#999`, centered, y=280):** "Signed and encrypted cookies trade instant revocation for lookup-free verification."

## 2. Binding claims against replay

A stolen cookie replays perfectly — the server cannot tell copy from original.

- **Binding:** context claims make an out-of-place copy fail.
- **IP binding:** encrypted or hashed client IP rides in the cookie.
- **Win:** strongest common binding against remote replay.
- **Risk:** mobile IP churn forces frequent false logouts.
- **Fingerprint binding:** hash of stable device signals survives IP change.
- **Risk:** drifts with browser updates; a capable thief copies it.
- **User-agent binding:** nearly free to implement.
- **Risk:** the thief copies the user-agent header too.

**Key point:** **Mobility trade-off:** bindings buy theft resistance by spending user mobility, so real deployments bind coarsely or not at all.

### Visualization (canvas `c2`, 720×320)

Grouped bar chart: theft resistance vs false-logout rate per binding.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Binding trade-off: theft resistance vs false logouts (illustrative)".
- **Axes:** origin x=60, baseline y=260, plot 620×190, stroke `#999` width 1.5 (y-axis up from baseline, x-axis across). Implicit scale 0–100.
- **Groups:** 4 groups centered at x=137/292/447/602; each group has two 40px-wide bars with an 8px gap, green `#27ae60` (theft resistance, 0.6 alpha fill, solid 1px stroke) on the left and red `#e74c3c` (false logouts, same style) on the right; bar height = value/100×190. Value in bold 10px `#2c3e50` centered above each bar; group label 11px `#2c3e50` centered at y=278.
- **Data (group, theft resistance, false logouts):**
  | Binding | theft resistance | false logouts |
  |---|---|---|
  | No binding | 5 | 0 |
  | User-agent | 15 | 2 |
  | Device fingerprint | 60 | 10 |
  | IP address | 80 | 35 |
- **Legend** (11px `#2c3e50`, left-aligned at x=90, swatches 12×12 at x=75): "theft resistance" (green swatch) at y=88, "false logouts (mobile users)" (red swatch) at y=106.
- **Caption (11px `#999`, centered, y=305):** "Stronger binding buys theft resistance by spending user mobility."

## 3. Rotation & lifecycle

- **Fixation:** attacker plants a known session ID before login.
- **Rotate on login:** a fresh ID kills the whole attack class.
- **Rotate on elevation:** pre-elevation copies never gain new powers.
- **Idle timeout:** caps abandoned sessions.
- **Absolute timeout:** caps stolen-cookie value even under constant use.
- **Sliding expiry:** idle window renews per request, inside the hard cap.

**Key point:** **Exposure window:** the interval between rotations is how long a stolen copy stays useful.

### Visualization (canvas `c3`, 720×300)

Session-lifecycle timeline with rotation points and timeout markers.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Session lifecycle: rotation points and timeouts".
- **ID segments** (rects y=110 to y=140, fill = color at 0.25 alpha, stroke = color width 1.5, centered label bold 11px in color at y=129):
  | Segment | x range | color | label |
  |---|---|---|---|
  | pre-login id | 60–190 | #999 | "pre-login id" |
  | authenticated id | 190–420 | #1a5276 | "authenticated id" |
  | elevated id | 420–620 | #e67e22 | "elevated id" |
- **Rotation markers:** vertical `#27ae60` width-2 lines at x=190 and x=420 from y=95 to y=155; bold 11px `#27ae60` centered labels at y=88: "login → rotate" (x=190) and "elevation → rotate" (x=420).
- **Absolute timeout:** dashed `#e74c3c` width-2 vertical line at x=620 from y=95 to y=155 (dash [5,4]); bold 11px `#e74c3c` centered label at y=88: "absolute timeout".
- **Idle window:** dashed `#2980b9` bracket (dash [4,3], width 1.5): horizontal line y=180 from x=250 to x=350 with end ticks up to y=170; 11px `#2980b9` centered label at (x=300, y=200): "idle window — slides forward with each request".
- **Caption (11px `#999`, centered, y=250):** "A copy of the cookie stolen before a rotation point is worthless after it."

## 4. Prevalence: current vs historical

- **URL session IDs:** early frameworks rewrote the ID into every link.
- **Leak paths:** referrer headers, logs, and copy-pasted URLs.
- **Status:** now considered a defect wherever it survives.
- **HttpOnly flag:** hides the cookie from page scripts.
- **Secure flag:** confines the cookie to encrypted transport.
- **SameSite defaults:** cookies withheld from most cross-site requests.
- **Win:** killed cross-site replay with zero application code.
- **Rotation default:** rotate-on-login is now framework-default.
- **Frontier:** device-bound sessions tie the cookie to a device key.
- **Win:** a copied cookie fails anywhere but that device.

**Key point:** **One-way arc:** from durable portable secret to short-lived, context-bound pointer.

### Visualization (canvas `c4`, 720×300)

Era Gantt: hardening practices over time.

- **Title (bold 13px `#1a5276`, top center, y=20):** "Session-cookie hardening eras (illustrative)".
- **X axis:** years 1995–2025 mapped to x=150–660 (x = 150 + (year−1995)×17); tick labels 10px `#999` at years 1995/2000/2005/2010/2015/2020/2025, y=272; light `#f0f0f0` vertical gridlines width 1 from y=40 to y=260 at each tick.
- **Era bars** (height 24, fill = color at 0.35 alpha, stroke = color width 1; row labels 11px `#2c3e50` right-aligned ending at x=145, vertically centered on the bar):
  | Era | years | y | color |
  |---|---|---|---|
  | Session IDs in URLs | 1995–2007 | 50 | #e74c3c |
  | Plain cookies | 1997–2012 | 90 | #e67e22 |
  | HttpOnly + Secure flags | 2005–2025 | 130 | #2980b9 |
  | SameSite defaults | 2016–2025 | 170 | #1a5276 |
  | Device-bound sessions | 2022–2025 | 210 | #27ae60 |

## Regeneration instructions

- **Layout:** backlog detail-page style — h1 with `border-bottom: 2px solid #2980b9`, `.subtitle`, one `.intro` callout, then one `.lang-section` per numbered h2 (also blue-underlined). Each section holds a `table.layout` (border-collapse, full width) with one `<tr>`: left `td.text-col` (45%) for paragraphs/bullets/`.key-point`, right `td.viz-col` (55%) for the canvas.
- **Bullet style:** every bullet is one line that must not wrap in the 45% column — a bold colored label plus a phrase of ≤ ~55 characters. HTML form: `<li><span class="pt-label" style="color:COLOR">Label:</span> phrase</li>` with `.pt-label { font-weight: 600; }`. Long ideas are split into more labeled bullets, never wrapped. Lead paragraphs are at most one short sentence. Key-point boxes open with the same colored bold lead label followed by one short sentence.
- **Label colors by meaning:** `#1a5276` blue = design/fact, `#27ae60` green = win/strength, `#e74c3c` red = flaw/risk, `#e67e22` orange = context/history/trend. On this page: blue — Random ID, Signed cookie, Encrypted cookie, Binding, IP binding, Fingerprint binding, User-agent binding, Idle timeout, Absolute timeout, Sliding expiry, HttpOnly flag, Secure flag, SameSite defaults; green — Win, Rotate on login, Rotate on elevation; red — Cost, Risk, Fixation, Leak paths; orange — URL session IDs, Status, Rotation default, Frontier. Key-point leads: "No row to delete" red, "Mobility trade-off" orange, "Exposure window" red, "One-way arc" orange. Intro-callout lead: "Three answers" blue.
- **Page CSS:** body system-ui sans-serif, background `#fff`, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; h2 1.3rem `#1a5276`; `.subtitle` `#666` 0.95rem; `.intro` background `#f0f4f8` with `border-left: 3px solid #2980b9`; `.key-point` background `#f8f9fa` with `border-left: 3px solid #e74c3c`, 0.9rem; ul 0.92rem; canvases `width: 100%` with `1px solid #e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic sizes `c1` 720×300, `c2` 720×320, `c3` 720×300, `c4` 720×300; a shared `setupCanvas(id, w, h)` helper sizes the backing store from the displayed width × `window.devicePixelRatio` and scales the context accordingly.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, plus `#2980b9`, `#8e44ad`, gray text `#666`/`#999`.
- In regenerated HTML, any card/page links use `.html` extensions (this page has none).
