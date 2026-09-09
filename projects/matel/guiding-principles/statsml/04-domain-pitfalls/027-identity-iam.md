# Identity / IAM — Domain Pitfalls

**Page type:** detail page (one h2 per pitfall, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** 27. Identity / IAM — Domain Pitfalls

**Subtitle:** Data traps specific to identity management, authentication, and access control analytics

## Callout (philosophy box)

Identity data combines human behavior, system automation, and policy enforcement. The same event (a login) can mean completely different things depending on context that the log alone doesn't capture. Most "anomaly detection" on identity data produces overwhelming false positives because the baseline assumptions are wrong.

## Impossible Travel False Positives

**Obj-title:** Geographic impossibility alerts are almost always wrong

- User logs in from NYC, then London 10 minutes later
- Alert fires: "impossible travel detected!"
- Real causes: VPN egress changed, mobile IP geolocation wrong, shared/service account, corporate proxy routing
- IP geolocation databases are 50-80% accurate at city level
- Corporate VPNs make everyone appear at HQ or a data center

**Impact:** 95%+ of "impossible travel" alerts are false positives. Alert fatigue causes real compromises to be ignored.

### Visualization (canvas `c1`, 720×300)

Map-style diagram of two login locations connected by an "impossible" travel arc, plus a timeline.

- **Continents:** two light ellipses (fill `#eef4f8`, stroke `#ccd9e0` width 1.5): North America at (200,130) radii 80×55; Europe at (450,110) radii 70×50.
- **Markers:** blue (`#2980b9`) 8px dots for NYC at (220,140) and London at (440,100), labeled "NYC" and "London" in bold `#1a5276`.
- **Travel arc:** dashed red (`#e74c3c`, dash 6/4, width 2) quadratic curve between the two markers, labeled in bold red 13px: "10 min?!".
- **Timeline (bottom):** gray (`#666`) line at y=230 from x=100 to x=600 with labels "9:00 AM" and "9:10 AM".
- **Green caption (`#27ae60`, 12px):** "Real causes: VPN | Bad GeoIP | Shared Acct | Proxy".
- **Big label (right, red):** "95% FALSE" in bold 32px, "POSITIVES" in 14px below it.

## MFA Fatigue Attacks Look Legitimate

**Obj-title:** Successful MFA doesn't mean legitimate access

- Attacker spams MFA push notifications repeatedly
- Exhausted user approves at 3am to make it stop
- Event log shows: "MFA: Success" — indistinguishable from real login
- Without behavioral context (time-of-day, request frequency, denial count before approval), attack is invisible
- Standard audit logs don't correlate denied pushes with eventual approval

**Impact:** MFA fatigue attacks bypass the strongest auth control while leaving a clean audit trail.

### Visualization (canvas `c2`, 720×300)

Sequence of six phone icons showing repeated MFA pushes, then the resulting clean audit log entry.

- **Title (bold 15px `#1a5276`):** "MFA Push Sequence (Attacker Spamming)".
- **Pushes (six 45×70 phone outline rectangles at y=60, 105px apart, colored by result; each shows a 🔕 icon for DENY or ✓ for APPROVE, the result word in bold 11px, and the time in `#666` 10px below):**
  - 2:41 AM — DENY, green `#27ae60`
  - 2:43 AM — DENY, green
  - 2:47 AM — DENY, green
  - 2:52 AM — DENY, green
  - 2:58 AM — DENY, green
  - 3:01 AM — APPROVE, red `#e74c3c`
- **Arrow:** gray (`#999`) downward arrow from the sequence to the audit log box.
- **Audit log box:** light gray (`#f8f9fa`, border `#dee2e6`) rectangle at (150,190) 420×60; bold label "AUDIT LOG:" in `#1a5276`; monospace 14px line: `3:01 AM | user@corp | MFA: SUCCESS`.
- **Warning (bold red 14px):** "Looks 100% legitimate — attack invisible in logs".

## Nested Group Explosion

**Obj-title:** Transitive permissions hide in group hierarchies

- User is member of Group A
- Group A is nested in Group B
- Group B is nested in Group C
- Group C has admin access
- Effective permissions require full graph traversal
- Flat permission lists miss inherited access entirely

**Impact:** n groups x m nesting levels = O(n x m) complexity. Most access reviews only check direct membership, missing transitive admin paths.

### Visualization (canvas `c3`, 720×300)

Chain graph of group nesting from user to admin, contrasted with flat-list vs graph-traversal boxes.

- **Title (bold 14px `#1a5276`):** "Transitive Group Membership — Hidden Privilege"; subtitle in `#666` 12px: "O(n x m) complexity: n groups x m nesting depth".
- **Nodes (30px-radius circles at y=150, white bold 11px labels, connected left-to-right by gray `#666` arrows):**
  - "User" `#2980b9` at x=80
  - "Group A" `#27ae60` at x=220
  - "Group B" `#f39c12` at x=360
  - "Group C" `#e67e22` at x=500
  - "ADMIN" `#e74c3c` at x=640
- **Flat list box:** blue-tinted (`#f0f4f8`, border `#2980b9`) at (50,220) 280×60; bold "Flat Permission List:"; monospace `user: member of "Group A"`; green note "(looks harmless)".
- **Effective box:** red-tinted (`#fdf2f2`, border `#e74c3c`) at (380,220) 300×60; bold red "Effective (graph traversal):"; monospace `user -> A -> B -> C -> ADMIN`; bold red "HIDDEN ADMIN ACCESS!".

## Service Account vs Human Confusion

**Obj-title:** Two fundamentally different account types analyzed as one

- Service accounts: 24/7 access, no location pattern, no MFA, high-volume API calls
- Human accounts: 9-5 activity, single location, MFA, interactive sessions
- Analyzing together: service accounts look like "anomalous humans"
- Anomaly detection trained on mixed data produces garbage baselines
- Must segment account types before any behavioral analysis

**Impact:** Unsegmented analysis floods SOC with false alerts on normal automation while missing real human account compromise.

### Visualization (canvas `c4`, 720×300)

Line chart over a 24-hour axis: a human bell curve vs a flat service-account line.

- **Title (bold 14px `#1a5276`):** "Mixed analysis = service accounts flagged as anomalous".
- **Axes:** x from 0h to 24h with tick labels every 3h ("0h"…"24h"); rotated y label "Activity Level"; axes stroked `#666`. Plot area: x 80–680, y 50–220.
- **Human series (blue `#2980b9`, width 3):** Gaussian bell curve activity = exp(−0.5·((hour−12)/2.5)²), peak scaled to 85% of chart height.
- **Service series:** dashed red (`#e74c3c`, dash 8/4, width 3) flat horizontal line at 45% of chart height.
- **Legend:** blue swatch "Human (9-5 pattern)"; dashed red line "Service Acct (24/7 flat)".
- **Annotation (bold red 13px above the flat line):** "\"Anomalous\" if treated as human!".
- **Bottom note (`#666` 12px):** "Must segment account types BEFORE behavioral analysis".

## Token Lifetime vs Session Mismatch

**Obj-title:** Multiple overlapping auth layers create timeline ambiguity

- OAuth access token: 1 hour lifetime
- Session cookie: 8 hour lifetime
- SSO session: 24 hour lifetime
- Which "login time" is the real one?
- Audit logs show different timestamps depending on which layer you query
- Token refresh looks like "new login" in some systems

**Impact:** Timeline of access is ambiguous. Incident response cannot determine actual compromise window without understanding all auth layers.

### Visualization (canvas `c5`, 720×300)

Horizontal Gantt-style bars of the three auth-layer lifetimes over a 24-hour axis.

- **Title (bold 14px `#1a5276`):** "Overlapping Auth Layer Lifetimes".
- **Bars (30px tall, starting at x=180, width proportional to hours/24 of a 470px track, 70% alpha fill with solid 2px border of the same color; layer name right-aligned to the left in `#333`; duration label "Nhr" in bold white inside the bar, or in the bar color beside it when the bar is narrower than 60px):**
  - "OAuth Token" — 1hr, `#3498db`, y=70
  - "Session Cookie" — 8hr, `#f39c12`, y=130
  - "SSO Session" — 24hr, `#27ae60`, y=190
- **Timeline axis:** gray (`#666`) line at y=240 with tick marks and labels every 4 hours ("0h"…"24h").
- **Question marks:** bold red 22px "?" at each bar's expiry boundary (start, 1h, 8h positions).
- **Bottom question (bold red 14px, centered):** "Which timestamp is the \"real\" login? Different logs disagree.".

## Dormant Account Risk Invisible

**Obj-title:** Account inactivity alone doesn't indicate risk level

- Account unused for 6 months — what does it mean?
- Possibility 1: Terminated employee (HIGH RISK — should be disabled)
- Possibility 2: Parental leave (expected — will return)
- Possibility 3: Automation account between scheduled deployments (normal)
- Status field alone doesn't tell you the reason
- Need HR system integration, manager context, account purpose metadata

**Impact:** Without external context, dormant account cleanup either disables critical automation or leaves compromisable accounts active.

### Visualization (canvas `c6`, 720×300)

Diagram: three identical-looking dormant accounts mapped to three very different real reasons.

- **Title (bold 14px `#1a5276`):** "Dormant Accounts: Identical Appearance, Different Reality".
- **Timeline (top):** gray (`#999`) line at y=80 from x=100 to x=650 with month labels Jan–Dec in `#666` 11px; a red triangle marker at Jan labeled "Last activity"; blue "NOW" label at the right end.
- **Accounts (three gray circles, fill `#bdc3c7`, stroke `#95a5a6`, radius 22, at x=150), each labeled with account name in `#333` 13px and "6 months inactive" in `#999` 11px, with a dashed gray (`#ccc`, dash 4/3) arrow to its real reason (bold 13px in risk color) and risk label (bold 11px):**
  - Account A (y=140) → "Terminated Employee", "HIGH RISK", red `#e74c3c`
  - Account B (y=195) → "Parental Leave", "EXPECTED", green `#27ae60`
  - Account C (y=250) → "Automation (quarterly)", "NORMAL", blue `#2980b9`
- **Bracket:** gray (`#666`) left bracket spanning all three circles with rotated label "Look identical".
- **Bottom note (bold red 12px, centered):** "Requires HR/context integration to differentiate".

## Regeneration instructions

- **Layout:** domains detail-page template: h1, `.subtitle`, one `.philosophy` callout, then per pitfall an unnumbered `<h2>` (with an id slug; 1.4em `#1a5276`, 2px solid `#2980b9` bottom border) followed by a `.obj-table` with one `<tr>`: left `<td>` (45%) holding `.obj-title` + `<ul>` of bullets + an **Impact:** paragraph, right `<td>` (55%, centered) holding the canvas. Even rows background `#fafcfe`. No nav, no cross-page links.
- **Page CSS:** body system sans-serif, white background, text `#2a2a2a`, padding 40px 20px, line-height 1.6; h1 1.8em `#1a5276`; `.subtitle` `#666` 1.05em; `ul` 0.9em `#333`; `strong` `#1a5276`; `.philosophy` background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, 0.9em; table cell borders `1px solid #e0e0e0`, padding 20px 24px; `.obj-title` 1.05em weight 600 `#1a5276`.
- **Canvas:** each canvas declares intrinsic `width="720" height="300"`; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Charts drawn immediately in IIFEs. Base chart font 17px system sans-serif. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, mid blue `#2980b9`/`#3498db`, green `#27ae60`, red `#e74c3c`, orange `#f39c12`/`#e67e22`, grays `#666`/`#999`/`#bdc3c7`/`#95a5a6`.
- Card links elsewhere pointing to this page use the `.html` extension in regenerated HTML.
