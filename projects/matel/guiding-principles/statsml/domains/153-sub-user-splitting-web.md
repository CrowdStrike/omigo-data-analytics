# Sub-User Splitting in Web Experiments

**Page type:** detail page (h2 section headers, each followed by a two-column obj-table row: text left 50%, canvas right 50%)
**HTML title tag:** Sub-User Splitting in Web Experiments

**Subtitle:** Randomizing at session, cookie, device, or GUID level when the decision-making entity is the user puts the same person in both arms — independence is an illusion.

## The Core Problem

**The Core Problem**

- **Many identities:** One person carries a phone, a laptop, a tablet, and maybe a work browser.
- **Independent assignment:** Each device generates its own cookie/session/GUID and is split separately.
- **Both arms at once:** The same person sees Treatment on their phone and Control on their laptop.
- **One decision:** Conversion — purchase, signup, subscription — is a single user-level decision.
- **Not three points:** Three devices give three rows of data, not three independent observations.
- **Inflated count:** You count 3 "observations" but genuinely hold only 1 independent unit.
- **Correlated rows:** Within-user correlations are ignored, so variance comes out underestimated.
- **False significance:** With variance too small, everything looks more significant than it is.

Callout (philosophy box, inside the left cell): **Key insight:** The unit of analysis must match the unit of decision. If conversion is a user-level event, randomization must be user-level. Anything more granular creates correlated observations masquerading as independent ones.

### Visualization (canvas `c1`, 720×300)

Fan-out diagram: one user circle at top connecting to four device boxes with mixed arm assignments. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "One User — Multiple Identities — Inconsistent Assignment".
- **User node:** circle radius 35 at (center, 90), fill `rgba(26,82,118,0.1)`, stroke `#1a5276` width 3; label (13px `#1a5276`, centered): "👤 One Person".
- **Connectors:** light gray `#ccc` lines (width 1.5) from the user circle down to each device box.
- **Device boxes** (110×55 at y=170, centered at x positions; two-line label — device name 11px `#333`, cookie id 10px monospace `#888` — plus bold 12px arm label in arm color):
  - x=110: "Phone" / "cookie_7a3" — "Arm A" green `#27ae60`, box fill `rgba(39,174,96,0.15)`, stroke green width 2
  - x=270: "Laptop" / "cookie_f91" — "Arm B" red `#e74c3c`, box fill `rgba(231,76,60,0.15)`, stroke red
  - x=430: "Tablet" / "cookie_2bd" — "Arm A" green
  - x=590: "Work PC" / "cookie_e44" — "Arm B" red
- **Takeaway (bold 13px red `#e74c3c`, centered, y=260):** "Same person in both arms → not independent observations".
- **Caption (12px `#666`, centered, y=280):** "System thinks: 4 users, 2 in each arm. Reality: 1 user seeing both."

## How It Manifests

**How It Manifests**

- **Cookie-based split:** User clears cookies and gets a fresh assignment on the very next visit.
- **Oscillation:** The same person flips between arms across sessions, so neither arm is clean.
- **Session-level split:** User browses a product page in Treatment, with the new recommendation widget.
- **Split credit:** That same user purchases later in a Control session — which arm gets the conversion?
- **Device-level split:** User researches on mobile in Treatment, then converts on desktop in Control.
- **Invisible driver:** Treatment's contribution never shows up; Control collects the false credit.
- **GUID/install-ID split:** User reinstalls the app, gets a fresh GUID, and lands in a different arm.
- **Carryover:** Prior exposure from the old install still influences how that person behaves.

**Consequences:** Effect dilution (same person partially in both arms), attribution confusion (conversion counted in wrong arm), inflated N (3 devices ≠ 3 users), and inconsistent user experience (breaks trust).

### Visualization (canvas `c2`, 720×300)

Timeline diagram: a horizontal journey line with four event dots alternating between arms, ending in a mis-attributed conversion. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "Cross-Device Journey — Wrong Attribution".
- **Timeline:** horizontal `#ccc` line (width 2) from x=60 to x=660 at y=130.
- **Events** (dot radius 8 filled in arm color on the line; above the dot two lines of 11px text — action `#333`, device `#888`; below the dot the arm name in 10px arm color, then a detail line in 11px `#555`):
  - x=120: "Browse" / "(mobile)" — Treatment, green `#27ae60` — detail "Sees new UI"
  - x=280: "Research" / "(laptop)" — Control, red `#e74c3c` — detail "Sees old UI"
  - x=440: "Compare" / "(mobile)" — Treatment, green — detail "Sees new UI"
  - x=600: "Purchase" / "(laptop)" — Control, red — detail "💰 Converts!"
- **Attribution arrow:** short dashed red line (dash 4/3, width 2) dropping below the Purchase event.
- **Takeaway (bold 13px red `#e74c3c`, centered, y=240):** "Conversion attributed to Control — but Treatment drove the decision".
- **Caption (12px `#666`, centered, y=262):** "Treatment effect is invisible. Control gets false credit."

## Fix: Split by Authenticated User ID

**Fix: Split by Authenticated User ID**

- **Logged-in users:** Hash user-id + experiment-id to assign the variant deterministically.
- **Consistency:** That same assignment then holds everywhere, every session, on every device.
- **Anonymous users:** Genuinely hard, with three imperfect options and no clean winner among them.
- **Option 1 and 2:** First-party persistent ID via fingerprinting (privacy concerns), or logged-in users only.
- **Option 3:** Accept the noise and use a session-level split with larger N and robust standard errors.
- **Mixed auth states:** User browses anonymously, logs in, then converts in the same journey.
- **Unknown arms:** Under a cookie-level split, those pre-login sessions sit in unknown arms.
- **Reconciliation:** Attribute all of that person's sessions to the user-id arm assigned post-login.
- **Account sharing:** A family streaming account is one user-id but several distinct people.
- **Which unit:** If the household decides, user-id is right; if the viewer does, split per profile.

**The tell:** Ask "can one human see both variants?" If the answer is yes through any path (multiple devices, cookie churn, reinstall), your split level is below the independence boundary.

### Visualization (canvas `c3`, 720×300)

Flow diagram: user circle → hash-function box → consistent assignment fanned out to four devices, all in the same arm. White (unpainted) background.

- **Title (bold 14px `#1a5276`, top center):** "Fix: hash(user_id + experiment_id) → Consistent Assignment".
- **User node:** circle radius 30 at (center, 80), fill `rgba(39,174,96,0.15)`, stroke green `#27ae60` width 3; label (bold 12px green): "user_4281".
- **Hash box:** 180×30 rectangle centered at y=120, fill `#f0f4f8`, stroke `#2980b9` width 1.5; label (12px monospace `#2980b9`): 'hash("4281" + "exp_23") % 2 = 0'.
- **Result label (bold 14px green, centered, y=180):** "→ Treatment (always)" with a short green connector arrow above it.
- **Device boxes** (100×40 at y=210, centered at x = 130, 290, 450, 610), each fill `rgba(39,174,96,0.15)`, stroke green width 2, connected by thin green lines from the result label; labels (12px green, centered): "Phone → A", "Laptop → A", "Tablet → A", "Work PC → A".
- **Caption (13px `#333`, centered, y=280):** "Every device, every session → same variant. Clean measurement."

## Regeneration instructions

- **Layout:** detail page. h1 + `.subtitle`, then one `h2` per section, each followed by a `.obj-table` (full-width table, single `<tr>`): left `<td>` (40%) holds `.obj-title` div + `<ul>` of bullets (plus a `.philosophy` callout inside the first section's cell and a trailing `<p>` in sections 2 and 3), right `<td>` (60%, centered) holds the canvas. The `.obj-title` text duplicates the h2 text on this page.
- **Page CSS:** body system sans-serif (-apple-system, BlinkMacSystemFont, 'Segoe UI'), white background, text `#2a2a2a`, padding 20px 10px, line-height 1.6; h1 1.8em `#1a5276`; h2 1.4em `#1a5276` with 2px solid `#2980b9` bottom border, padding-bottom 8px; `.subtitle` `#666` 1.05em; ul 0.9em `#333`; `strong` `#1a5276`; `.obj-table td` border `1px solid #e0e0e0`, padding 20px 24px, even rows `#fafcfe`; `.obj-title` 1.05em weight 600 `#1a5276`; `.philosophy` background `#f0f4f8`, left border 4px solid `#2980b9`, padding 12px 16px, 0.9em. No nav bar, no back/home links.
- **Canvas:** intrinsic `width`/`height` attributes per chart; a shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases on this page have no painted background (white). Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, accent blue `#2980b9`, connector gray `#ccc`, gray text `#666`/`#555`/`#333`/`#888`.
- In regenerated HTML, any card links use `.html` extensions (this page has no outbound links).
