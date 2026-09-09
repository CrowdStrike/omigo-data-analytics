# Smart TV

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Smart TV

**Subtitle:** Not one streaming service — the screen itself. The TV's operating system samples whatever is displayed, from any input, and sells the viewing record as a second line of business.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** your TV account, installed apps, streaming subscriptions you link, settings and preferences.
- **Incidental:** ACR — automatic content recognition — samples the pixels and audio of *whatever is on screen* several times per second and fingerprints it against a reference library; because it watches the panel, not an app, it captures cable, discs, game consoles, and anything over HDMI.
- **Incidental:** viewing timestamps and every channel change; voice-remote audio; ad impressions and remote-button telemetry.
- **Incidental:** network and nearby-device scans — some TVs report the names of devices on your home network.
- **Inferred:** household viewing schedule and composition; political leaning from news diet; sports allegiances; gaming habits; purchase intent — all linked to your IP and identity graph.

**Key point (callout):** ACR does not care where the picture came from. The TV recognizes what you watch on a rival's streaming app, your game console, and even your home videos — anything the screen displays is sampled, fingerprinted, and logged.

### Visualization (canvas `c1`, 720×420)

Horizontal grouped bar chart: assumed vs realistic extent of collection, two bars per row.

- **Title (bold 13px, `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (row at y=30):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent"; label text `#2c3e50` 11px.
- **Rows (label, assumed, actual — values are % of 380px max bar width):**
  - Apps you install & open: 80 / 95
  - What you watch in TV’s own apps: 60 / 95
  - Rival apps / cable / HDMI inputs: 10 / 90
  - Game console & home video content: 5 / 75
  - Every channel change, timestamped: 15 / 90
  - Voice-remote audio: 25 / 70
  - Devices on your home network: 5 / 55
  - Sold to brokers, linked to identity: 10 / 80
- **Geometry:** right-aligned row labels at x=225 (`#2c3e50`, 11px), bars start at x=239, bar height 12px, assumed bar on top, actual bar 3px below, rows spaced 42px starting at y=52.
- **Colors:** assumed `rgba(26,82,118,0.35)`, actual `rgba(231,76,60,0.55)`.
- **Caption (bottom center, `#999`, 11px):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** run apps, continue-watching rows, content recommendations.
- **Ad targeting on the TV:** home-screen ads and in-stream ads keyed to your viewing record.
- **Cross-device retargeting:** the viewing record is matched to your identity graph by IP — watch a car ad on the TV, see the car brand on your phone.
- **Audience measurement:** second-by-second viewership sold to advertisers, networks, and studios.
- **Data licensing:** viewing data sold to brokers as a standalone revenue line — razor-thin hardware margins made the data the profit center.
- **Model training:** recognition, recommendation, and ad-ranking models.

The hardware is the loss leader; the viewing record is the product. Cheap TVs are cheap partly because you are paying with data.

### Visualization (canvas `c2`, 720×340)

Flow diagram: left column of data-category boxes connected by arrows to right column of use boxes.

- **Title (bold 13px, `#1a5276`, top center):** "From data category to use".
- **Left boxes** (x=40, width 185, height 32, centered on y): ACR screen fingerprints `#e74c3c` (y=55), Viewing timeline `#1a5276` (y=110), App & subscription list `#2980b9` (y=165), Voice-remote audio `#8e44ad` (y=220), IP / identity link `#e67e22` (y=275).
- **Right boxes** (x=485, width 210): Recommendations `#27ae60` (y=55), Ads on the TV `#e74c3c` (y=110), Cross-device retargeting `#e67e22` (y=165), Audience measurement (sold) `#8e44ad` (y=220), Model training `#1a5276` (y=275).
- **Box style:** fill in box color at 12% alpha, 1.5px stroke in box color, bold 12px centered label in box color.
- **Links (left index → right index):** 0→1, 0→3, 1→0, 1→2, 2→0, 3→4, 4→2. Lines `#bbb` 1.2px from x=225 to x=478 with small filled triangular arrowheads at the right end.
- **Caption (bottom center, `#999`, 11px):** "The screen fingerprint feeds the ad and measurement pipelines, not the picture quality."

## How long it's kept

- **ACR fingerprint logs:** months to years, held for "measurement and improvement".
- **Viewing history:** the life of the account.
- **Ad impression / telemetry logs:** months to years.
- **Voice-remote clips:** until deleted — often the life of the account.
- **Data-broker copies:** once sold, retention is the broker's policy, not yours — effectively uncontrolled.
- **Aggregates & trained models:** indefinite.
- **The identifier trick:** the longest retention applies not to the originals but to copies stripped of direct identifiers — raw identifiable data gets shorter windows, while "de-identified" versions are kept far longer or forever; stripping PII does not always prevent re-identification (pseudonymized ≠ anonymous).
- **Factory reset / opt-out:** stops new collection; it does not recall what already left the TV.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars with a dashed vertical opt-out marker.

- **Title (bold 13px, `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Geometry:** bars start at x0=220, axis max x=690, bar height 16px, rows start y=46, gap 22px; right-aligned labels left of bars (`#2c3e50`, 11px); bar fill in row color at 45% alpha with 1px solid stroke in row color; note text `#666` 10px right of each bar.
- **Rows (label, bar end x, color, note):**
  - Voice-remote clips: 380, `#2980b9`, "until deleted"
  - Ad impression / telemetry logs: 420, `#2980b9`, "months–years"
  - ACR fingerprint logs: 500, `#e67e22`, "months–years"
  - Viewing history: 545, `#e67e22`, "life of account"
  - Data-broker copies: 690, `#e74c3c`, "uncontrolled", with a filled triangular arrowhead extending past the bar end.
  - Aggregates / trained models: 690, `#e74c3c`, "indefinite", with a filled triangular arrowhead extending past the bar end.
- **Marker:** vertical dashed line (`#e67e22`, 2px, dash 5/4) at x=440 spanning all rows, labeled below in bold 11px `#e67e22`: "TV reset / opt-out".
- **Caption (bottom center, `#999`, 11px):** "Bars crossing the marker survive a reset or opt-out — including every copy already sold."

## What you get back

- **A typical export includes:** little or nothing — a settings page with toggles; in some jurisdictions, an export of viewing history and account data.
- **Typically excluded:** the ACR fingerprint logs themselves, cross-device identity links, ad interest segments, audience-measurement records, and every broker copy already sold.
- **Opt-out reality:** ACR controls are buried several menus deep under names that do not say "tracking" — think "viewing information services" or "interactive content features".

**Key point (callout):** The asymmetry: you might retrieve a list of shows. The second-by-second record of everything your screen displayed — and the identity-linked copies sold onward — were built from your living room but are treated as the platform's inventory, not your data.

### Visualization (canvas `c4`, 720×320)

Two side-by-side panels comparing retrievable vs retained data.

- **Title (bold 13px, `#1a5276`, top center):** "The export vs what exists".
- **Left panel** (x=35, width 310, y=40, height 235): green `#27ae60` — fill at 8% alpha, 2px stroke; bold 13px title "WHAT YOU CAN RETRIEVE"; items centered in 12px `#2c3e50`, 25px line spacing: Account & settings, Installed app list, Viewing history (some regions), Privacy toggle states.
- **Right panel** (x=375, width 310): red `#e74c3c` — title "EXISTS BUT NOT RETURNED"; items: ACR fingerprint logs, Second-by-second screen record, Cross-device identity links, Ad interest segments, Audience-measurement records, Data-broker copies (sold), Model training contributions.
- **Caption (bottom center, `#999`, 11px):** "You get a list of shows. The screen record and its sold copies stay."

## Regeneration instructions

- **Layout:** platform-privacy detail page: h1, `.subtitle` paragraph, `.disclaimer` callout, then a full-width `.obj-table` with one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullet list (+ optional `.key-point` callout or paragraph), right `<td>` (55%, centered) holds the canvas.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cells `border: 1px solid #2980b9`, padding 16px, vertical-align top; `.obj-title` bold `#1a5276` 1.1em; list items 0.93em. No nav bar, no back/home links.
- **Callouts:** `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; `display: block; margin: 0 auto`; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
