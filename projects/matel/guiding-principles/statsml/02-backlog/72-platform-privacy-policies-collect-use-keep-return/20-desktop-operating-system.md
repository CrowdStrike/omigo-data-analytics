# Desktop Operating System

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Desktop Operating System

**Subtitle:** The layer under every app on your computer — usage telemetry on by default, a search box that phones home per keystroke, and crash dumps that can carry fragments of your documents.

**Disclaimer (callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** account profile, settings and preferences you sync, files you explicitly choose to back up or sync to cloud storage.
- **Incidental:** diagnostic and usage telemetry on by default — which apps you launch, how long they run, which features you click; crash dumps that can include memory contents (fragments of the document that was open); OS search-box queries sent to web services as you type; a full inventory of installed software and hardware; activity history / timeline synced across devices; spell-check and typing data in some configurations; approximate location from nearby wifi; peripheral inventory (webcams, drives, printers).
- **Inferred:** work vs leisure patterns from app usage by hour; software interests and profession from what's installed; a device-value / income proxy from hardware specs; productivity and focus patterns.

**Key point (callout):** Type into the OS search box and the keystrokes can be shipped to a web service before you press enter — and when an app crashes, the dump uploaded for debugging can carry fragments of whatever you were writing at that moment.

### Visualization (canvas `c1`, 720×420)

Horizontal grouped bar chart: assumed vs realistic extent of collection, two bars per row.

- **Title (bold 13px, `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (row at y=30):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent"; label text `#2c3e50` 11px.
- **Rows (label, assumed, actual — values are % of 380px max bar width):**
  - Files you choose to sync: 85 / 90
  - App launches & durations: 15 / 90
  - Search-box keystrokes to cloud: 10 / 75
  - Crash dumps w/ memory contents: 10 / 60
  - Installed software inventory: 20 / 90
  - Hardware & peripheral inventory: 15 / 85
  - Advertising ID on the desktop: 5 / 80
  - Location from nearby wifi: 15 / 70
- **Geometry:** right-aligned row labels at x=225 (`#2c3e50`, 11px), bars start at x=239, bar height 12px, assumed bar on top, actual bar 3px below, rows spaced 42px starting at y=52.
- **Colors:** assumed `rgba(26,82,118,0.35)`, actual `rgba(231,76,60,0.55)`.
- **Caption (bottom center, `#999`, 11px):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** sync settings and files, deliver updates, answer search-box queries.
- **Product improvement:** telemetry decides which features get built, ranked, or removed; crash dumps drive bug fixing.
- **Personalize / rank:** app usage and activity history reorder menus, suggestions, and search results.
- **Ads in the shell:** an advertising ID on the desktop feeds "suggested apps" in the launcher and promotions on the lock screen.
- **Model training:** typing, speech, and usage data improve input-prediction and assistant models.
- **Sharing:** hardware partners receive device telemetry; on managed machines, your employer sees a second copy.

The app-launch log is effectively a diary of your working day — the OS sees every program before any single app does.

### Visualization (canvas `c2`, 720×340)

Flow diagram: left column of data-category boxes connected by arrows to right column of use boxes.

- **Title (bold 13px, `#1a5276`, top center):** "From data category to use".
- **Left boxes** (x=40, width 175, height 32, centered on y): Usage telemetry `#1a5276` (y=55), Search-box queries `#2980b9` (y=110), Crash dumps `#e74c3c` (y=165), Software/HW inventory `#e67e22` (y=220), Activity history `#8e44ad` (y=275).
- **Right boxes** (x=485, width 200): Feature ranking / roadmap `#1a5276` (y=55), Bug fixing / debugging `#27ae60` (y=110), Ads in the OS shell `#e74c3c` (y=165), Model training `#8e44ad` (y=220), Hardware partners / employer `#e67e22` (y=275).
- **Box style:** fill in box color at 12% alpha, 1.5px stroke in box color, bold 12px centered label in box color.
- **Links (left index → right index):** 0→0, 0→3, 1→2, 1→3, 2→1, 3→2, 3→4, 4→0. Lines `#bbb` 1.2px from x=215 to x=478 with small filled triangular arrowheads at the right end.
- **Caption (bottom center, `#999`, 11px):** "Telemetry collected \"to fix bugs\" also feeds ranking, ads, and partner sharing."

## How long it's kept

- **Crash dumps:** typically the shortest tier — days to weeks after analysis.
- **Search-box query logs:** months, often folded into the web-search history tier.
- **Diagnostic / usage telemetry:** months to a few years, held for "product improvement".
- **Activity history / timeline:** until you delete it — by default, the life of the account.
- **Software / hardware inventory:** refreshed continuously; historical snapshots persist in telemetry stores.
- **Aggregates & trained models:** anonymized statistics and model contributions kept indefinitely.
- **The longest tier is the stripped copy:** raw identifiable records get the shorter windows above, but copies with direct identifiers removed are kept far longer or indefinitely — and pseudonymized ≠ anonymous, so re-identification is not always prevented.
- **Enterprise-managed devices:** the employer's copy follows the employer's retention policy — a device reset clears neither copy.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars with a dashed vertical deletion marker.

- **Title (bold 13px, `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Geometry:** bars start at x0=220, axis max x=690, bar height 16px, rows start y=46, gap 22px; right-aligned labels left of bars (`#2c3e50`, 11px); bar fill in row color at 45% alpha with 1px solid stroke in row color; italic-free note text `#666` 10px right of each bar.
- **Rows (label, bar end x, color, note):**
  - Crash dumps: 290, `#2980b9`, "days–weeks"
  - Search-box query logs: 380, `#2980b9`, "months"
  - Usage telemetry: 500, `#e67e22`, "months–years"
  - Activity history / timeline: 540, `#e67e22`, "until you delete"
  - Employer copy (managed device): 580, `#e74c3c`, "employer policy"
  - Aggregates / trained models: 690, `#e74c3c`, "indefinite", with a filled triangular arrowhead extending past the bar end (continues indefinitely).
- **Marker:** vertical dashed line (`#e67e22`, 2px, dash 5/4) at x=480 spanning all rows, labeled below in bold 11px `#e67e22`: "device reset / account deleted".
- **Caption (bottom center, `#999`, 11px):** "Bars crossing the marker survive a device reset or account deletion."

## What you get back

- **A typical export includes:** account profile, synced settings, files you backed up, activity history shown on the privacy dashboard, search history tied to your account.
- **Typically excluded:** the raw telemetry stream (app launches, durations, feature clicks), crash dumps and their memory contents, the advertising-ID interest profile, software/hardware inventory snapshots, work-vs-leisure and income-proxy inferences, model training contributions.

**Key point (callout):** The asymmetry: the export returns what you deliberately put in — files, settings, account data. The telemetry describing *how you use your own computer*, and everything inferred from it, was collected by default and is treated as the platform's data, not yours.

### Visualization (canvas `c4`, 720×320)

Two side-by-side panels comparing exported vs retained data.

- **Title (bold 13px, `#1a5276`, top center):** "The export vs what exists".
- **Left panel** (x=35, width 310, y=40, height 235): green `#27ae60` — fill at 8% alpha, 2px stroke; bold 13px title "IN THE EXPORT"; items centered in 12px `#2c3e50`, 25px line spacing: Account profile, Synced settings, Files you backed up, Activity history (dashboard), Account search history.
- **Right panel** (x=375, width 310): red `#e74c3c` — title "EXISTS BUT NOT RETURNED"; items: Raw usage telemetry stream, Crash dumps (memory contents), Advertising-ID interest profile, Software / hardware inventory, Work-vs-leisure inference, Income proxy from hardware, Model training contributions.
- **Caption (bottom center, `#999`, 11px):** "You get back what you put in. How you used the machine stays behind."

## Regeneration instructions

- **Layout:** platform-privacy detail page: h1, `.subtitle` paragraph, `.disclaimer` callout, then a full-width `.obj-table` with one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + bullet list (+ optional `.key-point` callout or paragraph), right `<td>` (55%, centered) holds the canvas.
- **Page style:** body `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`, white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; table cells `border: 1px solid #2980b9`, padding 16px, vertical-align top; `.obj-title` bold `#1a5276` 1.1em; list items 0.93em. No nav bar, no back/home links.
- **Callouts:** `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** intrinsic `width`/`height` attributes as given per chart; `display: block; margin: 0 auto`; shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, gray text `#666`/`#999`.
