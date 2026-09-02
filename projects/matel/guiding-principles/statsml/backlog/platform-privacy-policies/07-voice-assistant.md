# Voice Assistant / Smart Speaker

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Voice Assistant / Smart Speaker

**Subtitle:** A microphone in the home that is always processing — wake-word buffers, voice prints, and the ambient context of a household, sometimes reviewed by humans.

**Disclaimer callout:** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** spoken commands and questions, shopping and to-do lists, alarms and reminders, linked accounts (music, calendar, shopping), per-person voice profiles you enroll.
- **Incidental:** a rolling audio buffer that constantly listens for the wake word; accidental activations that capture ambient home conversation; audio fragments before and after each trigger; connected smart-home state (locks, lights, thermostats, presence sensors); the timestamp of every request; device and network telemetry.
- **Inferred:** voice prints that distinguish household members; household composition (adults, children, guests); daily schedule (wake, leave, return, sleep); interests and purchase intent from questions; mood and health hints from tone of voice.

**Key point (callout box):** The mic is never off — a rolling buffer processes all room audio continuously to spot the wake word. False triggers ship snippets of ordinary home conversation to servers, and samples of those clips have historically been reviewed by human contractors for quality grading.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic collection extent, two bars per row.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (row at y=30, swatches 14×10 starting at x=239 and x=335):** "assumed" — fill `rgba(26,82,118,0.35)`; "realistic extent" — fill `rgba(231,76,60,0.55)`. Legend text 11px `#2c3e50`.
- **Rows** (label, assumed %, actual %): Spoken commands 90/95; Audio before/after wake word 15/70; Accidental ambient recordings 10/65; Voice print per person 15/80; Smart-home device state 25/85; Request timing / daily schedule 20/90; Human review of clips 5/45; Inferred household makeup 10/75.
- **Layout:** right-aligned labels at x=225 (11px `#2c3e50`), bars start at x=239, max width 380px (scale 0–100), bar height 12px, assumed bar on top, actual bar 3px below, group spacing 42px, first group at y=52.
- **Caption (bottom center, 11px `#999`):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** transcribe the request, answer it, execute the action.
- **Improve recognition:** recordings and transcripts train speech and language models; sampled clips graded by human reviewers.
- **Personalize / rank:** voice identification routes each speaker to their own calendar, music taste, and result ranking.
- **Ad targeting & measurement:** shopping questions, linked-account activity, and request topics feed interest segments.
- **Sharing:** third-party voice apps ("skills") receive the request text; affiliates receive usage and purchase signals; smart-home partners exchange device state.

The request log doubles as a timeline of home life: when the household wakes, cooks, shops, and goes to bed.

### Visualization (canvas `c2`, 720×340)

Two-column flow diagram: data-category boxes on the left linked by arrows to use boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (x=40, width 175, height 32 centered on y = 55/110/165/220/275):** Voice recordings `#1a5276`; Transcripts `#2980b9`; Smart-home state `#e67e22`; Request timeline `#8e44ad`; Voice prints `#e74c3c`. Style per box: stroke in its color (1.5px), fill same color at 12% alpha, bold 12px centered label in its color.
- **Right boxes (x=485, width 200, same y positions):** Answer the request `#27ae60`; Train speech models `#1a5276`; Personalize per speaker `#2980b9`; Interest segments / ads `#e74c3c`; Third-party skills / partners `#e67e22`.
- **Links (gray `#bbb` lines 1.2px with filled arrowheads), left index → right index:** Voice recordings → Answer the request; Voice recordings → Train speech models; Transcripts → Interest segments / ads; Transcripts → Third-party skills / partners; Smart-home state → Third-party skills / partners; Request timeline → Interest segments / ads; Voice prints → Personalize per speaker.
- **Caption (bottom center, 11px `#999`):** "One recording feeds many pipelines — answering the request is only the first."

## How long it's kept

- **Voice recordings:** kept until you manually delete them — by default, the life of the account.
- **Transcripts:** often stored separately and can survive audio deletion.
- **Request history / logs:** months to years, held for "service improvement".
- **Smart-home state logs:** rolling windows that vary per connected device.
- **After account deletion:** a purge tail of weeks to months across backups.
- **Aggregates & model contributions:** anonymized data and trained models keep the value indefinitely.
- **"As required by law":** legal holds override every deletion promise.
- **Identifiable vs de-identified:** the longest retention lands on copies stripped of direct identifiers, not the originals — raw identifiable recordings get the shorter windows. The catch: a "de-identified" clip still carries a voiceprint, so removing the account link does not always prevent re-identification.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars per data category with an "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Axis:** bars start at x0=220, max extent xMax=690; account-deleted marker at x=480.
- **Rows** (label, bar end x, color, note): Smart-home state logs 300 `#2980b9` "rolling ~90d"; Request history / logs 400 `#2980b9` "months–years"; Voice recordings 480 `#e67e22` "until you delete"; Transcripts 545 `#e67e22` "can outlive audio"; Backup copies 560 `#e74c3c` "purge tail"; Aggregates / trained models 690 `#e74c3c` "indefinite" with a right-pointing arrowhead.
- **Bar style:** height 16px, gap 22px, first at y=46; fill in row color at 45% alpha, 1px stroke in row color. Notes in 10px `#666` just right of each bar end (inside near left edge for full-length bars). Labels right-aligned at x=210, 11px `#2c3e50`.
- **Marker:** vertical dashed orange line (`#e67e22`, 2px, dash 5/4) at x=480 spanning the rows, labeled below in bold 11px `#e67e22` centered: "account deleted".
- **Caption (11px `#999`, bottom):** "Bars crossing the marker survive account deletion."

## What you get back

- **A typical export includes:** your voice recordings, request transcripts, shopping and to-do lists, reminders, device list, settings.
- **Typically excluded:** the voice prints themselves, inferred household composition and schedule, human-review annotations, ad interest segments, contributions baked into trained models, raw internal telemetry.

**Key point (callout box):** The asymmetry: the export returns what you *said*. The derived layer — who spoke it, who lives in the house, and when everyone is home — was built from your audio but is treated as the platform's data, not yours.

### Visualization (canvas `c4`, 720×320)

Two side-by-side comparison panels: export contents vs retained data.

- **Title (bold 13px `#1a5276`, top center):** "The export vs what exists".
- **Left panel (x=35, width 310, y=40, height 235, green `#27ae60` — 2px stroke, 8% alpha fill), bold 13px title "IN THE EXPORT",** items (12px `#2c3e50`, centered, 25px spacing): Voice recordings / Request transcripts / Shopping / to-do lists / Reminders & alarms / Device list & settings.
- **Right panel (x=375, width 310, red `#e74c3c`), bold 13px title "EXISTS BUT NOT RETURNED",** items: Voice prints (biometric); Household composition model; Daily schedule inference; Interest / ad segments; Human-review annotations; Model training contributions; Raw internal telemetry.
- **Caption (bottom center, 11px `#999`):** "You get the inputs back. The derived layer stays."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout or paragraph, right `<td>` (55%, `text-align: center`) holds the canvas. Cell borders `1px solid #e0e0e0`, padding 16px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with 6px bottom margin.
- **Callouts:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases are `display: block; margin: 0 auto`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`. No nav bar, no back/home links.
- Note: in regenerated HTML, any card/grid links referencing this page use the `.html` extension.
