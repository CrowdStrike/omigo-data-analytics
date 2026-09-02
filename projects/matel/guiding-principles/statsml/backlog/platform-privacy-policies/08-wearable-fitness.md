# Wearable / Fitness

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Wearable / Fitness

**Subtitle:** A 24/7 physiology stream — heart rate, sleep, cycles, GPS routes — collected as consumer data, outside the protection of medical privacy law.

**Disclaimer callout:** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** profile (age, height, weight, sex), logged workouts, food and water logs, menstrual cycle entries, goals and streaks.
- **Incidental:** continuous heart rate and HRV around the clock, sleep stages every night, skin temperature, SpO2, glucose readings from a paired CGM sensor, GPS route of every outdoor workout, step counts, raw accelerometer motion, device and sync telemetry.
- **Inferred:** stress scores, "fitness age", pregnancy likelihood, illness onset (resting heart-rate shifts), sleep-disorder flags, home and work location from where routes start and end.

**Key point (callout box):** You log a few workouts; the device samples your body 24/7. And because the vendor is not a healthcare provider, this hospital-grade physiology sits outside medical privacy law — governed only by a consumer terms-of-service that can change at any time.

### Visualization (canvas `c1`, 720×420)

Grouped horizontal bar chart: assumed vs realistic collection extent, two bars per row.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (row at y=30, swatches 14×10 starting at x=239 and x=335):** "assumed" — fill `rgba(26,82,118,0.35)`; "realistic extent" — fill `rgba(231,76,60,0.55)`. Legend text 11px `#2c3e50`.
- **Rows** (label, assumed %, actual %): Logged workouts 90/95; 24/7 heart rate & HRV 35/95; Sleep stages every night 30/90; Skin temperature / SpO2 15/75; GPS route of every run 40/90; Menstrual cycle data 25/80; Illness / pregnancy inference 5/60; Home location (route starts) 10/85.
- **Layout:** right-aligned labels at x=225 (11px `#2c3e50`), bars start at x=239, max width 380px (scale 0–100), bar height 12px, assumed bar on top, actual bar 3px below, group spacing 42px, first group at y=52.
- **Caption (bottom center, 11px `#999`):** "Bar lengths are illustrative, not measured percentages."

## How it gets used

- **Provide the service:** dashboards, streaks, coaching, alerts.
- **Rank / recommend:** workout suggestions, recovery and readiness advice tuned to your physiology.
- **Model training:** population-scale physiology models; your nights of sleep tune everyone's sleep-stage algorithm.
- **Research programs:** health studies, sometimes opt-in by default.
- **Ads / marketing:** activity-based segments and premium-tier upsells.
- **Sharing:** "wellness programs" pipe activity data to employers and insurers; aggregated route heatmaps have publicly revealed sensitive locations.

### Visualization (canvas `c2`, 720×340)

Two-column flow diagram: data-category boxes on the left linked by arrows to use boxes on the right.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (x=40, width 165, height 32 centered on y = 55/110/165/220/275):** Heart rate / HRV `#e74c3c`; Sleep stages `#8e44ad`; GPS routes `#2980b9`; Cycle & body logs `#e67e22`; Steps & motion `#1a5276`. Style per box: stroke in its color (1.5px), fill same color at 12% alpha, bold 12px centered label in its color.
- **Right boxes (x=470, width 215, same y positions):** Dashboards & coaching `#27ae60`; Readiness / stress scores `#8e44ad`; Population model training `#1a5276`; Route heatmaps (public) `#e74c3c`; Wellness programs (employer) `#e67e22`.
- **Links (gray `#bbb` lines 1.2px with filled arrowheads), left → right:** Heart rate / HRV → Dashboards & coaching; Heart rate / HRV → Readiness / stress scores; Sleep stages → Readiness / stress scores; Sleep stages → Population model training; GPS routes → Route heatmaps (public); Cycle & body logs → Population model training; Steps & motion → Dashboards & coaching; Steps & motion → Wellness programs (employer).
- **Caption (bottom center, 11px `#999`):** "The same route that draws your map can end up in a public heatmap or an employer report."

## How long it's kept

- **Sensor history:** the life of the account — years of minute-level physiology, by design.
- **After account deletion:** a typical 30–90 day purge window before data leaves primary systems.
- **Backups:** a tail of weeks to months after the purge.
- **Aggregates & derived stats:** anonymized population data survives deletion indefinitely.
- **Wellness-program copies:** whatever the employer or insurer received follows *their* retention rules, outside your control.
- **"As required by law":** legal holds can suspend deletion indefinitely.
- **Identifiable vs de-identified:** the longest retention applies to copies stripped of direct identifiers, not the originals — raw identifiable records get the shorter windows. The catch: minute-level physiology is itself a fingerprint, so stripping PII does not always prevent re-identification.

### Visualization (canvas `c3`, 720×330)

Horizontal retention-timeline bars per data category with an "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Axis:** bars start at x0=220, max extent xMax=690; account-deleted marker at x=480.
- **Rows** (label, bar end x, color, note): Sync / device telemetry 340 `#2980b9` "months"; Raw sensor streams 430 `#2980b9` "downsampled over time"; Workouts & health history 480 `#e67e22` "life of account"; Backup copies 555 `#e67e22` "30–90d purge + tail"; Wellness-program copies 690 `#e74c3c` "their rules" with right-pointing arrowhead; Aggregates / trained models 690 `#e74c3c` "indefinite" with right-pointing arrowhead.
- **Bar style:** height 16px, gap 22px, first at y=46; fill in row color at 45% alpha, 1px stroke in row color. Notes in 10px `#666` just right of each bar end (inside near left edge for full-length bars). Labels right-aligned at x=210, 11px `#2c3e50`.
- **Marker:** vertical dashed orange line (`#e67e22`, 2px, dash 5/4) at x=480 spanning the rows, labeled below in bold 11px `#e67e22` centered: "account deleted".
- **Caption (11px `#999`, bottom):** "Bars crossing the marker survive account deletion."

## What you get back

- **A typical export includes:** workout list, daily step and heart-rate summaries, sleep logs, profile and goals, sometimes raw sensor files.
- **Typically excluded:** inferred stress / fitness-age / pregnancy scores, illness-onset flags, model training contributions, ad segments, the copies already sent to wellness programs, internal telemetry — and raw streams are often downsampled.

**Key point (callout box):** The asymmetry: you get back summaries of your body. The *predictions* about your body — the part with real consequence for insurance, employment, and health — stay with the platform.

### Visualization (canvas `c4`, 720×320)

Two side-by-side comparison panels: export contents vs retained data.

- **Title (bold 13px `#1a5276`, top center):** "The export vs what exists".
- **Left panel (x=35, width 310, y=40, height 235, green `#27ae60` — 2px stroke, 8% alpha fill), bold 13px title "IN THE EXPORT",** items (12px `#2c3e50`, centered, 25px spacing): Workout list & GPS files / Daily step / HR summaries / Sleep logs / Profile, goals, streaks / Some raw sensor files.
- **Right panel (x=375, width 310, red `#e74c3c`), bold 13px title "EXISTS BUT NOT RETURNED",** items: Stress / readiness scores; "Fitness age" & derived indices; Illness / pregnancy inferences; Model training contributions; Copies sent to wellness programs; Ad / marketing segments; Full-resolution raw streams.
- **Caption (bottom center, 11px `#999`):** "Summaries come back. Predictions stay."

## Regeneration instructions

- **Layout:** detail page using `.obj-table` — full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` callout, right `<td>` (55%, `text-align: center`) holds the canvas. Cell borders `1px solid #e0e0e0`, padding 16px.
- **Page style:** body system sans-serif (-apple-system stack), white background, text `#2c3e50`, padding 30px 40px, line-height 1.6; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em with 6px bottom margin.
- **Callouts:** `.disclaimer` — background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`. `.key-point` — background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart; a shared `setupCanvas(id, w, h)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Canvases are `display: block; margin: 0 auto`.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`/`#2c3e50`. No nav bar, no back/home links.
- Note: in regenerated HTML, any card/grid links referencing this page use the `.html` extension.
