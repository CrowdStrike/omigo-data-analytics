# Mobile OS / App Store

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Mobile OS / App Store

**Subtitle:** The layer beneath every app — the operating system and its store see what no single app can, and they see it for every app at once.

**Disclaimer (orange callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** account details, payment method, store reviews and ratings, support requests, opted-in cloud backups.
- **Incidental:** a device-wide advertising identifier shared across every app; the full app inventory — installs, uninstalls, and usage duration per app; precise location from wifi, bluetooth, and cell-tower triangulation even when GPS is off; motion-sensor streams; keyboard telemetry (typing stats, sometimes text snippets for model improvement); the push-notification token graph linking device to every service that can reach it; crash and diagnostic telemetry, typically on by default; store browsing and search — including apps you looked at and never installed.
- **Inferred:** demographics and income band from the app inventory alone; activity state (walking, driving, sleeping) from the accelerometer; home and work from location clustering; interest segments for ads.

**Key point (blue-left-border box):** The app list alone is the surprise: which apps you have installed predicts age, gender, income band, and relationship status with usable accuracy — no message content needed.

### Visualization (canvas `c1`, 720×430)

Grouped horizontal bar chart: assumed vs realistic extent of collection, per data category.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x=200 and x=300):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent". Labels in `#2c3e50` 11px.
- **Rows (label, assumed a, realistic b — values on 0–100 scale):**
  - Cross-app ad identifier: a=25, b=92
  - Full app inventory + usage time: a=40, b=90
  - Location with GPS off: a=10, b=80
  - Motion-sensor activity: a=8, b=65
  - Keyboard telemetry: a=5, b=55
  - Store searches never acted on: a=20, b=85
  - Crash / diagnostic uploads: a=30, b=88
  - Demographics inferred from apps: a=5, b=75
- **Geometry:** right-aligned labels at x=220, bars start at x=230, max bar width 400px, bar height 13px, group gap 18px, start y=54. Assumed bar on top (`rgba(26,82,118,0.35)`), realistic bar below (`rgba(231,76,60,0.55)`). Numeric value printed just past each bar end: assumed value in `#999`, realistic value in `#e74c3c`.
- **Caption (bottom center, `#999` 11px):** "Numbers are illustrative relative extents, not measured statistics."

## How it gets used

- **Provide the service:** updates, sync, backup, find-my-device, push delivery.
- **Rank and recommend:** store search results and "suggested for you" apps, tuned by your inventory and browsing.
- **Ad targeting and measurement:** the advertising identifier lets the store's ad network — and third-party networks — target and attribute across every app on the device.
- **Model training:** keyboard, speech, and diagnostic data train OS-level models.
- **Sharing:** the identifier and coarse segments flow to app developers and ad partners; diagnostics flow to hardware and carrier partners.

### Visualization (canvas `c2`, 720×340)

Bipartite flow diagram: left column of data categories connected by gray bezier arrows to right column of uses.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (200×36 at x=30, centered on y; color = box stroke, fill same color at 0.12 alpha, bold 11px `#2c3e50` label):**
  - App inventory + usage (y=50, `#1a5276`)
  - Location + sensors (y=105, `#2980b9`)
  - Store browsing / search (y=160, `#8e44ad`)
  - Diagnostics / telemetry (y=215, `#e67e22`)
  - Ad identifier (y=270, `#e74c3c`)
- **Right boxes (200×36 at x=490, same styling):**
  - Provide device services (y=50, `#27ae60`)
  - Store ranking / recommend (y=105, `#2980b9`)
  - Ad targeting + measurement (y=160, `#e74c3c`)
  - OS model training (y=215, `#8e44ad`)
  - Sharing with partners (y=270, `#e67e22`)
- **Arrows (bezier curves, `#bbb` 1px, small filled arrowhead at right end), [left index, right index] pairs:** [0,0],[0,1],[0,2],[1,0],[1,2],[2,1],[2,2],[3,0],[3,3],[3,4],[4,2],[4,4],[1,3].
- **Caption (bottom center, `#999` 11px):** "Every category feeds at least one use beyond providing the service."

## How long it's kept

- **Active account:** app inventory, purchase history, and store activity are kept for the life of the account.
- **Location history:** retained until you find the setting and clear it — often years by default.
- **Diagnostics and crash logs:** rolling windows, commonly months to a couple of years.
- **After deletion:** a 30–90 day grace window, then a backup tail where copies persist in cold storage.
- **"As required by law":** purchase and payment records typically sit in a multi-year legal bucket regardless of deletion.
- **Aggregated / de-identified analytics:** effectively indefinite — the longest retention applies to copies stripped of direct identifiers, not the originals, which get the shorter windows. The catch: stripping PII does not always prevent re-identification.

### Visualization (canvas `c3`, 720×360)

Horizontal retention-timeline bar chart with a dashed "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Geometry:** bars start at x0=210, timeline max x1=690; bar height 18px, gap 22px, start y=50. Bars filled at 0.45 alpha of their color plus a 1px solid stroke of the same color. Right-aligned row labels in `#2c3e50` 11px; note text in `#666` 10px to the right of each bar.
- **Rows (label, bar end x, color, note):**
  - Store search logs: end=340, `#27ae60`, "months"
  - Diagnostics / crash logs: end=390, `#2980b9`, "1–2 yrs rolling"
  - Location history: end=470, `#e67e22`, "until cleared + tail"
  - App inventory / activity: end=510, `#e67e22`, "account life + grace"
  - Purchase / payment records: end=620, `#e74c3c`, "legal hold, multi-yr"
  - Aggregated analytics: end=690, `#e74c3c`, "indefinite", with a filled arrowhead continuing past the bar end (runs off the timeline).
- **Marker:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at x=430 spanning the rows, labeled below in bold red: "account deleted".
- **Caption (bottom center, `#999` 11px):** "time →   (bar lengths illustrative; several categories outlive the account)".

## What you get back

- **In a typical export:** account details and settings, purchase and download history, reviews, opted-in backups.
- **Typically not returned:** the ad-identifier profile and interest segments; inferences drawn from your app inventory; sensor and keyboard telemetry; what other apps reported about you keyed to the shared identifier; internal fraud and risk scores; diagnostic logs.

**Key point (blue-left-border box):** The asymmetry: the export returns what you gave; it rarely returns what was derived. The derived layer — segments, inferences, cross-app linkages — is usually the more valuable half, and it stays behind.

### Visualization (canvas `c4`, 720×330)

Two side-by-side panels comparing the export with what is withheld.

- **Title (bold 13px `#1a5276`, top center):** "The export vs what exists but is not returned".
- **Panels:** 300px wide × 250px tall starting at y=36; fill = panel color at 0.10 alpha, 2px stroke of panel color; bold 12px title in panel color; items centered in `#2c3e50` 11px, 22px line spacing.
  - Left panel at x=30, green `#27ae60`, title "IN THE EXPORT", items: "Account details + settings", "Purchase / download history", "Reviews and ratings", "Opted-in backups", "Support tickets".
  - Right panel at x=390, red `#e74c3c`, title "EXISTS BUT NOT RETURNED", items: "Ad-identifier profile + segments", "Inferences from app inventory", "Sensor + keyboard telemetry", "Cross-app linkage records", "Fraud / risk scores", "Diagnostic logs", "Location-derived home / work", "Store browsing behavior detail".
- **Caption (bottom center, `#999` 11px):** "You get back your inputs. The derived layer stays behind."

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` box, right `<td>` (55%, `text-align: center`) holds the canvas. Table cell borders `1px solid #e0e0e0`, padding 16px. Above the table: h1, `.subtitle`, `.disclaimer`.
- **Page CSS:** body system sans-serif (-apple-system stack), `line-height 1.6`, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; `canvas { display: block; margin: 0 auto; }`.
- Any links in regenerated HTML use `.html` extensions.
