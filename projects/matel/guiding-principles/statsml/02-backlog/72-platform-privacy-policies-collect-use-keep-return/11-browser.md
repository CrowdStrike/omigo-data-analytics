# Browser

**Page type:** detail page (two-column obj-table layout: text left 45%, canvas right 55%, one row per section)
**HTML title tag:** Browser

**Subtitle:** The observation point for everything you do outside apps — and with sync on, that observation point reports to an account.

**Disclaimer (orange callout):** **Disclaimer:** Generalized, illustrative synthesis of common practices for this platform category. No company names, no reference to any specific product's policy. Real policies vary and change constantly — read the actual policy for the actual product.

## What gets collected

- **Declared:** bookmarks, saved passwords, autofill identities — names, addresses, phone numbers, payment cards — and the sync account itself.
- **Incidental:** full browsing history with timestamps, uploaded to the account by sync and merged across every device; address-bar keystrokes sent to a suggestion service as you type; extension installs — an extension with "read and change all your data on all websites" permission can see every page, form, and password field; feature and page-load telemetry; crash reports that can include open-tab URLs; the fingerprintable surface — canvas rendering, installed fonts, GPU model — that identifies the browser without any cookies.
- **Inferred:** interest categories derived from history; demographics; purchase intent from shopping and comparison pages.

**Key point (blue-left-border box):** Private mode is the surprise: it only deletes the local trace. The network operator, employer, DNS resolver, and every site you visit still see the traffic — and fingerprinting still works.

### Visualization (canvas `c1`, 720×400)

Grouped horizontal bar chart: assumed vs realistic extent of collection, per data category.

- **Title (bold 13px `#1a5276`, top center):** "What people assume is collected vs realistic extent (illustrative)".
- **Legend (top, at x=200 and x=300):** swatch `rgba(26,82,118,0.35)` labeled "assumed"; swatch `rgba(231,76,60,0.55)` labeled "realistic extent". Labels in `#2c3e50` 11px.
- **Rows (label, assumed a, realistic b — values on 0–100 scale):**
  - History synced to account: a=35, b=90
  - Address-bar keystrokes sent live: a=10, b=85
  - Autofill: cards, addresses, IDs: a=45, b=88
  - Extension access to every page: a=15, b=80
  - Feature / page telemetry: a=20, b=75
  - Fingerprint (no cookies needed): a=8, b=82
  - Private-mode traffic still visible: a=12, b=90
- **Geometry:** right-aligned labels at x=225, bars start at x=235, max bar width 395px, bar height 13px, group gap 18px, start y=54. Assumed bar on top (`rgba(26,82,118,0.35)`), realistic bar below (`rgba(231,76,60,0.55)`). Numeric value printed just past each bar end: assumed value in `#999`, realistic value in `#e74c3c`.
- **Caption (bottom center, `#999` 11px):** "Numbers are illustrative relative extents, not measured statistics."

## How it gets used

- **Provide the service:** sync, autofill, password fill, tab restore across devices.
- **Rank and recommend:** address-bar suggestions and new-tab content ranked from your history and partial keystrokes.
- **Ad targeting and measurement:** interest categories computed from history can feed ad selection and campaign measurement, in-browser or account-side.
- **Model training:** telemetry trains autofill, translation, phishing-detection, and suggestion models.
- **Sharing:** keystrokes flow to the default search partner; URLs to safe-browsing services; usage stats to affiliates.

### Visualization (canvas `c2`, 720×340)

Bipartite flow diagram: left column of data categories connected by gray bezier arrows to right column of uses.

- **Title (bold 13px `#1a5276`, top center):** "From data category to use".
- **Left boxes (200×36 at x=30, centered on y; box stroke in its color, fill same color at 0.12 alpha, bold 11px `#2c3e50` label):**
  - History + sync (y=50, `#1a5276`)
  - Keystrokes / suggestions (y=105, `#2980b9`)
  - Autofill vault (y=160, `#8e44ad`)
  - Telemetry / crash logs (y=215, `#e67e22`)
  - Fingerprint surface (y=270, `#e74c3c`)
- **Right boxes (200×36 at x=490, same styling):**
  - Provide sync + autofill (y=50, `#27ae60`)
  - Rank suggestions / new tab (y=105, `#2980b9`)
  - Ad interest + measurement (y=160, `#e74c3c`)
  - Feature model training (y=215, `#8e44ad`)
  - Search + safety partners (y=270, `#e67e22`)
- **Arrows (bezier curves, `#bbb` 1px, small filled arrowhead at right end), [left index, right index] pairs:** [0,0],[0,1],[0,2],[1,1],[1,4],[2,0],[2,3],[3,3],[3,4],[4,2],[0,3],[1,2].
- **Caption (bottom center, `#999` 11px):** "History is the hub: it feeds service, ranking, ads, and training at once."

## How long it's kept

- **Local history:** from ~90 days to forever depending on settings — most users never change the default.
- **Synced history and passwords:** server-side for the life of the account.
- **Telemetry:** rolling windows, commonly 6–18 months, then folded into aggregates.
- **After deletion:** synced data lingers in server backups for weeks to months after you clear it.
- **Safe-browsing / abuse logs:** longer retention justified by security.
- **"As required by law" and aggregates:** effectively indefinite buckets — the longest retention applies to copies stripped of direct identifiers, while identifiable records get the shorter windows. The catch: browsing patterns remain re-identifiable even after PII is stripped.

### Visualization (canvas `c3`, 720×360)

Horizontal retention-timeline bar chart with a dashed "account deleted" marker line.

- **Title (bold 13px `#1a5276`, top center):** "Retention per data category (illustrative)".
- **Geometry:** bars start at x0=210, timeline max x1=690; bar height 18px, gap 22px, start y=50. Bars filled at 0.45 alpha of their color plus a 1px solid stroke of the same color. Right-aligned row labels in `#2c3e50` 11px; note text in `#666` 10px to the right of each bar.
- **Rows (label, bar end x, color, note):**
  - Suggestion keystroke logs: end=320, `#27ae60`, "weeks–months"
  - Telemetry: end=385, `#2980b9`, "6–18 mo rolling"
  - Synced history / passwords: end=480, `#e67e22`, "account life + backup tail"
  - Autofill vault: end=480, `#e67e22`, "account life + backup tail"
  - Safe-browsing / abuse logs: end=600, `#e74c3c`, "multi-yr security hold"
  - Aggregated usage stats: end=690, `#e74c3c`, "indefinite", with a filled arrowhead continuing past the bar end (runs off the timeline).
- **Marker:** vertical dashed red line (`#e74c3c`, dash 6/4, width 2) at x=430 spanning the rows, labeled below in bold red: "account deleted".
- **Caption (bottom center, `#999` 11px):** "time →   (bar lengths illustrative; \"clear history\" clears the visible copy first)".

## What you get back

- **In a typical export:** bookmarks, history, saved passwords, autofill entries, settings, extensions list.
- **Typically not returned:** the inferred interest categories; fingerprint-derived identifiers; telemetry and crash logs; the suggestion-service keystroke logs; whatever your extensions collected — those are separate parties with separate policies.

**Key point (blue-left-border box):** The asymmetry: the export is a copy of your inputs — the profile built *from* those inputs, and the third-party copies made along the way, are not in the file.

### Visualization (canvas `c4`, 720×330)

Two side-by-side panels comparing the export with what is withheld.

- **Title (bold 13px `#1a5276`, top center):** "The export vs what exists but is not returned".
- **Panels:** 300px wide × 250px tall starting at y=36; fill = panel color at 0.10 alpha, 2px stroke of panel color; bold 12px title in panel color; items centered in `#2c3e50` 11px, 22px line spacing.
  - Left panel at x=30, green `#27ae60`, title "IN THE EXPORT", items: "Bookmarks", "Browsing history", "Saved passwords", "Autofill entries", "Settings + extensions list".
  - Right panel at x=390, red `#e74c3c`, title "EXISTS BUT NOT RETURNED", items: "Inferred interest categories", "Fingerprint-derived identifiers", "Telemetry + crash logs", "Suggestion keystroke logs", "Safe-browsing URL checks", "What extensions collected", "Search-partner query copies", "Internal abuse / risk signals".
- **Caption (bottom center, `#999` 11px):** "You get back your inputs. The profile and the third-party copies stay behind."

## Regeneration instructions

- **Layout:** detail page with `.obj-table`: full-width `border-collapse: collapse` table, one `<tr>` per section; left `<td>` (45%) holds `.obj-title` + `<ul>` bullets and optional `.key-point` box, right `<td>` (55%, `text-align: center`) holds the canvas. Table cell borders `1px solid #e0e0e0`, padding 16px. Above the table: h1, `.subtitle`, `.disclaimer`.
- **Page CSS:** body system sans-serif (-apple-system stack), `line-height 1.6`, text `#2c3e50`, white background, padding 30px 40px; h1 1.8rem `#1a5276`; `.subtitle` `#666` 1.05em; `.disclaimer` background `#fdf3e7`, left border `3px solid #e67e22`, padding 10px 14px, 0.9em, text `#7d5a29`; `.obj-title` bold `#1a5276` 1.1em; `li` 0.93em; `.key-point` background `#f8f9fa`, left border `3px solid #1a5276`, padding 10px 14px, 0.93em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, bar fill `rgba(26,82,118,0.35)`, red bar fill `rgba(231,76,60,0.55)`, gray text `#666`/`#999`.
- **Canvas:** declare intrinsic `width`/`height` attributes per chart as given; scale by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates) via a shared `setupCanvas(id, w, h)` helper; `canvas { display: block; margin: 0 auto; }`.
- Any links in regenerated HTML use `.html` extensions.
