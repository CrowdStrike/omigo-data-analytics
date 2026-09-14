# Electron

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Electron

**Subtitle:** Electron packages a web page together with its own browser and Node.js, so one web codebase becomes a Mac, Windows, and Linux desktop app — the desktop app that's secretly a browser

## The Coffee-Shop App That Ships as a Web Page in a Box

**Tags:** `core idea` (blue), `one codebase` (green), `desktop` (orange)

- **The team** — three developers build BrewDesk, an order screen coffee shops run on the counter PC
- **The ask** — shops want a real desktop app: an icon, offline use, and access to the receipt printer
- **The problem** — Mac, Windows, and Linux each need their own native app: three codebases, three skill sets
- **The trick** — Electron packages the existing web page with its own browser (Chromium) and Node.js
- **The result** — one HTML/CSS/JS codebase builds a Mac .dmg, a Windows .exe, and a Linux .deb

*Example (italic):* The team writes the order screen once as a web page and ships installable desktop apps for all three systems in the same week.

**Key point:** An Electron app is a web app shipped inside its own private browser — the "desktop app" window is really a Chromium page with the address bar removed.

### Visualization (canvas `c1`, 720×300)

Left-to-right flow diagram: one web codebase box feeding an Electron packaging box, which fans out to three installer boxes.

- **Title (bold 15px, `#1a5276`, top center):** "One Web Codebase, Three Desktop Installers".
- **Source box:** blue `#2a78d6` rounded box at x=40, y=125, 170×60, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` two-line label "BrewDesk web code / HTML + CSS + JS — 6 MB".
- **Packager box:** violet `#4a3aa7` rounded box at x=280, y=125, 180×60, fill `rgba(74,58,167,0.12)`, 12px label "Electron / bundles Chromium + Node.js".
- **Installer boxes:** three aqua `#199e70` rounded boxes at x=550, y=60 / 130 / 200, each 130×46, fill `rgba(25,158,112,0.12)`, 12px labels "Mac — .dmg", "Windows — .exe", "Linux — .deb".
- **Arrows:** 3px `#6b7280` arrow from source to packager; three 3px `#6b7280` arrows from the packager's right edge fanning to the three installer boxes.
- **Box style:** 8px corner radius, 2px borders in each box's line color.
- **Annotation (bold 13px green `#008300`, centered near y=280):** "write the page once — install it anywhere".
- **Caption (12px `#444`, bottom right):** "sizes illustrative".

## Counting BrewDesk's Processes and Megabytes

**Tags:** `worked example` (blue), `main + renderer` (green)

- **The main process** — one Node.js process owns the windows, menus, files, and the receipt printer
- **The renderers** — each open window is its own Chromium process rendering plain HTML
- **The count** — orders window + settings window = 1 main + 2 renderers = 3 processes
- **The RAM** — main 90 MB plus 110 MB per renderer: 90 + 110 + 110 = 310 MB total
- **The disk** — the 6 MB web app ships in a 120 MB installer; the difference is the bundled browser

*Example (italic):* The counter PC's task manager shows three BrewDesk entries — one main process and two windows — totalling 310 MB, where a native version used 40 MB.

**Key point:** Electron = one Node.js main process for OS work plus one Chromium renderer per window; the app's own logic is the smallest slice of everything installed.

### Visualization (canvas `c2`, 720×300)

Stacked bar vs plain bar: BrewDesk-on-Electron memory broken into its three processes next to a single small native-app bar.

- **Title (bold 15px, `#1a5276`, top center):** "Where BrewDesk's 310 MB of RAM Goes (Native App: 40 MB)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = MB 0 to 320, gridlines `#e5e9ef` at 80/160/240 with 12px `#444` tick labels.
- **Electron bar (x=180, width 140):** stacked segments bottom-up in MB `[90, 110, 110]` — main process blue `#2a78d6` fill `rgba(42,120,214,0.35)`, orders renderer green `#008300` fill `rgba(0,131,0,0.30)`, settings renderer aqua `#199e70` fill `rgba(25,158,112,0.30)`; 12px `#2c3e50` segment labels "main 90", "orders window 110", "settings window 110"; bold 13px `#1a5276` total "310 MB" above the bar; 12px `#444` label "BrewDesk (Electron)" below the baseline.
- **Native bar (x=460, width 140):** single violet `#4a3aa7` bar, fill `rgba(74,58,167,0.25)`, height for 40 MB; bold 13px `#1a5276` "40 MB" above; 12px `#444` label "same app, native" below the baseline.
- **Annotation (bold 13px magenta `#d55181`, near x=330, y=70):** "two windows = two browsers in RAM".
- **Caption (12px `#444`, bottom right):** "MB figures illustrative — hand-check: 90 + 110 + 110 = 310".

## Why Web Teams Ship Desktop Apps This Way

**Tags:** `where it's used` (blue), `one team` (green), `trade-off` (orange)

- **The pattern** — many familiar desktop tools (editors, chat clients, note apps) are Electron inside
- **The skills** — a web developer already knows everything needed; no Swift, C#, or GTK to learn
- **The speed** — a feature lands once and reaches all three platforms in the same release
- **The reuse** — the desktop app and the website can literally share the same UI code
- **The trade** — you pay in download size and RAM to save on engineering time

*Example (italic):* BrewDesk's three-person team estimates 30 dev-weeks for three native apps versus 12 dev-weeks for one Electron app.

**Key point:** Electron trades machine resources for developer time — one web team covers three platforms, which is why so much of the modern desktop is quietly a browser.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart: dev-weeks to cover all three platforms, native (three stacked platform segments) vs Electron (one short bar).

- **Title (bold 15px, `#1a5276`, top center):** "Effort to Cover Mac + Windows + Linux (dev-weeks)".
- **Axis:** vertical 2px `#999` baseline at x=190, bars extend right, scale 16 px per dev-week (max width 480 = 30 weeks); light `#e5e9ef` gridlines at 10/20/30 weeks with 12px `#444` labels along y=260.
- **Row 1 (bar top y=95, height 34), left label 12px `#444` at x=20 "three native apps":** three segments of 10 weeks each (160px): Mac blue `#2a78d6` fill `rgba(42,120,214,0.35)`, Windows aqua `#199e70` fill `rgba(25,158,112,0.30)`, Linux violet `#4a3aa7` fill `rgba(74,58,167,0.25)`; 12px `#2c3e50` in-bar labels "Mac 10", "Win 10", "Linux 10"; bold 12px `#1a5276` "30" at the bar end.
- **Row 2 (bar top y=175, height 34), left label "one Electron app":** single green `#008300` bar 12 weeks (192px), fill `rgba(0,131,0,0.30)`, in-bar label "all three 12"; bold 12px `#1a5276` "12" at the bar end.
- **Annotation (bold 13px green `#008300`, near x=420, y=195):** "one codebase saves 18 dev-weeks".
- **Caption (12px `#444`, bottom right):** "dev-week estimates illustrative".

## Five Apps, Five Copies of the Same Browser

**Tags:** `common mistake` (red), `RAM cost` (orange)

- **The assumption** — people expect five small desktop apps to use less memory than one browser
- **The reality** — every Electron app bundles and runs its own private Chromium; nothing is shared
- **The math** — five Electron apps at 280 MB each hold 5 × 280 = 1,400 MB of RAM
- **The contrast** — one browser with the same five pages as tabs shares one engine: about 800 MB
- **The disk** — same story on disk: five installs mean five separate ~120 MB browser copies

*Example (italic):* A laptop running a chat app, notes, music, mail, and a to-do app has five hidden Chromiums going before the user opens their actual browser.

**Common mistake:** Judging an Electron app like a native one. Each app is a full browser instance — the RAM and disk cost repeats per app, because bundled Chromiums are never shared.

### Visualization (canvas `c4`, 720×300)

Stacked bar vs stacked bar: five separate Electron apps (five equal segments) next to one browser holding the same five pages as tabs.

- **Title (bold 15px, `#1a5276`, top center):** "Five Electron Apps vs Five Tabs in One Browser (RAM)".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 180; y = MB 0 to 1500, gridlines `#e5e9ef` at 500/1000 with 12px `#444` tick labels.
- **Electron bar (x=180, width 140):** five stacked 280 MB segments (segment heights 280 × 0.12 = 33.6px each, total 168px) cycling fills `rgba(42,120,214,0.35)` / `rgba(25,158,112,0.30)` / `rgba(74,58,167,0.25)` / `rgba(201,133,0,0.30)` / `rgba(213,81,129,0.25)` with 2px matching borders blue `#2a78d6` / aqua `#199e70` / violet `#4a3aa7` / yellow `#c98500` / magenta `#d55181`; 11px `#2c3e50` segment labels "chat 280", "notes 280", "music 280", "mail 280", "to-do 280"; bold 13px `#1a5276` "1,400 MB" above; 12px `#444` label "5 Electron apps" below the baseline.
- **Browser bar (x=460, width 140):** bottom segment "shared engine 350" blue `#2a78d6` fill `rgba(42,120,214,0.35)` (42px), then five 90 MB tab segments (10.8px each) green `#008300` fill `rgba(0,131,0,0.30)` with thin white separators, bracketed 12px `#2c3e50` side label "5 tabs × 90"; bold 13px `#1a5276` "800 MB" above; 12px `#444` label "1 browser, 5 tabs" below the baseline.
- **Annotation (bold 13px red `#e74c3c`, near x=330, y=60):** "600 MB spent on duplicate browsers".
- **Caption (12px `#444`, bottom right):** "MB figures illustrative — hand-check: 5 × 280 = 1,400; 350 + 5 × 90 = 800".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all values are the hardcoded numbers above (no randomness); every figure is invented and labeled illustrative — process RAM `[90, 110, 110]` totalling 310 MB vs native 40 MB, web code 6 MB inside a 120 MB installer, dev-weeks `[10, 10, 10]` native vs 12 Electron, and five 280 MB apps (1,400 MB) vs one browser at 350 + 5 × 90 = 800 MB.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
