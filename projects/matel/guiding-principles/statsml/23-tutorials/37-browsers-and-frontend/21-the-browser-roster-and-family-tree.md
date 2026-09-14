# The Browser Roster & Family Tree

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** The Browser Roster & Family Tree

**Subtitle:** Every browser name you will meet in a user-agent log — the fifteen live ones, the retired ones, and the single codebase most of them descend from

## Every Name That Shows Up in a User-Agent Log

**Tags:** `core idea` (blue), `roster` (green), `user agents` (orange)

- **The long tail** — an analytics browser column has four big rows and then a dozen names nobody recognises
- **Chrome, Safari, Edge, Firefox** — the four majors, each from a different large organisation
- **Chromium** — Google's open-source base that Chrome itself is built from; also shipped as its own browser
- **Brave, Vivaldi, Opera** — independent shells on Chromium, differing in features and defaults, not engine
- **DuckDuckGo, Tor Browser** — privacy-first; DuckDuckGo wraps the platform engine, Tor is a hardened Firefox
- **Samsung Internet, Yandex, UC Browser** — huge by volume, nearly invisible in Western-centric reporting
- **Arc, Zen, LibreWolf, SeaMonkey** — niche shells that still appear in logs and still must render correctly

*Example (italic):* A store's log shows twelve distinct browser names; ten of them render with an engine already covered by testing Chrome.

**Key point:** The roster is long but shallow — a dozen names collapse to three rendering engines, so a new name in the log is usually a new interface, not a new rendering behaviour.

### Visualization (canvas `c1`, 720×360 — taller than the others so the nine-chip Blink column is not clipped)

Grouped roster diagram: three engine columns, each listing the browser brands that ship on it, as stacked labelled chips.

- **Title (bold 15px, `#1a5276`, top center):** "Fifteen Browser Names, Three Engines".
- **Column headers (bold 14px, y=52):** "Blink" at x=155 in blue `#2a78d6`, "WebKit" at x=395 in aqua `#199e70`, "Gecko" at x=615 in orange `#d95926`; a rounded 8px header box behind each (width 200/180/150, height 28) filled at 0.15 alpha of its colour with a 2px border in that colour.
- **Blink column chips (x=60, width 190, height 22, 4px gap, starting y=82):** "Chrome", "Chromium", "Edge", "Opera", "Brave", "Vivaldi", "Samsung Internet", "Arc", "Yandex".
- **WebKit column chips (x=305, width 180, starting y=82):** "Safari", "any iOS browser*", "Orion", "GNOME Web".
- **Gecko column chips (x=540, width 150, starting y=82):** "Firefox", "Tor Browser", "LibreWolf", "SeaMonkey", "Zen".
- **Chip style:** white fill, 1px `#e5e9ef` border, 6px radius, 12px `#2c3e50` centred label.
- **Annotation (bold 12px violet `#4a3aa7`, y=332, centered):** "nine of the fifteen names are Chromium shells".
- **Caption (12px `#444`, bottom right):** "*outside the EU; engine assignments documented".

## One Codebase, Nine Brands: What a Shell Actually Changes

**Tags:** `worked example` (blue), `chromium shells` (green), `defaults` (orange)

- **The base** — Chromium is open source, so anyone can ship a browser without writing a rendering engine
- **What is inherited** — layout, CSS support, JS engine, developer tools, security sandbox, release cadence
- **What a shell changes** — default search engine, tracker blocking, tab interface, sync backend, telemetry
- **Brave** — blocks trackers and third-party cookies by default, so the same page loads fewer requests
- **Vivaldi** — adds power-user interface features: tab stacking, split view, built-in notes panel
- **Opera** — the oldest name here (1995), rebuilt on Chromium in 2013 after retiring its own Presto engine
- **The consequence** — a shell changes what a page *receives*, never how the page gets *drawn*

*Example (italic):* A page with four tracking scripts issues four fewer network requests in Brave than in Chrome, yet lays out pixel-identically in both.

**Key point:** A Chromium shell differs in policy, not in rendering — which is exactly why blocking-sensitive analytics breaks across shells while layout does not.

### Visualization (canvas `c2`, 720×300)

Split diagram: one shared "Chromium base" block at the bottom feeding three shell boxes, with an inherited-vs-changed two-column list.

- **Title (bold 15px, `#1a5276`, top center):** "Same Base, Different Policies".
- **Base block (rounded 8px, x=210 width 300, y=252 height 34):** fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, bold 13px `#2a78d6` label "Chromium base — layout, CSS, JS, sandbox".
- **Shell boxes (rounded 8px, 150×46, y=186, at x = 100, 285, 470):** "Chrome" filled `rgba(42,120,214,0.10)`, "Brave" filled `rgba(0,131,0,0.10)` with `#008300` border, "Vivaldi" filled `rgba(74,58,167,0.10)` with `#4a3aa7` border; bold 12px name at y=204 in matching colours, with the per-shell caption inside the same box at y=222 so no connector crosses text.
- **Connectors:** 2px lines from the base block top (360,252) up to the bottom edge of each shell box (y=232), coloured to the shell.
- **Per-shell caption (11px `#444`, inside each shell box at y=222):** "default search + sync", "trackers blocked by default", "tab stacks, split view".
- **Inherited/changed lists (top half):** bold 12px header "inherited (identical)" in blue at x=140 y=78 and "changed by the shell" in orange `#d95926` at x=500 y=78; below each, 11px `#2c3e50` items at 18px line spacing — left: "layout engine", "CSS feature set", "JS engine", "security sandbox"; right: "default search", "tracker blocking", "tab interface", "telemetry".
- **Divider:** dashed 1px `#6b7280` vertical line at x=350 from y=64 to y=160.
- **Annotation (bold 12px green `#008300`, y=170, at x=500):** "policy differences, not pixel differences".
- **Caption (12px `#444`, bottom LEFT at x=15 y=294, to clear the base block):** "feature attributions documented".

## The Family Tree, Including the Dead Branches

**Tags:** `worked example` (blue), `lineage` (green), `retired` (red)

- **Netscape (1994)** — the first mass-market browser; its 1998 engine rewrite became Gecko and was open-sourced
- **Internet Explorer (1995–2022)** — Microsoft's browser on the Trident engine, with support ending in June 2022
- **Edge Legacy (2015)** — IE's replacement on EdgeHTML, retired when Edge moved to Chromium in 2020
- **Opera Presto (1995–2013)** — Opera's own engine for eighteen years, dropped for Chromium
- **KHTML (1998)** — KDE's engine, forked by Apple into WebKit in 2002 and shipped in Safari in 2003
- **Blink (2013)** — Google's fork of WebKit, now the base under nine of the live brands
- **The reading** — four engines died and their brands moved to survivors, so the engine count keeps shrinking

*Example (italic):* Microsoft has shipped three engines under two brands — Trident, then EdgeHTML, then Google's Blink — while the logo changed only once.

**Key point:** Every retirement moved a brand onto someone else's engine, never the reverse — the tree has been consolidating for twenty-five years, not branching.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline family tree, 1994 to 2026, with live lanes drawn solid and retired lanes ending in a stop marker.

- **Title (bold 15px, `#1a5276`, top center):** "Engine Lineage: Four Retirements, Three Survivors".
- **X scale:** `xOf(year) = 70 + (year - 1994) / (2026 - 1994) * 580`; year gridlines 1px `#e5e9ef` at 1994, 2003, 2013, 2020, 2026 with 12px `#444` labels along the baseline y=278.
- **Lane 1 — Gecko (y=66, orange `#d95926`, 3px solid):** starts at 1998 with a filled dot, runs to 2026; bold 12px label above: "Gecko (1998, from Netscape) → Mozilla Suite → Firefox"; a filled orange dot at 2017 with an 11px `#d95926` label below reading "Quantum: Rust parts swapped in".
- **Lane 2 — KHTML→WebKit (y=110):** grey `#6b7280` 3px from 1998 to 2002, then aqua `#199e70` 3px to 2026; bold 12px aqua label "WebKit (Apple fork 2002) → Safari"; 11px grey label "KHTML" at the segment start.
- **Lane 3 — Blink (y=154, blue `#2a78d6`, 3px):** branches from the WebKit lane at 2013 (dogleg down) and runs to 2026; bold 12px label "Blink (Google fork 2013) → nine brands".
- **Lane 4 — Trident (y=192, red `#e74c3c`, 2px):** 1997 to 2022, ending in a 10px red "✕" marker; 12px label "Trident → Internet Explorer, retired 2022".
- **Lane 5 — EdgeHTML (y=222, red `#e74c3c`, 2px):** 2015 to 2020, ending in a red "✕"; 11px label "EdgeHTML → Edge Legacy, retired 2020".
- **Lane 6 — Presto (y=250, red `#e74c3c`, 2px):** 1995 to 2013, ending in a red "✕"; 11px label "Presto → Opera, retired 2013".
- **Merge arrows (dashed 1px `#6b7280`):** from the EdgeHTML stop marker and the Presto stop marker up to the Blink lane, each labelled 10px `#6b7280` "brand moves to Blink".
- **Annotation (bold 12px magenta `#d55181`, near x=430, y=88):** "every retirement fed a survivor".
- **Caption (12px `#444`, bottom right):** "fork and retirement years documented; lane positions schematic".

## "Mozilla" Names Three Different Things, and Only One Is Testable

**Tags:** `common mistake` (red), `user agents` (orange), `naming` (blue)

- **Mozilla the browser** — the Mozilla Application Suite, a 2002–2004 browser plus mail client on Gecko
- **Its end** — discontinued in 2005; volunteers continue that same codebase today as SeaMonkey
- **Mozilla the organisation** — the non-profit foundation and its corporation, which today ships Firefox
- **Firefox's origin** — split out of the Suite in 2002 as Phoenix, shipping 1.0 in 2004 on the same Gecko
- **The Rust part** — Servo, a from-scratch Rust engine begun in 2012, never shipped as Firefox's engine
- **Quantum (2017)** — Firefox 57 put Servo's Rust pieces inside Gecko: Stylo for CSS, WebRender for paint
- **Mozilla the token** — nearly every browser's user-agent opens with literal `Mozilla/5.0`, Chrome included
- **Internet Explorer** — retired June 2022, so an IE row today means a bot, a spoof, or Edge's IE mode

*Example (italic):* Grepping logs for "Mozilla" to count Firefox users returns essentially every visit, because Chrome and Safari both announce themselves as `Mozilla/5.0` too.

**Common mistake:** Reading "Mozilla" as one thing. It was a real browser, it is an organisation, and it is a meaningless prefix on almost every request — and Firefox inherited Gecko rather than replacing it, so the Rust rewrite is a partial transplant inside a 1998 codebase, not a new engine.

### Visualization (canvas `c4`, 720×300)

Annotated user-agent string dissection: three real-shaped strings stacked, with the misleading `Mozilla/5.0` prefix highlighted in every one.

- **Title (bold 15px, `#1a5276`, top center):** "Three Different Browsers, One Misleading Prefix".
- **Rows (y = 90, 155, 220), each a monospace 12px string on a white row with a 1px `#e5e9ef` bottom rule:**
  - Row 1 label (bold 12px blue `#2a78d6`, x=20): "Chrome"; string: `Mozilla/5.0 (Windows NT 10.0) ... Chrome/xxx Safari/537.36`
  - Row 2 label (bold 12px aqua `#199e70`, x=20): "Safari"; string: `Mozilla/5.0 (Macintosh; Intel Mac OS X) ... Version/xx Safari/605`
  - Row 3 label (bold 12px orange `#d95926`, x=20): "Firefox"; string: `Mozilla/5.0 (X11; Linux x86_64) ... Gecko/xxx Firefox/xxx`
- **Prefix highlight:** a `rgba(231,76,60,0.15)` rectangle behind the `Mozilla/5.0` token in each row (x=100, width 92, height 20), with a 1px `#e74c3c` border.
- **Real-signal highlight:** a `rgba(0,131,0,0.15)` rectangle with 1px `#008300` border behind the token that actually identifies the browser — `Chrome/xxx`, `Version/xx`, `Firefox/xxx` respectively — positioned at the right end of each string.
- **Bracket + label (bold 12px red `#e74c3c`, x=146, y=68):** a short vertical bracket over the three prefix highlights labelled "identical in all three — carries no information".
- **Right-side label (bold 12px green `#008300`, x=560, y=262):** "this token is the signal".
- **Annotation (bold 12px `#e74c3c`, centered, y=282):** "an Internet Explorer row in a current report is a bot, not a user".
- **Caption (12px `#444`, bottom right):** "strings abbreviated with '...' and version digits masked; shape is real".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300 except `c1` at 720×360; shared `setup(id)` helper reads each canvas's logical size from its `width`/`height` attributes once and caches it in `dataset` (the attributes are overwritten with device pixels, so they cannot be re-read on resize), then sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms). Shared `roundedBox` and `arrowLine` helpers as in the sibling pages.
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** no random generation; all content is hardcoded literal arrays. Documented facts: engine assignments per brand; Netscape 1994 and the 1998 Gecko rewrite/open-sourcing; the Mozilla Application Suite shipping 2002–2004 and discontinued 2005, continued as SeaMonkey; Firefox starting as Phoenix in 2002 and shipping 1.0 in 2004 on the same C++ Gecko; Servo started 2012 as a separate Rust engine and never shipped as Firefox's engine; Firefox 57 "Quantum" (2017) landing Servo's Stylo and WebRender inside Gecko; IE support ending June 2022; Edge Legacy on EdgeHTML retired when Edge moved to Chromium in 2020; Opera founded 1995 and Presto dropped for Chromium in 2013; KHTML 1998, Apple's WebKit fork 2002, Safari shipping 2003; Google's Blink fork 2013; the iOS WebKit requirement outside the EU since 2024; the `Mozilla/5.0` compatibility prefix in nearly all user-agent strings; Edge's IE mode reporting as Edge. The "nine of fifteen" count in c1 is arithmetic over the listed roster. User-agent strings in c4 are abbreviated and version-masked, not measured captures.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
