# Android Studio

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Android Studio

**Subtitle:** IntelliJ wearing Google's platform — Google rented a world-class IDE foundation from JetBrains and built only the Android layer on top

## The 2013 Choice: Build, Keep Patching, or Partner

**Tags:** `core idea` (blue), `build vs partner` (green), `2013` (orange)

- **The old door** — before 2013, official Android tooling was ADT, a plugin bolted onto Eclipse
- **Three roads** — build an IDE from scratch, keep patching the Eclipse plugin, or build on someone else's
- **The pick** — at Google I/O 2013, Google announced Android Studio, built on JetBrains' IntelliJ platform
- **Why it was open** — IntelliJ IDEA Community Edition is open source, so Google could build on it legally
- **The sunset** — Android Studio hit 1.0 in December 2014; Google ended Eclipse ADT support in 2015

*Example (italic):* Instead of spending years rebuilding an editor, Google shipped a full official IDE about 18 months after the announcement — the editor was already world-class on day one.

**Key point:** A platform vendor does not have to own every layer — Google rented the IDE foundation and spent its effort only where Android is genuinely different.

### Visualization (canvas `c1`, 720×300)

Horizontal timeline of the documented milestones from the Eclipse era to the Kotlin-first era, showing how one 2013 decision kept compounding.

- **Title (bold 15px, `#1a5276`, top center):** "One Decision in 2013, Compounding Through 2019".
- **Timeline:** 3px `#1a5276` horizontal line at y=170 from x=60 to x=690; year mapped as x = 80 + (year − 2013) × 100 (2013→80 ... 2019→680).
- **Event markers (8px filled circles on the line, labels alternate above/below in bold 12px, date in 11px `#6b7280`):**
  - x=80, blue `#2a78d6`, above: "Android Studio announced" / "I/O 2013"
  - x=180, blue `#2a78d6`, below: "version 1.0 ships" / "Dec 2014"
  - x=280, orange `#d95926`, above: "Eclipse ADT support ends" / "2015"
  - x=480, green `#008300`, below: "Kotlin made first-class" / "I/O 2017"
  - x=680, green `#008300`, above: "Kotlin-first announced" / "I/O 2019"
- **Era bands (14px tall rounded bars under the line at y=225):** `rgba(217,89,38,0.25)` bar from x=60 to x=280 labeled "Eclipse plugin era" (11px `#d95926`); `rgba(42,120,214,0.25)` bar from x=280 to x=690 labeled "IntelliJ-based era" (11px `#2a78d6`).
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=70, x≈430):** "rent the foundation, build the platform layer".
- **Caption (12px `#444`, bottom right):** "milestone dates as publicly announced".

## Tracing the Anatomy: Who Built Which Layer

**Tags:** `worked example` (blue), `anatomy` (green)

- **Rented layers** — the editor, the semantic code model (PSI), refactorings, and inspections come from IntelliJ
- **Built layers** — SDK/emulator management, the visual layout editor, Gradle integration, APK analyzers, profilers
- **Hand-check** — of 9 major subsystems on the chart, 4 are rented from IntelliJ and 5 are built by Google
- **The hard parts** — the emulator fleet (5 API levels × 4 screen sizes = 20 test configs, illustrative), and adb
- **The famous pain** — Gradle build times are a long-documented community complaint the Google layer keeps attacking

*Example (italic):* Rename a Kotlin variable and IntelliJ's engine does the refactoring; press Run and Google's layer builds the APK with Gradle, boots an emulator, and installs over adb — one action per company.

**Key point:** Every keystroke-level feature is JetBrains' semantic engine; everything that touches a device, an APK, or the Play pipeline is Google's added layer.

### Visualization (canvas `c2`, 720×300)

Two-column layer diagram: the rented IntelliJ foundation (blue, left) and the Google-built Android layer (green, right), with an arrow showing the second stacks on the first.

- **Title (bold 15px, `#1a5276`, top center):** "9 Subsystems: 4 Rented From IntelliJ, 5 Built by Google".
- **Column headers (bold 13px at y=52):** "IntelliJ platform (rented)" in `#2a78d6` centered at x=200; "Android layer (built)" in `#008300` centered at x=530.
- **Left column boxes (blue):** 4 rounded boxes at x=70, width 260, height 34, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` centered text, tops at y = 70, 114, 158, 202: "editor + semantic model (PSI)", "refactorings & inspections", "debugger & VCS UI", "plugin platform".
- **Right column boxes (green):** 5 rounded boxes at x=400, width 260, height 34, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, tops at y = 70, 114, 158, 202, 246: "SDK & emulator manager", "layout editor & resource tools", "Gradle build integration", "APK analyzer & profilers", "adb device bridge".
- **Arrow:** 3px `#6b7280` arrow from x=330 to x=400 at y=160, bold 12px `#6b7280` label "stacks on" above it.
- **Annotation (bold 12px orange `#d95926`, below left column at y=270, centered x=200):** "Google wrote none of these".
- **Caption (12px `#444`, bottom right):** "subsystem grouping simplified".

## The Compounding Move: Kotlin

**Tags:** `where it's used` (blue), `Kotlin` (green), `strategy` (orange)

- **The twist** — Kotlin is a JetBrains language, born inside the same company whose IDE Google rented
- **First-class** — at I/O 2017 Google made Kotlin an officially supported Android language
- **Kotlin-first** — at I/O 2019 Google announced Android development would be Kotlin-first
- **The smoothing** — the IDE partnership meant Kotlin tooling was already excellent inside Android Studio
- **The lesson** — language, IDE, and platform strategies reinforce each other instead of being separate bets

*Example (italic):* On the chart, Kotlin use among professional Android developers goes from roughly 20% in 2017 to about 50% by 2019 and 63% by 2021 (illustrative) — the announcements land on an adoption curve the IDE made easy.

**Key point:** Renting the IDE did not just save build cost — it put Google inside JetBrains' ecosystem, so adopting JetBrains' language later came with the tooling already in place.

### Visualization (canvas `c3`, 720×300)

Line chart of Kotlin adoption among professional Android developers (illustrative), with the two documented announcements marked on the curve.

- **Title (bold 15px, `#1a5276`, top center):** "Kotlin Adoption Rides the Announcements (illustrative)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = years 2016 to 2021, 12px `#444` tick labels each year; y = "% of pro Android devs using Kotlin" 0 to 100, gridlines `#e5e9ef` at 25/50/75 with 12px `#444` labels.
- **Adoption line:** green `#008300` 3px line with 5px dots through years `[2016, 2017, 2018, 2019, 2020, 2021]`, percent `[5, 20, 35, 50, 60, 63]`.
- **Marker 1:** vertical dashed `#6b7280` (dash 4/3) line at 2017, bold 12px `#2a78d6` label "I/O 2017: first-class" near its top.
- **Marker 2:** vertical dashed `#6b7280` line at 2019, bold 12px `#4a3aa7` label "I/O 2019: Kotlin-first" near its top.
- **Annotation (bold 13px green `#008300`, near 2020, y=95):** "63% by 2021 — the IDE made the switch cheap".
- **Caption (12px `#444`, bottom right):** "percentages illustrative; announcement dates documented".

## Not a Walled Garden Like Its Sibling

**Tags:** `common mistake` (red), `platform control` (orange)

- **The confusion** — assuming "official IDE" means the same locked door on every platform
- **Build anywhere** — Android Studio runs on Windows, macOS, and Linux; Apple's toolchain requires a Mac
- **Other doors** — Android has documented alternative toolchains and command-line builds outside the IDE
- **Sideloading** — Android apps install without any store; iOS mostly goes through Apple
- **Still the default** — Android Studio remains the strongly recommended path, just not the only one

*Example (italic):* A student on a Linux laptop can build, sideload, and debug an Android app end to end — the same student cannot ship an iOS app without buying a Mac.

**Common mistake:** Treating all mandatory-tool platforms as equally closed. Android Studio is the official door, but the documented contrast is that Android leaves the side doors unlocked.

### Visualization (canvas `c4`, 720×300)

Two-column check/cross grid comparing the openness of the two official doors along three documented dimensions.

- **Title (bold 15px, `#1a5276`, top center):** "Two Official Doors, Different Locks".
- **Column headers (bold 13px at y=60):** "iOS / Xcode" in `#d95926` centered at x=340; "Android / Android Studio" in `#008300` centered at x=560.
- **Row labels (12px `#444`, left-aligned at x=20, vertically centered per row; rows at y = 85, 145, 205, each cell 44px tall):** "build OS choice", "toolchain required to build", "install without a store".
- **Cells:** rounded 200px-wide boxes at x=240 (iOS column) and x=460 (Android column), 8px radius, 12px `#2c3e50` centered text:
  - Row 1: iOS `rgba(231,76,60,0.12)` "macOS only ✗"; Android `rgba(0,131,0,0.12)` "any OS ✓"
  - Row 2: iOS `rgba(231,76,60,0.12)` "Apple toolchain to ship ✗"; Android `rgba(0,131,0,0.12)` "CLI / alternatives exist ✓"
  - Row 3: iOS `rgba(231,76,60,0.12)` "store review, few exceptions ✗"; Android `rgba(0,131,0,0.12)` "sideloading allowed ✓"
- **Annotation (bold 13px magenta `#d55181`, centered near y=280):** "official and recommended — but not the only way in".
- **Caption (12px `#444`, bottom right):** "platform policies as publicly documented".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); Kotlin adoption percentages `[5, 20, 35, 50, 60, 63]` and the 20-config emulator matrix are invented and labeled illustrative; milestone dates (I/O 2013 announcement, 1.0 in Dec 2014, ADT sunset in 2015, Kotlin first-class at I/O 2017, Kotlin-first at I/O 2019) and the platform-control contrasts are publicly documented.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
