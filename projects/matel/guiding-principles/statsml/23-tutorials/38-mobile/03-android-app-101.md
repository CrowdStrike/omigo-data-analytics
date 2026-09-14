# Android App 101

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Android App 101

**Subtitle:** Your first Android app is one Kotlin file describing one screen — Android Studio builds it, an emulator runs it, and the same screen later ships to a billion different phones

## One Screen, One Job: the Tip Calculator

**Tags:** `core idea` (blue), `Kotlin` (green), `Android Studio` (orange)

- **The app** — a tip calculator: type a bill amount, see the 18% tip and the total, nothing else
- **The tool** — Android Studio's New Project wizard ("Empty Activity") generates a runnable app skeleton
- **The language** — the generated code is Kotlin, Google's preferred Android language since 2019
- **The screen** — the UI lives in one function that *describes* the screen with Jetpack Compose
- **The run** — pressing Run boots a virtual Pixel (the emulator) and installs the app on it

*Example (italic):* Shortly after the wizard finishes, the emulator shows the tip screen: bill $60.00, tip $10.80, total $70.80.

**Key point:** An Android app at its smallest is one Kotlin file that describes one screen; Android Studio and the emulator turn that file into a phone you can poke.

### Visualization (canvas `c1`, 720×300)

Four-box flow diagram of the first-app loop: New Project wizard, the Kotlin file, the Run button, the emulator showing the tip screen.

- **Title (bold 15px, `#1a5276`, top center):** "From Empty Project to a Running Screen in Four Steps".
- **Layout:** four rounded boxes on one row centered at y=150, x centers ≈ 105, 285, 445, 615; boxes 130–150px wide, 64px tall, 8px radius; 3px `#6b7280` arrows between them.
- **Box 1 (blue `#2a78d6` border, fill `rgba(42,120,214,0.15)`):** "Android Studio\nNew Project (Empty Activity)" in 12px `#2c3e50`.
- **Box 2 (violet `#4a3aa7` border, fill `rgba(74,58,167,0.12)`):** "MainActivity.kt\nKotlin + Compose".
- **Box 3 (yellow `#c98500` border, fill `rgba(201,133,0,0.12)`):** "Run ▶\nbuild + install".
- **Box 4 (green `#008300` border, fill `rgba(0,131,0,0.12)`):** "Emulator (virtual Pixel)\nbill $60 → total $70.80".
- **Step labels (12px `#6b7280`, above each box at y=95):** "1. scaffold", "2. write the screen", "3. one click", "4. poke it".
- **Annotation (bold 13px green `#008300`, centered near y=245):** "one file, one screen, one click to run".
- **Caption (12px `#444`, bottom right):** "flow schematic; dollar figures illustrative".

## Bill $60, Tip 18%: State In, Pixels Out

**Tags:** `worked example` (blue), `Compose` (green), `state` (orange)

- **The state** — one line, `remember { mutableStateOf("60") }`, holds the bill text the user typed
- **The input** — a `TextField` reads that state and writes every keystroke back into it
- **The math** — plain Kotlin: on a $60 bill, tip = 60 × 0.18 = $10.80, total = $70.80
- **The output** — a `Text` composable shows "Total: $70.80"; there is no "update the label" line anywhere
- **The loop** — change the state and Compose recomposes: it re-runs the function and redraws the screen

*Example (italic):* Typing 60 into the bill field makes Compose recompute and redraw on its own — tip $10.80, total $70.80.

**Key point:** Compose UI is declarative — you describe the screen as a function of state; edit the state and the pixels follow, with no manual view updates.

### Visualization (canvas `c2`, 720×300)

Line chart of the tip amount on a $60 bill as the tip percentage slides from 0% to 30%, with the worked example's 18% point marked.

- **Title (bold 15px, `#1a5276`, top center):** "One Function of State: Tip on a $60 Bill vs Tip %".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = tip percent 0 to 30 with 12px `#444` tick labels every 5%; y = tip dollars 0 to 18, gridlines `#e5e9ef` at 4.50/9.00/13.50.
- **Tip line:** blue `#2a78d6` 3px line through percents `[0, 5, 10, 15, 18, 20, 25, 30]`, tip dollars `[0, 3.00, 6.00, 9.00, 10.80, 12.00, 15.00, 18.00]`.
- **Example marker:** green `#008300` filled 6px dot at (18, 10.80), dashed `#6b7280` (dash 4/3) drop lines to both axes, bold 12px green label "18% → $10.80 (total $70.80)" beside the dot.
- **Annotation (bold 13px violet `#4a3aa7`, near x=7, y=80):** "slide the state, the screen recomputes".
- **Caption (12px `#444`, bottom right):** "tip = 60 × percent, exact; bill amount illustrative".

## From the Emulator to a Billion Different Phones

**Tags:** `where it's used` (blue), `Play Store` (green), `APK / AAB` (orange)

- **The reach** — the same one-screen app can install on phones, tablets, and foldables from hundreds of makers
- **Fragmentation** — devices differ in screen size, chip type, and Android version; the emulator is only one of them
- **The APK** — the installable package: your compiled code plus images and resources for a device
- **The AAB** — the app bundle you upload to Play; the store generates a slimmed APK per device from it
- **The payoff** — a 38 MB do-everything APK becomes a 24 MB download on a mid-range phone after splitting

*Example (italic):* Uploaded as an AAB, the tip calculator downloads as 21 MB on a budget phone, 24 MB mid-range, and 26 MB on a tablet instead of 38 MB everywhere.

**Key point:** You build one project, but the Play Store path — signing, an AAB upload, per-device APKs — exists because "Android" is thousands of different devices, not one.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart comparing the one-size universal APK download against the per-device downloads Play generates from an AAB.

- **Title (bold 15px, `#1a5276`, top center):** "One Upload, Right-Sized Downloads: Universal APK vs AAB Splits".
- **Axis:** bars start at x=230, max width 440 for 40 MB (11 px per MB); vertical 2px `#999` baseline at x=230; left-aligned 12px `#444` row labels at x=20.
- **Rows (14px-tall bars, top to bottom at y = 70, 125, 180, 235):**
  - "universal APK — every device": orange `#d95926` bar width 418 (38 MB), 11px label "38 MB" at bar end
  - "AAB split — budget phone": blue `#2a78d6` bar width 231 (21 MB), label "21 MB"
  - "AAB split — mid-range phone": aqua `#199e70` bar width 264 (24 MB), label "24 MB"
  - "AAB split — tablet": violet `#4a3aa7` bar width 286 (26 MB), label "26 MB"
- **Annotation (bold 13px green `#008300`, right side near y=250):** "the store ships each phone only what it needs".
- **Caption (12px `#444`, bottom right):** "sizes illustrative; splitting mechanism real".

## Where's setText? Compose Redraws, You Don't

**Tags:** `common mistake` (red), `recomposition` (orange)

- **The reflex** — the old Android View system updated screens by hand: find the label, call `setText`
- **The confusion** — newcomers hunt for the widget handle to update; in Compose there isn't one
- **The trap** — `var bill = 60` compiles fine, but changing a plain variable never redraws anything
- **The fix** — `remember { mutableStateOf(...) }` is what Compose watches; only state edits trigger redraws
- **The rule** — recomposition re-runs your screen function when state it *read* changes, and only then

*Example (italic):* With a plain `var`, tapping "+$10" changes the number in memory but the screen stays frozen at $70.80; with `mutableStateOf` the same tap redraws to $82.60.

**Common mistake:** Mutating an ordinary variable and expecting the screen to follow. Compose only tracks `State` objects — no `mutableStateOf`, no recomposition, no redraw.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: a tap updating a plain variable (screen frozen) vs a tap updating Compose state (screen recomposes), shown as boxes with arrows.

- **Title (bold 15px, `#1a5276`, top center):** "Plain var vs mutableStateOf: Only One Redraws".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain var"; blue `#2a78d6` rounded box at x=170 labeled "tap +$10 → bill = 70" (12px), 3px arrow to a red `#e74c3c` box at x=430 labeled "no recomposition — screen stuck at $70.80" with bold 12px red "✗ frozen".
- **Row 2 (y=205), label:** "mutableStateOf"; blue box at x=170 labeled "tap +$10 → state = 70", 3px arrow to a green `#008300` box at x=380 labeled "recomposition runs", then arrow to a green box at x=580 labeled "screen shows $82.60" with bold 12px green "✓".
- **Box style:** 150–190px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "Compose watches state, not variables — describe, don't set".
- **Caption (12px `#444`, bottom right):** "dollar figures illustrative ($70 bill, 18% tip)".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the tip line on the $60 bill (percents 0–30, dollars 0–18.00, marker 18% → $10.80, total $70.80) is exact arithmetic on an illustrative bill; download sizes (38 / 21 / 24 / 26 MB) and the $82.60 after-tap total are invented and labeled illustrative; Kotlin as Google's preferred Android language since 2019 and Jetpack Compose's 2021 stable release are documented platform facts.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
