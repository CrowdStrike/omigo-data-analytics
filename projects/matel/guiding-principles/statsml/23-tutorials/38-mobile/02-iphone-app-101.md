# iPhone App 101

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** iPhone App 101

**Subtitle:** With Swift and Xcode, one screen — a working tip calculator — goes from empty project to running app in an afternoon

## One Screen, One Afternoon: a Tip Calculator

**Tags:** `core idea` (blue), `Swift` (green), `Xcode` (orange)

- **The app** — one screen: type a bill, drag a tip slider, read the total; nothing else
- **The start** — Xcode's File > New Project gives a runnable (blank) app before you write a line
- **The screen** — one SwiftUI view: a TextField for the bill, a Slider for the tip, a Text for the total
- **The rule** — the total label is computed from two numbers: bill $42.50 and tip 18%
- **The run** — press Run and the app opens in the iPhone simulator on your Mac
- **The size** — the whole thing is roughly 20 lines of Swift, written in one sitting

*Example (italic):* Type 42.50, drag the slider to 18% — the label reads "Tip $7.65 — Total $50.15" the instant the slider moves.

**Key point:** An iPhone app is not a huge undertaking to start — one SwiftUI view with two inputs and one computed label is a complete, runnable app.

### Visualization (canvas `c1`, 720×300)

Flow diagram of the one screen: two input controls feed a compute step that feeds the total label.

- **Title (bold 15px, `#1a5276`, top center):** "The Whole App: Two Inputs, One Rule, One Label".
- **Left column (state), blue `#2a78d6` rounded boxes 190×44, fill `rgba(42,120,214,0.15)`:** "TextField — bill $42.50" at (x=50, y=80); "Slider — tip 18%" at (x=50, y=180).
- **Middle box, violet `#4a3aa7` 200×50 at (x=290, y=127), fill `rgba(74,58,167,0.10)`:** "body recomputes: 42.50 × 1.18" (12px `#2c3e50`).
- **Right box, green `#008300` 170×50 at (x=530, y=127), fill `rgba(0,131,0,0.12)`:** "Text — Total $50.15" (bold 12px).
- **Arrows:** 3px `#6b7280` from each blue box to the violet box, and from the violet box to the green box.
- **Annotation (bold 13px blue `#2a78d6`, centered near y=262):** "the screen is just a function of two numbers".
- **Caption (12px `#444`, bottom right):** "amounts illustrative".

## The Four Pieces Behind Those 20 Lines

**Tags:** `worked example` (blue), `SwiftUI` (green), `@State` (orange)

- **Swift** — Apple's language (introduced 2014); typed, compiled, and what the whole app is written in
- **SwiftUI** — the view framework (introduced 2019); you declare what the screen shows, not how to draw it
- **@State** — a marked variable (`@State var tipPercent = 18.0`) that SwiftUI watches for changes
- **The simulator** — a fake iPhone on your Mac; Run compiles the Swift and boots the app into it
- **The sketch** — a `ContentView` struct: two @State vars (bill, tipPercent), then TextField, Slider, Text
- **Hand-check** — total = 42.50 × (1 + 18/100) = 42.50 × 1.18 = $50.15; the label must show exactly that

*Example (italic):* Drag the slider from 15% to 20% and the total walks $48.88 → $50.15 (at 18%) → $51.00 — no update code written anywhere.

**Key point:** The Slider is bound to `@State tipPercent`; when the value changes, SwiftUI recomputes the body and the total label redraws itself.

### Visualization (canvas `c2`, 720×300)

Bar chart: total bill at five slider positions, showing the label recomputing as tipPercent changes; the current 18% position highlighted.

- **Title (bold 15px, `#1a5276`, top center):** "Drag the Slider, the Total Recomputes Itself (bill $42.50)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = tip percent categories `[10, 15, 18, 20, 25]` labeled "10%"–"25%" (12px `#444`); y = total dollars 0 to 60, gridlines `#e5e9ef` at 15/30/45 with 12px `#444` labels.
- **Bars:** totals `[46.75, 48.88, 50.15, 51.00, 53.13]`, 70px wide, fill `rgba(42,120,214,0.35)` with 2px `#2a78d6` border; the 18% bar solid green `#008300`.
- **Value labels:** bold 12px `#2c3e50` on top of each bar ("$46.75" ... "$53.13"), the 18% one bold green.
- **Annotation (bold 13px green `#008300`, above the 18% bar near y=55):** "@State tipPercent = 18 → label shows $50.15".
- **Caption (12px `#444`, bottom right):** "bill fixed at $42.50; amounts illustrative".

## From Your Simulator to a Stranger's Phone

**Tags:** `where it's used` (blue), `App Store` (green), `signing` (orange)

- **The simulator** — free and instant, but it only proves the app works on your own Mac
- **Signing** — to run on a real iPhone, Xcode signs the app with your Apple developer identity
- **TestFlight** — Apple's beta service; upload a build and invited testers install it on their phones
- **App Review** — every App Store submission is reviewed by Apple before it goes live
- **The path** — simulator → your iPhone → TestFlight testers → review → App Store; each step widens the audience
- **The gate** — review is the one step you don't control; plan for it, don't be surprised by it

*Example (italic):* The tip calculator runs in the simulator on day one, on your own iPhone that evening, and reaches TestFlight testers before it ever faces review.

**Key point:** Writing the screen is the small half — signing, TestFlight, and App Review are the pipeline that turns code on your Mac into an app on someone else's phone.

### Visualization (canvas `c3`, 720×300)

Horizontal pipeline diagram: five stages from Run button to App Store, each box with an audience label beneath it.

- **Title (bold 15px, `#1a5276`, top center):** "The Road from the Run Button to the App Store".
- **Stages (rounded boxes 116×46 at y=120, x = 30, 168, 306, 444, 582), 3px `#6b7280` arrows between:**
  - "Simulator" — blue `#2a78d6`, fill `rgba(42,120,214,0.15)`
  - "Your iPhone" — blue `#2a78d6`, fill `rgba(42,120,214,0.15)`, 11px `#6b7280` tag "signing" above
  - "TestFlight" — aqua `#199e70`, fill `rgba(25,158,112,0.12)`
  - "App Review" — orange `#d95926`, fill `rgba(217,89,38,0.12)`
  - "App Store" — green `#008300`, fill `rgba(0,131,0,0.12)`, bold border
- **Audience labels (12px `#444`, centered under each box at y=190):** "just you", "you + device", "invited testers", "Apple", "everyone".
- **Annotation (bold 13px orange `#d95926`, centered near y=240):** "App Review is the one gate you don't control".
- **Caption (12px `#444`, bottom right):** "stage order factual; box sizes schematic".

## You Never Set the Label — You Change the State

**Tags:** `common mistake` (red), `@State` (orange)

- **The instinct** — newcomers look for the label object so they can write the new total into it
- **No handle** — SwiftUI gives you no label to grab; the Text is rebuilt from state on every change
- **The miss** — declare `var tipPercent` without `@State` and the slider can't even change it
- **Why** — SwiftUI views are throwaway structs, recreated constantly; plain vars don't survive
- **The fix** — mark it `@State`; SwiftUI stores the value outside the struct and redraws on change
- **The habit** — every "update the screen" thought should become a "change which variable?" thought

*Example (italic):* With plain `var tipPercent`, Xcode refuses to compile the slider binding; add `@State` and the same line works and the label follows.

**Common mistake:** Trying to push new text into the label. In SwiftUI the label is read-only output — you change `@State`, and the framework rebuilds the screen for you.

### Visualization (canvas `c4`, 720×300)

Two-row flow diagram: mutating a plain var (fails) vs changing an @State var (screen redraws).

- **Title (bold 15px, `#1a5276`, top center):** "Change the State, Not the Label".
- **Row 1 (y=95), label 12px `#444` at x=20:** "plain var"; blue `#2a78d6` rounded box at x=150 labeled "var tipPercent = 18.0" (12px), 3px arrow to a red `#e74c3c` box at x=420 labeled "slider can't write — won't compile" with bold 12px red "✗ screen never updates".
- **Row 2 (y=205), label:** "@State var"; blue box at x=150 "@State var tipPercent = 18.0", 3px arrow to a green `#008300` box at x=400 labeled "SwiftUI stores it, watches it", then arrow to a green box at x=590 labeled "label: $50.15" with bold 12px green "✓".
- **Box style:** 150–200px wide, 40px tall, 8px radius, fills `rgba(42,120,214,0.15)` / `rgba(231,76,60,0.12)` / `rgba(0,131,0,0.12)`, 12px `#2c3e50` text.
- **Annotation (bold 13px orange `#d95926`, centered near y=270):** "views are recreated constantly; @State is what survives".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); the bill $42.50 is invented and labeled illustrative, and the totals `[46.75, 48.88, 50.15, 51.00, 53.13]` follow exactly from total = 42.50 × (1 + tip/100) at tips `[10, 15, 18, 20, 25]`; platform facts (Swift 2014, SwiftUI 2019, App Store review before release) are documented; pipeline box sizes are schematic.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
