# Xcode

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Xcode

**Subtitle:** The only door to Apple platforms — every iPhone, iPad, and Watch app must pass through Xcode to be signed and shipped, and Xcode only runs on a Mac

## Every Road to the iPhone Runs Through One App

**Tags:** `core idea` (blue), `mandatory tool` (orange), `Apple platforms` (green)

- **The team** — two developers build the same app twice: an Android build and an iOS build
- **Android side** — the Android build runs on their Linux laptop, their Windows desktop, any CI box
- **iOS side** — the iOS build refuses to exist without Xcode, and Xcode refuses to run outside macOS
- **No detour** — React Native and Flutter write the code elsewhere, but the final iOS build still calls Xcode
- **The machinery** — signing, provisioning, simulators, and App Store submission all live inside it

*Example (italic):* The team's Flutter app is 100% Dart, yet release day still means one person on a Mac clicking through Xcode — the framework's own docs say so.

**Key point:** Xcode is not the best way to ship to Apple devices — it is the only way; whatever tool starts the build, Xcode's toolchain finishes it.

### Visualization (canvas `c1`, 720×300)

Convergence flow diagram: four source-framework boxes on the left all funnel into one central Xcode toolchain box, which alone connects to the device/App Store box on the right.

- **Title (bold 15px, `#1a5276`, top center):** "Four Ways In, One Way Out: Every iOS Build Ends in Xcode".
- **Left column (x=30, boxes 150px wide, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text), at y = 70, 125, 180, 235:** "Swift / SwiftUI", "React Native (JS)", "Flutter (Dart)", "Unity (C#)".
- **Center box (x=300, y=115, 170px wide, 70px tall, fill `rgba(217,89,38,0.15)`, 3px `#d95926` border):** two lines of 12px bold text "Xcode toolchain" / "xcodebuild · codesign".
- **Right box (x=550, y=125, 150px wide, 50px tall, fill `rgba(0,131,0,0.12)`, 2px `#008300` border):** "App Store / iPhone".
- **Arrows:** 2px `#6b7280` lines with filled triangle heads from each left box's right edge to the center box's left edge; one 3px `#008300` arrow from center box to right box.
- **Blocked path:** dashed red `#e74c3c` (dash 5/4) line from below the left column (y=270) curving toward the right box, crossed by a bold 14px red "✗" at x≈450 with 12px red label "no path around the box".
- **Annotation (bold 13px `#d95926`, under the center box at y≈215):** "runs only on macOS".
- **Caption (12px `#444`, bottom right):** "framework list illustrative; the funnel is documented".

## One Release, Five Artifacts: the Signing Chain

**Tags:** `worked example` (blue), `code signing` (orange)

- **The certificate** — a developer certificate proves who signs; development certs expire after 1 year
- **The App ID** — one identifier (`com.team.coffeelog`) names which app the signature covers
- **The devices** — a test profile lists device UDIDs; Apple caps it at 100 devices per type per year
- **The profile** — a provisioning profile bundles certificate + App ID + device list into one file
- **Entitlements** — push notifications and keychain access are opt-in flags baked into the signature
- **The output** — only after all five line up does the export step produce a signed `.ipa` the phone will run

*Example (italic):* The team's release needs 1 certificate, 1 App ID, 12 test-device UDIDs, 1 profile, and 2 entitlements — get any one wrong and the install fails with a one-line error.

**Key point:** Signing is a chain of five artifacts that must agree exactly; Xcode is where the chain is assembled, which is a big part of why it cannot be skipped.

### Visualization (canvas `c2`, 720×300)

Assembly flow diagram: three input boxes merge into a provisioning profile, entitlements join, and the chain ends in a signed `.ipa` — with a second row showing the one-mismatch failure.

- **Title (bold 15px, `#1a5276`, top center):** "The Signing Chain: Five Things That Must Agree".
- **Row 1 inputs (three boxes at x=25, y = 55 / 105 / 155, each 165px wide, 34px tall, 8px radius, fill `rgba(42,120,214,0.15)`, 12px `#2c3e50` text):** "certificate — who (1 yr)", "App ID — which app", "12 device UDIDs — where".
- **Merge box (x=255, y=95, 175px wide, 54px tall, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border):** "provisioning profile"; 2px `#6b7280` arrows from each input box into its left edge.
- **Entitlements box (x=255, y=175, 175px wide, 34px tall, fill `rgba(201,133,0,0.12)`, 2px `#c98500` border):** "entitlements: push, keychain"; arrow up into the flow.
- **Output box (x=500, y=110, 190px wide, 44px tall, fill `rgba(0,131,0,0.12)`, 3px `#008300` border, bold text):** "signed .ipa ✓"; 3px `#008300` arrow from the merge box.
- **Failure row (y=255):** 12px `#444` label "one stale UDID:" at x=25, then a small red `#e74c3c` box (x=140, 250px wide, 30px tall, fill `rgba(231,76,60,0.12)`) reading "install fails on that device", bold 12px red "✗" at its right.
- **Annotation (bold 13px violet `#4a3aa7`, x≈500, y=230):** "all five must match — no partial credit".
- **Caption (12px `#444`, bottom right):** "counts from the worked example; 100-device cap and 1-yr expiry are Apple's documented limits".

## Captive Users and the Price of the Gate

**Tags:** `where it's used` (blue), `platform economics` (orange), `incentives` (red)

- **No exit** — Xcode's crashes on big projects and slow indexing are perennial complaints, yet nobody leaves
- **Why not** — leaving Xcode means leaving iOS; complaints don't convert to switching, so pressure is weak
- **The contrast** — editors like the ones that compete for users must earn every install; a gate tool need not
- **Sells Macs** — developing for iPhone requires buying a Mac: the tool is one strand of vertical integration
- **CI bill** — iOS CI means macOS build machines; hosted macOS minutes cost about 10× Linux minutes
- **Full stack** — hardware + OS + Swift + Xcode + App Store + 15–30% commission form one documented loop

*Example (italic):* On a typical hosted CI, the team's Linux minute lists at $0.008 while the macOS minute for the iOS build lists at $0.08 — the same test suite costs 10× to run.

**Key point:** When users are captive, tool quality and tool success decouple — the vendor's incentive is the platform's revenue, not the editor's polish.

### Visualization (canvas `c3`, 720×300)

Horizontal bar chart of hosted CI list price per build minute by runner OS, showing the macOS premium the iOS gate imposes.

- **Title (bold 15px, `#1a5276`, top center):** "The Gate's CI Bill: Price per Hosted Build Minute".
- **Axis:** vertical 2px `#999` baseline at x=180, bars extend right, max width 480; x scale linear $0 to $0.08 with 12px `#444` gridline labels at $0.02/$0.04/$0.06/$0.08 (gridlines `#e5e9ef`).
- **Rows (18px tall bars at y = 90, 150, 210), left-aligned 12px `#444` labels at x=20:**
  - "Linux runner": blue `#2a78d6` bar width 48 (=$0.008), 12px value label "$0.008" at bar end
  - "Windows runner": aqua `#199e70` bar width 96 (=$0.016), label "$0.016"
  - "macOS runner (iOS builds)": orange `#d95926` bar width 480 (=$0.08), bold label "$0.08"
- **Bracket:** thin `#6b7280` bracket spanning the Linux and macOS bar ends with bold 13px `#d95926` label "10× — because the build must run on a Mac" at x≈360, y=60.
- **Annotation (bold 12px `#e74c3c`, x≈360, y=255):** "captive demand: the price faces no substitute".
- **Caption (12px `#444`, bottom right):** "typical hosted-CI list prices, illustrative; ratios match published tiers".

## It's Not an Editor Contest

**Tags:** `common mistake` (red), `what's genuinely there` (green)

- **The mistake** — reviewing Xcode like a rival editor ("worse autocomplete, so avoid it") misses the point
- **Not optional** — there is no avoiding it; the real question is how much of your workflow it must own
- **Real strengths** — Instruments (first-rate profiler), Interface Builder's lineage, a simulator per device
- **The escape hatch** — `xcodebuild` on the command line lets CI script builds without opening the GUI
- **Right lens** — evaluate the platform economics (Mac hardware, commission, CI cost), not the tool's polish

*Example (italic):* The team writes Swift in their favorite editor all week and touches Xcode only for signing, Instruments, and the archive step — minimizing the gate instead of rating it.

**Common mistake:** Treating a mandatory tool as a choice. A gate is judged by what it costs to pass through, not by whether you would have picked it in an open market.

### Visualization (canvas `c4`, 720×300)

Two-row feedback-loop diagram: how complaints flow for a tool that must earn users vs a tool that is a gate.

- **Title (bold 15px, `#1a5276`, top center):** "Earned Tool vs Gate Tool: Where Complaints Go".
- **Row 1 (y=95), label 12px `#444` at x=20:** "earned (editors)"; three rounded boxes (150px wide, 36px tall, 8px radius, 12px `#2c3e50` text) left to right at x = 150, 350, 550: blue `rgba(42,120,214,0.15)` "users complain" → green `rgba(0,131,0,0.12)` "users can switch" → green "vendor must fix"; 2px `#6b7280` arrows between, bold 12px green "✓ pressure works" under the last box.
- **Row 2 (y=205), label:** "gate (Xcode)"; blue box "users complain" → red `rgba(231,76,60,0.12)` box "switching = leaving iOS" → orange `rgba(217,89,38,0.12)` box "complaints repeat yearly"; a dashed `#e74c3c` (dash 4/3) arrow loops from the last box back to the first, bold 12px red "✗ pressure leaks away" under the middle box.
- **Divider:** 1px `#e5e9ef` horizontal line at y=150.
- **Annotation (bold 13px `#4a3aa7`, centered at y=272):** "judge the gate by the toll, not by the paint".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all boxes, arrows, and bar widths use the hardcoded coordinates above (no randomness); CI minute prices ($0.008 / $0.016 / $0.08) are typical hosted-CI list-price tiers labeled illustrative; the 100-device cap, 1-year development-certificate expiry, macOS-only requirement, and 15–30% commission are Apple's documented facts; the 12-device / 2-entitlement worked example is invented and labeled as such.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
