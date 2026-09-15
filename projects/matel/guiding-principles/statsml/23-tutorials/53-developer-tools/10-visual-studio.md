# Visual Studio

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Visual Studio

**Subtitle:** One vendor owns the language, the runtime, the frameworks, the cloud, and the IDE — Visual Studio (the full Windows IDE, not VS Code) is what total integration looks like

## One Vendor Owns Every Layer

**Tags:** `core idea` (blue), `vendor stack` (green), `not VS Code` (orange)

- **The stack** — Microsoft owns C#, the .NET runtime, ASP.NET/WPF, Azure, and Visual Studio itself
- **Not VS Code** — Visual Studio is a separate heavyweight Windows IDE; VS Code is a different product
- **The payoff** — a tool whose vendor owns every layer integrates to depths a neutral editor cannot
- **The debugger** — steps from your code into the vendor's framework source without leaving the session
- **The designers** — visual editors exist for exactly the vendor's UI frameworks (WPF, WinForms)
- **The profiler** — tuned to the .NET runtime: it knows GC pauses and JIT time, not just CPU samples

*Example (italic):* A payroll team writes C#, debugs into ASP.NET source, profiles .NET garbage collection, and deploys to Azure — the same vendor at every step.

**Key point:** Total integration is only possible when the IDE vendor also owns the language, runtime, frameworks, and cloud — Visual Studio is the archetype of that model.

### Visualization (canvas `c1`, 720×300)

Layer-stack diagram: five vendor-owned layers on the left that the IDE reaches in full, versus a neutral editor on the right whose plugins stop at the surface.

- **Title (bold 15px, `#1a5276`, top center):** "Five Layers, One Vendor — the IDE Reaches All the Way Down".
- **Left stack (five boxes, x=80, width 250, height 30, 6px radius, at y = 62, 100, 138, 176, 214):** labels 12px `#2c3e50` centered: "Visual Studio (IDE)", "C# (language)", ".NET (runtime)", "ASP.NET / WPF (frameworks)", "Azure (cloud)"; all fill `rgba(42,120,214,0.18)` with 2px `#2a78d6` borders.
- **Reach bracket:** 3px green `#008300` vertical line at x=352 from y=62 to y=244 with arrowheads at both ends; bold 12px green label "debug · design · profile · deploy" rotated or stacked beside it at x=362.
- **Right column:** gray box at x=470, y=62, width 180, height 30, fill `rgba(107,114,128,0.15)`, 2px `#6b7280` border, 12px label "neutral editor + plugins"; two dashed 2px `#6b7280` arrows (dash 4/3) from its bottom to the "C# (language)" and ".NET (runtime)" layers only.
- **Annotation (bold 13px orange `#d95926`, at x=470, y=200):** "plugins stop at the surface".
- **Caption (12px `#444`, bottom right):** "layer reach schematic".

## One Afternoon Inside the Stack

**Tags:** `worked example` (blue), `debugging` (green)

- **The bug** — an invoice endpoint stalls intermittently; the developer sets one breakpoint in C#
- **Step into** — F11 crosses from their handler into ASP.NET framework source, fetched automatically
- **Mixed mode** — the same session steps into a native C++ imaging library without reattaching
- **The profiler** — flags an 18 ms GC pause caused by allocations inside the rendering loop
- **The fix** — edit-and-continue applies the change to the running process; no restart needed
- **The ship** — right-click Publish deploys the patched service to Azure in one step

*Example (italic):* One debug session crosses three layers — C# handler, ASP.NET framework source, native C++ — with a single F11 keypress at each boundary.

**Key point:** The depth is the product: crossing language and layer boundaries inside one live session is exactly what owning the whole stack buys.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram of the debug afternoon: row one steps across three code layers; row two runs profile → fix → deploy.

- **Title (bold 15px, `#1a5276`, top center):** "One Session, Three Layers, One Keypress per Boundary".
- **Row 1 (boxes at y=85, height 44, 8px radius):** blue box at x=40, width 190, fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px text "your C# handler (breakpoint)"; 3px `#2c3e50` arrow labeled bold 11px "F11" to aqua box at x=270, width 190, fill `rgba(25,158,112,0.15)`, 2px `#199e70` border, "ASP.NET framework source"; second "F11" arrow to violet box at x=500, width 180, fill `rgba(74,58,167,0.12)`, 2px `#4a3aa7` border, "native C++ imaging lib".
- **Row 2 (boxes at y=195, height 44):** orange box at x=40, width 190, fill `rgba(217,89,38,0.12)`, 2px `#d95926` border, "profiler: 18 ms GC pause"; arrow to green box at x=270, width 190, fill `rgba(0,131,0,0.12)`, 2px `#008300` border, "edit-and-continue fix"; arrow to green box at x=500, width 180, "Publish → Azure" with bold 12px green "✓" suffix.
- **Row connector:** dashed 2px `#6b7280` elbow (dash 4/3) from row-1 right box down to row-2 left box.
- **Annotation (bold 13px green `#008300`, centered near y=270):** "no reattach, no restart, no second tool".
- **Caption (12px `#444`, bottom right):** "GC pause value illustrative".

## The Arc: 1997 Bundle to Split Identity

**Tags:** `where it's used` (blue), `history` (green), `enterprise` (orange)

- **1997** — the first Visual Studio bundles Visual C++, Visual Basic, and siblings into one box
- **2002** — Visual Studio .NET ships C# and .NET; it becomes the enterprise-Windows default
- **IntelliSense** — the 1996 autocomplete feature whose brand name became the generic term
- **C++ and games** — the MSVC toolchain is the Windows/game-industry standard; console SDKs plug in
- **2014** — the free Community edition arrives; Professional and Enterprise stay the paid business
- **2015** — the same company launches VS Code, running both tool strategies simultaneously

*Example (italic):* A game studio ships on the MSVC toolchain with console-SDK integrations while its web team lives in VS Code — two strategies, one vendor.

**Key point:** Entrenchment came in layers — bundling in 1997, the .NET-era enterprise default, then a free tier below and paid Professional/Enterprise tiers above.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline 1996–2025 with six documented milestones on alternating stems above and below the axis.

- **Title (bold 15px, `#1a5276`, top center):** "One Arc: Bundle → Enterprise Default → Split Identity".
- **Axis:** 2px `#999` horizontal line at y=170 from x=60 to x=690; 12px `#444` year ticks at 1996 (x=70), 2005 (x=262), 2015 (x=476), 2025 (x=690); linear mapping x = 70 + (year − 1996) × 21.38.
- **Milestones (dot 6px radius on the axis, 2px stem, 12px label at stem end, alternating up/down):**
  - 1996 (x=70, stem up to y=110, blue `#2a78d6`): "IntelliSense debuts"
  - 1997 (x=91, stem down to y=230, ink `#1a5276`): "Visual Studio 97 bundle"
  - 2002 (x=198, stem up to y=95, green `#008300`): "VS .NET: C# + .NET era"
  - 2014 (x=455, stem down to y=230, orange `#d95926`): "free Community edition"
  - 2015 (x=476, stem up to y=110, violet `#4a3aa7`): "VS Code launches"
  - 2024 (x=669, stem down to y=230, red `#e74c3c`): "Mac version retired"
- **Annotation (bold 13px violet `#4a3aa7`, centered near x=560, y=60):** "since 2015: two strategies, one company".
- **Caption (12px `#444`, bottom right):** "milestone years documented".

## The Lock-In Calculus

**Tags:** `common mistake` (red), `trade-off` (orange)

- **The conflation** — "I use Visual Studio" often means VS Code; they are entirely different products
- **Windows only** — the full IDE runs only on Windows; the Mac version was retired in August 2024
- **Outside the stack** — for Python, Go, or front-end work, the heavyweight IDE offers little
- **The calculus** — enterprises accept lock-in knowingly: depth today against switching cost later
- **The rule** — total integration's value is exactly proportional to your commitment to the stack

*Example (italic):* A team 90% inside .NET gets near-full value from the IDE; a polyglot team 20% inside pays its full weight for a fraction of its depth.

**Common mistake:** Judging Visual Studio as a general-purpose editor. It is a stack tool — unmatched inside .NET/Windows/C++, near-irrelevant outside — and conflating it with VS Code hides exactly that trade.

### Visualization (canvas `c4`, 720×300)

Line chart: value delivered by each tool as a function of how much of your work lives inside the Microsoft stack.

- **Title (bold 15px, `#1a5276`, top center):** "Value Rises with Stack Commitment — the Honest Trade".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = "% of work inside the Microsoft stack" 0 to 100, 12px `#444` tick labels every 20; y = "value delivered" 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Visual Studio line:** blue `#2a78d6` 3px line through commitment `[0, 20, 40, 60, 80, 100]`, value `[10, 28, 48, 68, 86, 100]` — steep, ends at the top.
- **VS Code line:** aqua `#199e70` 3px line through the same commitment grid, value `[62, 63, 64, 64, 65, 65]` — flat.
- **Line labels:** bold 12px blue "Visual Studio" near (x=88, above the blue line); bold 12px aqua "VS Code" near (x=10, above the aqua line).
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at commitment ≈ 56 where the curves cross, 12px `#6b7280` label "break-even" at its top.
- **Annotation (bold 13px orange `#d95926`, near x=60, y=70):** "below the crossover, the weight is pure cost".
- **Caption (12px `#444`, bottom right):** "value curves illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays and coordinates above (no randomness); timeline years (1996 / 1997 / 2002 / 2014 / 2015 / 2024) are documented product history; the 18 ms GC pause and both value-vs-commitment curves are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
