# Model Context Protocol (MCP)

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Model Context Protocol (MCP)

**Subtitle:** MCP is one shared plug standard between AI assistants and outside tools — build a tool once and any assistant can use it, instead of wiring every assistant to every tool by hand

## One Plug Instead of Fifteen Cables

**Tags:** `core idea` (blue), `plug standard` (green), `any tool, any model` (orange)

- **The office** — a small company runs 3 AI assistants: a chat app, a coding helper, a support bot
- **The tools** — the assistants need 5 things: calendar, email, files, customer database, web search
- **The old way** — each assistant gets its own custom wiring to each tool: 3 × 5 = 15 separate cables
- **The idea** — agree on one plug shape both sides speak; MCP is that shared plug standard
- **The payoff** — a tool plugged in once works with every assistant, today's and next year's

*Example (italic):* The calendar team builds one MCP server, and the chat app, the coding helper, and the support bot can all check the calendar through it — no extra wiring per assistant.

**Key point:** MCP is a shared connector standard between models and tools — one socket shape, so any assistant can plug into any tool.

### Visualization (canvas `c1`, 720×300)

Two-panel before/after wiring diagram: on the left, 3 assistant dots wired to 5 tool dots with all 15 pairwise lines (spaghetti); on the right, the same dots connected through one vertical MCP bar with only 3 + 5 = 8 lines.

- **Title (bold 15px, `#1a5276`, top center):** "3 Assistants, 5 Tools: 15 Custom Wires vs One Shared Plug".
- **Left panel (x 40–340):** panel label bold 12px `#444` "without MCP" centered at x=190, y=55. Assistants: 3 filled blue `#2a78d6` circles r=9 at x=95, y = 105, 165, 225, with right-aligned 12px `#444` labels at x=82 ("chat app", "coding helper", "support bot"). Tools: 5 filled green `#008300` circles r=9 at x=290, y = 85, 120, 155, 190, 225, with left-aligned 12px `#444` labels at x=304 ("calendar", "email", "files", "database", "search"). Wires: all 15 assistant→tool lines, 1px `#c9ced6`. Below: bold 13px orange `#d95926` label "15 wires" centered at x=190, y=278.
- **Right panel (x 380–700):** panel label bold 12px `#444` "with MCP" centered at x=545, y=55. Assistants: 3 blue circles r=9 at x=430, y = 105, 165, 225 (no text labels). MCP bar: violet `#4a3aa7` rounded rect (radius 6) from x=525 to x=560, y=75 to y=245, with bold 13px white "MCP" centered vertically inside. Tools: 5 green circles r=9 at x=660, y = 85, 120, 155, 190, 225 (no text labels). Wires: 3 blue `#2a78d6` 2px lines assistants→bar, 5 green `#008300` 2px lines bar→tools. Below: bold 13px green `#008300` label "3 + 5 = 8 pieces" centered at x=545, y=278.
- **Annotation (bold 12px violet `#4a3aa7`, centered at x=545, y=40, just under the title):** "same 15 pairs served by 8 pieces".
- **Caption (12px `#444`, bottom right):** "illustrative — a made-up 3-assistant, 5-tool office".

## Counting the Cables by Hand

**Tags:** `worked example` (blue), `simple counting` (green)

- **Custom wiring** — every assistant–tool pair is its own project: 3 assistants × 5 tools = 15 wires
- **With MCP** — each assistant learns the plug once (3 clients), each tool offers it once (5 servers)
- **The total** — 3 + 5 = 8 pieces instead of 15 wires, and every pair still connects
- **Add a tool** — a 6th tool costs 3 more custom wires, but only 1 more MCP server
- **Add an assistant** — a 4th assistant costs 5 more wires, but only 1 more MCP client
- **The gap grows** — at 10 tools it is 30 wires vs 13 pieces; multiplication against addition

*Example (italic):* Grow the shelf from 5 to 10 tools: custom wiring climbs 15 → 30, while the MCP count only ticks up 8 → 13.

**Key point:** Custom integrations grow like 3 × tools; MCP pieces grow like 3 + tools — multiply versus add, checkable on your fingers.

### Visualization (canvas `c2`, 720×300)

Single-panel two-line chart: pieces of wiring to build versus number of tools (assistants fixed at 3), one line for custom wiring and one for MCP, with the worked example's 5-tool case marked.

- **Title (bold 15px, `#1a5276`, top center):** "Wiring to Build as the Tool Shelf Grows (3 assistants)".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; x = number of tools 1 to 10 with 12px `#444` tick labels "1"–"10"; y = pieces to build 0 to 30, light `#e5e9ef` gridlines at 5, 10, 15, 20, 25, 30 with 12px `#444` labels; 11px `#6b7280` axis caption "tools" below the x labels, centered.
- **Custom line:** orange `#d95926` 3px line with 5px square markers through tools = `[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]`, wires = `[3, 6, 9, 12, 15, 18, 21, 24, 27, 30]`; 12px orange label "custom: 3 × tools" above the line near tools=8.
- **MCP line:** green `#008300` 3px line with 5px round dots through the same tools grid, pieces = `[4, 5, 6, 7, 8, 9, 10, 11, 12, 13]`; 12px green label "MCP: 3 + tools" below the line near tools=8.
- **Worked-example marker:** vertical dashed `#6b7280` (dash 4/3) guide line at tools=5 from the baseline up to the orange point; bold 13px orange label "15 wires" beside the orange point, bold 13px green label "8 pieces" beside the green point (staggered to avoid overlap).
- **Annotation (bold 12px violet `#4a3aa7`, near tools=3, y=90):** "multiplication vs addition — the gap keeps widening".
- **Caption (12px `#444`, bottom right):** "illustrative — one piece = one integration to build and maintain".

## Build the Tool Once, Ship It Everywhere

**Tags:** `where it's used` (blue), `reuse` (green), `swap freely` (orange)

- **Tool builders** — write one MCP server for the database and every MCP-speaking assistant can use it
- **Model swaps** — replace an assistant with a newer model and all 5 tools keep working unchanged
- **Discovery** — an assistant can ask a server "what can you do?" and get its tool list at run time
- **Shared shelf** — teams share MCP servers the way they share libraries: build once, plug anywhere
- **Without it** — every model upgrade or new tool restarts the wiring work; integrations rot in pairs

*Example (italic):* When the support bot is swapped for a newer model, nobody touches the calendar, email, files, database, or search servers — the plug shape didn't change.

**Key point:** The standard breaks the pairing: tools stop caring which model calls them, and models stop caring how each tool is built.

### Visualization (canvas `c3`, 720×300)

Grouped bar chart: the cost of one change (new integrations to build) for two scenarios — adding a 4th assistant and adding a 6th tool — comparing custom wiring against MCP.

- **Title (bold 15px, `#1a5276`, top center):** "What One Change Costs: New Integrations to Build".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 190; y = 0 to 6 with light `#e5e9ef` gridlines at 2, 4, 6 and 12px `#444` labels; no x-axis ticks, group labels instead.
- **Group 1 (centered x=210):** label bold 12px `#444` "add a 4th assistant" below the baseline; orange `#d95926` bar (fill `rgba(217,89,38,0.75)`) width 70 at height 5, green `#008300` bar (fill `rgba(0,131,0,0.75)`) width 70 at height 1, 30px gap between them; bold 13px value labels above the bars: orange "5 wires", green "1 client".
- **Group 2 (centered x=510):** label "add a 6th tool"; orange bar height 3, green bar height 1, same widths and gap; bold 13px labels: orange "3 wires", green "1 server".
- **Legend (top left, x=75, y=60):** 12px `#444` text with 12×12 swatches — orange "custom wiring", green "with MCP".
- **Annotation (bold 12px green `#008300`, centered near x=360, y=100):** "with MCP, every change is one piece of work".
- **Caption (12px `#444`, bottom right):** "illustrative — counts from the 3-assistant, 5-tool office".

## MCP Is the Socket, Not the Brain

**Tags:** `common mistake` (red), `who decides` (orange)

- **The mix-up** — people hear "MCP lets models use tools" and think MCP decides what to do
- **The brain** — the model still chooses which tool to call and with what inputs (tool calling)
- **The socket** — MCP only standardizes how that call travels and how the result comes back
- **The tool** — the calendar server does the actual work; it never sees the model's reasoning
- **A quick test** — unplug MCP and the model still thinks fine; it just can't reach outside tools

*Example (italic):* Asked "is my Friday free?", the assistant picks the calendar tool and fills in the date; MCP just carries that request across and brings the answer back.

**Common mistake:** Crediting or blaming MCP for a model's tool choices. MCP is plumbing — it carries requests and results; the deciding stays inside the model.

### Visualization (canvas `c4`, 720×300)

Left-to-right pipeline diagram of one request: five labeled boxes (you → assistant → MCP client → MCP server → calendar tool) with forward arrows, a dashed return arrow underneath, and role labels marking who decides versus who carries.

- **Title (bold 15px, `#1a5276`, top center):** "Who Does What When You Ask 'Is My Friday Free?'".
- **Boxes (five rounded rects, radius 6, 110 wide × 60 tall, top y=120, centers at x = 95, 235, 375, 515, 655):** "you" (border 2px `#6b7280`, fill `rgba(107,114,128,0.10)`), "assistant (model)" (border 2px blue `#2a78d6`, fill `rgba(42,120,214,0.10)`), "MCP client" (border 2px violet `#4a3aa7`, fill `rgba(74,58,167,0.10)`), "MCP server" (border 2px violet, same fill), "calendar tool" (border 2px green `#008300`, fill `rgba(0,131,0,0.10)`); box labels bold 12px in the border color, centered, wrapped to two lines where needed.
- **Forward arrows:** 2px `#444` arrows between consecutive boxes at mid-height (y=150) with 11px `#444` labels above each: "question", "tool call", "request", "look up".
- **Return path:** dashed 2px `#6b7280` (dash 6/4) arrow at y=215 from below the calendar box back to below the "you" box, 12px `#6b7280` label "answer flows back" centered above it.
- **Role labels:** bold 12px blue `#2a78d6` "decides which tool" centered above the assistant box at y=105; bold 12px violet `#4a3aa7` "carries the call — the socket" centered above the two MCP boxes (x=445) at y=105.
- **Annotation (bold 13px magenta `#d55181`, centered near x=375, y=272):** "the model chooses, MCP carries, the tool does the work".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all counts, line points, and bar heights are the hardcoded arrays above (no randomness); c2's two lines are exactly `3 × tools` and `3 + tools` over tools 1–10; text numbers (15, 8, 30, 13, 5, 3, 1) must match the chart numbers.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
