# Context Compression

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Context Compression

**Subtitle:** When a conversation grows too long for the model to read, you don't delete it — you rewrite it shorter, keeping the meaning and dropping the words around it

## A 4,000-Token Chat About One Broken Blender

**Tags:** `core idea` (blue), `meaning per token` (green), `long chats` (orange)

- **The chat** — a customer and a support bot trade 40 messages about one broken blender
- **The size** — the full transcript is about 4,000 tokens, and every new reply must reread all of it
- **The fluff** — greetings (600), repeated back-and-forth (2,200), and apologies (900) fill 3,700 tokens
- **The meat** — the facts that actually decide the case fit in roughly 300 tokens
- **The trick** — swap the transcript for a short brief that keeps the meaning, not the words

*Example (italic):* A colleague taking over the case doesn't reread all 40 messages — they ask for the two-line handoff note; that note is the compressed context.

**Key point:** Context compression means fitting the same meaning into fewer tokens — keep whatever changes the next answer, drop the words wrapped around it.

### Visualization (canvas `c1`, 720×300)

Two horizontal stacked bars on a shared token axis: the full 4,000-token transcript broken into its four ingredients, and below it the 300-token compressed brief, making the size gap physical.

- **Title (bold 15px, `#1a5276`, top center):** "One Blender Chat: 4,000 Tokens of Words, 300 Tokens of Meaning".
- **Axis:** horizontal 2px `#999` line at y=250 from x=170 to x=690 (width 520), tokens 0 to 4,000; tick labels "0", "1,000", "2,000", "3,000", "4,000" every 1,000 (12px `#444`) below.
- **Row 1 (bar top y=88, 34px tall), left label 12px `#444` at x=20:** "full transcript — 4,000"; stacked segments left to right: greetings & sign-offs 600 fill `rgba(107,114,128,0.35)`, troubleshooting back-and-forth 2,200 fill `rgba(42,120,214,0.35)`, apologies & filler 900 fill `rgba(201,133,0,0.35)`, key facts 300 fill `rgba(0,131,0,0.55)`; 1px `#6b7280` borders; 12px `#444` segment labels above the bar ("greetings 600", "back-and-forth 2,200", "apologies 900", "facts 300", staggered to avoid overlap).
- **Row 2 (bar top y=185, 34px tall), left label:** "compressed brief — 300"; single 300-token bar, fill `rgba(0,131,0,0.35)`, 2px `#008300` border.
- **Annotation (bold 13px green `#008300`, near x=400, y=205):** "13× fewer tokens — the same decision-ready facts".
- **Caption (12px `#444`, bottom right):** "token counts illustrative".

## Squeezing 4,000 Tokens Into 300

**Tags:** `worked example` (blue), `compression ratio` (green)

- **Full transcript** — greetings 600 + back-and-forth 2,200 + apologies 900 + key facts 300 = 4,000 tokens
- **The brief** — five freshly written one-line facts costing 40 + 70 + 80 + 50 + 60 = 300 tokens
- **What the lines hold** — order #8412 and model; the fault; the three fixes tried; status; refund wanted
- **The check** — nothing in the 40 messages changes the bot's next reply that isn't in those five lines
- **The ratio** — 4,000 ÷ 300 ≈ 13, so one token survives for every thirteen, with the meaning intact

*Example (italic):* A reply drafted from the 300-token brief matches the one drafted from the full 4,000-token chat — the 3,700 dropped tokens changed nothing.

**Key point:** Compression ratio = 4,000 / 300 ≈ 13× — earned by rewriting the meaning shorter, not by deleting lines until it fits.

### Visualization (canvas `c2`, 720×300)

Horizontal bar chart of the five brief lines, each bar's width its token cost, so the reader can re-add 40 + 70 + 80 + 50 + 60 = 300 by eye.

- **Title (bold 15px, `#1a5276`, top center):** "The 300-Token Brief, Line by Line".
- **Axis:** horizontal 2px `#999` line at y=252 from x=280 to x=690 (width 410), tokens 0 to 100; tick labels "0", "20", "40", "60", "80", "100" (12px `#444`) below.
- **Rows (bar tops at y = 62, 100, 138, 176, 214; bars 24px tall), each with a left-aligned 12px `#444` label at x=20:**
  - "order #8412, Nova blender, Mar 3" — bar width 40 tokens
  - "fault: blade won't spin, motor hums" — bar width 70 tokens
  - "tried: reset, new outlet, blade refit" — bar width 80 tokens
  - "status: still broken after 3 attempts" — bar width 50 tokens
  - "wants: refund, not a replacement" — bar width 60 tokens
- **Bar style:** fill `rgba(42,120,214,0.35)`, 1px `#2a78d6` border; bold 12px `#2a78d6` token count ("40", "70", "80", "50", "60") just past each bar's right end.
- **Annotation (bold 13px green `#008300`, right-aligned at the top right near y=45):** "40+70+80+50+60 = 300 — down from 4,000".
- **Caption (12px `#444`, bottom right):** "line costs illustrative".

## The Window Fills Up: Compress or Overflow

**Tags:** `where it's used` (blue), `context window` (green), `cost` (orange)

- **The limit** — the model reads at most 8,000 tokens at once; whatever exceeds that is simply not seen
- **The growth** — at about 200 tokens per turn, an uncompressed chat crosses 8,000 near turn 40
- **The sawtooth** — folding the history into a 300-token brief every 20 turns keeps the chat alive
- **The bill** — every reply resends the whole context, so a 13× smaller context is also far cheaper
- **The focus** — models answer better from 300 sharp tokens than from 4,000 tokens of buried facts

*Example (italic):* An agent that summarizes its own history every 20 turns finished the 50-turn case under budget; its uncompressed twin hit the wall at turn 40.

**Key point:** Compression is what lets a conversation outlive the context window — without it, around turn 40 the chat simply stops fitting.

### Visualization (canvas `c3`, 720×300)

Line chart of context size versus conversation turn: an uncompressed line climbing straight through the window limit, and a compressed sawtooth that drops back down every 20 turns and never overflows.

- **Title (bold 15px, `#1a5276`, top center):** "Compress Every 20 Turns, or Overflow at Turn 40".
- **Axes:** origin x=70, baseline y=245, plot width 590, plot height 190; x = turn 0 to 50, tick labels "0", "10", "20", "30", "40", "50" (12px `#444`); y = context tokens 0 to 10,000, light `#e5e9ef` gridlines at 2,000 / 4,000 / 6,000 with 12px `#444` labels "2k", "4k", "6k", "8k", "10k".
- **Window limit:** horizontal dashed red `#e74c3c` 2px (dash 6/4) line at 8,000; bold 12px red label above its left end: "context window: 8,000".
- **No compression:** orange `#d95926` 3px line through turns `[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50]`, tokens `[0, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]`; 12px orange label "no compression" near turn 33 above the line.
- **With compression:** green `#008300` 3px line, same turn grid, tokens `[0, 1000, 2000, 3000, 4000, 1300, 2300, 3300, 4300, 1300, 2300]`; 6px green dots at the two drop points (turns 25 and 45); 11px green label near the first drop: "history → 300-token brief".
- **Annotation (bold 13px red `#e74c3c`, near turn 40, y at 8,700):** "overflows at turn 40".
- **Caption (12px `#444`, bottom right):** "illustrative — 200 tokens per turn, brief rebuilt every 20 turns".

## Compression Is Not Cutting

**Tags:** `common mistake` (red), `truncation vs summary` (orange)

- **Two diets** — truncation deletes the oldest messages; compression rewrites the whole story shorter
- **Same budget** — both squeeze the history down to about 300 tokens, but they keep different things
- **What truncation loses** — order number, model, and the fixes tried all sat in the oldest messages
- **What truncation keeps** — only the fault (repeated recently) and the refund wish survive by luck
- **The tell** — a bot that suddenly asks "what was your order number again?" was truncated, not compressed

*Example (italic):* Two bots trimmed the same chat to 300 tokens: the truncating one lost the order number stated in message 1; the summarizing one carried all five facts forward.

**Common mistake:** Treating "drop the oldest messages" as compression. Age is a terrible measure of importance — the case-defining facts are usually stated first.

### Visualization (canvas `c4`, 720×300)

Fact-survival grid: the five case facts as columns, the two trimming strategies as rows, each cell a green check or red cross showing which facts each strategy carries into the next reply.

- **Title (bold 15px, `#1a5276`, top center):** "Trim to 300 Tokens Two Ways: Which Facts Survive?".
- **Column headers (five columns centered at x = 200, 310, 420, 530, 640):** 12px `#444` fact name with an 11px `#6b7280` "said in" line under it — "order #8412" / "msg 1", "model & date" / "msg 1", "the fault" / "msg 2, msg 37", "fixes tried" / "msgs 5–30", "refund wish" / "msg 38"; headers at y=80.
- **Row 1 (cells centered at y=145), left label 12px `#444` at x=20:** "truncate oldest"; cells: cross, cross, check, cross, check.
- **Row 2 (cells centered at y=215), left label:** "summarize"; cells: check, check, check, check, check.
- **Cell style:** 44×44px rounded squares; check cells fill `rgba(0,131,0,0.12)` with a bold 20px `#008300` "✓"; cross cells fill `rgba(231,76,60,0.10)` with a bold 20px `#e74c3c` "✗"; 1px `#e5e9ef` borders.
- **Annotation (bold 13px magenta `#d55181`, centered near y=278):** "cutting by age lost 3 of the 5 case facts — the summary kept all 5".
- **Caption (12px `#444`, top right):** "illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`. Red reserved for the overflow line and lost-fact cells (genuine error states).
- **Data:** all bar widths, line points, and cell verdicts are the hardcoded literal values above (no randomness); segment sums must stay consistent (600+2,200+900+300 = 4,000; 40+70+80+50+60 = 300; ratio 4,000/300 ≈ 13×) so text and charts always agree.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
