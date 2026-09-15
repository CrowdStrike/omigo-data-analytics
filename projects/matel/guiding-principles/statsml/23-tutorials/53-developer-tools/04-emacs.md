# Emacs

**Page type:** detail page (tutorial layout: `.card-section` blocks, each h2 + two-column table, text left 50% / canvas right 50%)
**HTML title tag:** Emacs

**Subtitle:** Emacs is a tiny C core running a Lisp interpreter — everything else is live Lisp you can rewrite while the editor runs, making it less a text editor than an operating system that happens to edit text

## The Editor With Almost No Editor Inside

**Tags:** `core idea` (blue), `architecture` (green), `Lisp` (orange)

- **The core** — a small C program: memory, screen drawing, and a Lisp interpreter — nothing more
- **Everything else** — every editing command, menu, and mode is Lisp code the interpreter runs
- **Even a keypress** — typing the letter "e" runs a Lisp function named `self-insert-command`
- **Live** — you can read, edit, and replace any of that Lisp while the editor is running
- **The contrast** — other editors offer an extension API; emacs IS its extension language

*Example (italic):* Press `C-h k` then any key and emacs names the Lisp function that key runs, with a link to its source — the editor documents and exposes its own machinery.

**Key point:** Emacs is a live Lisp environment that ships with text-editing code preloaded — the "editor" is user-space software running on the real product, the interpreter underneath.

### Visualization (canvas `c1`, 720×300)

Layered tower diagram: a short C-core box at the bottom, a tall stack of Lisp layers above it, heights proportional to their real line counts.

- **Title (bold 15px, `#1a5276`, top center):** "A Small C Core Under a Tower of Live Lisp".
- **Tower:** centered column at x=90, width 230; C box from y=205 to y=245 (40px tall), Lisp region from y=65 to y=205 (140px tall) — 40 : 140 matches the roughly 400k : 1.4M line ratio.
- **C box:** fill `rgba(107,114,128,0.20)`, 2px `#6b7280` border, bold 12px `#2c3e50` label "C core — display, memory, Lisp interpreter (~400k lines)".
- **Lisp region:** three stacked boxes of 46px each (y=65/111/157), fills `rgba(42,120,214,0.15)` / `rgba(0,131,0,0.12)` / `rgba(230,126,34,0.12)`, 2px borders `#2a78d6` / `#008300` / `#e67e22`, 12px labels "org, magit, mail, shells…", "modes and menus", "editing commands (~1.4M lines of Lisp total)".
- **Right-side callouts:** bold 13px green `#008300` at (x=380, y=130) "you can read and replace all of this while it runs", with a 2px green arrow to the Lisp region; 12px `#6b7280` at (x=380, y=225) "the only part fixed at startup", arrow to the C box.
- **Caption (12px `#444`, bottom right):** "line counts approximate (GNU Emacs 29)".

## Rewiring a Key While Emacs Runs

**Tags:** `worked example` (blue), `live redefinition` (green)

- **The itch** — you want every file save to strip trailing whitespace automatically
- **Step 1** — `C-h k` then `C-x C-s` reveals that the save key runs the Lisp function `save-buffer`
- **Step 2** — write 3 lines of Lisp adding `delete-trailing-whitespace` to the before-save hook
- **Step 3** — put the cursor after the code and press `C-x C-e` to eval it: the change is live
- **The count** — 3 lines, 0 restarts, about 20 seconds; every save from now on strips whitespace

*Example (italic):* The very next `C-x C-s` strips 14 stray trailing spaces from the buffer — the editor learned a new behavior mid-session, in the same window you were editing in.

**Key point:** The change loop is edit → eval → use, all inside one running process — the same change in a plugin-API editor means a build, an install, and a restart.

### Visualization (canvas `c2`, 720×300)

Two-row flow diagram comparing the change loop: a plugin-API editor (four steps ending in a restart) vs emacs (three steps, live immediately).

- **Title (bold 15px, `#1a5276`, top center):** "Changing Editor Behavior: Plugin API vs Live Lisp".
- **Row 1 (boxes centered on y=105), label 12px `#444` at x=20:** "plugin API"; four rounded boxes at x=100/255/410/565, each 130px wide, 40px tall: "write plugin", "build + install", "restart editor" (red `#e74c3c` border, fill `rgba(231,76,60,0.12)`), "test it"; 3px `#6b7280` arrows between boxes; bold 12px red label under the restart box: "session state lost".
- **Row 2 (boxes centered on y=215), label:** "emacs"; three rounded boxes at x=100/300/500, 150px wide, 40px tall: "write 3 lines of Lisp", "C-x C-e (eval)", "live on next save" (green `#008300` border, fill `rgba(0,131,0,0.12)`); 3px arrows.
- **Box style:** 8px radius, default fill `rgba(42,120,214,0.15)`, 2px `#2a78d6` border, 12px `#2c3e50` text.
- **Timing labels (bold 12px, right edge x=690, right-aligned):** red `#e74c3c` "minutes, 1 restart" at y=80; green `#008300` "~20 seconds, 0 restarts" at y=190.
- **Annotation (bold 13px violet `#4a3aa7`, centered near y=275):** "the editor never stops being editable".
- **Caption (12px `#444`, bottom right):** "timings illustrative".

## An Operating System With a Decent Editor

**Tags:** `where it's used` (blue), `ecosystem` (green), `history` (orange)

- **Born 1976** — Stallman's editor macros at MIT; GNU Emacs (1985) became the GNU project's flagship
- **Living inside** — since everything is Lisp, people built mail readers, shells, and file managers as modes
- **Magit** — a git interface widely described as better than the command-line git it wraps
- **Org-mode** — outlines, TODO tracking, documents with executable code blocks; recruits non-programmers
- **The editor wars** — the emacs-vs-vi rivalry is friendly lore, and evil-mode is the peace treaty

*Example (italic):* A researcher drafts a paper in Org-mode where every chart regenerates from a code block in the same file — a literate document, written by someone who says they can't program.

**Key point:** The ecosystem grew from the architecture, not the editing features: when the editor is a programmable environment, any workflow — email, git, planning — can move inside it.

### Visualization (canvas `c3`, 720×300)

Horizontal timeline of the lore, 1976 to 2026, with milestone dots alternating labels above and below the line.

- **Title (bold 15px, `#1a5276`, top center):** "Fifty Years of Emacs Lore".
- **Baseline:** 3px `#1a5276` horizontal line at y=170, from x=70 to x=660; scale 11.8px per year (1976 at x=70, 2026 at x=660); 12px `#444` year ticks "1976 / 1990 / 2005 / 2026" below the line.
- **Milestone dots (7px radius) with 12px labels on staggered rows to avoid collisions — above rows y=137/120/103, below rows y=215/232/249:**
  - x=70, blue `#2a78d6`: "1976 — Emacs born at MIT (Stallman)" (above, y=103)
  - x=176, blue: "1985 — GNU Emacs, the GNU flagship" (below, y=215)
  - x=247, orange `#d95926`: "1991 — vim arrives; editor wars peak" (above, y=120)
  - x=389, green `#008300`: "2003 — Org-mode" (below, y=232)
  - x=448, green: "2008 — Magit" (above, y=137)
  - x=483, violet `#4a3aa7`: "2011 — evil-mode: vim keys inside emacs" (below, y=249)
  - x=530, mute `#6b7280`: "2015 — VS Code era begins" (above, y=103)
- **Annotation (bold 13px green `#008300`, near x=470, y=70):** "the killer apps are Lisp programs, not editor features".
- **Caption (12px `#444`, bottom right):** "dates are release years; positions to scale".

## The Costs, Told Honestly

**Tags:** `common mistake` (red), `trade-offs` (orange)

- **The keys** — default bindings predate modern Ctrl-C conventions; the Ctrl-gymnastics jokes are earned
- **The rabbit hole** — the init file is a program, and tuning it can quietly become the hobby itself
- **The mistake** — judging emacs by week one, when the payoff arrives over months of shaping it to you
- **The reality** — far smaller mindshare than VS Code, yet a fiercely durable and self-renewing niche
- **The echo** — every key answers "what do you do?" (`C-h k`); platform editors copied extensibility-as-product

*Example (italic):* In week one a newcomer runs at a third of their old speed; the curves cross near month 4, and by month 12 the tailored tool is clearly ahead — if they stopped tweaking long enough to work.

**Common mistake:** Confusing configuring emacs with using it — the environment rewards investment, but the init-file rabbit hole can consume the very hours the tool was meant to save.

### Visualization (canvas `c4`, 720×300)

Line chart of relative productivity over the first 12 months: a familiar editor's flat line vs the emacs learning curve that starts low and crosses above it.

- **Title (bold 15px, `#1a5276`, top center):** "The Learning Investment: Slow Start, Higher Ceiling".
- **Axes:** origin x=60, baseline y=245, plot width 600, plot height 180; x = months 0 to 12 with 12px `#444` tick labels every 3 months; y = relative productivity 0 to 100, gridlines `#e5e9ef` at 25/50/75.
- **Familiar-editor line:** blue `#2a78d6` 3px line through months `[0, 1, 2, 3, 4, 6, 8, 10, 12]`, productivity `[70, 71, 72, 72, 73, 73, 74, 74, 74]` — flat, labeled bold 12px blue "familiar editor" near (x=month 1.5, above the line).
- **Emacs line:** green `#008300` 3px line through the same months, productivity `[25, 40, 55, 65, 72, 80, 85, 88, 90]` — labeled bold 12px green "emacs, shaped to you" near (x=month 9, above the line).
- **Crossover marker:** vertical dashed `#6b7280` (dash 4/3) line at month 4, 12px `#6b7280` label "curves cross ~month 4" at its top.
- **Annotation (bold 13px red `#e74c3c`, near month 2, y=220):** "week one is the worst it will ever be".
- **Caption (12px `#444`, bottom right):** "productivity values illustrative".

## Regeneration instructions

- **Template:** tutorials topic-page layout (per `tutorials/CLAUDE.md`). Body: h1 with 2px `#2980b9` bottom border, `.subtitle`, then four `.card-section` blocks, each `<h2>` (1.3rem `#1a5276`, 2px `#2980b9` bottom border) + `table.layout` with one row: `td.text-col` (50%) and `td.viz-col` (50%).
- **Text column structure:** `.tags` pill row, `<ul>` of one-line bullets each opening `<li><b>term</b> — ...`, one italic `.example` paragraph, one `.key-point` callout (`background #f8f9fa`, left border 3px `#e74c3c`, `<strong>` label).
- **Tag pill styles:** 0.72rem bold, 2px 10px padding, 10px radius; blue `rgba(26,82,118,0.12)`/`#1a5276`, green `rgba(39,174,96,0.15)`/`#27ae60`, red `rgba(231,76,60,0.12)`/`#e74c3c`, orange `rgba(230,126,34,0.15)`/`#e67e22`.
- **Page CSS:** body system-ui sans-serif, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276`; subtitle `#666` 0.95rem; `li b` in `#1a5276`; ul 0.92rem; inline `code` in ui-monospace 0.85em `#2c3e50` on `#f4f6f8`, 1px 4px padding, 3px radius; canvases `width:100%`, 1px `#e0e0e0` border, 4px radius. No nav bar, no back/home links.
- **Canvas:** intrinsic 720×300; shared `setup(id)` helper sizes the backing store to the rendered width × `window.devicePixelRatio` (display capped at the logical width via `style.maxWidth`) and calls `ctx.scale` so drawing stays in logical coordinates. Chart draw functions are registered in a `__charts` array and re-run on window resize (debounced 150ms).
- **Chart palette object:** blue `#2a78d6`, green `#008300`, magenta `#d55181`, yellow `#c98500`, aqua `#199e70`, orange `#d95926`, violet `#4a3aa7`, ink `#1a5276`, text `#2c3e50`, mute `#6b7280`, grid `#e5e9ef`. Site palette: primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Data:** all points are the hardcoded arrays above (no randomness); c1 line counts (~400k C, ~1.4M Lisp) are approximate real figures for GNU Emacs 29; c3 milestone years (1976/1985/1991/2003/2008/2011/2015) are actual release years; c2 timings and c4 productivity curves are invented and labeled illustrative.
- In regenerated HTML, any card links would use `.html` extensions (this page has no links).
