export const meta = {
  name: 'regenerate-v3-pages',
  description: 'Pass B clean room: generate NN-topic.v3.html from txt.md + viz.md only, one page per agent, 5 in parallel',
  phases: [
    { title: 'Regenerate', detail: 'one page per agent, batches of 5' },
  ],
}

// args: single-folder: { baseDir, folder, pages, template, sharedJs, depth, vizmd, folderRules }
//       multi-folder:  { baseDir, folders: [{ folder, pages, template, sharedJs, depth, vizmd, folderRules }, ...] }
// template  = ui-templates file name, e.g. '06-sectioned-cards-callout.html'
// sharedJs  = 'base.js' or 'three-col-dist.js' (what <script src> should load; viz.md header may override per page)
// depth     = number of path segments in folder ('metrics' -> 1, 'backlog/digital-theft' -> 2); src prefix = '../'.repeat(depth)
// vizmd     = false for prose-only folders (no viz.md, no <script>)
const BASE = args.baseDir
const FOLDERS = args.folders || [{
  folder: args.folder, pages: args.pages, template: args.template,
  sharedJs: args.sharedJs, depth: args.depth, vizmd: args.vizmd, folderRules: args.folderRules,
}]

const SCHEMA = {
  type: 'object',
  properties: {
    page: { type: 'string' },
    status: { type: 'string', enum: ['created', 'skipped', 'failed'] },
    canvases: { type: 'number' },
    gaps: { type: 'string' },
    notes: { type: 'string' },
  },
  required: ['page', 'status', 'gaps', 'notes'],
}

function prompt(F, name) {
  const FOLDER = F.folder
  const REL = '../'.repeat(F.depth || 1)
  const VIZMD = F.vizmd !== false
  const chartSpec = VIZMD ? `
CHARTS — from ${name}.viz.md:
- Load the shared js before your chart code: <script src="${REL}ui-templates/js/${F.sharedJs || 'base.js'}"></script> (if the viz.md **Shared js:** header names a different file, use that one instead). base.js already provides: setupCanvas(id,w,h) -> dpr-scaled bare ctx, setup() alias, registerChart(fn) (draw now + redraw on resize), mulberry32(seed)/rng/randn()/randExp(), roundRectPath, drawArrow. Do NOT re-implement these; do NOT use Math.random() or Date.now() — seeded rng only.
- One <canvas id="..."> per brief, same ids, same document order, inside the section the brief's [sec-N] names. Use the exact logical width/height, data values, hexes, fonts, annotation positions, footer lines the brief quotes. A **Sketch** block shows the intended composition — follow it. Ignore **Improve** bullets: regenerate what the brief specifies, not the improvement.
- If the viz.md has a "**Page-local lib note:**", implement those helpers inside a LIB fence before the chart code.
- Wrap each chart's code in fence comments inside the single inline <script> (after the src tag):
  // ==== SEC-<N> VIZ <canvasId> ====
  registerChart(function () { var ctx = setupCanvas('<canvasId>', <w>, <h>); if (!ctx) return; ... });
  // ==== /SEC-<N> VIZ <canvasId> ====
- Page-local helpers (if any):
  // ==== LIB (page-local: <short reason>) ====
  ...
  // ==== /LIB ====
- **Theme:** header in viz.md names the accent theme; its hexes are quoted in the briefs — use them. House palette default: #1a5276 primary, #27ae60 green, #e74c3c red, #e67e22 orange, rgba(26,82,118,0.35) bar fill.` : `
NO CHARTS — this is a prose-only page (no viz.md, no <canvas>, no <script> at all).`

  return `You are regenerating ONE documentation page as a NEW html file, in a CLEAN-ROOM test of the markdown sources. Base dir: ${BASE}. Your page: ${FOLDER}/${name}

READ ONLY these files (via Read):
1. ${BASE}/${FOLDER}/${name}.txt.md — the page's verbatim text, sectioned as "## [sec-N] <heading>".${VIZMD ? `
2. ${BASE}/${FOLDER}/${name}.viz.md — per-canvas regeneration briefs (Type/Data/Colors/Shows/Notes/Sketch).` : ''}
${VIZMD ? '3' : '2'}. ${BASE}/ui-templates/${F.template} — the folder's layout template: copy its CSS/markup conventions (fonts, colors, section/card/table structure, tag pills, callout boxes).

CLEAN ROOM — you must NOT open: ${name}.html (the original), ${name}.v2.html, ${name}.md, FORMAT.md, THEMES.md, base.js, or ANY sibling page. Everything you need is in the three files above plus this prompt. If a brief under-specifies something (a position, a color, a scale), make the minimal reasonable choice and record it in your "gaps" output — do NOT look anywhere else.

CREATE exactly one new file: ${BASE}/${FOLDER}/${name}.v3.html — a complete standalone page.

INCREMENTAL WRITING — REQUIRED (a stall watchdog kills any agent that goes minutes without a tool call — whether one giant Write OR long silent composing/planning; every attempt like that dies and retries forever):
- Start writing IMMEDIATELY after reading your inputs: your very next tool call must be the chunk-1 Write. Do NOT plan all sections/charts upfront — compose ONE section, append it, then compose the next. Interleave thinking with appends so no gap between tool calls exceeds ~1 minute.
- Build the file in SMALL APPENDS, never one big Write. Each chunk must stay under ~60 lines.
- Chunk 1: use the Write tool for the file head only (<!DOCTYPE> through </head> incl. CSS, <body>, PAGE-HEAD TEXT fence block).
- Then append ONE SECTION PER TOOL CALL with Bash heredoc appends: cat >> '${BASE}/${FOLDER}/${name}.v3.html' <<'CHUNK_EOF' ... CHUNK_EOF (quoted delimiter, so html/js is verbatim).
- Then append the <script src> line and the LIB fence (if any) as one chunk, then ONE VIZ FENCE (one chart's code) PER TOOL CALL, then the closing </script></body></html> chunk.

Page structure:
- <title> and <h1> = the txt.md title lines (they are already index-number-free; keep them that way).
- Subtitle paragraph if txt.md has a "**Subtitle:**" line.
- One section per "## [sec-N]" in txt.md, in order, using the template's section/card structure. ALL text VERBATIM from txt.md — no rewording, no summarizing, no additions. Bold labels stay bold; "**Tags:** name (color), ..." becomes the template's tag pills; labeled callout lines ("**Example:** ...", "**Key point:** ...", "**Intro:** ...", "**Key-point —** ...") become the template's styled callout boxes — the label word is TYPE scaffolding: pick the box style from it but do NOT print the label text itself, only the content after it; markdown tables become html tables.
- Fence every text block with html comments:
  <!-- ==== PAGE-HEAD TEXT ==== --> h1 + subtitle <!-- ==== /PAGE-HEAD TEXT ==== -->
  <!-- ==== SEC-<N> TEXT ==== --> that section's text markup <!-- ==== /SEC-<N> TEXT ==== -->
- No nav elements, no back/home links, no cross-page links (grid/hub structure-specs are the exception: reproduce their card links verbatim as specified).
${chartSpec}

FOLDER RULES:
${F.folderRules}

CONSTRAINTS:
- CREATE-ONLY: never modify or delete ANY existing file. No git commands. No render/browser checks. Temp files go in /tmp, not the repo.
- If ${name}.v3.html already exists AND contains a closing </html> tag, it is complete: do nothing and return status "skipped". If it exists WITHOUT </html>, it is a dead agent's partial output: Read its first few lines to confirm it is a v3 attempt, then start over — Write chunk 1 over it and append as normal. Never use rm.

VERIFY before returning (mechanically, with a small python script via Bash — do not re-read whole files into context):
- Every visible text token of ${name}.txt.md (strip markdown syntax, the [sec-N] anchors, and "HTML title tag:"/"Subtitle:"/"Tags:" label scaffolding) appears in the v3 html's visible text (strip tags/script/style), and vice versa — the v3 must add no text of its own beyond template-implied labels.${VIZMD ? `
- Every brief's canvas id has exactly one <canvas> and one "SEC-<N> VIZ <id>" fence pair; all fences pair up.
- node --check passes on the inline <script> body.` : ''}
Fix any failure before returning.

Return JSON: page, status (created|skipped|failed), canvases (count in v3), gaps (semicolon-separated list of under-specified points you had to decide yourself, or "none"), notes (1-2 sentences incl. verification result).`
}

const all = []
for (const F of FOLDERS) {
  const PAGES = F.pages
  const results = []
  for (let i = 0; i < PAGES.length; i += 5) {
    const chunk = PAGES.slice(i, i + 5)
    log(`${F.folder} batch ${Math.floor(i / 5) + 1}/${Math.ceil(PAGES.length / 5)}: ${chunk.join(', ')}`)
    const batch = await parallel(chunk.map(p => () =>
      agent(prompt(F, p), { label: `${F.folder}/${p}`, phase: F.folder, schema: SCHEMA })))
    results.push(...batch)
  }
  const out = results.filter(Boolean)
  log(`${F.folder}: ${out.filter(r => r.status === 'created').length} created, ${out.filter(r => r.status === 'skipped').length} skipped, ${out.filter(r => r.status === 'failed').length} failed`)
  all.push(...out)
}
return all
