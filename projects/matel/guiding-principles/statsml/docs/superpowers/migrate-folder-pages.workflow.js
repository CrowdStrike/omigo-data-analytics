export const meta = {
  name: 'migrate-folder-pages',
  description: 'Pass A: generate txt.md + viz.md from each original html (v2.html deferred), one page per agent, 5 in parallel',
  phases: [
    { title: 'Extract', detail: 'one page per agent, batches of 5' },
  ],
}

// args: single-folder: { baseDir, folder, pages, template, sharedJs, vizmd, folderRules }
//       multi-folder:  { baseDir, folders: [{ folder, pages, template, sharedJs, vizmd, folderRules }, ...] }
const BASE = args.baseDir
const FOLDERS = args.folders || [{
  folder: args.folder, pages: args.pages, template: args.template,
  sharedJs: args.sharedJs, vizmd: args.vizmd, folderRules: args.folderRules,
}]

const SCHEMA = {
  type: 'object',
  properties: {
    page: { type: 'string' },
    status: { type: 'string', enum: ['created', 'skipped', 'failed'] },
    canvases: { type: 'number' },
    notes: { type: 'string' },
  },
  required: ['page', 'status', 'notes'],
}

function prompt(F, name) {
  const FOLDER = F.folder
  const VIZMD = F.vizmd !== false
  const vizSpec = VIZMD ? `
2. ${name}.viz.md — the chart-regeneration brief file. Header block first:

   # <Page Title> — Viz Briefs

   **Template:** ${F.template}
   **Shared js:** ${F.sharedJs}
   **Theme:** house-blue   (unless the page uses its own accent theme — then name it and list its hexes)

   Optionally next, a "**Page-local lib note:**" paragraph describing the page's own helper functions (name, signature, what they draw, how they differ from a plain histogram/bar helper: option names, density bands, overlays, legends, margins, fonts, whether they mutate canvas.width for dpr) and any seeded rng (algorithm + seed + which sections consume the stream in what order).

   Then ONE brief per canvas, in document order:

   ## [sec-N] <canvasId> — <Chart Title>

   - **Type:** chart family + key geometry (bar/line/histogram/scatter/custom; orientation, bar widths, margins/paddings, base Y, scales/max values)
   - **Data:** the EXACT data — literal arrays/values with labels, or the generator recipe (distribution, parameters, seed, sample count)
   - **Colors:** every hex/rgba used and what it colors (bars, strokes, gridlines, annotations, text), fonts/sizes for titles/labels/footers
   - **Shows:** the insight the chart demonstrates + any footer takeaway line (quote it)
   - **Notes:** annotations, arrows, dashed reference lines, legends, gridline positions, anything hand-placed (with coordinates)
   - **Sketch:** (optional, in a fenced code block) an ASCII-art sketch of the chart when words alone can't capture the composition — panel arrangement, where annotations point, how overlays stack, the rough shape of the curve/bars. Use it whenever it captures the intention of the viz better than prose.
   - **Improve:** (optional) 1 short bullet on how a regeneration could do better — e.g. cramped labels, overlapping annotations, a clearer chart type for this data, missing axis label. Do NOT change the original; just record the opportunity.

   The goal: someone can regenerate the chart from the brief with near-zero thinking. Extract all of this from the original page's inline chart code — quote exact numbers, positions, and hexes; do not approximate.` : `
2. NO .viz.md — this is a prose-only folder (pages have no canvases). If your page unexpectedly HAS a canvas, write a viz.md after all (header + per-canvas briefs with Type/Data/Colors/Shows/Notes) and say so in your notes.`

  return `You are extracting ONE documentation page into markdown source files. This is a CREATE-ONLY migration, Pass A: you produce ONLY markdown files; the html is NOT touched and no new html is written. Base dir: ${BASE}. Your page: ${FOLDER}/${name}.html

READ ONLY the original page: ${BASE}/${FOLDER}/${name}.html
Do NOT read FORMAT.md, ui-templates, worked examples, old .md files, or any sibling page — every convention you need is in this prompt (agent context is tight; the original page is large).

CREATE these new files next to the original:

1. ${name}.txt.md — the page's VERBATIM text content, structured as markdown:
   - Line 1: "# <page title>" — the h1 text WITHOUT any leading index number ("12. Topic" -> "Topic").
   - Then "**HTML title tag:** <the <title> tag's text, also without index number>".
   - Then "**Subtitle:** <subtitle text>" if the page has one.
   - Then one "## [sec-N] <section heading>" per content section, in document order, numbered from 1 (a section = one card/table/topic unit of the original).
   - Under each section: ALL of that section's visible text, verbatim — bold labels kept bold ("**Label** — phrase"), tag pills as "**Tags:** name (color), name (color)", callout/example/key-point boxes as labeled bold lines ("**Example:** ...", "**Key point:** ..."), table text as markdown tables.
   - Text inside <canvas> fallbacks or drawn BY JavaScript is NOT page text — exclude it.
   - No index numbers in any title line. No commentary, no summarizing, no rewording — verbatim only.
${vizSpec}

FOLDER RULES:
${F.folderRules}

CONSTRAINTS:
- CREATE-ONLY: never modify or delete the original .html, the old .md, or ANY other existing file. No git commands. No render/browser checks. Temp files go in /tmp, not the repo.
- If ${name}.txt.md ${VIZMD ? 'AND ' + name + '.viz.md both ' : ''}already exist, do nothing and return status "skipped".
- If a ${name}.v2.html exists, ignore it entirely.

VERIFY before returning (mechanically, e.g. with a small python script via Bash — do not re-read whole files into context):
- Every visible text phrase of the original html appears in ${name}.txt.md (strip the original's script/style/tags, collapse whitespace, compare token streams; only allowed loss = removed index numbers; markdown syntax characters in txt.md are fine).
${VIZMD ? `- Every <canvas id="..."> in the original has exactly one "## [sec-N] <canvasId>" brief in ${name}.viz.md, and every brief has Type/Data/Colors/Shows bullets.` : ''}
Fix any failure before returning.

Return JSON: page, status (created|skipped|failed), canvases (count in the original), notes (1-2 sentences: section count, theme/lib observations, anything unusual, verification result).`
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
