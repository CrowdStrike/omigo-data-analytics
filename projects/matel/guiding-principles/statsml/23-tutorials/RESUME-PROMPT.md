# Resume Prompt — Tutorials Section Build

Paste everything below this line into a fresh Claude Code session started in
`projects/matel/guiding-principles/statsml/` if the tutorials build needs to be
resumed or repaired.

---

Continue building the Tutorials section of this project. Most of the work is
done; your job is to find the gap between what the grid pages promise and what
exists on disk, and fill it.

## What already exists

- `index.html` has card "23. Tutorials" → `23-tutorials.html` (hub grid, 18 category cards, 5 per row)
- `23-tutorials/CLAUDE.md` — the BINDING page spec (read it first)
- Category grid pages: `23-tutorials/01-math-foundations.html` … `23-tutorials/64-search-queries.html`, each listing numbered topic cards whose hrefs (`<category-folder>/NN-slug.html`) are the canonical manifest of every topic page that must exist
- Most of the ~285 topic pages in the per-category folders

## Step 1 — Find missing pages

The grids are the source of truth. Run:

```bash
cd 23-tutorials
for g in 0*.html 1*.html; do
  grep -o 'href="[^"]*/[^"]*\.html"' "$g" | sed 's/href="//;s/"//' | while read f; do
    [ -f "$f" ] || echo "MISSING: $f (listed in $g)"
  done
done
```

Also flag incomplete files (interrupted writers): any topic page not ending in
`</html>` or under 100 lines is suspect — rewrite it.

## Step 2 — Write the missing pages via subagents

Fan out background subagents, ≤5 pages per agent, grouped by category. Each
agent prompt must be self-contained and say:

1. Working directory (absolute path to `statsml/`)
2. Read in order: `23-tutorials/CLAUDE.md`; `21-most-powerful-signals/07-social-graph-and-connections.html` (skeleton, CSS, tag pills, one-line bullets); `23-tutorials/07-relationships-and-regression/11-multiple-regression.html` (two canvases side by side in one viz cell when a row needs two charts); `22-recently-added-misc/03-tracking-data-collection-methods/06-session-replay.html` (content-amount ceiling)
3. If a target file already exists and looks complete (closing `</html>`), skip it
4. Per page: h1 (title from the grid card, no number) + one-line subtitle; 3-4 content-named `.card-section` blocks in the roles idea-via-running-example → hand-checkable worked example → why it matters → optional common confusion; per section 2-4 tag pills, 4-6 ONE-line non-wrapping bold-lead bullets, one italic `.example`, one `.key-point`, one 720×300 dpr-scaled canvas (bold 15-16px titles, ≥12px labels, palette colors, one bold in-chart annotation)
5. Hardcode all chart data (no `Math.random`); text numbers must match chart numbers; invented figures labeled "illustrative"; no cross-page links, no nav/back/home
6. Pick a concrete everyday running example per topic (orders table, coffee-shop sales, fraud scores, coin flips) — example first, definition after
7. Return one line per file created

Audience: layman / newbie data scientist. Simple words. More visual than textual.

## Step 3 — Known failure mode

Subagents occasionally die with "Request timed out" or 500 API errors —
relaunch the same prompt with the skip-complete-files instruction. Don't assume
a silent agent succeeded: verify its files exist.

## Step 4 — Final verification

- Re-run the Step 1 script until it prints nothing
- Spot-check a few new pages: every `<script>` must execute without throwing (stub a canvas context in node); bullets must not wrap
- Do NOT run git commands; the user handles git
