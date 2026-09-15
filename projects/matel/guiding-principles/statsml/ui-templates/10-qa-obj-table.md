# Template 10 — Q&A Obj-Table Detail

**Use for:** survey/reference detail pages where each row answers one question about a mechanism: what it is, how it works, what the data/payload looks like. The right cell holds either a chart OR a monospace payload block.

**Source pages:** all `recently-added-misc/` subfolders (tracking-data-collection-methods, platform-apis-fetch/activity, data-exports, sport-specific-wearables), `backlog/platform-privacy-policies/`, `backlog/programming-languages/` (reduced).

## Anatomy

- `h1` (1.8rem) + `.subtitle`.
- Single `<table class="obj-table">`; each `<tr>` is one Q&A section:
  - **left 50%** — `.obj-title` (1.1em bold accent, acts as the section header — no real h2), `.lede` one-sentence answer, labeled bullets (`li b` accent leads), optional `.key-point` callout, optional `.lbl-*` chips (purpose/effect — colored, information-bearing).
  - **right 50%** — centered `<canvas>` OR `.payload` block (monospace fake-log/JSON/code sample with `.payload-note` caption).
- Optional `.disclaimer` (orange border) for caveat text.

## Type & color

- Font: Type A apple-em stack; h1 1.8rem; body 0.93-0.95em; text `#2c3e50`.
- Background `#fff`; table borders `#2980b9`; accent `#1a5276` (themeable per THEMES.md).
- `.payload`: `#263238` bg, `#eceff1` text, 0.8em monospace, radius 6px.

## Viz / js

- Shared: `js/base.js` (setupCanvas, registerChart, seeded RNG). Charts simple: 1-4 per page, diagrams and small plots drawn with page-local code inside the row's VIZ fence.
- Payload blocks are content, not viz — they live with the text in txt.md.
