# applied-game-theory-behavioral-design/

Game theory and behavioral design mechanics applied to product/pricing decisions — decoys, streaks, auctions, social proof, subscriptions, charm pricing.

## Format (migrated 2026-08-29)

- Three-file pages: `NN-topic.txt.md` (text) + `NN-topic.viz.md` (viz briefs) + `NN-topic.html` (fenced). See `../ui-templates/FORMAT.md`.
- **Template:** 05-two-col-catalog-clean, game-theory variant — h2 sections, `.obj-table` 45|55, `.philosophy` intro/outro, `.math-box` worked-math blocks with inline `code`.
- **Theme:** house-blue. **Fonts:** Type A apple-em (see `../ui-templates/THEMES.md`).
- **Charts:** original .html keeps page-local LIB (bare-ctx setupCanvas) for comparison; `.v2.html` pages use `../ui-templates/js/base.js` (canonical bare-ctx setupCanvas + registerChart) with boilerplate removed.
- Detail-page `<title>`/`<h1>` carry no index number; the `N.` lives on the grid card (root `19-applied-game-theory-behavioral-design.html`) and in filenames.
- Old single `.md` files are kept until the user reviews this folder's migration, then deleted.

## On moves in/out

Pages are fully self-contained (no script src) — moving needs only the three files moved together and the grid card updated.
