# Project Instructions

## Critical: Challenge All Claims

When the user makes a statement, conclusion, or design decision, do NOT simply agree. Instead:

1. Verify that the claim is logically sound and scientifically valid
2. If it contradicts known statistics/ML theory, say so directly
3. If the reasoning has a gap or unstated assumption, point it out
4. If a simpler explanation or counterexample exists, present it
5. If the conclusion is correct, confirm it with the reasoning — not just "yes"
6. nefore creating a new html page or grid, check if there is something from backlog that already exists

This applies especially to:
- Statistical assumptions and when they hold
- Claims about sample sizes, thresholds, distributions
- Design decisions that might introduce the same problems being solved (e.g., replacing magic numbers with different magic numbers)
- Overgeneralizations about ML algorithms

Disagreement is expected and preferred over false agreement. Be direct.

## Project Context

See `./STATSML.md` for core ideas, principles, architecture, and phases.

This is a statistical ML library (omigo-data-analytics-statsml) focused on verifying statistical preconditions before applying tests/models, multi-candidate parameter validation, and feature profiling pipelines.

## Conversation Style

- Wait for the user to guide the conversation direction
- Use prior knowledge rather than web search unless asked otherwise
- Keep responses concise and direct
- Use professional language that conveys the message but is not offensive.

## Brainstorming & Design Process

When brainstorming or designing a new concept:

1. Talk it through with examples first — don't jump to implementation
2. Take notes at high level, think about structure
3. Capture the core idea in a visual HTML doc (see layout rules below)
4. Sit on it — the user may want days to incubate before formalizing
5. Only then convert to a mathematical data model with confidence, coverage

## Documentation Style

Create docs that can be read and reviewed quickly. Keep them short and scannable:

- One canvas visualization + a few sentences per concept. No walls of text.
- Show only the latest/best classifier results (v3-full). Do not include historical v1/v2 numbers.
- Per feature: title, histogram, detected shape (one line), gap/valley (yes/no), 2-3 sentence summary, result. That's it.
- No extended feature lists, no categorical analysis sections, no supplementary output dumps.
- Tables over prose. Short rows over verbose paragraphs.
- If a section requires scrolling past 2 screens, it's too long — split or cut.

### HTML Design Doc Layout

When creating HTML docs, use templates from `docs/statsml/ui-templates/` as the starting point. The templates cover:

| Template | Use for |
|----------|---------|
| 01-landing-page | Top-level hub pages |
| 02-nav-grid | Section navigation |
| 03-toc-reference | Long reference docs with TOC |
| 04-two-col-catalog-badges | Catalog pages with status badges |
| 05-two-col-catalog-clean | Clean two-column catalogs |
| 06-sectioned-cards-callout | Card-based sections with callouts |

See `ui-templates/README.md` for full usage guide.

Additional canvas/chart rules:
- **Canvas sizing:** minimum 720px width, height 300-460px, use `width: 100%`
- **Grid galleries:** max 3 charts per row, minimum 200px height per chart
- **devicePixelRatio scaling:** always use `window.devicePixelRatio` for retina
- **Color palette:** #1a5276 (primary blue), #27ae60 (green), #e74c3c (red), #e67e22 (orange), rgba(26,82,118,0.35) (bar fill)


## Best Practices
 - Use predefined ui-templates to understand style, format, coloring, font etc scheme. Esp when multiple agents are created to write docs under a grid.
 - Dont include count of items in summary or in the card. Because thats a lose dependent number that need to be updated everytime.
 - Dont run any Git commands. I will do all Git stuff on my own. If git history is needed to find some previous version etc. then ask.
 - Dont do extensive testing to verify the rendering of html docs unless told to do so specifically.
 - dont put any cross reference links from one page to another except the index grid cards where it is for navigation. no back, home links kind of things either.
 - Some grid pages like backlog may have self reference links which is okay
 - Grid pages with cards should hae index number for each card. That index number should match the file index number in naming convention

## Folder-Level Instructions

Each subfolder has its own `CLAUDE.md` with folder-specific context and pending TODOs. See:

| Folder | Focus |
|--------|-------|
| `ab-testing/` | A/B testing pitfalls and methodology |
| `domains/` | Industry-specific data traps |
| `folk-wisdom/` | Popular sayings decomposed for hidden fallacies |
| `real-world-distributions/` | Surprising shapes from real data |
| `cognitive-biases/` | Human psychology failures in analysis |
| `metrics/` | Good/bad metrics, vanity metrics, reporting |
| `backlog/` | Unresolved topics and future directions |
| `interesting-problems-paradoxes/` | Classic puzzles and real-world analogues |
| `common-bad-practices/` | Organizational anti-patterns in data teams |

## Global TODO

- Remove dead `.nav` CSS rules from ~289 HTML files (the `<div class="nav">` elements are already gone, but the style blocks remain as unused code)

## Thoughts to added to the docs in the correct places
- 17-folk-wisdom-fallacies.html: girls grow faster than boys
- concepts: bayesian statistics wrt AB testing, Multi Arm Bandit - challenges in real life
- real-world-distributions/01-ecommerce.html: multiple items buying in single session. not restricting to just the item at the start
- domain pitfalls - without proper identification, AI agents masquerading as humans poisonsing user generated content and tracking data
- Double Counting AI assisted work - AI agents writing emails from sender, then AI agents reading emails to summarize.
- rename 151-forced-participation-data-poisoning.html to forced survey participation impact.
- Human Psychology - building affection, attraction, attachment to AI generated content faking human expressions, emotions. Changing the reality, twisting expections
  from real world interaction 
- Human Psychology: Freedom to talk to Chat Bot without having to be super politically correct which at times can be restricting.
- High churn rate at 4, 8 yrs mark - Iniial Stock grants expiry
- Survey and Feedback question bias response to the position rank
- 30% tax on app store that goes away in web purchase. youtube example.
- subscription tier trap where older tiers are reduced in functionality and newer ones are projected at higher price.
- minimum balance in bank mathematics - hidden cost 
- mobile app vs web browser tracking
- browser tracking
- cookies
- fix domains/140-ai-code-feature-capacity-musical-chair.html
- metrics that brag about capability for publicity, but actually are not good. Storage Size counter in GMail/Yahoo, Number of Parameters in LLM models, Number of events processed per day
  (noisy data can inflate this number), Number of friends deciding popularity, Number of Likes or Social Media Followers, Number of results in search engine, Tokenmaxxing
- Graph Theory
- Combinatorics
- Logical Thinking
- Induction, examples of induction from real life
- Concepts: Compilers and Interpreters
- P and NP Complete
- Client vs Server Based Architecture
- rename domains/143-silent-data-poisoning.html to Silent Bugs Poisoning Data
- linked in aspect of what kind of candidates are more likely to be searching for jobs.
- realtime analytics
- graph analytics, visualizations
- visualization libraries, pros and cons
- linkedin search problem, job recommendation as query, ranking signal
- pinterest data science problems - image search using sections, refer to medium blog posts
- engineering systems are enablers for data science - google big table, facebook image serving, tagging, tiktok speed, twitter realtime, netflix mutli level data compression
- interesting problems - snapchat world map live stream
- Philosophical Thoughts / Words of Wisdom for engineers and data scientists. Example: "Slow is steady, steady is fast" — patience and consistency compound into speed.
 
