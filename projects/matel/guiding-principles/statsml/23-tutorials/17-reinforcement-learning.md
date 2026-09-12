# Reinforcement Learning

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Reinforcement Learning

**Subtitle:** How an agent learns by trial and error — try a move, get a score back, and slowly work out which actions pay off.

## Cards

Each card links to a topic page under `reinforcement-learning/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | FOUNDATIONS | Reinforcement Learning Basics | [17-reinforcement-learning/01-reinforcement-learning-basics.md](17-reinforcement-learning/01-reinforcement-learning-basics.md) | A learner tries a move and the world hands back a score — state is where it stands, action is what it tries, reward is the points that come back. | state / action / reward, gridworld, trial and error |
| 2 | FOUNDATIONS | Exploration vs Exploitation | [17-reinforcement-learning/02-exploration-vs-exploitation.md](17-reinforcement-learning/02-exploration-vs-exploitation.md) | Exploitation is going back to the best lunch spot you know; exploration is trying the one you don't — doing well over time means mixing the two. | the trade-off, bandits, trying new things |
| 3 | FOUNDATIONS | Q-Learning | [17-reinforcement-learning/03-q-learning.md](17-reinforcement-learning/03-q-learning.md) | A robot keeps a scorecard of how good each move is in each room — nudging one score after every step, the scorecard alone teaches it the way to the cheese. | Q-table, scorecard, value updates |
| 4 | AGENTS IN THE WILD | Games & Self-Play | [17-reinforcement-learning/04-games-and-self-play.md](17-reinforcement-learning/04-games-and-self-play.md) | A game player can be its own teacher: play both sides, see who wins, nudge the winning moves — run millions of times, that loop is how AlphaGo taught itself Go. | self-play, learning from wins, game AI |
| 5 | AGENTS IN THE WILD | Reward Hacking | [17-reinforcement-learning/05-reward-hacking.md](17-reinforcement-learning/05-reward-hacking.md) | When you score an agent on a stand-in for the real goal, a good enough learner will maximize the score in ways that ignore — or wreck — the goal itself. | proxy reward, gaming the score, misaligned goals |
| 6 | AGENTS IN THE WILD | Feedback Control Loops | [17-reinforcement-learning/06-feedback-control-loops.md](17-reinforcement-learning/06-feedback-control-loops.md) | A control loop measures, compares to a target, and acts — over and over; PID is three ways of reading the error so a thermostat lands on 21° without overshooting. | the loop, PID, oscillation |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "FOUNDATIONS" `#2980b9`, "AGENTS IN THE WILD" `#27ae60`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
