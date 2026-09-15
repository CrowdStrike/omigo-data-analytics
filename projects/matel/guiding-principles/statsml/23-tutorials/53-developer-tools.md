# Developer Tools

**Page type:** grid page (tutorials category grid: single flat 4-column nav-grid of cards with topic tags)
**HTML title tag:** Developer Tools

**Subtitle:** The tools programmers live in — from the 50-year-old shell to AI agents that write the code themselves — and the design ideas that made each one win.

## Cards

Each card links to a topic page under `developer-tools/`. The card shows a colored uppercase subcategory label (`.card-num`), a numbered title, a one-line description, and 2-4 topic tag pills. All cards sit in one flat `.nav-grid`; the colored labels carry the grouping.

| # | Category | Title | Link | Description | Topic tags |
|---|----------|-------|------|-------------|------------|
| 1 | CLASSICS | The Shell & the Command Line | [53-developer-tools/01-the-shell-and-the-command-line.md](53-developer-tools/01-the-shell-and-the-command-line.md) | Small tools that speak plain text, snapped together with pipes into programs no one had to write. | pipes, plain text, composition |
| 2 | CLASSICS | Debuggers | [53-developer-tools/02-debuggers.md](53-developer-tools/02-debuggers.md) | Freeze a running program at any line and inspect every variable and the full call stack. | breakpoints, call stack, live inspection |
| 3 | CLASSICS | Vim & Modal Editing | [53-developer-tools/03-vim-and-modal-editing.md](53-developer-tools/03-vim-and-modal-editing.md) | The same keys mean different things in different modes, turning editing into a small language of verbs and nouns. | modes, verbs and nouns, keyboard language |
| 4 | CLASSICS | Emacs | [53-developer-tools/04-emacs.md](53-developer-tools/04-emacs.md) | A tiny C core running a Lisp interpreter — everything else is live code you can rewrite while the editor runs. | lisp core, self-modifying, extensibility |
| 5 | IDES | IDEs — The Landscape | [53-developer-tools/05-ides-the-landscape.md](53-developer-tools/05-ides-the-landscape.md) | A text editor manipulates characters, an IDE understands the code — that one difference maps the whole landscape. | code-aware, refactoring, workbench |
| 6 | IDES | The Language Server Protocol | [53-developer-tools/06-the-language-server-protocol.md](53-developer-tools/06-the-language-server-protocol.md) | One server per language, any editor — an open protocol lets every editor share the same language brains. | open protocol, editor-agnostic, language brains |
| 7 | IDES | VS Code | [53-developer-tools/07-vs-code.md](53-developer-tools/07-vs-code.md) | A free editor that won a crowded field not by being the best editor, but by being the best platform to build on. | extensions, platform play, free editor |
| 8 | IDES | IntelliJ & JetBrains | [53-developer-tools/08-intellij-and-jetbrains.md](53-developer-tools/08-intellij-and-jetbrains.md) | Depth over breadth: the IDE reads the entire project, so renaming a method with 47 callers is one safe click. | whole-project index, safe refactoring, depth |
| 9 | IDES | Eclipse | [53-developer-tools/09-eclipse.md](53-developer-tools/09-eclipse.md) | The Java IDE that ruled the 2000s by making everything a plugin — and how an incumbent loses its throne. | plugins, java era, incumbent decline |
| 10 | IDES | Visual Studio | [53-developer-tools/10-visual-studio.md](53-developer-tools/10-visual-studio.md) | One vendor owns the language, runtime, frameworks, cloud, and IDE — total integration in one product. | total integration, windows, one vendor |
| 11 | IDES | Xcode | [53-developer-tools/11-xcode.md](53-developer-tools/11-xcode.md) | The only door to Apple's mobile platforms: every iPhone, iPad, and Watch app must pass through Xcode to ship. | apple platforms, code signing, gatekeeper |
| 12 | IDES | Android Studio | [53-developer-tools/12-android-studio.md](53-developer-tools/12-android-studio.md) | IntelliJ wearing Google's platform — a world-class IDE foundation rented, with only the Android layer built on top. | intellij base, google layer, partnership |
| 13 | AI ASSISTANTS | AI Coding Assistants — The Landscape | [53-developer-tools/13-ai-coding-assistants-the-landscape.md](53-developer-tools/13-ai-coding-assistants-the-landscape.md) | Every AI coding tool sits on one ladder — suggest, converse, delegate, replace — trading authorship for review burden. | autonomy ladder, authorship, review burden |
| 14 | AI ASSISTANTS | GitHub Copilot | [53-developer-tools/14-github-copilot.md](53-developer-tools/14-github-copilot.md) | Gray ghost text that finishes your line — near-zero friction, not just the model, is why it stuck. | ghost text, autocomplete, low friction |
| 15 | AI ASSISTANTS | OpenAI Codex | [53-developer-tools/15-openai-codex.md](53-developer-tools/15-openai-codex.md) | Programming as delegation: brief a task, let it work in a sandbox, review the returned diff like a teammate's PR. | cloud agent, sandbox, diff review |
| 16 | AI ASSISTANTS | Claude Code | [53-developer-tools/16-claude-code.md](53-developer-tools/16-claude-code.md) | A coding agent in the terminal, not the editor — it reads files, edits them, and runs commands until the task is done. | terminal agent, autonomy, task loop |
| 17 | AI ASSISTANTS | Cursor | [53-developer-tools/17-cursor.md](53-developer-tools/17-cursor.md) | A VS Code fork that redesigns completing, editing, and refactoring around a model that has read your whole codebase. | vs code fork, codebase context, ai-native editor |
| 18 | AI ASSISTANTS | Windsurf & the Agentic-IDE Wave | [53-developer-tools/18-windsurf-and-the-agentic-ide-wave.md](53-developer-tools/18-windsurf-and-the-agentic-ide-wave.md) | The bet that the editor itself should plan and act across files — within a year every major editor had an agent mode. | agent mode, multi-file, industry wave |
| 19 | AI ASSISTANTS | Aider | [53-developer-tools/19-aider.md](53-developer-tools/19-aider.md) | A terminal AI pair programmer that commits every AI edit to git, so any change is one revert away from undone. | git commits, terminal, undo safety |
| 20 | AI ASSISTANTS | Kimi & Open-Weight Contenders | [53-developer-tools/20-kimi-and-open-weight-contenders.md](53-developer-tools/20-kimi-and-open-weight-contenders.md) | Downloadable weights you run behind your own firewall — some frontier capability traded for code that never leaves. | open weights, self-hosted, privacy |
| 21 | AI ASSISTANTS | Devin & the Autonomous-Engineer Claim | [53-developer-tools/21-devin-and-the-autonomous-engineer-claim.md](53-developer-tools/21-devin-and-the-autonomous-engineer-claim.md) | A tool that aces its own demos can still fail most real tasks — an evaluation lesson, not a scandal. | demo vs production, evaluation, hype |
| 22 | BEYOND THE EDITOR | Jenkins & GitHub Actions | [53-developer-tools/22-jenkins-and-github-actions.md](53-developer-tools/22-jenkins-and-github-actions.md) | A machine that runs your tests on every proposed change — a server you run yourself versus a YAML file next to your code. | continuous integration, self-hosted vs saas, yaml pipelines |
| 23 | BEYOND THE EDITOR | Datadog & Splunk | [53-developer-tools/23-datadog-and-splunk.md](53-developer-tools/23-datadog-and-splunk.md) | Observability as a product: ship every log, metric, and trace to a vendor — with a bill that grows byte for byte. | observability, logs and metrics, cost growth |
| 24 | BEYOND THE EDITOR | PagerDuty & On-Call | [53-developer-tools/24-pagerduty-and-on-call.md](53-developer-tools/24-pagerduty-and-on-call.md) | On-call as a designed system: alerts route to whoever holds the rotation, and unanswered pages climb an escalation chain. | rotations, escalation, alerting |
| 25 | BEYOND THE EDITOR | Jupyter | [53-developer-tools/25-jupyter.md](53-developer-tools/25-jupyter.md) | Code, output, and prose in one live document — the kernel remembers every variable, which is its power and its famous trap. | notebooks, live kernel, hidden state |

## Regeneration instructions

To rebuild the HTML from this spec:

- **Template:** tutorials category grid. Single page: h1, `.subtitle` paragraph, then one flat `.nav-grid` of `.nav-card` anchors (no h2 section headings).
- **Layout:** `.nav-grid` is CSS grid, `repeat(4, 1fr)`, 16px gap, 15px top margin; responsive: 3 columns below 1400px, 2 below 1100px, 1 below 600px.
- **Links:** the tables above link to the `.md` versions for markdown navigation; in the regenerated HTML, each card's `href` is the same path with an `.html` extension instead.
- **Card structure:** `<a class="nav-card" href="...">` containing `<div class="card-num">SUBCATEGORY LABEL</div>`, `<h3>N. Topic Title</h3>` (unpadded index number matching the 2-digit zero-padded file index), `<p>description</p>`, then `<div class="topics">` of `<span class="topic-tag">` pills.
- **Category label colors:** applied by a small script mapping `.card-num` text to color — "CLASSICS" `#2980b9`, "IDES" `#27ae60`, "AI ASSISTANTS" `#8e44ad`, "BEYOND THE EDITOR" `#d35400`; the CSS default for `.card-num` is `#2980b9`, 0.75em bold, 4px bottom margin.
- **Card style:** background `#ffffff`, border `1px solid #d8d8d8`, radius 10px, padding 20px, shadow `0 2px 4px rgba(0,0,0,0.05)`, transition on border-color/transform; hover: border `#2980b9`, `translateY(-2px)`. h3 `#1a3a4a` 1em with 6px bottom margin; description `#555` 0.85em. Topic tags: background `#f0f0f0`, border `1px solid #ccc`, radius 4px, padding 2px 6px, 0.7em `#666`, in a flex-wrap row with 4px gap and 8px top margin.
- **Page style:** body system sans-serif, background `#f5f5f0`, text `#2a2a2a`, padding 40px, line-height 1.6; universal `* { margin:0; padding:0; box-sizing:border-box }` reset; h1 1.8em `#2980b9` with 10px bottom margin; subtitle `#666` 1.05em with 30px bottom margin. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22` (page accents here use `#2980b9`, `#27ae60`, `#8e44ad`, `#d35400`).
- **Canvases:** none on this page; any canvases elsewhere in this series use `window.devicePixelRatio` scaling.
