# 5. JavaScript

**Page type:** detail page (philosophy callout + two-column attribute table: label left 32%, content right; no canvases)
**HTML title tag:** 5. JavaScript

**Subtitle:** Simple Java-like syntax for the browser — the only language the frontend runs natively

## Callout (philosophy box)

**Core trade-off:** The language of the web by accident. Designed in 10 days, became the only native browser language. Everything compiles to it or uses it. Ubiquity over elegance.

## What It Is

A dynamically-typed, prototype-based language with first-class functions. Originally designed for browser scripting, now runs everywhere via Node.js. Single-threaded with an event loop for non-blocking I/O.

## Unique Contribution

Monopoly on browser execution — no other language runs natively in all browsers. Event-loop concurrency model that handles thousands of concurrent connections without threads. Enabled the interactive web and single-page applications.

## Strengths

Runs everywhere (browser + server via Node.js), non-blocking I/O handles high concurrency, massive npm ecosystem (2M+ packages), JSON is native, rapid iteration cycle, V8 engine makes it surprisingly fast.

## Weaknesses

Type coercion surprises (== vs ===, "1" + 1 = "11"), callback hell (mitigated by async/await), fragmented ecosystem churn (new framework every year), no integers (everything was float64 until BigInt), "this" binding confusion.

## Business Use Case

All frontend web development, Node.js backends (Express, Fastify), React Native mobile apps, Electron desktop apps (VS Code, Slack), serverless functions, real-time applications (WebSocket servers).

## Example

Code block (`<pre><code>`):

```javascript
// Event loop: non-blocking I/O without threads
async function fetchUserData(userId) {
    // This doesn't block the main thread
    const response = await fetch(`/api/users/${userId}`);
    const user = await response.json();
    return user;
}

// Process 1000 requests concurrently — single thread
const userIds = Array.from({length: 1000}, (_, i) => i);
const results = await Promise.all(
    userIds.map(id => fetchUserData(id))
);
// All 1000 requests in-flight simultaneously
// No threads, no locks, no race conditions
```

## Regeneration instructions

- **Layout:** single page: h1, `.subtitle` paragraph, one `.philosophy` callout, then a full-width two-column table with one `<tr>` per section above (label in first `<td>`, content in second). The Example row's content cell holds a `<pre><code>` block with the code verbatim.
- **Table style:** `border-collapse: collapse`; cells `border: 1px solid #cfe0f0`, padding 14px 16px, `vertical-align: top`; even rows background `#f7fbff`; first column width 32%, weight 600, color `#1a5276`.
- **Callout style:** `.philosophy` — background `#f0f4f8`, left border `4px solid #2980b9`, padding 12px 16px, margin 20px 0 28px 0, 0.95em, color `#222`.
- **Code block style:** `pre` — background `#f4f6f8`, border `1px solid #dde4ea`, radius 4px, padding 12px 14px, 0.9em, `overflow-x: auto`; `code` in 'SF Mono'/'Fira Code'/'Consolas' monospace.
- **Page style:** body -apple-system/BlinkMacSystemFont/'Segoe UI' sans-serif, padding 32px 28px, white background, text `#1a1a1a`, font-size 15px, line-height 1.6; h1 1.6em `#1a5276`; subtitle `#666` 1.05em. No nav bar, no back/home links.
- **Palette:** primary blue `#1a5276`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`.
- **Canvases:** none on this page; if any are added, use `window.devicePixelRatio` scaling. In regenerated HTML, any card links use `.html` extensions.
