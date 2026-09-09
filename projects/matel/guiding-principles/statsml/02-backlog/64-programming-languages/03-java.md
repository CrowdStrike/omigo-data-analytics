# Java

**Page type:** detail page (single two-column layout row: text left 45% with bullets/key-point/tags, viz right 55% with canvas + code block)
**HTML title tag:** Java

**Subtitle:** Write once, run anywhere. Bytecode + JIT + GC. Safety over flexibility.

## Text column

- JVM — interpreter + JIT optimises hot paths
- Garbage collection — no manual memory
- Strict single-inheritance OOP
- Platform independence via bytecode

**Key point (callout):** **Trade-off:** verbose boilerplate and GC pauses in exchange for safety, portability, and decades of backwards compatibility.

**Tag pills:** JVM (blue), GC managed (green), portable (green), verbose (orange), GC pauses (red)

## Viz column

### Visualization (canvas `jvm`, 720×220)

Vertical pipeline / architecture diagram: source → bytecode → JVM container holding three components.

- **Box helper style:** each labeled box has a fill of its color at hex-alpha `18` (~9%), 2px stroke in its color, bold 13px centered label `#2c3e50`; optional sub-label 11px `#666`.
- **Pipeline (centered at x=360):**
  - Box ".java source" at (280, 20), 160×40, color `#2980b9`.
  - Arrow down (gray `#7f8c8d`, 1.5px, filled arrowhead) from y=60 to y=80.
  - Box ".class bytecode" at (280, 82), 160×40, color `#8e44ad`, sub-label "portable".
  - Arrow down from y=122 to y=148.
- **JVM container:** rectangle at (60, 150), width 600, height 110, fill `rgba(26,82,118,0.05)`, stroke `#1a5276` 2px, bold 14px label "JVM" centered at (360, 170). Note: the container's declared height (110, bottom edge y=260) extends past the 220px canvas height, so its bottom border is clipped in the original.
- **Inner boxes (inside the container, each 34px tall at y=180):** "Interpreter" at x=80 width 160, color `#27ae60`; "JIT Compiler" at x=280 width 160, color `#e67e22`; "GC" at x=480 width 140, color `#e74c3c`.
- **Caption (12px `#555`, centered at bottom, y=h−16):** "Same bytecode → Linux, macOS, Windows".

**Code block (below the canvas, inline-styled `<pre><code>`):**

```java
public class BankAccount {
    private double balance;

    public BankAccount(double initial) {
        this.balance = initial;
    }

    public synchronized void deposit(double amount) {
        if (amount <= 0)
            throw new IllegalArgumentException("Positive only");
        this.balance += amount;
    }

    public double getBalance() { return balance; }
}
// No malloc, no free, no pointers.
// GC reclaims when no references remain.
```

## Regeneration instructions

- **Layout:** compact-signals style detail page: h1 with bottom border, `.subtitle`, then one `table.layout` with a single `<tr>`: `td.text-col` (45%) holds a bullet `<ul>`, a `.key-point` callout, and a `.tags` pill row; `td.viz-col` (55%) holds the canvas followed by an inline-styled `<pre><code>` block (background `#f4f6f8`, border `1px solid #e0e0e0`, radius 4px, padding 12px 14px, 0.82rem, line-height 1.5, margin-top 12px).
- **Page style:** universal reset; body `system-ui, -apple-system, sans-serif`, white background, text `#2c3e50`, padding 40px, line-height 1.6; h1 2rem `#1a5276` with `border-bottom: 2px solid #2980b9` and 8px bottom padding; `.subtitle` `#666` 0.95rem; `table.layout` full width, `border-collapse: collapse`, cells vertical-align top with 12px padding; `ul` 0.92rem. No nav bar, no back/home links.
- **Callout:** `.key-point` background `#f8f9fa`, left border `3px solid #e74c3c`, padding 8px 12px, 0.9rem.
- **Tags:** `.tags` flex row with 8px gap; `.tag` padding 4px 10px, radius 4px, 0.78rem, weight 600; `.tag-green` `#eafaf1`/`#1e8449`, `.tag-red` `#fdedec`/`#c0392b`, `.tag-blue` `#eaf2f8`/`#1a5276`, `.tag-orange` `#fef9e7`/`#b7540c`.
- **Canvas:** CSS `width: 100%`, border `1px solid #e0e0e0`, radius 4px; intrinsic 720×220 scaled by `window.devicePixelRatio` (cap display at the logical width via `style.maxWidth`, backing store = rendered width × dpr, `ctx.scale` back to logical coordinates).
- **Palette:** primary blue `#1a5276`, secondary blue `#2980b9`, green `#27ae60`, red `#e74c3c`, orange `#e67e22`, purple `#8e44ad`, arrow gray `#7f8c8d`.
