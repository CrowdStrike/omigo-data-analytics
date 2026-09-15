/* ============================================================
   ui-templates/js/base.js — shared canvas foundation
   Load before any chart code:
     <script src="../ui-templates/js/base.js"></script>
   Conventions match the corpus-canonical page code:
   - setupCanvas(id, w, h) returns a bare ctx (high-DPI scaled)
   - registerChart(fn) draws now and re-draws on resize
   Pages may still define local helpers; page-level declarations
   override these (page script runs after this file).
   ============================================================ */

/* ---- High-DPI canvas setup (corpus-canonical: returns bare ctx) ----
   setupCanvas('c1', 720, 380)  -> explicit logical size
   setupCanvas('c1')            -> reads width/height attributes
   Backing store is sized to displayed width x devicePixelRatio. */
function setupCanvas(id, wIn, hIn) {
    var c = document.getElementById(id); if (!c) return null;
    var w = wIn || c.getAttribute('width') * 1 || 720;
    var h = hIn || c.getAttribute('height') * 1 || 300;
    var dpr = window.devicePixelRatio || 1;
    c.style.maxWidth = w + 'px';
    var cssW = c.getBoundingClientRect().width || w;
    var scale = (cssW / w) * dpr;
    c.width = Math.round(w * scale); c.height = Math.round(h * scale);
    var ctx = c.getContext('2d'); ctx.scale(scale, scale);
    ctx.clearRect(0, 0, w, h);
    return ctx;
}
/* Back-compat alias used by older pages */
function setup(id, wIn, hIn) { return setupCanvas(id, wIn, hIn); }

/* ---- Chart registry: draw immediately, re-draw on resize ----
   registerChart(function () { var ctx = setupCanvas('c1', 720, 380); ... });
   Legacy pages that fill __charts themselves may keep doing so and
   call __renderCharts() once at the end. */
var __charts = [];
function registerChart(fn) { __charts.push(fn); fn(); }
function __renderCharts() { __charts.forEach(function (f) { f(); }); }
var __redrawTimer;
window.addEventListener('resize', function () {
    clearTimeout(__redrawTimer);
    __redrawTimer = setTimeout(__renderCharts, 150);
});

/* ---- Seeded randomness (deterministic charts; never Math.random) ---- */
function mulberry32(a) {
    return function () {
        a |= 0; a = a + 0x6D2B79F5 | 0;
        var t = Math.imul(a ^ a >>> 15, 1 | a);
        t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t;
        return ((t ^ t >>> 14) >>> 0) / 4294967296;
    };
}
var rng = mulberry32(42);
function randn() {
    var u = 0, v = 0;
    while (u === 0) u = rng();
    while (v === 0) v = rng();
    return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}
function randExp(lambda) { return -Math.log(1 - rng()) / lambda; }

/* ---- House drawing primitives (defaults per THEMES.md) ---- */
function roundRectPath(ctx, x, y, w, h, r) {
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
}
function drawArrow(ctx, x1, y1, x2, y2, color, width) {
    color = color || '#e67e22'; width = width || 2;
    var ang = Math.atan2(y2 - y1, x2 - x1), hl = 8;
    ctx.strokeStyle = color; ctx.fillStyle = color; ctx.lineWidth = width;
    ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2); ctx.stroke();
    ctx.beginPath();
    ctx.moveTo(x2, y2);
    ctx.lineTo(x2 - hl * Math.cos(ang - Math.PI / 6), y2 - hl * Math.sin(ang - Math.PI / 6));
    ctx.lineTo(x2 - hl * Math.cos(ang + Math.PI / 6), y2 - hl * Math.sin(ang + Math.PI / 6));
    ctx.closePath(); ctx.fill();
}
function drawAxes(ctx, margin, plotW, plotH, color) {
    color = color || '#ccc';
    ctx.strokeStyle = color; ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(margin.left, margin.top);
    ctx.lineTo(margin.left, margin.top + plotH);
    ctx.lineTo(margin.left + plotW, margin.top + plotH);
    ctx.stroke();
}
