/* ============================================================
   ui-templates/js/three-col-dist.js — chart library for the
   three-column distribution template (09-three-col-distribution).
   Requires base.js (setupCanvas, mulberry32, randn, randExp).
   Canonical sources: real-world-distributions/12-healthtech.html
   (drawHistogram) and 01-ecommerce.html (drawBarChart), extracted
   verbatim.
   ============================================================ */

// Histogram drawing utility
function drawHistogram(canvasId, data, options) {
    var canvas = document.getElementById(canvasId);
    var dpr = window.devicePixelRatio || 1;
    var w = canvas.width;
    var h = canvas.height;
    canvas.style.maxWidth = w + 'px';
    var __cssW = canvas.getBoundingClientRect().width || w;
    var __scale = (__cssW / w) * dpr;
    canvas.width = Math.round(w * __scale);
    canvas.height = Math.round(h * __scale);
    var ctx = canvas.getContext('2d');
    ctx.scale(__scale, __scale);

    var opts = options || {};
    var bins = opts.bins || 40;
    var title = opts.title || '';
    var xLabel = opts.xLabel || '';
    var color = opts.color || 'rgba(39,174,96,0.4)';
    var strokeColor = opts.strokeColor || '#1a5276';
    var overlays = opts.overlays || null;

    var margin = { top: 40, right: 20, bottom: 45, left: 50 };
    var plotW = w - margin.left - margin.right;
    var plotH = h - margin.top - margin.bottom;

    // Compute histogram from data
    function computeHist(arr, numBins, minVal, maxVal) {
        var counts = new Array(numBins).fill(0);
        var binWidth = (maxVal - minVal) / numBins;
        for (var i = 0; i < arr.length; i++) {
            var idx = Math.floor((arr[i] - minVal) / binWidth);
            if (idx >= numBins) idx = numBins - 1;
            if (idx < 0) idx = 0;
            counts[idx]++;
        }
        return { counts: counts, binWidth: binWidth };
    }

    // Determine global min/max
    var allData = data;
    if (overlays) {
        for (var o = 0; o < overlays.length; o++) {
            allData = allData.concat(overlays[o].data);
        }
    }
    var minVal = opts.minVal !== undefined ? opts.minVal : Math.min.apply(null, allData);
    var maxVal = opts.maxVal !== undefined ? opts.maxVal : Math.max.apply(null, allData);
    var range = maxVal - minVal;
    minVal -= range * 0.02;
    maxVal += range * 0.02;

    var hist = computeHist(data, bins, minVal, maxVal);
    var maxCount = Math.max.apply(null, hist.counts);

    var overlayHists = [];
    if (overlays) {
        for (var o = 0; o < overlays.length; o++) {
            var oh = computeHist(overlays[o].data, bins, minVal, maxVal);
            overlayHists.push(oh);
            var ohMax = Math.max.apply(null, oh.counts);
            if (ohMax > maxCount) maxCount = ohMax;
        }
    }

    // Background
    ctx.fillStyle = '#fff';
    ctx.fillRect(0, 0, w, h);

    // Title
    ctx.fillStyle = '#1a5276';
    ctx.font = 'bold 13px -apple-system, BlinkMacSystemFont, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(title, w / 2, 22);

    // Draw axes
    ctx.strokeStyle = '#ccc';
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(margin.left, margin.top);
    ctx.lineTo(margin.left, margin.top + plotH);
    ctx.lineTo(margin.left + plotW, margin.top + plotH);
    ctx.stroke();

    // Draw primary histogram bars
    var barW = plotW / bins;
    function drawBars(histData, fillC, strokeC) {
        for (var i = 0; i < bins; i++) {
            var barH = (histData.counts[i] / maxCount) * plotH;
            var x = margin.left + i * barW;
            var y = margin.top + plotH - barH;
            ctx.fillStyle = fillC;
            ctx.fillRect(x, y, barW - 1, barH);
            ctx.strokeStyle = strokeC;
            ctx.lineWidth = 0.5;
            ctx.strokeRect(x, y, barW - 1, barH);
        }
    }

    drawBars(hist, color, strokeColor);

    // Draw overlays
    if (overlays) {
        for (var o = 0; o < overlayHists.length; o++) {
            drawBars(overlayHists[o], overlays[o].color || 'rgba(231,76,60,0.3)', overlays[o].strokeColor || '#e74c3c');
        }
    }


    // === Density line + SE band ===
    var _sigma = 1.5, _kernelR = Math.ceil(_sigma * 3);
    var _numBins = hist.counts.length;
    var _smoothed = new Array(_numBins).fill(0);
    for (var _i = 0; _i < _numBins; _i++) {
        var _wtSum = 0, _vSum = 0;
        for (var _j = Math.max(0, _i - _kernelR); _j <= Math.min(_numBins - 1, _i + _kernelR); _j++) {
            var _d = _j - _i;
            var _wt = Math.exp(-0.5 * (_d / _sigma) * (_d / _sigma));
            _vSum += hist.counts[_j] * _wt;
            _wtSum += _wt;
        }
        _smoothed[_i] = _vSum / _wtSum;
    }
    var _effN = Math.min(200, Math.max(30, data.length));
    var _upper = [], _lower = [];
    for (var _i = 0; _i < _numBins; _i++) {
        var _se = 1.96 * _smoothed[_i] / Math.sqrt(_effN);
        _upper.push(_smoothed[_i] + _se);
        _lower.push(Math.max(0, _smoothed[_i] - _se));
    }
    ctx.save();
    ctx.fillStyle = 'rgba(46,204,113,0.2)';
    ctx.beginPath();
    for (var _i = 0; _i < _numBins; _i++) {
        var _sx = margin.left + _i * barW + barW / 2;
        var _sy = margin.top + plotH - (_upper[_i] / maxCount) * plotH;
        if (_i === 0) ctx.moveTo(_sx, _sy); else ctx.lineTo(_sx, _sy);
    }
    for (var _i = _numBins - 1; _i >= 0; _i--) {
        var _sx = margin.left + _i * barW + barW / 2;
        var _sy = margin.top + plotH - (_lower[_i] / maxCount) * plotH;
        ctx.lineTo(_sx, _sy);
    }
    ctx.closePath(); ctx.fill();
    ctx.strokeStyle = '#1e8449';
    ctx.lineWidth = 2;
    ctx.beginPath();
    for (var _i = 0; _i < _numBins; _i++) {
        var _sx = margin.left + _i * barW + barW / 2;
        var _sy = margin.top + plotH - (_smoothed[_i] / maxCount) * plotH;
        if (_i === 0) ctx.moveTo(_sx, _sy); else ctx.lineTo(_sx, _sy);
    }
    ctx.stroke();
    ctx.restore();
    // === End density line + SE band ===

    // X-axis labels
    ctx.fillStyle = '#555';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'center';
    var numTicks = 5;
    for (var i = 0; i <= numTicks; i++) {
        var val = minVal + (maxVal - minVal) * (i / numTicks);
        var x = margin.left + (i / numTicks) * plotW;
        ctx.fillText(val.toFixed(opts.decimals !== undefined ? opts.decimals : 1), x, margin.top + plotH + 18);
        ctx.strokeStyle = '#ddd';
        ctx.beginPath();
        ctx.moveTo(x, margin.top + plotH);
        ctx.lineTo(x, margin.top + plotH + 5);
        ctx.stroke();
    }

    // X-axis label
    if (xLabel) {
        ctx.fillStyle = '#333';
        ctx.font = '12px sans-serif';
        ctx.textAlign = 'center';
        ctx.fillText(xLabel, margin.left + plotW / 2, margin.top + plotH + 38);
    }

    // Y-axis label
    ctx.fillStyle = '#555';
    ctx.font = '11px sans-serif';
    ctx.textAlign = 'right';
    ctx.fillText(maxCount, margin.left - 5, margin.top + 10);
    ctx.fillText('0', margin.left - 5, margin.top + plotH + 4);

    // Legend if overlays
    if (opts.legend) {
        var lx = margin.left + plotW - 10;
        var ly = margin.top + 10;
        ctx.textAlign = 'right';
        ctx.font = '11px sans-serif';
        for (var i = 0; i < opts.legend.length; i++) {
            ctx.fillStyle = opts.legend[i].color;
            ctx.fillRect(lx - 50, ly + i * 16 - 8, 12, 12);
            ctx.fillStyle = '#333';
            ctx.fillText(opts.legend[i].label, lx, ly + i * 16 + 2);
        }
    }
}

// Draw bar chart utility (for categorical/ordinal data)
function drawBarChart(canvasId, labels, values, options) {
  var canvas = document.getElementById(canvasId);
  var dpr = window.devicePixelRatio || 1;
  var cssW = canvas.width, cssH = canvas.height;
  canvas.style.maxWidth = cssW + 'px';
  var __cssW = canvas.getBoundingClientRect().width || cssW;
  var __scale = (__cssW / cssW) * dpr;
  canvas.width = Math.round(cssW * __scale);
  canvas.height = Math.round(cssH * __scale);
  var ctx = canvas.getContext('2d');
  ctx.scale(__scale, __scale);

  var opts = options || {};
  var title = opts.title || '';
  var xLabel = opts.xLabel || '';
  var yLabel = opts.yLabel || '';
  var barColor = opts.barColor || 'rgba(26,82,118,0.55)';
  var borderColor = opts.borderColor || '#1a5276';

  var margin = { top: 35, right: 20, bottom: 45, left: 55 };
  var w = cssW - margin.left - margin.right;
  var h = cssH - margin.top - margin.bottom;

  var maxVal = Math.max.apply(null, values);

  // Background
  ctx.fillStyle = '#fff';
  ctx.fillRect(0, 0, cssW, cssH);

  // Title
  ctx.fillStyle = '#1a5276';
  ctx.font = 'bold 13px -apple-system, sans-serif';
  ctx.textAlign = 'center';
  ctx.fillText(title, cssW / 2, 18);

  // Axes
  ctx.strokeStyle = '#999';
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(margin.left, margin.top);
  ctx.lineTo(margin.left, margin.top + h);
  ctx.lineTo(margin.left + w, margin.top + h);
  ctx.stroke();

  // Bars
  var barW = w / labels.length;
  var barPad = barW * 0.15;
  for (var i = 0; i < labels.length; i++) {
    var barH = (values[i] / maxVal) * h;
    var x = margin.left + i * barW + barPad;
    var y = margin.top + h - barH;
    ctx.fillStyle = barColor;
    ctx.fillRect(x, y, barW - 2 * barPad, barH);
    ctx.strokeStyle = borderColor;
    ctx.lineWidth = 0.5;
    ctx.strokeRect(x, y, barW - 2 * barPad, barH);
  }

  // X-axis labels
  ctx.fillStyle = '#555';
  ctx.font = '11px -apple-system, sans-serif';
  ctx.textAlign = 'center';
  for (var i = 0; i < labels.length; i++) {
    var xPos = margin.left + i * barW + barW / 2;
    ctx.fillText(labels[i], xPos, margin.top + h + 16);
  }

  // X label
  if (xLabel) {
    ctx.fillStyle = '#333';
    ctx.font = '12px -apple-system, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(xLabel, margin.left + w / 2, margin.top + h + 36);
  }

  // Y-axis labels
  ctx.textAlign = 'right';
  ctx.fillStyle = '#555';
  ctx.font = '11px -apple-system, sans-serif';
  for (var t = 0; t <= 4; t++) {
    var yVal = maxVal * (t / 4);
    var yPos = margin.top + h - (h * t / 4);
    ctx.fillText(opts.yFormat ? opts.yFormat(yVal) : yVal.toFixed(1), margin.left - 6, yPos + 4);
  }
}
