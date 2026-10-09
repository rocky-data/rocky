// Rocky docs site: search and the interactive lineage graph.
// Reads window.ROCKY_DOCS (assets/data.js). No network access, no libraries.
(function () {
  'use strict';
  var D = window.ROCKY_DOCS;
  if (!D) { return; }
  var ROOT = document.body.getAttribute('data-root') || '';
  var SVGNS = 'http://www.w3.org/2000/svg';

  var byName = {};
  D.models.forEach(function (m) { byName[m.name] = { kind: 'model', item: m }; });
  D.sources.forEach(function (s) { byName[s.name] = { kind: 'source', item: s }; });

  function pageUrl(name) {
    var o = byName[name];
    if (!o) { return null; }
    return ROOT + (o.kind === 'model' ? 'models/' : 'sources/') + o.item.slug + '.html';
  }

  // ---------------------------------------------------------------- search
  function initSearch() {
    var box = document.getElementById('search');
    var out = document.getElementById('search-results');
    if (!box || !out) { return; }
    var entries = [];
    D.models.forEach(function (m) {
      entries.push({
        label: m.name, sub: 'model' + (m.description ? ' - ' + m.description : ''),
        href: pageUrl(m.name), hay: (m.name + ' ' + (m.description || '')).toLowerCase(), rank: 0
      });
      m.columns.forEach(function (c) {
        entries.push({
          label: m.name + '.' + c.name,
          sub: 'column ' + c.type + (c.description ? ' - ' + c.description : ''),
          href: pageUrl(m.name) + '#col-' + c.name,
          hay: (c.name + ' ' + m.name + ' ' + (c.description || '')).toLowerCase(), rank: 1
        });
      });
    });
    D.sources.forEach(function (s) {
      entries.push({ label: s.name, sub: 'source', href: pageUrl(s.name), hay: s.name.toLowerCase(), rank: 0 });
    });
    var active = -1;
    var shown = [];

    function render() {
      while (out.firstChild) { out.removeChild(out.firstChild); }
      shown.forEach(function (e, i) {
        var a = document.createElement('a');
        a.href = e.href;
        a.className = 'result' + (i === active ? ' active' : '');
        var l = document.createElement('span');
        l.className = 'result-label';
        l.textContent = e.label;
        var s = document.createElement('span');
        s.className = 'result-sub';
        s.textContent = e.sub;
        a.appendChild(l);
        a.appendChild(s);
        out.appendChild(a);
      });
      out.hidden = shown.length === 0;
    }

    function run() {
      var q = box.value.trim().toLowerCase();
      active = -1;
      if (!q) { shown = []; render(); return; }
      var tokens = q.split(/\s+/);
      var scored = [];
      entries.forEach(function (e) {
        for (var i = 0; i < tokens.length; i++) {
          if (e.hay.indexOf(tokens[i]) === -1) { return; }
        }
        var label = e.label.toLowerCase();
        var score = e.rank * 10 + (label.indexOf(q) === 0 ? 0 : label.indexOf(q) > 0 ? 2 : 5);
        scored.push({ e: e, score: score + label.length / 1000 });
      });
      scored.sort(function (a, b) { return a.score - b.score; });
      shown = scored.slice(0, 15).map(function (s) { return s.e; });
      render();
    }

    box.addEventListener('input', run);
    box.addEventListener('focus', run);
    box.addEventListener('keydown', function (ev) {
      if (ev.key === 'ArrowDown') { active = Math.min(shown.length - 1, active + 1); render(); ev.preventDefault(); }
      else if (ev.key === 'ArrowUp') { active = Math.max(0, active - 1); render(); ev.preventDefault(); }
      else if (ev.key === 'Enter' && shown.length) { window.location.href = shown[Math.max(0, active)].href; }
      else if (ev.key === 'Escape') { shown = []; render(); box.blur(); }
    });
    document.addEventListener('click', function (ev) {
      if (ev.target !== box && !out.contains(ev.target)) { shown = []; render(); }
    });
    document.addEventListener('keydown', function (ev) {
      if (ev.key === '/' && document.activeElement !== box && !/input|textarea/i.test(document.activeElement.tagName)) {
        box.focus();
        ev.preventDefault();
      }
    });
  }

  // --------------------------------------------------------- table filter
  function initTableFilter() {
    var input = document.getElementById('filter');
    if (!input) { return; }
    input.addEventListener('input', function () {
      var q = input.value.trim().toLowerCase();
      var rows = document.querySelectorAll('table.filterable tbody tr');
      for (var i = 0; i < rows.length; i++) {
        var hay = rows[i].getAttribute('data-hay') || '';
        rows[i].hidden = q !== '' && hay.indexOf(q) === -1;
      }
    });
  }

  // -------------------------------------------------------------- lineage
  function el(name, attrs, text) {
    var n = document.createElementNS(SVGNS, name);
    Object.keys(attrs || {}).forEach(function (k) { n.setAttribute(k, attrs[k]); });
    if (text !== undefined) { n.textContent = text; }
    return n;
  }

  function initDag() {
    var svg = document.getElementById('dag');
    if (!svg) { return; }
    var panel = document.getElementById('panel');
    var NODE_W = 200, NODE_H = 34, GAP_X = 90, GAP_Y = 16;

    var ups = {}, downs = {};
    var edges = D.edges.filter(function (e) { return byName[e[0]] && byName[e[1]]; });
    edges.forEach(function (e) {
      (downs[e[0]] = downs[e[0]] || []).push(e[1]);
      (ups[e[1]] = ups[e[1]] || []).push(e[0]);
    });

    function closure(start, next) {
      var seen = {};
      seen[start] = true;
      var queue = [start];
      while (queue.length) {
        var n = queue.shift();
        (next[n] || []).forEach(function (m) { if (!seen[m]) { seen[m] = true; queue.push(m); } });
      }
      return seen;
    }

    var focus = null;
    var m = /focus=([^&]+)/.exec(window.location.hash);
    if (m) { focus = decodeURIComponent(m[1]); }
    if (focus && !byName[focus]) { focus = null; }

    var visible = {};
    if (focus) {
      var a = closure(focus, ups), b = closure(focus, downs);
      Object.keys(a).forEach(function (k) { visible[k] = true; });
      Object.keys(b).forEach(function (k) { visible[k] = true; });
    } else {
      Object.keys(byName).forEach(function (k) {
        var o = byName[k];
        if (o.kind === 'model' || ups[k] || downs[k]) { visible[k] = true; }
      });
    }
    var names = Object.keys(visible).sort();
    var vEdges = edges.filter(function (e) { return visible[e[0]] && visible[e[1]]; });

    // Layers: longest path from the roots. The pass bound guards a cycle.
    var layer = {};
    names.forEach(function (n) { layer[n] = 0; });
    for (var pass = 0; pass < names.length; pass++) {
      var changed = false;
      vEdges.forEach(function (e) {
        if (layer[e[1]] < layer[e[0]] + 1 && layer[e[0]] + 1 <= names.length) {
          layer[e[1]] = layer[e[0]] + 1;
          changed = true;
        }
      });
      if (!changed) { break; }
    }
    var layers = [];
    names.forEach(function (n) { (layers[layer[n]] = layers[layer[n]] || []).push(n); });
    for (var li0 = 0; li0 < layers.length; li0++) { layers[li0] = layers[li0] || []; }
    var pos = {};
    function place() {
      layers.forEach(function (l, li) {
        l.forEach(function (n, i) { pos[n] = { x: li * (NODE_W + GAP_X), y: i * (NODE_H + GAP_Y) }; });
      });
    }
    place();
    for (var sweep = 0; sweep < 3; sweep++) {
      layers.forEach(function (l, li) {
        if (li === 0) { return; }
        var bary = {};
        l.forEach(function (n) {
          var ps = (ups[n] || []).filter(function (p) { return visible[p]; });
          bary[n] = ps.length ? ps.reduce(function (s, p) { return s + pos[p].y; }, 0) / ps.length : pos[n].y;
        });
        l.sort(function (x, y) { return bary[x] - bary[y] || (x < y ? -1 : 1); });
        l.forEach(function (n, i) { pos[n] = { x: li * (NODE_W + GAP_X), y: i * (NODE_H + GAP_Y) }; });
      });
    }

    var vp = el('g', {});
    svg.appendChild(vp);
    var edgeEls = {};
    var nodeEls = {};
    vEdges.forEach(function (e) {
      var p = pos[e[0]], q = pos[e[1]];
      var x1 = p.x + NODE_W, y1 = p.y + NODE_H / 2, x2 = q.x, y2 = q.y + NODE_H / 2;
      var mx = (x1 + x2) / 2;
      var path = el('path', { d: 'M' + x1 + ' ' + y1 + ' C' + mx + ' ' + y1 + ' ' + mx + ' ' + y2 + ' ' + x2 + ' ' + y2, 'class': 'edge' });
      vp.appendChild(path);
      edgeEls[e[0] + '\u0000' + e[1]] = path;
    });
    names.forEach(function (n) {
      var o = byName[n];
      var g = el('g', { 'class': 'node ' + o.kind, transform: 'translate(' + pos[n].x + ',' + pos[n].y + ')', tabindex: '0' });
      g.appendChild(el('title', {}, n));
      g.appendChild(el('rect', { width: NODE_W, height: NODE_H, rx: 6 }));
      g.appendChild(el('text', { x: 10, y: NODE_H / 2 + 4 }, n.length > 26 ? n.slice(0, 25) + '…' : n));
      g.addEventListener('click', function (ev) { ev.stopPropagation(); select(n); });
      g.addEventListener('keydown', function (ev) { if (ev.key === 'Enter') { select(n); } });
      vp.appendChild(g);
      nodeEls[n] = g;
    });

    // Pan and zoom.
    var view = { x: 20, y: 20, k: 1 };
    function apply() { vp.setAttribute('transform', 'translate(' + view.x + ',' + view.y + ') scale(' + view.k + ')'); }
    function fit() {
      var maxX = 0, maxY = 0;
      names.forEach(function (n) { maxX = Math.max(maxX, pos[n].x + NODE_W); maxY = Math.max(maxY, pos[n].y + NODE_H); });
      var r = svg.getBoundingClientRect();
      var k = Math.min((r.width - 40) / Math.max(maxX, 1), (r.height - 40) / Math.max(maxY, 1), 1.2);
      view.k = Math.max(k, 0.15);
      view.x = 20;
      view.y = Math.max(20, (r.height - maxY * view.k) / 2);
      apply();
    }
    svg.addEventListener('wheel', function (ev) {
      ev.preventDefault();
      var r = svg.getBoundingClientRect();
      var cx = ev.clientX - r.left, cy = ev.clientY - r.top;
      var f = ev.deltaY < 0 ? 1.12 : 1 / 1.12;
      var k = Math.min(3, Math.max(0.1, view.k * f));
      view.x = cx - (cx - view.x) * (k / view.k);
      view.y = cy - (cy - view.y) * (k / view.k);
      view.k = k;
      apply();
    }, { passive: false });
    var drag = null;
    svg.addEventListener('mousedown', function (ev) { drag = { x: ev.clientX, y: ev.clientY, vx: view.x, vy: view.y }; });
    window.addEventListener('mousemove', function (ev) {
      if (!drag) { return; }
      view.x = drag.vx + ev.clientX - drag.x;
      view.y = drag.vy + ev.clientY - drag.y;
      apply();
    });
    window.addEventListener('mouseup', function () { drag = null; });
    svg.addEventListener('click', function () { clearHighlight(); describe(null); });

    // Highlight helpers.
    function clearHighlight() {
      Object.keys(nodeEls).forEach(function (n) { nodeEls[n].setAttribute('class', 'node ' + byName[n].kind); });
      Object.keys(edgeEls).forEach(function (k) { edgeEls[k].setAttribute('class', 'edge'); });
      svg.setAttribute('class', '');
    }
    function mark(nodeSet, edgeSet, selected) {
      clearHighlight();
      svg.setAttribute('class', 'dimmed');
      Object.keys(nodeSet).forEach(function (n) {
        if (nodeEls[n]) { nodeEls[n].setAttribute('class', 'node ' + byName[n].kind + ' hot' + (n === selected ? ' sel' : '')); }
      });
      Object.keys(edgeSet).forEach(function (k) { if (edgeEls[k]) { edgeEls[k].setAttribute('class', 'edge hot'); } });
    }

    function select(n) {
      var up = closure(n, ups), down = closure(n, downs);
      var nodeSet = {}, edgeSet = {};
      Object.keys(up).forEach(function (k) { nodeSet[k] = true; });
      Object.keys(down).forEach(function (k) { nodeSet[k] = true; });
      vEdges.forEach(function (e) {
        if ((up[e[0]] && up[e[1]]) || (down[e[0]] && down[e[1]])) { edgeSet[e[0] + '\u0000' + e[1]] = true; }
      });
      mark(nodeSet, edgeSet, n);
      describe(n);
    }

    function traceColumn(model, column) {
      var lines = [], nodeSet = {}, edgeSet = {};
      nodeSet[model] = true;
      function walk(dir) {
        var seen = {};
        var queue = [[model, column]];
        seen[model + '\u0000' + column] = true;
        while (queue.length) {
          var cur = queue.shift();
          D.column_lineage.forEach(function (e) {
            var from = dir === 'up' ? [e[2], e[3]] : [e[0], e[1]];
            var to = dir === 'up' ? [e[0], e[1]] : [e[2], e[3]];
            if (from[0] !== cur[0] || from[1] !== cur[1]) { return; }
            nodeSet[to[0]] = true;
            edgeSet[e[0] + '\u0000' + e[2]] = true;
            lines.push({ dir: dir, text: e[0] + '.' + e[1] + ' → ' + e[2] + '.' + e[3] + '  (' + e[4] + ')' });
            var key = to[0] + '\u0000' + to[1];
            if (!seen[key]) { seen[key] = true; queue.push(to); }
          });
        }
      }
      walk('up');
      walk('down');
      return { lines: lines, nodeSet: nodeSet, edgeSet: edgeSet };
    }

    function text(tag, cls, t) {
      var n = document.createElement(tag);
      if (cls) { n.className = cls; }
      n.textContent = t;
      return n;
    }

    function describe(n) {
      while (panel.firstChild) { panel.removeChild(panel.firstChild); }
      if (!n) { panel.appendChild(text('p', 'muted', 'Click a node to trace its upstream and downstream. Pick a column to trace column-level lineage.')); return; }
      var o = byName[n];
      var head = document.createElement('h2');
      var link = document.createElement('a');
      link.href = pageUrl(n);
      link.textContent = n;
      head.appendChild(link);
      panel.appendChild(head);
      panel.appendChild(text('p', 'muted', o.kind));
      if (o.item.description) { panel.appendChild(text('p', '', o.item.description)); }
      var cols = o.kind === 'model' ? o.item.columns : (o.item.columns || []).map(function (c) { return { name: c, type: '' }; });
      if (!cols.length) { panel.appendChild(text('p', 'muted', 'No column information.')); return; }
      panel.appendChild(text('h3', '', 'Columns'));
      var list = document.createElement('div');
      list.className = 'col-list';
      cols.forEach(function (c) {
        var b = document.createElement('button');
        b.type = 'button';
        b.textContent = c.name + (c.type ? '  ' + c.type : '');
        b.addEventListener('click', function (ev) {
          ev.stopPropagation();
          var t = traceColumn(n, c.name);
          mark(t.nodeSet, t.edgeSet, n);
          var old = document.getElementById('trace');
          if (old) { old.parentNode.removeChild(old); }
          var box = document.createElement('div');
          box.id = 'trace';
          box.appendChild(text('h3', '', 'Lineage of ' + n + '.' + c.name));
          if (!t.lines.length) { box.appendChild(text('p', 'muted', 'No column-level lineage recorded.')); }
          t.lines.forEach(function (l) { box.appendChild(text('div', 'trace-line ' + l.dir, l.text)); });
          panel.appendChild(box);
        });
        list.appendChild(b);
      });
      panel.appendChild(list);
    }

    var reset = document.getElementById('dag-reset');
    if (reset) { reset.addEventListener('click', function () { clearHighlight(); describe(null); fit(); }); }
    var all = document.getElementById('dag-all');
    if (all) {
      all.hidden = !focus;
      all.addEventListener('click', function () { window.location.hash = ''; window.location.reload(); });
    }
    describe(null);
    fit();
    if (focus) { select(focus); }
    window.addEventListener('resize', fit);
  }

  initSearch();
  initTableFilter();
  initDag();
})();
