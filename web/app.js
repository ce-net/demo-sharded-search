// The page: replays proof/run.json + frames.json + seconds.jsonl, or (served by search-dashboard) shows the live
// state from /api/events. Every number on it comes from those files or that feed.
(function () {
  "use strict";
  var $ = function (id) { return document.getElementById(id); };
  var esc = function (s) { return String(s).replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); };
  var fmt = function (n, d) { return Number(n).toLocaleString("en-US", { maximumFractionDigits: d == null ? 0 : d, minimumFractionDigits: d == null ? 0 : d }); };
  var NS = "http://www.w3.org/2000/svg";

  // theme switch
  (function () {
    var b = $("tog"), r = document.documentElement;
    function set() { b.textContent = r.dataset.theme === "light" ? "Dark" : "Light"; }
    set();
    b.addEventListener("click", function () {
      var l = r.dataset.theme !== "light";
      if (l) r.dataset.theme = "light"; else delete r.dataset.theme;
      try { localStorage.setItem("ce-theme", l ? "light" : "dark"); } catch (e) {}
      set();
    });
  })();

  function stat(label, num, sub, feat, dot) {
    return '<div class="glass stat' + (feat ? " ticks" : "") + '"><div class="label"><i' + (dot ? ' class="' + dot + '"' : "") + "></i>" + esc(label) +
      '</div><div class="num' + (feat ? " acc" : "") + '">' + num + '</div><div class="sub">' + esc(sub) + "</div></div>";
  }

  var LETTER = "ABCDEFGHIJ";
  function drawMap(shards, machines) {
    // shards: [[{m: machineIndex, s: "L"|"F"|"J"|"X"}...]...]
    var h = "";
    shards.forEach(function (ms, i) {
      var lead = ms.some(function (x) { return x.s === "L"; });
      h += '<div class="sh' + (lead ? "" : " amber") + '" title="shard ' + i + '"><div class="n">' + (i < 10 ? "0" : "") + i + "</div>";
      ms.forEach(function (x) {
        h += '<div class="m ' + x.s + '" title="' + esc(machines[x.m] || "?") + '">' + (x.m >= 0 ? LETTER[x.m] : "?") + "</div>";
      });
      h += "</div>";
    });
    $("map").innerHTML = h;
  }

  function legend(machines) {
    var l = machines.map(function (m, i) { return "<span><b>" + LETTER[i] + "</b> " + esc(m) + "</span>"; }).join("");
    l += '<span><i class="m L"></i>leader</span><span><i class="m F"></i>follower</span><span><i class="m J"></i>installing</span><span><i class="m X"></i>dead</span><span>amber border: no live leader</span>';
    $("legend").innerHTML = l;
  }

  function el(tag, attrs, text) {
    var e = document.createElementNS(NS, tag);
    for (var k in attrs) e.setAttribute(k, attrs[k]);
    if (text != null) e.textContent = text;
    return e;
  }

  function lineChart(svg, secs, keys, ymax, h, marks, fmtY) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var W = 900, L = 44, R = 8, T = 10, B = 20, H = h;
    if (!secs.length) return;
    var t0 = secs[0].t_ms, t1 = secs[secs.length - 1].t_ms || t0 + 1;
    var x = function (t) { return L + (W - L - R) * (t - t0) / Math.max(1, t1 - t0); };
    var y = function (v) { return T + (H - T - B) * (1 - Math.min(v, ymax) / ymax); };
    svg.appendChild(el("line", { class: "ax", x1: L, x2: W - R, y1: H - B, y2: H - B }));
    [0, 0.5, 1].forEach(function (f) {
      svg.appendChild(el("text", { x: 2, y: y(ymax * f) + 3 }, fmtY(ymax * f)));
    });
    for (var s = 0; s <= (t1 - t0) / 1000; s += 10) svg.appendChild(el("text", { x: x(t0 + s * 1000) - 6, y: H - 5 }, s + "s"));
    (marks || []).forEach(function (m, i) {
      if (m.t == null) return;
      svg.appendChild(el("line", { class: m.cls, x1: x(m.t), x2: x(m.t), y1: T, y2: H - B }));
      svg.appendChild(el("text", { x: x(m.t) + 4, y: T + 10 + i * 13 }, m.label));
    });
    keys.forEach(function (k) {
      var d = secs.filter(function (s) { return s.n > 0; }).map(function (s, i) { return (i ? "L" : "M") + x(s.t_ms).toFixed(1) + " " + y(s[k.f]).toFixed(1); }).join(" ");
      svg.appendChild(el("path", { class: k.cls, d: d }));
    });
  }

  function histChart(svg, phases) {
    while (svg.firstChild) svg.removeChild(svg.firstChild);
    var B = [1, 2, 3, 5, 7.5, 10, 15, 20, 30, 50, 75, 100, 150, 250, 500, 1000, 2000, "more"];
    var W = 440, H = 180, L = 6, bw = (W - L) / B.length;
    var max = 1;
    phases.forEach(function (p) { p.h.forEach(function (v, i) { max = Math.max(max, v / p.total); }); });
    B.forEach(function (b, i) {
      phases.forEach(function (p, j) {
        var v = p.h[i] / p.total, hh = (H - 30) * v / max;
        svg.appendChild(el("rect", { class: "b" + (p.d ? " d" : ""), x: L + i * bw + j * (bw / 2 - 1), y: H - 20 - hh, width: bw / 2 - 2, height: hh }));
      });
      if (i % 2 === 0) svg.appendChild(el("text", { x: L + i * bw, y: H - 6 }, String(b)));
    });
  }

  function recorded(run, framesDoc, secs) {
    secs = secs.filter(function (s) { return s.phase !== "warm-up"; });
    var k = run.kill, lat = run.latency;
    $("mode").textContent = "Recorded run, " + run.recorded_at + ". " + run.setup.physical_hosts + ".";
    $("stats").innerHTML =
      stat("Documents", fmt(run.corpus.docs), run.corpus.shards + " shards x " + run.corpus.replicas + " replicas", true) +
      stat("Instances", fmt(run.boot.instances), "each with its own key", false, "g") +
      stat("New leaders", k.leaders_lost ? fmt(k.elect_ms_max / 1000, 1) + "<small> s max</small>" : "0", k.leaders_lost + " shards lost a leader; lease " + fmt(run.setup.lease_ttl_ms / 1000, 0) + " s") +
      stat("Recall@10", fmt(run.recall_before.recall_at_10, 4), run.answers_identical ? "after: identical answers" : "after: " + fmt(run.recall_after.recall_at_10, 4), false, "g");
    $("lede").textContent = "We index " + fmt(run.corpus.docs) + " English Wikipedia abstracts into " + run.corpus.shards + " shards with " + run.corpus.replicas +
      " replicas each: " + run.boot.instances + " shard instances, each with its own identity. A query fans out to the leader of every shard and the merged top 10 comes back. Then we kill every instance on " +
      k.machine + " while the query loop keeps running.";
    var machines = framesDoc.machines, frames = framesDoc.frames;
    legend(machines);
    var decode = function (f) { return f.s.map(function (s) { return s ? s.split(",").map(function (c) { return { m: parseInt(c, 10), s: c.slice(-1) }; }) : []; }); };
    var sc = $("scrub");
    sc.max = frames.length - 1;
    var show = function (i) {
      var f = frames[i];
      drawMap(decode(f), machines);
      var rel = (f.t - framesDoc.t_kill) / 1000;
      $("tread").textContent = (rel < 0 ? "kill in " + fmt(-rel, 1) + " s" : "+" + fmt(rel, 1) + " s after the kill") + (f.t >= framesDoc.t_whole ? " · whole" : "");
    };
    var ki = frames.findIndex(function (f) { return f.t >= framesDoc.t_kill; });
    sc.value = Math.max(0, ki - 4);
    show(+sc.value);
    sc.addEventListener("input", function () { show(+sc.value); });
    var timer = null;
    $("play").addEventListener("click", function () {
      if (timer) { clearInterval(timer); timer = null; $("play").textContent = "Play"; return; }
      if (+sc.value >= frames.length - 1) sc.value = 0;
      $("play").textContent = "Pause";
      timer = setInterval(function () { if (+sc.value >= frames.length - 1) { clearInterval(timer); timer = null; $("play").textContent = "Play"; return; } sc.value = +sc.value + 1; show(+sc.value); }, 250);
    });

    var killS = secs.find(function (s) { return s.phase === "during"; }), afterS = secs.find(function (s) { return s.phase === "after"; });
    var ymax = Math.max(10, Math.ceil(Math.max.apply(null, secs.map(function (s) { return s.p99_ms; })) / 10) * 10);
    lineChart($("lat"), secs, [{ f: "p50_ms", cls: "p50" }, { f: "p99_ms", cls: "p99" }], ymax, 220,
      [{ t: killS && killS.t_ms - 1000, cls: "ev", label: "kill " + k.machine }, { t: afterS && afterS.t_ms - 1000, cls: "ev2", label: "whole" }], function (v) { return fmt(v) + "ms"; });
    lineChart($("rec"), secs, [{ f: "recall", cls: "rec" }], 1, 90, [], function (v) { return fmt(v, 1); });
    $("latnote").textContent = "Amber: p99 per second. Grey: p50. Green, below: mean recall@10 of the queries answered in that second. " +
      fmt(lat.errors) + " failed queries, " + fmt(lat.missing_shard_answers) + " shard answers missing, " + fmt(lat.hedged) + " shard calls hedged to a follower, over the whole run.";

    var ph = [["Before", lat.before], ["During", lat.during], ["After", lat.after]];
    $("phases").innerHTML = "<tr><th></th><th>Queries</th><th>QPS</th><th>p50 ms</th><th>p99 ms</th></tr>" +
      ph.map(function (p) { return "<tr><td>" + p[0] + ' (' + fmt(p[1].secs, 1) + ' s)</td><td class="v">' + fmt(p[1].queries) + '</td><td class="v">' + fmt(p[1].qps, 1) + '</td><td class="v">' + fmt(p[1].p50_ms, 2) + '</td><td class="v">' + fmt(p[1].p99_ms, 2) + "</td></tr>"; }).join("") +
      "<tr><td>Recall@10</td><td colspan=\"4\" class=\"v\">" + fmt(run.recall_before.recall_at_10, 4) + " before, " + fmt(run.recall_after.recall_at_10, 4) + " after (" + run.queries.count + " queries, same baseline)</td></tr>";
    var sum = function (p) { var h = []; secs.filter(function (s) { return s.phase === p; }).forEach(function (s) { s.hist.forEach(function (v, i) { h[i] = (h[i] || 0) + v; }); }); var t = h.reduce(function (a, b) { return a + b; }, 0) || 1; return { h: h, total: t }; };
    var b = sum("before"), d = sum("during"); d.d = true;
    histChart($("hist"), [b, d]);

    $("elect").innerHTML = "<tr><th>Shard</th><th>Old leader</th><th>New leader</th><th>On</th><th>Elected after</th></tr>" +
      k.elect_ms.map(function (e) { return "<tr><td>" + e.shard + "</td><td>" + esc(e.old) + '</td><td class="v">' + esc(e.new) + "</td><td>" + esc(e.machine) + '</td><td class="v">' + fmt(e.ms / 1000, 2) + " s</td></tr>"; }).join("");
    $("electnote").textContent = k.instances_lost + " instances and " + k.leaders_lost + " leaders lost at once; at most " + k.amber_max + " shards without a live leader. Every shard had a new leader within " +
      fmt(k.elect_ms_max / 1000, 2) + " s (" + (k.elect_under_two_leases ? "under" : "NOT under") + " two lease periods of " + fmt(run.setup.lease_ttl_ms / 1000, 0) + " s). All " + run.corpus.shards + " shards back to " +
      run.corpus.replicas + " ready replicas " + fmt(k.replicas_whole_ms / 1000, 2) + " s after the kill (" + k.rebalancer_actions + " rebalancer actions).";

    var c = run.cap_test, row = (c.audit_rows || [])[0];
    $("cap").textContent = "The key's grant, in CAL:\n" + c.grant_cal + "\nhealth -> " + c.health + "\nquery  -> " + c.query +
      (row ? "\n\nAudit row on instance " + row.server + ":\n" + row.op + " needs " + row.need + ", verdict " + row.verdict + "\n" + row.reason : "");

    var a = run.add_machine;
    $("add").innerHTML = a ? "<tr><th>Machine</th><th>First replica ready</th><th>Settled</th></tr><tr><td class=\"v\">" + esc(a.machine) + '</td><td class="v">' + fmt((a.first_replica_ready_ms || 0) / 1000, 1) + ' s</td><td class="v">' + (a.settled_ms != null ? fmt(a.settled_ms / 1000, 1) + " s" : "not settled") + "</td></tr>" +
      "<tr><td colspan=\"3\">Ready instances per machine: " + Object.keys(a.ready_instances_per_machine).map(function (m) { return esc(m) + " " + a.ready_instances_per_machine[m]; }).join(", ") + "; recall@10 " + fmt(a.recall_at_10, 4) + "</td></tr>" : "<tr><td>Not part of this run.</td></tr>";

    $("idsum").textContent = run.identities.length + " instance identities with their machine";
    $("ids").innerHTML = "<tr><th>Instance</th><th>Machine</th><th>Shard</th><th>Role</th><th>Public key</th></tr>" +
      run.identities.map(function (i) { return '<tr><td class="v">' + esc(i.id) + "</td><td>" + esc(i.machine) + "</td><td>" + i.shard + "</td><td>" + esc(i.role) + "</td><td>" + esc(i.pubkey.slice(0, 32)) + "…</td></tr>"; }).join("");
    $("honest").textContent = "Corpus manifest " + run.corpus.manifest_hash + ". " + run.setup.physical_hosts + ": the machines above are fault domains made of separate host processes, each with its own segment cache and its own instances. Run the same binaries on more real machines (one search.host each) and they join with no config change.";
  }

  function live() {
    $("mode").textContent = "Live, from search.dashboard.";
    $("scrubrow").style.display = "none";
    Array.prototype.forEach.call(document.querySelectorAll(".rec-only"), function (e) { e.style.display = "none"; });
    var es = new EventSource("api/events");
    es.onmessage = function (ev) {
      var s = JSON.parse(ev.data), machines = s.machines.map(function (m) { return m.machine; });
      legend(machines);
      drawMap(s.shards.map(function (sh) { return sh.members.map(function (m) { return { m: machines.indexOf(m.machine), s: !m.live ? (m.machine ? "X" : "J") : m.role === "leader" ? "L" : m.ready ? "F" : "J" }; }); }), machines);
      var inst = s.shards.reduce(function (a, sh) { return a + sh.members.filter(function (m) { return m.live; }).length; }, 0);
      var leaders = s.shards.filter(function (sh) { return sh.members.some(function (m) { return m.live && m.role === "leader"; }); }).length;
      var last = s.metrics.length ? s.metrics[s.metrics.length - 1].second : null;
      $("stats").innerHTML = stat("Documents", s.corpus ? fmt(s.corpus.docs) : "0", s.corpus ? s.corpus.shards + " shards x " + s.corpus.replicas : "no corpus loaded", true) +
        stat("Live instances", fmt(inst), machines.length + " machines up", false, "g") +
        stat("Leaders", fmt(leaders) + "<small>/ " + s.shards.length + "</small>", "shards with a live leader") +
        stat("p99", last ? fmt(last.p99_ms, 1) + "<small> ms</small>" : "–", last ? fmt(last.n) + " queries last second · recall " + fmt(last.recall, 3) : "no query load running");
      var secs = s.metrics.map(function (m) { var x = m.second; x.phase = m.phase; return x; });
      if (secs.length > 1) {
        var ymax = Math.max(10, Math.ceil(Math.max.apply(null, secs.map(function (x) { return x.p99_ms; })) / 10) * 10);
        lineChart($("lat"), secs, [{ f: "p50_ms", cls: "p50" }, { f: "p99_ms", cls: "p99" }], ymax, 220, [], function (v) { return fmt(v) + "ms"; });
        lineChart($("rec"), secs, [{ f: "recall", cls: "rec" }], 1, 90, [], function (v) { return fmt(v, 1); });
      }
    };
  }

  fetch("api/state", { cache: "no-store" }).then(function (r) { if (!r.ok) throw 0; return r.json(); }).then(live).catch(function () {
    Promise.all([
      fetch("proof/run.json").then(function (r) { return r.json(); }),
      fetch("proof/frames.json").then(function (r) { return r.json(); }),
      fetch("proof/seconds.jsonl").then(function (r) { return r.text(); }),
    ]).then(function (v) {
      recorded(v[0], v[1], v[2].trim().split("\n").filter(Boolean).map(JSON.parse));
    }).catch(function (e) { $("mode").textContent = "Could not load the recorded run: " + e; });
  });
})();
