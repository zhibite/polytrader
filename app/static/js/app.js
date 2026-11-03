/* Trader Strategy Lab — UI helpers */
(function () {
  "use strict";

  // ── navigation ──────────────────────────────────────────────────────────
  const burger = document.querySelector(".burger");
  const sidebar = document.querySelector(".sidebar");
  if (burger && sidebar) {
    burger.addEventListener("click", () => sidebar.classList.toggle("open"));
  }

  // ── tabs ────────────────────────────────────────────────────────────────
  document.querySelectorAll("[data-tabs]").forEach((tabs) => {
    const buttons = tabs.querySelectorAll(".tab");
    const panels = tabs.parentElement.querySelectorAll(".tab-panel");
    buttons.forEach((button) => {
      button.addEventListener("click", () => {
        buttons.forEach((b) => b.classList.remove("active"));
        panels.forEach((p) => p.classList.remove("active"));
        button.classList.add("active");
        const target = document.getElementById(button.dataset.target);
        if (target) target.classList.add("active");
        setTimeout(() => window.dispatchEvent(new Event("resize")), 60);
      });
    });
  });

  // ── copy address ────────────────────────────────────────────────────────
  document.querySelectorAll("[data-copy]").forEach((node) => {
    node.addEventListener("click", async () => {
      try {
        await navigator.clipboard.writeText(node.dataset.copy);
        const old = node.textContent;
        node.textContent = "copied";
        setTimeout(() => (node.textContent = old), 900);
      } catch (e) {
        /* clipboard may be unavailable on http */
      }
    });
  });

  // ── job polling ─────────────────────────────────────────────────────────
  const jobBar = document.querySelector(".jobbar[data-job]");
  if (jobBar) {
    const jobId = jobBar.dataset.job;
    const statusEl = jobBar.querySelector(".js-job-status");
    const messageEl = jobBar.querySelector(".js-job-message");
    const bar = jobBar.querySelector(".bar > i");
    let reloaded = false;
    const tick = async () => {
      try {
        const response = await fetch(`/api/jobs/${jobId}`, { cache: "no-store" });
        if (!response.ok) return;
        const payload = await response.json();
        const job = payload.job;
        if (!job) return;
        if (statusEl) statusEl.textContent = job.status;
        if (messageEl) messageEl.textContent = job.message || job.error || "";
        if (bar) bar.style.width = `${Math.max(2, job.progress || 0)}%`;
        if ((job.status === "DONE" || job.status === "ERROR") && !reloaded) {
          reloaded = true;
          setTimeout(() => window.location.reload(), 700);
        }
      } catch (e) {
        /* keep polling */
      }
    };
    tick();
    setInterval(tick, 2000);
  }

  // ── collector dot ───────────────────────────────────────────────────────
  const collectorDot = document.querySelector(".js-collector-dot");
  const collectorText = document.querySelector(".js-collector-text");
  if (collectorDot) {
    const refresh = async () => {
      try {
        const response = await fetch("/api/collector/status", { cache: "no-store" });
        const status = await response.json();
        collectorDot.className = "dot js-collector-dot " + (status.connected ? "ok" : status.running ? "warn" : "bad");
        if (collectorText) {
          collectorText.textContent = status.connected
            ? `采集器在线 · ${status.slug || ""}`
            : status.running
              ? "采集器等待中"
              : "采集器已停止";
        }
      } catch (e) {
        /* ignore */
      }
    };
    refresh();
    setInterval(refresh, 10000);
  }

  // ── chart theme ─────────────────────────────────────────────────────────
  const COLORS = {
    brand: "#5b8cff",
    purple: "#8b5cf6",
    cyan: "#22d3ee",
    green: "#34d399",
    red: "#fb7185",
    amber: "#fbbf24",
    soft: "#a7b6cc",
    grid: "#1d2a3d",
    muted: "#6d7f98",
  };

  function baseOptions() {
    return {
      chart: {
        background: "transparent",
        foreColor: COLORS.soft,
        toolbar: { show: false },
        zoom: { enabled: false },
        fontFamily: 'Inter, "Segoe UI", "PingFang SC", system-ui, sans-serif',
        animations: { enabled: true, speed: 350 },
        parentHeightOffset: 0,
      },
      theme: { mode: "dark" },
      grid: { borderColor: COLORS.grid, strokeDashArray: 4, padding: { left: 6, right: 10 } },
      tooltip: { theme: "dark" },
      dataLabels: { enabled: false },
      legend: { labels: { colors: COLORS.soft }, fontSize: "12px", markers: { size: 4 } },
      xaxis: {
        labels: { style: { colors: COLORS.muted, fontSize: "10.5px" }, rotate: 0, hideOverlappingLabels: true },
        axisBorder: { show: false },
        axisTicks: { show: false },
        tooltip: { enabled: false },
      },
      yaxis: {
        labels: { style: { colors: COLORS.muted, fontSize: "10.5px" }, formatter: (v) => shortNum(v) },
      },
      stroke: { curve: "smooth", width: 2 },
    };
  }

  function shortNum(value) {
    if (value === null || value === undefined) return "";
    const n = Number(value);
    if (!isFinite(n)) return "";
    const abs = Math.abs(n);
    if (abs >= 1e9) return (n / 1e9).toFixed(1) + "B";
    if (abs >= 1e6) return (n / 1e6).toFixed(1) + "M";
    if (abs >= 1e3) return (n / 1e3).toFixed(1) + "K";
    return n.toFixed(abs < 10 ? 2 : 0);
  }

  function mount(el, options) {
    if (!el) return null;
    if (el._chart) {
      try { el._chart.destroy(); } catch (e) { /* noop */ }
    }
    el.innerHTML = "";
    el._chart = new ApexCharts(el, options);
    el._chart.render();
    return el._chart;
  }

  function line(id, labels, series, opts) {
    const el = document.getElementById(id);
    if (!el) return;
    const options = Object.assign(baseOptions(), {
      series,
      chart: Object.assign(baseOptions().chart, { type: "area", height: el.clientHeight || 280 }),
      colors: (opts && opts.colors) || [COLORS.brand, COLORS.purple, COLORS.green],
      xaxis: Object.assign(baseOptions().xaxis, { categories: labels }),
      yaxis: Object.assign(baseOptions().yaxis, {
        labels: {
          style: { colors: COLORS.muted, fontSize: "10.5px" },
          formatter: (v) => ((opts && opts.money) ? "$" + shortNum(v) : shortNum(v)),
        },
      }),
      fill: {
        type: "gradient",
        gradient: { shadeIntensity: 1, opacityFrom: 0.25, opacityTo: 0.02, stops: [0, 90, 100] },
      },
      stroke: { curve: "smooth", width: (opts && opts.width) || 2 },
      markers: { size: 0, hover: { size: 4 } },
      tooltip: Object.assign(baseOptions().tooltip, {
        y: { formatter: (v) => ((opts && opts.money) ? "$" + Number(v).toFixed(2) : Number(v).toFixed(3)) },
      }),
    }, opts && opts.extra ? opts.extra : {});
    mount(el, options);
  }

  function bar(id, labels, series, opts) {
    const el = document.getElementById(id);
    if (!el) return;
    const options = Object.assign(baseOptions(), {
      series,
      chart: Object.assign(baseOptions().chart, {
        type: "bar",
        height: el.clientHeight || 280,
        stacked: Boolean(opts && opts.stacked),
      }),
      colors: (opts && opts.colors) || [COLORS.brand, COLORS.purple],
      plotOptions: {
        bar: {
          columnWidth: (opts && opts.width) || "52%",
          borderRadius: 4,
          borderRadiusApplication: "end",
          horizontal: Boolean(opts && opts.horizontal),
        },
      },
      xaxis: Object.assign(baseOptions().xaxis, { categories: labels }),
      fill: { opacity: 1 },
      stroke: { width: 0 },
      tooltip: Object.assign(baseOptions().tooltip, {
        y: { formatter: (v) => ((opts && opts.money) ? "$" + Number(v).toFixed(2) : Number(v).toFixed(2)) },
      }),
    }, opts && opts.extra ? opts.extra : {});
    mount(el, options);
  }

  function scatter(id, points, opts) {
    const el = document.getElementById(id);
    if (!el) return;
    const groups = {};
    points.forEach((point) => {
      const key = point.classification || "未分类";
      groups[key] = groups[key] || [];
      groups[key].push({ x: round(point.x), y: round(point.y), z: round(point.r || 8), name: point.name });
    });
    const options = Object.assign(baseOptions(), {
      series: Object.keys(groups).map((key) => ({ name: key, data: groups[key] })),
      chart: Object.assign(baseOptions().chart, { type: "scatter", height: el.clientHeight || 300 }),
      colors: [COLORS.brand, COLORS.purple, COLORS.green, COLORS.amber, COLORS.red],
      markers: { size: 5, strokeWidth: 0, hover: { size: 7 } },
      xaxis: Object.assign(baseOptions().xaxis, {
        title: { text: (opts && opts.xTitle) || "", style: { color: COLORS.muted, fontSize: "10px" } },
        tickAmount: 6,
      }),
      yaxis: Object.assign(baseOptions().yaxis, {
        title: { text: (opts && opts.yTitle) || "", style: { color: COLORS.muted, fontSize: "10px" } },
      }),
      tooltip: Object.assign(baseOptions().tooltip, {
        custom: (ctx) => {
          const p = ctx.w.config.series[ctx.seriesIndex].data[ctx.dataPointIndex];
          return `<div style="padding:6px 9px"><b>${p.name || ""}</b><br/>x: ${p.x}<br/>y: ${p.y}</div>`;
        },
      }),
    }, opts && opts.extra ? opts.extra : {});
    mount(el, options);
  }

  function donut(id, labels, values, opts) {
    const el = document.getElementById(id);
    if (!el) return;
    const options = {
      chart: { type: "donut", height: el.clientHeight || 240, background: "transparent", fontFamily: baseOptions().chart.fontFamily },
      labels,
      series: values,
      colors: [COLORS.brand, COLORS.purple, COLORS.green, COLORS.amber, COLORS.red, COLORS.cyan],
      theme: { mode: "dark" },
      stroke: { width: 0 },
      legend: { position: "bottom", labels: { colors: COLORS.soft }, fontSize: "11px" },
      dataLabels: { enabled: false },
      plotOptions: { pie: { donut: { size: "68%", labels: { show: true, total: { show: true, label: (opts && opts.totalLabel) || "", color: COLORS.soft } } } } },
      tooltip: { theme: "dark" },
    };
    mount(el, options);
  }

  function round(value) {
    const n = Number(value);
    return isFinite(n) ? Math.round(n * 1000) / 1000 : 0;
  }

  window.LabCharts = { line, bar, scatter, donut, colors: COLORS, shortNum };

  // Auto-init charts declared as <div data-chart='...'> is handled by each page.
})();
