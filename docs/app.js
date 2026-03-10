const DATA_PATH = "./data/sports_correlations.json";
const charts = [];

async function loadDashboard() {
  try {
    const response = await fetch(DATA_PATH, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`Request failed with ${response.status}`);
    }
    const payload = await response.json();
    renderDashboard(payload);
  } catch (error) {
    renderError(error);
  }
}

function renderDashboard(payload) {
  updateOverview(payload);
  renderHeadline(payload.headline_matchup, payload.generated_at);
  renderMatchups(payload.correlations || []);
}

function updateOverview(payload) {
  const windowRange = document.getElementById("window-range");
  const pairsScanned = document.getElementById("pairs-scanned");
  const pairsReturned = document.getElementById("pairs-returned");
  const startDate = payload.window?.start_date;
  const endDate = payload.window?.end_date;

  windowRange.textContent = startDate && endDate ? `${startDate} to ${endDate}` : "Pending refresh";
  pairsScanned.textContent = formatInteger(payload.metadata?.evaluated_pairs || 0);
  pairsReturned.textContent = formatInteger(payload.metadata?.returned_pairs || 0);
}

function renderHeadline(matchup, generatedAt) {
  const title = document.getElementById("headline-title");
  const summary = document.getElementById("headline-summary");
  const rScore = document.getElementById("headline-r-score");
  const sample = document.getElementById("headline-sample");
  const generated = document.getElementById("generated-at");
  const canvas = document.getElementById("headline-chart");

  if (!matchup) {
    title.textContent = "Waiting for this week’s nonsense";
    summary.textContent = "Run the harvester locally or let the scheduled GitHub Action publish the first weekly set of correlations.";
    rScore.textContent = "--";
    sample.textContent = "--";
    generated.textContent = generatedAt ? formatDateTime(generatedAt) : "--";
    return;
  }

  title.textContent = matchup.headline;
  summary.textContent = matchup.summary;
  rScore.textContent = matchup.r_score.toFixed(3);
  sample.textContent = `${matchup.sample_size} weeks`;
  generated.textContent = generatedAt ? formatDateTime(generatedAt) : "--";

  charts.push(createCorrelationChart(canvas, matchup, true));
}

function renderMatchups(correlations) {
  const container = document.getElementById("matchups-grid");
  container.innerHTML = "";

  if (!correlations.length) {
    const empty = document.createElement("div");
    empty.className = "empty-state";
    empty.textContent = "No high-correlation pairs are available yet. Once the data job completes, the strongest cross-sport metric pairings will appear here.";
    container.appendChild(empty);
    return;
  }

  correlations.forEach((matchup) => {
    const card = document.createElement("article");
    card.className = "matchup-card";

    const header = document.createElement("div");
    header.className = "card-header";
    header.innerHTML = `
      <div>
        <span class="card-meta">${matchup.league_a} vs ${matchup.league_b}</span>
        <h3>${matchup.headline}</h3>
      </div>
    `;

    const summary = document.createElement("p");
    summary.className = "matchup-summary";
    summary.textContent = matchup.summary;

    const badgeRow = document.createElement("div");
    badgeRow.className = "badge-row";
    badgeRow.appendChild(createBadge("Correlation", matchup.r_score.toFixed(3), matchup.r_score >= 0 ? "positive" : "negative"));
    badgeRow.appendChild(createBadge("Overlap", `${matchup.sample_size} weeks`));
    badgeRow.appendChild(createBadge("Metrics", `${matchup.metric_a_label} / ${matchup.metric_b_label}`));

    const chartShell = document.createElement("div");
    chartShell.className = "chart-shell";
    const canvas = document.createElement("canvas");
    chartShell.appendChild(canvas);

    card.appendChild(header);
    card.appendChild(summary);
    card.appendChild(badgeRow);
    card.appendChild(chartShell);
    container.appendChild(card);

    charts.push(createCorrelationChart(canvas, matchup, false));
  });
}

function createBadge(label, value, tone = "") {
  const badge = document.createElement("div");
  badge.className = "badge";
  badge.innerHTML = `<span class="metric-label">${label}</span><strong class="${tone}">${value}</strong>`;
  return badge;
}

function createCorrelationChart(canvas, matchup, featured) {
  const points = matchup.points || [];
  const labels = points.map((point) => point.label);
  const leftColor = "#f28f3b";
  const rightColor = "#7ecbff";

  return new Chart(canvas, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: `${matchup.league_a} ${matchup.metric_a_label}`,
          data: points.map((point) => point.series_a_z),
          borderColor: leftColor,
          backgroundColor: "rgba(242, 143, 59, 0.18)",
          borderWidth: featured ? 3 : 2,
          pointRadius: featured ? 2.5 : 2,
          pointHoverRadius: 5,
          tension: 0.28,
          yAxisID: "y",
        },
        {
          label: `${matchup.league_b} ${matchup.metric_b_label}`,
          data: points.map((point) => point.series_b_z),
          borderColor: rightColor,
          backgroundColor: "rgba(126, 203, 255, 0.18)",
          borderWidth: featured ? 3 : 2,
          pointRadius: featured ? 2.5 : 2,
          pointHoverRadius: 5,
          tension: 0.28,
          yAxisID: "y1",
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: {
        duration: featured ? 900 : 600,
      },
      interaction: {
        mode: "index",
        intersect: false,
      },
      plugins: {
        legend: {
          labels: {
            color: "#f5f0e8",
            usePointStyle: true,
            font: {
              family: "IBM Plex Sans Condensed",
            },
          },
        },
        tooltip: {
          backgroundColor: "rgba(7, 12, 10, 0.94)",
          borderColor: "rgba(216, 198, 170, 0.18)",
          borderWidth: 1,
          titleFont: {
            family: "Space Grotesk",
          },
          bodyFont: {
            family: "IBM Plex Sans Condensed",
          },
          callbacks: {
            label(context) {
              const point = matchup.points[context.dataIndex];
              const isFirst = context.datasetIndex === 0;
              const rawValue = isFirst ? point.series_a_raw : point.series_b_raw;
              const normalizedValue = context.parsed.y;
              const metricLabel = isFirst ? matchup.metric_a_label : matchup.metric_b_label;
              return `${context.dataset.label}: ${formatNumber(rawValue)} raw, ${normalizedValue.toFixed(2)} z-score (${metricLabel})`;
            },
          },
        },
      },
      scales: {
        x: {
          ticks: {
            color: "#d8c6aa",
            maxRotation: 0,
            autoSkip: true,
            font: {
              family: "IBM Plex Sans Condensed",
            },
          },
          grid: {
            color: "rgba(216, 198, 170, 0.08)",
          },
        },
        y: {
          position: "left",
          ticks: {
            color: leftColor,
            callback(value) {
              return `${value}z`;
            },
          },
          grid: {
            color: "rgba(216, 198, 170, 0.08)",
          },
          title: {
            display: true,
            color: leftColor,
            text: `${matchup.league_a} normalized`,
          },
        },
        y1: {
          position: "right",
          ticks: {
            color: rightColor,
            callback(value) {
              return `${value}z`;
            },
          },
          grid: {
            drawOnChartArea: false,
          },
          title: {
            display: true,
            color: rightColor,
            text: `${matchup.league_b} normalized`,
          },
        },
      },
    },
  });
}

function renderError(error) {
  updateOverview({ metadata: { evaluated_pairs: 0, returned_pairs: 0 }, window: {} });
  const title = document.getElementById("headline-title");
  const summary = document.getElementById("headline-summary");
  const container = document.getElementById("matchups-grid");
  title.textContent = "Dashboard unavailable";
  summary.textContent = `The local JSON feed could not be loaded. ${error.message}`;
  container.innerHTML = "<div class=\"empty-state\">Check that docs/data/sports_correlations.json exists and that GitHub Pages is serving the docs directory.</div>";
}

function formatDateTime(value) {
  return new Date(value).toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
}

function formatInteger(value) {
  return new Intl.NumberFormat().format(value);
}

function formatNumber(value) {
  return new Intl.NumberFormat(undefined, { maximumFractionDigits: 2 }).format(value);
}

window.addEventListener("DOMContentLoaded", loadDashboard);
