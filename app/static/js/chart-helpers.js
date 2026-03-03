/* ═══════════════════════════════════════════════════════════════════════════
   SEO Tools Platform — Chart.js Helpers
   Wrappers with design-system colors + dark mode support
   ═══════════════════════════════════════════════════════════════════════════ */

const DS_CHART_COLORS = [
    '#0f4c81', '#0e7490', '#3b82f6', '#10b981', '#f59e0b',
    '#ef4444', '#8b5cf6', '#ec4899', '#06b6d4', '#f97316',
    '#14b8a6', '#6366f1'
];

function dsChartTheme() {
    const isDark = document.documentElement.getAttribute('data-theme') === 'dark';
    return {
        isDark,
        textColor: isDark ? '#e2e8f0' : '#0f172a',
        gridColor: isDark ? 'rgba(148,163,184,0.15)' : 'rgba(15,23,42,0.08)',
        surfaceColor: isDark ? '#1e293b' : '#ffffff',
        mutedColor: isDark ? '#64748b' : '#94a3b8'
    };
}

function _baseOptions(theme) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                labels: { color: theme.textColor, font: { family: 'Inter', size: 12 } }
            }
        }
    };
}

/**
 * Doughnut gauge for scores (0-100)
 */
function createScoreGauge(canvasId, score, label) {
    const el = document.getElementById(canvasId);
    if (!el) return null;
    const theme = dsChartTheme();
    const pct = Math.max(0, Math.min(100, Number(score) || 0));
    const color = pct >= 70 ? '#10b981' : pct >= 50 ? '#f59e0b' : '#ef4444';

    return new Chart(el, {
        type: 'doughnut',
        data: {
            labels: [label || 'Score', ''],
            datasets: [{
                data: [pct, 100 - pct],
                backgroundColor: [color, theme.isDark ? '#334155' : '#e2e8f0'],
                borderWidth: 0
            }]
        },
        options: {
            ..._baseOptions(theme),
            cutout: '75%',
            plugins: {
                legend: { display: false },
                tooltip: { enabled: false }
            }
        },
        plugins: [{
            id: 'centerText',
            afterDraw(chart) {
                const { ctx, width, height } = chart;
                ctx.save();
                ctx.textAlign = 'center';
                ctx.textBaseline = 'middle';
                ctx.fillStyle = theme.textColor;
                ctx.font = 'bold 24px Inter';
                ctx.fillText(Math.round(pct), width / 2, height / 2 - 8);
                ctx.font = '11px Inter';
                ctx.fillStyle = theme.mutedColor;
                ctx.fillText(label || 'Score', width / 2, height / 2 + 14);
                ctx.restore();
            }
        }]
    });
}

/**
 * Bar chart (vertical)
 */
function createBarChart(canvasId, labels, datasets) {
    const el = document.getElementById(canvasId);
    if (!el) return null;
    const theme = dsChartTheme();

    const ds = datasets.map((d, i) => ({
        label: d.label || '',
        data: d.data || d,
        backgroundColor: d.color || DS_CHART_COLORS[i % DS_CHART_COLORS.length],
        borderRadius: 4,
        borderSkipped: false
    }));

    return new Chart(el, {
        type: 'bar',
        data: { labels, datasets: Array.isArray(datasets[0]) || typeof datasets[0] === 'number' ? [{ data: datasets, backgroundColor: DS_CHART_COLORS.slice(0, labels.length), borderRadius: 4, borderSkipped: false }] : ds },
        options: {
            ..._baseOptions(theme),
            scales: {
                x: { ticks: { color: theme.textColor, font: { size: 11 } }, grid: { display: false } },
                y: { ticks: { color: theme.mutedColor }, grid: { color: theme.gridColor } }
            },
            plugins: {
                legend: { display: ds.length > 1, labels: { color: theme.textColor } }
            }
        }
    });
}

/**
 * Radar chart
 */
function createRadarChart(canvasId, labels, data, dataLabel) {
    const el = document.getElementById(canvasId);
    if (!el) return null;
    const theme = dsChartTheme();

    const datasets = Array.isArray(data[0]) ? data.map((d, i) => ({
        label: d.label || `Set ${i + 1}`,
        data: d.data || d,
        borderColor: DS_CHART_COLORS[i % DS_CHART_COLORS.length],
        backgroundColor: DS_CHART_COLORS[i % DS_CHART_COLORS.length] + '20',
        pointBackgroundColor: DS_CHART_COLORS[i % DS_CHART_COLORS.length],
        borderWidth: 2,
        pointRadius: 3
    })) : [{
        label: dataLabel || 'Score',
        data: data,
        borderColor: DS_CHART_COLORS[0],
        backgroundColor: DS_CHART_COLORS[0] + '20',
        pointBackgroundColor: DS_CHART_COLORS[0],
        borderWidth: 2,
        pointRadius: 3
    }];

    return new Chart(el, {
        type: 'radar',
        data: { labels, datasets },
        options: {
            ..._baseOptions(theme),
            scales: {
                r: {
                    beginAtZero: true,
                    max: 100,
                    ticks: { color: theme.mutedColor, backdropColor: 'transparent', font: { size: 10 } },
                    grid: { color: theme.gridColor },
                    pointLabels: { color: theme.textColor, font: { size: 11, family: 'Inter' } },
                    angleLines: { color: theme.gridColor }
                }
            }
        }
    });
}

/**
 * Horizontal bar chart
 */
function createHorizontalBar(canvasId, labels, values, colorOrColors) {
    const el = document.getElementById(canvasId);
    if (!el) return null;
    const theme = dsChartTheme();

    const colors = Array.isArray(colorOrColors) ? colorOrColors :
        (typeof colorOrColors === 'string' ? Array(labels.length).fill(colorOrColors) :
        DS_CHART_COLORS.slice(0, labels.length));

    return new Chart(el, {
        type: 'bar',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: colors,
                borderRadius: 4,
                borderSkipped: false
            }]
        },
        options: {
            ..._baseOptions(theme),
            indexAxis: 'y',
            scales: {
                x: { ticks: { color: theme.mutedColor }, grid: { color: theme.gridColor } },
                y: { ticks: { color: theme.textColor, font: { size: 11 } }, grid: { display: false } }
            },
            plugins: { legend: { display: false } }
        }
    });
}

/**
 * Pie / Doughnut chart
 */
function createPieChart(canvasId, labels, values, isDoughnut) {
    const el = document.getElementById(canvasId);
    if (!el) return null;
    const theme = dsChartTheme();

    return new Chart(el, {
        type: isDoughnut ? 'doughnut' : 'pie',
        data: {
            labels,
            datasets: [{
                data: values,
                backgroundColor: DS_CHART_COLORS.slice(0, labels.length),
                borderWidth: 2,
                borderColor: theme.surfaceColor
            }]
        },
        options: {
            ..._baseOptions(theme),
            cutout: isDoughnut ? '60%' : 0,
            plugins: {
                legend: { position: 'bottom', labels: { color: theme.textColor, padding: 12, font: { size: 11 } } }
            }
        }
    });
}
