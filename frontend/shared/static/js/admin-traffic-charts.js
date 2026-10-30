/* Release the previous chart and document listeners before mounting updated data. */
(() => {
let dispose = () => {};
const seriesVisibility = [true, true];
function mount() {
  dispose();
  const node = document.getElementById('adminTrendConfig');
  if (!node) return;
  const config = JSON.parse(node.textContent);
  const { labels: chartLabels, values: chartValues, newValues: chartNewValues,
    seriesLabel: chartSeriesLabel, activeLabel: chartActiveLabel, hourly: chartHourly, hasData: chartHasData } = config;
  const listeners = new AbortController();
const chartCanvas = document.getElementById('visitChart');
const empty = document.getElementById('chartEmpty');
const rootStyles = getComputedStyle(document.body);
const axisColor = rootStyles.getPropertyValue('--chart-axis').trim() || '#cbd5f5';
const gridColor = rootStyles.getPropertyValue('--chart-grid').trim() || 'rgba(148,163,184,0.25)';
const lineColor = rootStyles.getPropertyValue('--chart-line').trim() || '#6366f1';
const newUserColor = rootStyles.getPropertyValue('--info-fg').trim() || '#286aa6';
const tooltipBg = rootStyles.getPropertyValue('--chart-tooltip-bg').trim() || '#0f172a';
const tooltipFg = rootStyles.getPropertyValue('--chart-tooltip-fg').trim() || '#e2e8f0';
const seriesButtons = document.querySelectorAll('[data-trend-series]');
let visitChart = null;
if (chartHasData && typeof Chart !== 'undefined') {
    visitChart = new Chart(chartCanvas.getContext('2d'), {
        type: 'bar',
        data: {
            labels: chartLabels,
            datasets: [
                {
                    label: chartSeriesLabel,
                    data: chartValues,
                    borderColor: lineColor,
                    backgroundColor: lineColor,
                    borderRadius: 5,
                    maxBarThickness: 28,
                    categoryPercentage: 0.72,
                    barPercentage: 0.86,
                },
                {
                    label: '新用戶',
                    data: chartNewValues,
                    borderColor: newUserColor,
                    backgroundColor: newUserColor,
                    borderRadius: 5,
                    maxBarThickness: 28,
                    categoryPercentage: 0.72,
                    barPercentage: 0.86,
                },
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            animation: false,
            font: { family: rootStyles.fontFamily, size: 11 },
            interaction: { mode: 'index', intersect: false, axis: 'x' },
            layout: { padding: { top: 12, right: 4 } },
            scales: {
                y: {
                    beginAtZero: true,
                    suggestedMax: Math.max(2, ...chartValues, ...chartNewValues),
                    ticks: { color: axisColor, precision: 0, maxTicksLimit: 5, padding: 10 },
                    grid: { color: gridColor, drawTicks: false },
                    border: { display: false }
                },
                x: {
                    ticks: {
                        color: axisColor,
                        maxRotation: 0,
                        minRotation: 0,
                        padding: 10,
                        autoSkip: true,
                        maxTicksLimit: window.innerWidth < 700 ? 4 : 10,
                        callback(value) {
                            const label = this.getLabelForValue(value);
                            const [day, hour] = label.split(' ');
                            return chartHourly ? [day.slice(5).replace('-', '/'), hour] : day.slice(5).replace('-', '/');
                        }
                    },
                    grid: { display: false },
                    border: { display: false }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    backgroundColor: tooltipBg,
                    borderColor: lineColor,
                    borderWidth: 1,
                    titleColor: tooltipFg,
                    bodyColor: tooltipFg,
                    padding: 12,
                    cornerRadius: 9,
                    bodySpacing: 7,
                    displayColors: true,
                    callbacks: {
                        title(items) { return `${items[0].label.replaceAll('-', '/')}（台北時間）`; },
                        label(item) { return `${item.datasetIndex === 1 ? '其中新用戶' : chartActiveLabel}：${item.parsed.y} ${item.datasetIndex === 1 || !chartHourly ? '人' : '個帳號'}`; }
                    }
                }
            }
        }
    });
} else if (chartHasData) {
    chartCanvas.hidden = true;
    empty.hidden = false;
    empty.querySelector('p').textContent = '圖表暫時無法載入，請查看下方詳細數值。';
    document.querySelector('.trend-values').open = true;
}
function syncSeriesButtons() {
    seriesButtons.forEach(button => {
        const visible = visitChart?.isDatasetVisible(Number(button.dataset.trendSeries)) ?? true;
        button.setAttribute('aria-pressed', String(visible));
        button.disabled = !visitChart || (visible && visitChart.getVisibleDatasetCount() === 1);
    });
}
seriesButtons.forEach(button => button.addEventListener('click', () => {
    if (!visitChart) return;
    const index = Number(button.dataset.trendSeries);
    visitChart.setDatasetVisibility(index, !visitChart.isDatasetVisible(index));
    seriesVisibility[index] = visitChart.isDatasetVisible(index);
    visitChart.update('none');
    syncSeriesButtons();
}));
if (visitChart) {
    seriesVisibility.forEach((visible, index) => visitChart.setDatasetVisibility(index, visible));
    visitChart.update('none');
}
syncSeriesButtons();
const trendDateForm = document.querySelector('.trend-dates');
const trendDatePicker = document.getElementById('trendDatePicker');
const trendStart = trendDateForm.elements.start;
const trendEnd = trendDateForm.elements.end;
function validateTrendDates() {
    trendEnd.setCustomValidity(trendStart.value > trendEnd.value ? '結束日期不可早於開始日期。' : '');
}
trendStart.addEventListener('change', validateTrendDates);
trendEnd.addEventListener('change', validateTrendDates);
document.addEventListener('pointerdown', event => {
    if (trendDatePicker.open && !trendDatePicker.contains(event.target)) trendDatePicker.open = false;
}, { signal: listeners.signal });
trendDatePicker.addEventListener('keydown', event => {
    if (event.key !== 'Escape' || !trendDatePicker.open) return;
    event.preventDefault();
    trendDatePicker.open = false;
    trendDatePicker.querySelector('summary').focus();
});
function syncAdminTheme() {
    const theme = document.documentElement.dataset.theme;
    document.querySelectorAll('[data-admin-theme]').forEach(button => {
        button.setAttribute('aria-pressed', String(button.dataset.adminTheme === theme));
    });
    if (!visitChart) return;
    const styles = getComputedStyle(document.body);
    const color = name => styles.getPropertyValue(name).trim();
    visitChart.data.datasets[0].backgroundColor = color('--primary');
    visitChart.data.datasets[0].borderColor = color('--primary');
    visitChart.data.datasets[1].backgroundColor = color('--info-fg');
    visitChart.data.datasets[1].borderColor = color('--info-fg');
    for (const axis of ['x', 'y']) visitChart.options.scales[axis].ticks.color = color('--muted');
    visitChart.options.scales.y.grid.color = color('--border');
    visitChart.options.scales.x.border.color = color('--border');
    Object.assign(visitChart.options.plugins.tooltip, {
        backgroundColor: color('--card'), borderColor: color('--primary'),
        titleColor: color('--fg'), bodyColor: color('--fg'),
    });
    visitChart.update('none');
}

const observer = new MutationObserver(syncAdminTheme);
observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
syncAdminTheme();
  dispose = () => { listeners.abort(); observer.disconnect(); visitChart?.destroy(); };
}
document.addEventListener('click', event => {
  const button = event.target.closest('[data-admin-theme]');
  if (!button) return;
  const theme = button.dataset.adminTheme;
  document.documentElement.dataset.theme = theme;
  document.documentElement.style.colorScheme = theme;
  try { localStorage.setItem('e3_theme', theme); } catch (_) {}
});
document.addEventListener('admin:reports-updated', mount);
mount();
})();
