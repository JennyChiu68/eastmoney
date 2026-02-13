const TREND_DAYS = 12;
const QUICK_PILLS = 10;

const state = {
  index: null,
  currentDate: null,
  dayData: null,
  marketFilter: "ALL",
  loadToken: 0,
  trendSeries: [],
  calendar: new Map(),
};

const el = {
  yearSelect: document.getElementById("yearSelect"),
  monthSelect: document.getElementById("monthSelect"),
  dateSelect: document.getElementById("dateSelect"),
  prevDateBtn: document.getElementById("prevDateBtn"),
  nextDateBtn: document.getElementById("nextDateBtn"),
  datePills: document.getElementById("datePills"),
  generatedAt: document.getElementById("generatedAt"),
  activeDateLabel: document.getElementById("activeDateLabel"),
  activeDateStats: document.getElementById("activeDateStats"),
  trendSparkline: document.getElementById("trendSparkline"),
  totalCount: document.getElementById("totalCount"),
  upCount: document.getElementById("upCount"),
  downCount: document.getElementById("downCount"),
  avgChange: document.getElementById("avgChange"),
  marketBars: document.getElementById("marketBars"),
  marketFilters: document.getElementById("marketFilters"),
  stockTbody: document.getElementById("stockTbody"),
  gainersList: document.getElementById("gainersList"),
  losersList: document.getElementById("losersList"),
};

function fmtPercent(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return "-";
  const n = Number(value);
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(2)}%`;
}

function classByChange(value) {
  if (value > 0) return "change-up";
  if (value < 0) return "change-down";
  return "change-flat";
}

function splitDate(dateStr) {
  const [year, month] = String(dateStr || "").split("-");
  return { year, month };
}

function toBeijingTimeText(utcIsoText) {
  if (!utcIsoText) return "-";
  const dt = new Date(utcIsoText);
  if (Number.isNaN(dt.getTime())) return String(utcIsoText);
  return new Intl.DateTimeFormat("zh-CN", {
    timeZone: "Asia/Shanghai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(dt);
}

async function fetchJson(path) {
  const resp = await fetch(path, { cache: "no-store" });
  if (!resp.ok) throw new Error(`Request failed: ${path}`);
  return resp.json();
}

function fillSelect(selectEl, items, selected) {
  selectEl.innerHTML = "";
  items.forEach((item) => {
    const option = document.createElement("option");
    option.value = item.value;
    option.textContent = item.label;
    if (item.value === selected) option.selected = true;
    selectEl.appendChild(option);
  });
}

function buildCalendar(dates) {
  const calendar = new Map();
  dates.forEach((dateStr) => {
    const { year, month } = splitDate(dateStr);
    if (!calendar.has(year)) calendar.set(year, new Map());
    const months = calendar.get(year);
    if (!months.has(month)) months.set(month, []);
    months.get(month).push(dateStr);
  });
  state.calendar = calendar;
}

function getYears() {
  return Array.from(state.calendar.keys());
}

function getMonths(year) {
  const months = state.calendar.get(year);
  return months ? Array.from(months.keys()) : [];
}

function getDates(year, month) {
  const months = state.calendar.get(year);
  if (!months) return [];
  return months.get(month) || [];
}

function currentIndex() {
  const dates = state.index?.dates || [];
  return dates.indexOf(state.currentDate);
}

function updateNavButtons() {
  const idx = currentIndex();
  const dates = state.index?.dates || [];
  el.prevDateBtn.disabled = idx < 0 || idx >= dates.length - 1;
  el.nextDateBtn.disabled = idx <= 0;
}

function renderDatePills() {
  const dates = state.index?.dates || [];
  const quick = dates.slice(0, QUICK_PILLS);
  el.datePills.innerHTML = "";
  quick.forEach((d) => {
    const btn = document.createElement("button");
    btn.className = `date-pill ${d === state.currentDate ? "active" : ""}`;
    btn.type = "button";
    btn.textContent = d.slice(5);
    btn.title = d;
    btn.addEventListener("click", async () => {
      if (d === state.currentDate) return;
      state.currentDate = d;
      await loadDay(d);
    });
    el.datePills.appendChild(btn);
  });
}

function renderCalendarControls() {
  const dates = state.index?.dates || [];
  if (!dates.length) return;

  const { year: currentYear, month: currentMonth } = splitDate(state.currentDate);
  const years = getYears();
  const year = years.includes(currentYear) ? currentYear : years[0];

  const months = getMonths(year);
  const month = months.includes(currentMonth) ? currentMonth : months[0];

  const dayDates = getDates(year, month);
  const selectedDate = dayDates.includes(state.currentDate) ? state.currentDate : dayDates[0];
  if (selectedDate && selectedDate !== state.currentDate) state.currentDate = selectedDate;

  fillSelect(
    el.yearSelect,
    years.map((y) => ({ value: y, label: `${y}年` })),
    year
  );
  fillSelect(
    el.monthSelect,
    months.map((m) => ({ value: m, label: `${Number(m)}月` })),
    month
  );
  fillSelect(
    el.dateSelect,
    dayDates.map((d) => ({ value: d, label: d })),
    state.currentDate
  );
}

function renderIndex() {
  const dates = state.index?.dates || [];

  if (!dates.length) {
    el.dateSelect.innerHTML = `<option value="">暂无数据</option>`;
    el.generatedAt.textContent = "数据生成时间: 暂无";
    return;
  }

  state.currentDate = state.currentDate || state.index.latest_date;
  buildCalendar(dates);
  renderCalendarControls();

  const bjText = toBeijingTimeText(state.index.generated_at_utc);
  el.generatedAt.textContent = `数据生成时间（北京时间）: ${bjText}`;
  renderDatePills();
  updateNavButtons();
}

function applyTrendTheme(avgChange) {
  document.body.classList.remove("trend-up", "trend-down");
  if (avgChange > 0) document.body.classList.add("trend-up");
  if (avgChange < 0) document.body.classList.add("trend-down");
}

function renderHero(dayData) {
  const s = dayData.summary || {};
  el.activeDateLabel.textContent = dayData.date || "-";
  el.activeDateStats.textContent = `上榜个股 ${s.total_count ?? "-"} | 平均涨跌幅 ${fmtPercent(
    s.avg_change
  )}`;
  applyTrendTheme(Number(s.avg_change));
}

function renderMetrics(dayData) {
  const s = dayData.summary || {};
  el.totalCount.textContent = s.total_count ?? "-";
  el.upCount.textContent = s.up_count ?? "-";
  el.downCount.textContent = s.down_count ?? "-";
  el.avgChange.textContent = fmtPercent(s.avg_change);
}

function renderMarketBars(dayData) {
  const list = dayData.market_breakdown || [];
  const maxCount = Math.max(...list.map((x) => x.count), 1);
  el.marketBars.innerHTML = "";
  list.forEach((item) => {
    const row = document.createElement("div");
    row.className = "bar-row";
    row.innerHTML = `
      <div class="bar-label">${item.label}</div>
      <div class="bar-track">
        <div class="bar-fill" style="width:${(item.count / maxCount) * 100}%"></div>
      </div>
      <div class="bar-value">${item.count}</div>
    `;
    el.marketBars.appendChild(row);
  });
}

function renderMarketFilters(dayData) {
  const base = [{ code: "ALL", label: "全部" }];
  const list = (dayData.market_breakdown || []).map((x) => ({
    code: x.code,
    label: x.label,
  }));
  const all = [...base, ...list];

  el.marketFilters.innerHTML = "";
  all.forEach((item) => {
    const btn = document.createElement("button");
    btn.className = `filter-btn ${state.marketFilter === item.code ? "active" : ""}`;
    btn.type = "button";
    btn.textContent = item.label;
    btn.addEventListener("click", () => {
      state.marketFilter = item.code;
      renderMarketFilters(dayData);
      renderTable(dayData);
    });
    el.marketFilters.appendChild(btn);
  });
}

function getFilteredStocks(dayData) {
  const stocks = dayData.stocks || [];
  if (state.marketFilter === "ALL") return stocks;
  return stocks.filter((x) => x.market_group === state.marketFilter);
}

function renderTable(dayData) {
  const rows = getFilteredStocks(dayData);
  el.stockTbody.innerHTML = "";

  if (!rows.length) {
    el.stockTbody.innerHTML = `<tr><td colspan="6">当前筛选条件无数据</td></tr>`;
    return;
  }

  rows.forEach((row, idx) => {
    const tr = document.createElement("tr");
    tr.style.animationDelay = `${Math.min(idx * 0.015, 0.24)}s`;
    tr.innerHTML = `
      <td>${row.security_code || "-"}</td>
      <td>${row.security_name || "-"}</td>
      <td>${row.market_label || row.market_group || "-"}</td>
      <td class="${classByChange(row.change_rate)}">${fmtPercent(row.change_rate)}</td>
      <td><a href="${row.detail_link}" target="_blank" rel="noreferrer">详情</a></td>
      <td><a href="${row.quote_link}" target="_blank" rel="noreferrer">行情</a></td>
    `;
    el.stockTbody.appendChild(tr);
  });
}

function renderRanks(dayData) {
  const fillList = (target, items) => {
    target.innerHTML = "";
    (items || []).forEach((row) => {
      const li = document.createElement("li");
      li.innerHTML = `<span>${row.security_name} (${row.security_code})</span><span class="${classByChange(
        row.change_rate
      )}">${fmtPercent(row.change_rate)}</span>`;
      target.appendChild(li);
    });
    if (!(items || []).length) {
      target.innerHTML = "<li><span>暂无数据</span></li>";
    }
  };

  fillList(el.gainersList, dayData.top_gainers);
  fillList(el.losersList, dayData.top_losers);
}

function drawSparkline(series) {
  if (!el.trendSparkline) return;
  if (!series.length) {
    el.trendSparkline.innerHTML = "";
    return;
  }

  const width = 240;
  const height = 68;
  const padding = 6;
  const vals = series.map((x) => x.total_count || 0);
  const max = Math.max(...vals, 1);
  const min = Math.min(...vals, 0);
  const span = Math.max(max - min, 1);

  const points = series.map((item, idx) => {
    const x =
      padding + (idx * (width - padding * 2)) / Math.max(series.length - 1, 1);
    const y =
      height - padding - ((item.total_count - min) / span) * (height - padding * 2);
    return [Number(x.toFixed(2)), Number(y.toFixed(2))];
  });

  const polyline = points.map((p) => p.join(",")).join(" ");
  const area = `M ${padding},${height - padding} L ${points
    .map((p) => p.join(","))
    .join(" L ")} L ${width - padding},${height - padding} Z`;

  const currentDate = state.currentDate;
  const markIdx = series.findIndex((x) => x.date === currentDate);
  const mark = markIdx >= 0 ? points[markIdx] : points[points.length - 1];

  el.trendSparkline.innerHTML = `
    <defs>
      <linearGradient id="sparkStroke" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#b6fff4" />
        <stop offset="100%" stop-color="#f7fff0" />
      </linearGradient>
      <linearGradient id="sparkArea" x1="0%" y1="0%" x2="0%" y2="100%">
        <stop offset="0%" stop-color="rgba(208,255,249,0.5)" />
        <stop offset="100%" stop-color="rgba(208,255,249,0.04)" />
      </linearGradient>
    </defs>
    <path d="${area}" fill="url(#sparkArea)"></path>
    <polyline points="${polyline}" fill="none" stroke="url(#sparkStroke)" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></polyline>
    <circle cx="${mark[0]}" cy="${mark[1]}" r="3.8" fill="#ffffff"></circle>
  `;
}

async function loadTrendSeries() {
  const dates = (state.index?.dates || []).slice(0, TREND_DAYS);
  const results = await Promise.all(
    dates.map(async (d) => {
      try {
        const day = await fetchJson(`./data/days/${d}.json`);
        return {
          date: d,
          total_count: Number(day.summary?.total_count || 0),
        };
      } catch (err) {
        return { date: d, total_count: 0 };
      }
    })
  );
  state.trendSeries = results.reverse();
  drawSparkline(state.trendSeries);
}

async function loadDay(date) {
  const token = ++state.loadToken;
  const dayData = await fetchJson(`./data/days/${date}.json`);
  if (token !== state.loadToken) return;

  state.dayData = dayData;
  state.marketFilter = "ALL";
  renderHero(dayData);
  renderMetrics(dayData);
  renderMarketBars(dayData);
  renderMarketFilters(dayData);
  renderTable(dayData);
  renderRanks(dayData);
  renderCalendarControls();
  renderDatePills();
  updateNavButtons();
  drawSparkline(state.trendSeries);
}

async function goOffset(step) {
  const dates = state.index?.dates || [];
  const idx = currentIndex();
  if (idx < 0) return;
  const target = dates[idx + step];
  if (!target) return;
  state.currentDate = target;
  await loadDay(target);
}

async function onYearChange() {
  const year = el.yearSelect.value;
  const months = getMonths(year);
  if (!months.length) return;

  const keepMonth = months.includes(el.monthSelect.value)
    ? el.monthSelect.value
    : months[0];
  const dates = getDates(year, keepMonth);
  if (!dates.length) return;

  state.currentDate = dates[0];
  await loadDay(state.currentDate);
}

async function onMonthChange() {
  const year = el.yearSelect.value;
  const month = el.monthSelect.value;
  const dates = getDates(year, month);
  if (!dates.length) return;
  state.currentDate = dates[0];
  await loadDay(state.currentDate);
}

async function init() {
  try {
    state.index = await fetchJson("./data/index.json");
    state.currentDate = state.index.latest_date;
    renderIndex();
    await loadTrendSeries();
    if (state.currentDate) {
      await loadDay(state.currentDate);
    }
  } catch (err) {
    el.stockTbody.innerHTML = `<tr><td colspan="6">加载失败: ${err.message}</td></tr>`;
  }
}

el.yearSelect.addEventListener("change", async () => {
  await onYearChange();
});

el.monthSelect.addEventListener("change", async () => {
  await onMonthChange();
});

el.dateSelect.addEventListener("change", async (e) => {
  state.currentDate = e.target.value;
  await loadDay(state.currentDate);
});

el.prevDateBtn.addEventListener("click", async () => {
  await goOffset(1);
});

el.nextDateBtn.addEventListener("click", async () => {
  await goOffset(-1);
});

init();
