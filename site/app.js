const state = {
  index: null,
  currentDate: null,
  dayData: null,
  marketFilter: "ALL",
};

const el = {
  dateSelect: document.getElementById("dateSelect"),
  generatedAt: document.getElementById("generatedAt"),
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

async function fetchJson(path) {
  const resp = await fetch(path, { cache: "no-store" });
  if (!resp.ok) throw new Error(`Request failed: ${path}`);
  return resp.json();
}

function renderIndex() {
  const dates = state.index?.dates || [];
  el.dateSelect.innerHTML = "";

  if (!dates.length) {
    el.dateSelect.innerHTML = `<option value="">暂无数据</option>`;
    el.generatedAt.textContent = "数据生成时间: 暂无";
    return;
  }

  dates.forEach((d) => {
    const option = document.createElement("option");
    option.value = d;
    option.textContent = d;
    el.dateSelect.appendChild(option);
  });

  state.currentDate = state.currentDate || state.index.latest_date;
  el.dateSelect.value = state.currentDate;
  el.generatedAt.textContent = `数据生成时间(UTC): ${state.index.generated_at_utc}`;
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

  rows.forEach((row) => {
    const tr = document.createElement("tr");
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

async function loadDay(date) {
  const dayData = await fetchJson(`./data/days/${date}.json`);
  state.dayData = dayData;
  state.marketFilter = "ALL";
  renderMetrics(dayData);
  renderMarketBars(dayData);
  renderMarketFilters(dayData);
  renderTable(dayData);
  renderRanks(dayData);
}

async function init() {
  try {
    state.index = await fetchJson("./data/index.json");
    state.currentDate = state.index.latest_date;
    renderIndex();
    if (state.currentDate) {
      await loadDay(state.currentDate);
    }
  } catch (err) {
    el.stockTbody.innerHTML = `<tr><td colspan="6">加载失败: ${err.message}</td></tr>`;
  }
}

el.dateSelect.addEventListener("change", async (e) => {
  state.currentDate = e.target.value;
  await loadDay(state.currentDate);
});

init();
