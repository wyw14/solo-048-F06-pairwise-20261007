const page = document.body.dataset.page;
const state = { overview: null };

function text(value) {
  return value === undefined || value === null ? "-" : String(value);
}

function panel(title, rows) {
  const body = rows.map(([name, value]) => `<dt>${name}</dt><dd>${text(value)}</dd>`).join("");
  return `<article class="panel"><h3>${title}</h3><dl>${body}</dl></article>`;
}

function metrics(data) {
  const active = data.zones.filter((zone) => zone.phase === "active").length;
  const online = data.stations.filter((station) => station.online).length;
  const incidents = data.incidents.filter((incident) => incident.active).length;
  const published = data.plans.filter((plan) => plan.status === "published").length;
  return [["运行区域", active], ["在线换热站", online], ["活动事故", incidents], ["已发布计划", published]]
    .map(([name, value]) => `<div class="metric"><span>${name}</span><strong>${value}</strong></div>`).join("");
}

const stageNames = { relief: "泄压", boundary: "边界阀", receipt: "回执", isolation: "最终隔离" };
const stepStates = { pending: "等待", running: "执行中", completed: "完成", failed: "失败" };

function isolationPanels(data) {
  const coordinator = data.isolation_coordinator || {};
  const header = panel("隔离协调器", [
    ["分区指纹", coordinator.epoch ? coordinator.epoch.partition : "-"],
    ["保护代次", coordinator.epoch ? coordinator.epoch.generation : "-"],
    ["状态", coordinator.frozen ? "已冻结（" + (coordinator.reason || "代次已变更") + "）" : "运行中"],
  ]);
  const sessions = (data.isolations || []).map((session) => {
    const branches = {};
    (session.steps || []).forEach((step) => {
      (branches[step.branch_id] = branches[step.branch_id] || []).push(step);
    });
    const rows = Object.keys(branches).flatMap((branch) =>
      branches[branch].map((step) => [
        (branch === "section" ? "公共" : branch) + " · " + (stageNames[step.stage] || step.stage),
        (stepStates[step.state] || step.state) + (step.valve_id ? "（" + step.valve_id + "）" : "") + (step.error ? "：" + step.error : ""),
      ])
    );
    rows.unshift(["状态", session.status], ["会话", session.id.slice(0, 8)]);
    return panel("隔离 " + session.section_id, rows);
  });
  return header + (sessions.length ? sessions.join("") : panel("无隔离会话", [["系统状态", "稳定"]]));
}

function pageContent(data) {
  if (page === "network") return data.zones.map((zone) => panel(zone.name, [["阶段", zone.phase], ["泵级", zone.pump_stage], ["旁通", zone.bypass_open ? "开启" : "关闭"], ["压差", (zone.supply_pressure - zone.return_pressure).toFixed(2) + " MPa"]])).join("");
  if (page === "stations") return data.stations.map((station) => panel(station.name, [["区域", station.zone_id], ["通信", station.online ? "在线" : "离线"], ["目标", station.target_celsius + " °C"], ["控制代际", station.controller_epoch], ["人工保持", station.hold ? "是" : "否"]])).join("");
  if (page === "incidents") {
    const incidents = data.incidents.length ? data.incidents.map((incident) => panel(incident.kind, [["站点", incident.station_id], ["等级", incident.severity], ["状态", incident.active ? "处理中" : "已解除"], ["说明", incident.message]])).join("") : panel("当前无活动事故", [["系统状态", "稳定"]]);
    return incidents + isolationPanels(data);
  }
  return data.plans.length ? data.plans.map((plan) => panel(plan.id.slice(0, 8), [["状态", plan.status], ["来源 revision", plan.source_revision], ["站点数", Object.keys(plan.targets || {}).length], ["说明", plan.message || "-"]])).join("") : panel("等待调度计划", [["当前 revision", Math.max(...data.stations.map((station) => station.target_revision))], ["人工保持", data.stations.filter((station) => station.hold).length]]);
}

async function load() {
  const connection = document.querySelector("#connection");
  try {
    const response = await fetch("/api/v1/overview", { cache: "no-store" });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.error || "request failed");
    state.overview = payload.data;
    document.querySelector("#summary").innerHTML = metrics(payload.data);
    document.querySelector("#content").innerHTML = pageContent(payload.data);
    connection.textContent = "实时连接";
  } catch (error) {
    connection.textContent = "连接异常";
    document.querySelector("#content").innerHTML = panel("无法加载状态", [["错误", error.message]]);
  }
}

document.querySelector("#refresh").addEventListener("click", load);
load();
