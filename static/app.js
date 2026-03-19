const form = document.getElementById("search-form");
const statusEl = document.getElementById("status");
const summaryEl = document.getElementById("summary");
const resultsEl = document.getElementById("results");
const buttonEl = document.getElementById("search-button");
const chipsEl = document.getElementById("destination-chips");
const providersStatusEl = document.getElementById("providers-status");
const providersInputEl = document.getElementById("providers");
const providersPickerEl = document.getElementById("providers-picker");
const applyProviderKeysBtn = document.getElementById("apply-provider-keys");
const providerKeysStatusEl = document.getElementById("provider-keys-status");
const applyBudgetPresetBtn = document.getElementById("apply-budget-preset");
const budgetPresetStatusEl = document.getElementById("budget-preset-status");
const exportBtn = document.getElementById("export-button");
const exportPdfBtn = document.getElementById("export-pdf-button");
const progressPanelEl = document.getElementById("progress-panel");
const progressMetaEl = document.getElementById("progress-meta");
const progressPhaseEl = document.getElementById("progress-phase");
const progressBarFillEl = document.getElementById("progress-bar-fill");
const providerHealthPanelEl = document.getElementById("provider-health-panel");
const providerHealthGridEl = document.getElementById("provider-health-grid");
const providerHealthNoteEl = document.getElementById("provider-health-note");
const progressLogEl = document.getElementById("progress-log");

const cardTemplate = document.getElementById("result-card-template");
const legTemplate = document.getElementById("leg-template");
const objectiveLabels = {
  best: "Best",
  cheapest: "Cheapest",
  fastest: "Fastest",
  price_per_km: "Price / 1000 km",
};
let providerCatalog = [];
let lastSearchPayload = null;
let lastSearchResponse = null;
let progressEventCursor = 0;
const destinationNameCatalog = new Map();
const destinationSortModes = new Map();
const fieldTooltips = {
  origins: "Departure airports used as starting points. Add multiple IATA codes comma-separated.",
  destinations:
    "Main destinations to search. Add multiple IATA codes comma-separated for one combined run.",
  "period-start": "Earliest departure date considered by the engine.",
  "period-end": "Latest departure date considered by the engine.",
  currency: "Display and query currency used for all fare comparisons.",
  "min-stay": "Minimum number of days in the main destination.",
  "max-stay": "Maximum number of days in the main destination.",
  "min-stopover": "Minimum stopover days in split-leg hub cities. Set 1+ to force >24h stopovers.",
  "max-stopover": "Maximum stopover days allowed in split-leg hub cities.",
  "max-connection-layover-hours":
    "Maximum regular connection wait inside booked legs. 0 means no limit.",
  "max-transfers-direction":
    "Single cap per direction counting both in-ticket stops and split-leg boundaries to the final destination.",
  adults: "Number of adult passengers used in fare requests.",
  "hand-bags": "Cabin bags per adult used for fare estimation.",
  "hold-bags": "Checked bags per adult used for fare estimation.",
  "market-compare-fares":
    "Also tests no-bag base fare and keeps it if cheaper than selected baggage profile.",
  objective: "Ranking strategy for final results: best, cheapest, fastest, or price per 1000 km.",
  "top-results": "How many final itineraries are displayed.",
  "validate-top": "Top estimated candidates per destination sent to live fare validation.",
  "auto-hubs": "How many hubs are auto-selected per direction during candidate generation.",
  "exhaustive-hubs":
    "If enabled, scans full hub pool instead of only top auto-selected hubs. Slower but broader.",
  "hub-candidates":
    "Hub airport pool used by auto-hub logic. Broader pool increases coverage and runtime.",
  providers:
    "Providers used for fare validation. Free providers are preselected and paid ones can be enabled with runtime keys.",
  "amadeus-client-id": "Runtime Amadeus client id (local session only).",
  "amadeus-client-secret": "Runtime Amadeus client secret (local session only).",
  "serpapi-api-key": "Runtime SerpApi API key (local session only).",
  "serpapi-return-scan-limit":
    "How many departure options SerpApi return flow expands with departure_token (1-5).",
  "io-workers": "Parallel network workers for live provider requests.",
  "pool-multiplier":
    "Multiplier for estimated route pool per destination before live provider validation.",
  "calendar-hubs-prefetch":
    "Limits hubs queried in calendar seeding stage. 0 means all hubs.",
  "max-validate-oneway-keys":
    "Hard cap for one-way live fare keys per destination. 0 means no cap.",
  "max-validate-return-keys":
    "Hard cap for round-trip live fare keys per destination. 0 means no cap.",
  "max-total-provider-calls":
    "Total paid-provider call budget for one search. Kiwi calls are still counted separately in Max Kiwi calls.",
  "max-calls-kiwi": "Per-search Kiwi call budget. 0 means no cap.",
  "max-calls-amadeus": "Per-search Amadeus call budget. 0 means no cap.",
  "max-calls-serpapi": "Per-search SerpApi call budget. 0 means no cap.",
  "serpapi-probe-oneway-keys":
    "Top-ranked one-way keys additionally probed on SerpApi when SerpApi is active.",
  "serpapi-probe-return-keys":
    "Top-ranked round-trip keys additionally probed on SerpApi when SerpApi is active.",
};

function parseCodes(input) {
  return input
    .split(",")
    .map((item) => item.trim().toUpperCase())
    .filter(Boolean);
}

function parseProviderIds(input) {
  const items = Array.isArray(input) ? input : String(input || "").split(",");
  const normalized = [];
  const seen = new Set();
  for (const item of items) {
    const providerId = String(item || "").trim().toLowerCase();
    if (!providerId || seen.has(providerId)) continue;
    seen.add(providerId);
    normalized.push(providerId);
  }
  return normalized;
}

function orderProviderIds(providerIds) {
  const normalized = parseProviderIds(providerIds);
  if (!providerCatalog.length) return normalized;
  const providerOrder = new Map(
    providerCatalog.map((provider, index) => [String(provider.id || "").toLowerCase(), index]),
  );
  return normalized.sort((left, right) => {
    const leftOrder = providerOrder.has(left)
      ? providerOrder.get(left)
      : Number.MAX_SAFE_INTEGER;
    const rightOrder = providerOrder.has(right)
      ? providerOrder.get(right)
      : Number.MAX_SAFE_INTEGER;
    if (leftOrder !== rightOrder) {
      return leftOrder - rightOrder;
    }
    return left.localeCompare(right);
  });
}

function selectedProviderIdsFromPicker() {
  if (!providersPickerEl) {
    return [];
  }
  return Array.from(
    providersPickerEl.querySelectorAll('input[type="checkbox"][data-provider-id]:checked'),
  ).map((checkbox) => String(checkbox.value || "").toLowerCase());
}

function getSelectedProviderIds() {
  if (providersPickerEl?.querySelector('input[type="checkbox"][data-provider-id]')) {
    return orderProviderIds(selectedProviderIdsFromPicker());
  }
  return orderProviderIds(parseProviderIds(providersInputEl?.value || ""));
}

function providerPickerMeta(provider) {
  const typeLabel = provider.requires_credentials ? "API key provider" : "Free provider";
  if (provider.missing_env?.length) {
    return `${typeLabel}. Needs ${provider.missing_env.join("/")}.`;
  }
  const hint = String(provider.configuration_hint || "").trim();
  if (hint) {
    return `${typeLabel}. ${hint}`;
  }
  if (provider.configured) {
    return provider.requires_credentials
      ? `${typeLabel}. Ready in this session.`
      : `${typeLabel}. Ready now.`;
  }
  return provider.requires_credentials
    ? `${typeLabel}. Add keys to enable.`
    : `${typeLabel}. Select to include it.`;
}

function providerStatusSuffix(provider, selected) {
  if (selected && provider.configured) {
    return "active";
  }
  if (provider.missing_env?.length) {
    return selected
      ? `selected, needs ${provider.missing_env.join("/")}`
      : `needs ${provider.missing_env.join("/")}`;
  }
  const hint = String(provider.configuration_hint || "").trim();
  if (hint) {
    return selected ? hint : `setup: ${hint}`;
  }
  if (provider.configured) {
    return "available";
  }
  return selected ? "selected but unavailable" : "unavailable";
}

function setSelectedProviderIds(providerIds) {
  const orderedIds = orderProviderIds(providerIds);
  const selectedSet = new Set(orderedIds);
  if (providersInputEl) {
    providersInputEl.value = orderedIds.join(",");
  }
  if (providersPickerEl) {
    for (const checkbox of providersPickerEl.querySelectorAll('input[type="checkbox"][data-provider-id]')) {
      checkbox.checked = selectedSet.has(String(checkbox.value || "").toLowerCase());
    }
  }
  renderProviderStatus(orderedIds);
  return orderedIds;
}

function syncSelectedProvidersFromPicker() {
  setSelectedProviderIds(selectedProviderIdsFromPicker());
}

function renderProviderPicker(selectedIds = getSelectedProviderIds()) {
  if (!providersPickerEl) return;
  providersPickerEl.innerHTML = "";
  if (!providerCatalog.length) {
    setSelectedProviderIds(selectedIds);
    return;
  }

  const selectedSet = new Set(orderProviderIds(selectedIds));
  const groups = [
    {
      title: "Free providers",
      providers: providerCatalog.filter((provider) => !provider.requires_credentials),
    },
    {
      title: "API key providers",
      providers: providerCatalog.filter((provider) => provider.requires_credentials),
    },
  ].filter((group) => group.providers.length > 0);

  for (const group of groups) {
    const section = document.createElement("section");
    section.className = "providers-picker-group";

    const title = document.createElement("p");
    title.className = "providers-picker-title";
    title.textContent = group.title;
    section.appendChild(title);

    const grid = document.createElement("div");
    grid.className = "providers-picker-grid";

    for (const provider of group.providers) {
      const providerId = String(provider.id || "").toLowerCase();
      const option = document.createElement("label");
      option.className = "provider-option";

      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.value = providerId;
      checkbox.dataset.providerId = providerId;
      checkbox.checked = selectedSet.has(providerId);
      checkbox.addEventListener("change", syncSelectedProvidersFromPicker);

      const body = document.createElement("span");
      body.className = "provider-option-body";

      const head = document.createElement("span");
      head.className = "provider-option-head";

      const name = document.createElement("span");
      name.className = "provider-option-name";
      name.textContent = provider.name || providerId.toUpperCase();

      const idTag = document.createElement("span");
      idTag.className = "provider-option-id";
      idTag.textContent = providerId;

      const meta = document.createElement("span");
      meta.className = "provider-option-meta";
      meta.textContent = providerPickerMeta(provider);

      head.append(name, idTag);
      body.append(head, meta);
      option.append(checkbox, body);
      grid.appendChild(option);
    }

    section.appendChild(grid);
    providersPickerEl.appendChild(section);
  }

  setSelectedProviderIds(selectedIds);
}

function extractLabelTitle(label) {
  const existingLine = label.querySelector(":scope > .label-line");
  if (existingLine) {
    const text = Array.from(existingLine.childNodes)
      .filter((node) => !(node.nodeType === Node.ELEMENT_NODE && node.classList?.contains("info-dot")))
      .map((node) => (node.textContent || "").trim())
      .join(" ")
      .replace(/\s+/g, " ")
      .trim();
    if (text) return text;
  }

  for (const node of Array.from(label.childNodes)) {
    if (node.nodeType === Node.TEXT_NODE) {
      const text = (node.textContent || "").replace(/\s+/g, " ").trim();
      if (text) return text;
      continue;
    }
    if (node.nodeType !== Node.ELEMENT_NODE) continue;
    const tag = node.tagName.toLowerCase();
    if (tag === "small") continue;
    if (["input", "select", "textarea", "button", "details", "div"].includes(tag)) break;
  }
  return "";
}

function attachFieldTooltips() {
  for (const [id, tip] of Object.entries(fieldTooltips)) {
    const control = document.getElementById(id);
    if (!control) continue;
    const label = control.closest("label");
    if (!label) continue;

    let labelLine = label.querySelector(":scope > .label-line");
    if (!labelLine) {
      const title = extractLabelTitle(label);
      if (!title) continue;
      labelLine = document.createElement("span");
      labelLine.className = "label-line";
      labelLine.append(document.createTextNode(title));
      for (const node of Array.from(label.childNodes)) {
        if (node.nodeType === Node.TEXT_NODE && (node.textContent || "").trim()) {
          node.textContent = "";
        }
      }
      label.insertBefore(labelLine, label.firstChild);
    }

    let dot = labelLine.querySelector(".info-dot");
    if (!dot) {
      dot = document.createElement("span");
      dot.className = "info-dot";
      dot.tabIndex = 0;
      dot.textContent = "i";
      labelLine.append(dot);
    }
    dot.setAttribute("data-tip", tip);
    dot.setAttribute("aria-label", tip);
  }
}

function renderProviderStatus(selectedIds) {
  if (!providersStatusEl) return;
  providersStatusEl.innerHTML = "";
  if (!providerCatalog.length) return;

  const selectedSet = new Set(orderProviderIds(selectedIds || []));
  for (const provider of providerCatalog) {
    const id = String(provider.id || "").toLowerCase();
    const selected = selectedSet.has(id);
    const active = selected && provider.configured;
    const badge = document.createElement("span");
    badge.className = "provider-pill";
    if (active) {
      badge.classList.add("active");
    } else {
      badge.classList.add("inactive");
    }
    if (!selected) {
      badge.classList.add("unselected");
    }
    const suffix = providerStatusSuffix(provider, selected);
    badge.textContent = `${id}: ${suffix}`;
    providersStatusEl.appendChild(badge);
  }
}

function providerSecretsPayload() {
  return {
    amadeus_client_id: document.getElementById("amadeus-client-id").value.trim(),
    amadeus_client_secret: document.getElementById("amadeus-client-secret").value.trim(),
    serpapi_api_key: document.getElementById("serpapi-api-key").value.trim(),
    serpapi_return_option_scan_limit: document
      .getElementById("serpapi-return-scan-limit")
      .value.trim(),
  };
}

function renderProviderKeySummary(runtimeConfig) {
  if (!providerKeysStatusEl) return;
  const amadeusReady =
    runtimeConfig?.amadeus_client_id_set && runtimeConfig?.amadeus_client_secret_set;
  const serpapiReady = runtimeConfig?.serpapi_api_key_set;
  const serpapiScanSet = runtimeConfig?.serpapi_return_option_scan_limit_set;
  providerKeysStatusEl.textContent =
    `Runtime keys: amadeus ${amadeusReady ? "ready" : "missing"}, ` +
    `serpapi ${serpapiReady ? "ready" : "missing"}, ` +
    `serpapi scan limit ${serpapiScanSet ? "set" : "default"}.`;
}

async function refreshProviderConfig() {
  try {
    const response = await fetch("/api/provider-config");
    if (!response.ok) return;
    const payload = await response.json();
    if (payload.providers?.length) {
      const selectedIds = getSelectedProviderIds();
      providerCatalog = payload.providers;
      renderProviderPicker(selectedIds);
    }
    renderProviderKeySummary(payload.runtime_provider_config || {});
  } catch (_error) {
    // Non-critical: keep current provider view.
  }
}

async function applyProviderKeys() {
  if (!applyProviderKeysBtn) return;
  applyProviderKeysBtn.disabled = true;
  if (providerKeysStatusEl) {
    providerKeysStatusEl.textContent = "Applying provider keys...";
  }
  try {
    const response = await fetch("/api/provider-config", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(providerSecretsPayload()),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Provider key update failed");
    }
    if (payload.providers?.length) {
      const selectedIds = getSelectedProviderIds();
      providerCatalog = payload.providers;
      renderProviderPicker(selectedIds);
    }
    renderProviderKeySummary(payload.runtime_provider_config || {});
  } catch (error) {
    if (providerKeysStatusEl) {
      providerKeysStatusEl.textContent = `Provider key update error: ${error.message}`;
    }
  } finally {
    applyProviderKeysBtn.disabled = false;
  }
}

function asInt(id) {
  return Number.parseInt(document.getElementById(id).value, 10);
}

function setInputValue(id, value) {
  const control = document.getElementById(id);
  if (!control) return;
  control.value = String(value);
}

function parseDateInput(id) {
  const value = document.getElementById(id).value;
  if (!value) return null;
  const parsed = new Date(`${value}T12:00:00`);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function providerIsConfigured(providerId) {
  const target = providerCatalog.find((provider) => String(provider.id || "").toLowerCase() === providerId);
  return Boolean(target?.configured);
}

function applyBudgetAwarePreset() {
  const destinationsCount = Math.max(1, parseCodes(document.getElementById("destinations").value).length);
  const start = parseDateInput("period-start");
  const end = parseDateInput("period-end");
  let periodDays = 14;
  if (start && end) {
    const diff = Math.round((end.getTime() - start.getTime()) / (24 * 3600 * 1000)) + 1;
    periodDays = Math.max(1, diff);
  }
  const periodWeeks = Math.max(1, Math.ceil(periodDays / 7));

  const amadeusConfigured = providerIsConfigured("amadeus");
  const serpapiConfigured = providerIsConfigured("serpapi");
  const enabledProviders = providerCatalog
    .filter((provider) => !provider.requires_credentials && provider.default_enabled !== false)
    .map((provider) => String(provider.id || "").toLowerCase())
    .filter(Boolean);
  if (amadeusConfigured) enabledProviders.push("amadeus");
  if (serpapiConfigured) enabledProviders.push("serpapi");
  const currentProviders = getSelectedProviderIds();
  const manuallySelectedProviders = currentProviders.length > 0 ? currentProviders : null;
  const providersForPreset = manuallySelectedProviders || enabledProviders || ["kiwi"];
  setSelectedProviderIds(providersForPreset);

  // "Smart Budget" preset: exhaustive Kiwi + higher-quality paid-provider comparison.
  const validateTop = Math.max(
    180,
    Math.min(320, Math.round(160 + destinationsCount * 18 + periodWeeks * 5)),
  );
  const amadeusCalls = amadeusConfigured
    ? Math.max(24, Math.min(120, Math.round(destinationsCount * 16 + periodWeeks * 6)))
    : 0;
  const serpapiCalls = serpapiConfigured
    ? Math.max(4, Math.min(25, Math.round(destinationsCount * 3 + periodWeeks * 1.2)))
    : 0;
  const serpapiReturnScanLimit = serpapiConfigured ? 2 : 1;
  const serpapiProbeReturn = serpapiConfigured
    ? Math.max(1, Math.min(4, Math.floor(serpapiCalls / 6)))
    : 0;
  const reservedSerpapiCalls = serpapiProbeReturn * (1 + serpapiReturnScanLimit);
  const serpapiProbeOneWay = serpapiConfigured
    ? Math.max(1, Math.min(40, serpapiCalls - reservedSerpapiCalls))
    : 0;
  const totalPaidBudget = amadeusCalls + serpapiCalls;

  setInputValue("objective", "best");
  document.getElementById("market-compare-fares").checked = true;
  setInputValue("max-connection-layover-hours", 0);
  setInputValue("validate-top", validateTop);
  setInputValue("top-results", 20);
  setInputValue("pool-multiplier", 50);
  setInputValue("io-workers", 32);
  document.getElementById("exhaustive-hubs").checked = true;

  setInputValue("calendar-hubs-prefetch", 0);
  setInputValue("max-validate-oneway-keys", 0);
  setInputValue("max-validate-return-keys", 0);
  setInputValue("max-total-provider-calls", totalPaidBudget || 0);
  setInputValue("max-calls-kiwi", 0);
  setInputValue("max-calls-amadeus", amadeusCalls);
  setInputValue("max-calls-serpapi", serpapiCalls);
  setInputValue("serpapi-probe-oneway-keys", serpapiProbeOneWay);
  setInputValue("serpapi-probe-return-keys", serpapiProbeReturn);
  if (serpapiConfigured) {
    setInputValue("serpapi-return-scan-limit", serpapiReturnScanLimit);
  }

  syncAutoHubControls();
  if (budgetPresetStatusEl) {
    budgetPresetStatusEl.textContent =
      `Budget-aware preset applied (Smart Budget): ` +
      `providers ${providersForPreset.join("/")}, validate-top ${validateTop}, CPU auto-max, IO 32, pool x50 per destination, ` +
      `Amadeus cap ${amadeusCalls || "off"}, SerpApi cap ${serpapiCalls || "off"}, total paid cap ${totalPaidBudget || "off"}.`;
  }
}

function formatMoney(value, currency) {
  if (value == null) return "N/A";
  if (currency === "RON") {
    return `${new Intl.NumberFormat("ro-RO").format(value)} lei`;
  }
  return `${new Intl.NumberFormat("en-US").format(value)} ${currency}`;
}

function formatPriceModeLabel(mode) {
  const normalized = String(mode || "").trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "explicit_total") return "explicit total from provider";
  if (normalized === "per_person_scaled") return "per-person fare scaled to total travelers";
  if (normalized === "displayed") return "provider displayed fare";
  if (normalized === "missing_price") return "missing provider price";
  return normalized.replaceAll("_", " ");
}

function parseDateTime(value) {
  if (!value) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed;
}

function formatDateLabel(dateIso) {
  const raw = String(dateIso || "").slice(0, 10);
  const parts = raw.split("-");
  if (parts.length !== 3) return String(dateIso || "N/A");
  const [year, month, day] = parts.map((item) => Number.parseInt(item, 10));
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    return String(dateIso || "N/A");
  }
  const localDate = new Date(year, month - 1, day, 12, 0, 0);
  return new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    day: "2-digit",
    month: "short",
  }).format(localDate);
}

function formatDateTimeLabel(value) {
  const parsed = parseDateTime(value);
  if (!parsed) return "N/A";
  return new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    day: "2-digit",
    month: "short",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(parsed);
}

function formatTimeLabel(value) {
  const parsed = parseDateTime(value);
  if (!parsed) return "";
  return new Intl.DateTimeFormat(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).format(parsed);
}

function formatDuration(valueSeconds) {
  const seconds = Number.parseInt(valueSeconds, 10);
  if (!Number.isFinite(seconds) || seconds <= 0) return "";
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.round((seconds % 3600) / 60);
  if (hours > 0 && minutes > 0) return `${hours}h ${minutes}m`;
  if (hours > 0) return `${hours}h`;
  return `${minutes}m`;
}

function formatEta(valueSeconds) {
  const seconds = Math.max(0, Math.round(Number(valueSeconds) || 0));
  if (seconds < 60) return `${seconds}s`;
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remSeconds = seconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${remSeconds}s`;
  return `${seconds}s`;
}

function computeLayoverSummaries(segments) {
  const layovers = [];
  for (let idx = 0; idx < segments.length - 1; idx += 1) {
    const current = segments[idx];
    const next = segments[idx + 1];
    const arrive = parseDateTime(current.arrive_local);
    const depart = parseDateTime(next.depart_local);
    const airport = current.to || "UNKNOWN";

    if (!arrive || !depart) {
      layovers.push(airport);
      continue;
    }

    const diffSeconds = Math.max(0, Math.round((depart.getTime() - arrive.getTime()) / 1000));
    const duration = formatDuration(diffSeconds);
    layovers.push(duration ? `${airport} ${duration}` : airport);
  }
  return layovers;
}

function formatSegmentsInline(segments) {
  return (segments || [])
    .map((segment) => {
      const departTime = formatTimeLabel(segment.depart_local);
      const arriveTime = formatTimeLabel(segment.arrive_local);
      const depart = departTime ? `${segment.from} ${departTime}` : segment.from;
      const arrive = arriveTime ? `${segment.to} ${arriveTime}` : segment.to;
      return `${depart} -> ${arrive}`;
    })
    .join(" | ");
}

function setStatus(message, type = "info") {
  statusEl.textContent = message;
  statusEl.dataset.type = type;
}

function setSearchButtonBusy(busy, label = "Search Flights") {
  if (!buttonEl) return;
  buttonEl.disabled = busy;
  buttonEl.textContent = label;
}

function setProgressVisible(visible) {
  if (!progressPanelEl) return;
  progressPanelEl.hidden = !visible;
}

function resetProgressDisplay() {
  setProgressVisible(false);
  progressEventCursor = 0;
  if (progressMetaEl) {
    progressMetaEl.textContent = "0% complete.";
  }
  if (progressPhaseEl) {
    progressPhaseEl.textContent = "Waiting to start.";
  }
  if (progressBarFillEl) {
    progressBarFillEl.style.width = "0%";
  }
  if (progressLogEl) {
    progressLogEl.innerHTML = "";
    delete progressLogEl.dataset.renderedCount;
  }
  if (providerHealthPanelEl) {
    providerHealthPanelEl.hidden = true;
  }
  if (providerHealthGridEl) {
    providerHealthGridEl.innerHTML = "";
  }
  if (providerHealthNoteEl) {
    providerHealthNoteEl.textContent = "Selected / blocked / no result / error counts update live.";
  }
}

function providerMetaById(providerId) {
  const normalized = String(providerId || "").trim().toLowerCase();
  return (
    providerCatalog.find((provider) => String(provider.id || "").toLowerCase() === normalized) || null
  );
}

function renderProviderHealth(snapshot) {
  if (!providerHealthPanelEl || !providerHealthGridEl) return;
  const health = snapshot?.provider_health || snapshot?.runtime_data?.provider_health || {};
  const audit = snapshot?.coverage_audit || snapshot?.runtime_data?.coverage_audit || {};
  const providers = health?.providers || {};
  const providerIds = providerCatalog.length
    ? providerCatalog
      .map((provider) => String(provider.id || "").toLowerCase())
      .filter((providerId) => providers[providerId])
    : Object.keys(providers);

  if (!providerIds.length) {
    providerHealthPanelEl.hidden = true;
    providerHealthGridEl.innerHTML = "";
    return;
  }

  providerHealthPanelEl.hidden = false;
  providerHealthGridEl.innerHTML = "";

  const budget = health?.budget || {};
  const auditDestinations = Array.isArray(audit?.destinations) ? audit.destinations : [];
  const auditNames = auditDestinations
    .map((entry) => String(entry?.destination || "").trim().toUpperCase())
    .filter(Boolean);
  let note = "Selected / blocked / no result / error counts update live.";
  if (budget.max_total_calls != null) {
    note += ` Paid-provider budget ${budget.used_total_calls || 0}/${budget.max_total_calls}.`;
  }
  if (auditNames.length) {
    note += ` Coverage audit: ${auditNames.join(", ")}.`;
  }
  if (providerHealthNoteEl) {
    providerHealthNoteEl.textContent = note;
  }

  for (const providerId of providerIds) {
    const stats = providers[providerId] || {};
    const providerMeta = providerMetaById(providerId);
    const card = document.createElement("article");
    card.className = "provider-health-card";
    card.dataset.status = String(stats.status || "idle");

    const header = document.createElement("div");
    header.className = "provider-health-card-head";

    const name = document.createElement("strong");
    name.className = "provider-health-name";
    name.textContent = providerMeta?.name || providerId;

    const status = document.createElement("span");
    status.className = "provider-health-status";
    status.textContent = String(stats.status || "idle").replace(/_/g, " ");

    header.append(name, status);

    const counts = document.createElement("p");
    counts.className = "provider-health-counts";
    counts.textContent =
      `selected ${stats.selected || 0} | ` +
      `blocked ${stats.blocked || 0} | ` +
      `no result ${stats.no_result || 0} | ` +
      `error ${stats.errors || 0}`;

    const detail = document.createElement("p");
    detail.className = "provider-health-detail";
    const detailParts = [
      `calls ${stats.calls || 0}`,
      `calendar ${stats.calendar_selected || 0}`,
      `one-way ${stats.oneway_selected || 0}`,
      `return ${stats.return_selected || 0}`,
    ];
    if (stats.skipped_cooldown) {
      detailParts.push(`cooldown skips ${stats.skipped_cooldown}`);
    }
    if (stats.cooldown_seconds) {
      detailParts.push(`retry ~${stats.cooldown_seconds}s`);
    }
    detail.textContent = detailParts.join(" | ");

    card.append(header, counts, detail);

    const issueMessage = String(stats.last_issue_message || "").trim();
    if (issueMessage && ["blocked", "error"].includes(String(stats.status || ""))) {
      const issue = document.createElement("p");
      issue.className = "provider-health-issue";
      issue.textContent = issueMessage;
      card.appendChild(issue);
    }

    const manualSearchUrl = String(stats.manual_search_url || "").trim();
    if (manualSearchUrl) {
      const link = document.createElement("a");
      link.className = "provider-health-link";
      link.href = manualSearchUrl;
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      link.textContent =
        String(stats.status || "") === "blocked" ? "Open search manually" : "Open provider search";
      card.appendChild(link);
    }

    providerHealthGridEl.appendChild(card);
  }
}

function appendProgressLogRow(event) {
  if (!progressLogEl) return;
  const row = document.createElement("div");
  row.className = "progress-log-entry";

  const ts = document.createElement("span");
  ts.className = "progress-log-time";
  ts.textContent = new Date(event.timestamp || Date.now()).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const message = document.createElement("span");
  message.className = "progress-log-message";
  message.textContent = event.message || "";

  row.append(ts, message);
  progressLogEl.appendChild(row);
}

function renderProgressLog(payload) {
  if (!progressLogEl) return;
  const list = Array.isArray(payload?.events) ? payload.events : Array.isArray(payload) ? payload : [];
  const renderedCount = Number(progressLogEl.dataset.renderedCount || 0);
  const startIndex = Number(payload?.startIndex ?? payload?.events_start_index ?? 0);
  const nextIndex =
    Number(payload?.nextIndex ?? payload?.next_event_index) || startIndex + list.length;
  const shouldReset = renderedCount !== startIndex;
  const stickToBottom =
    shouldReset ||
    progressLogEl.scrollHeight - progressLogEl.clientHeight - progressLogEl.scrollTop < 24;

  if (shouldReset) {
    progressLogEl.innerHTML = "";
  }

  for (const event of list) {
    appendProgressLogRow(event);
  }

  progressLogEl.dataset.renderedCount = String(nextIndex);
  if (stickToBottom) {
    progressLogEl.scrollTop = progressLogEl.scrollHeight;
  }
}

function renderProgress(snapshot) {
  if (!snapshot || !progressPanelEl) return;
  setProgressVisible(true);
  const percent = Math.max(0, Math.min(100, Number(snapshot.progress_percent) || 0));
  const roundedPercent = percent >= 10 ? Math.round(percent) : percent.toFixed(1);
  const etaLabel = snapshot.eta_seconds == null ? "ETA calculating..." : `ETA ${formatEta(snapshot.eta_seconds)}`;
  const elapsedLabel = `Elapsed ${formatEta(snapshot.elapsed_seconds)}`;
  const current = Number(snapshot.current) || 0;
  const total = Number(snapshot.total) || 0;
  const phaseLabel = snapshot.phase_label || "Working";
  const phaseDetail = snapshot.phase_detail || phaseLabel;

  if (progressMetaEl) {
    progressMetaEl.textContent = `${roundedPercent}% complete. ${etaLabel}. ${elapsedLabel}.`;
  }
  if (progressPhaseEl) {
    progressPhaseEl.textContent =
      total > 0 ? `${phaseDetail} (${current}/${total})` : phaseDetail;
  }
  if (progressBarFillEl) {
    progressBarFillEl.style.width = `${percent}%`;
  }
  renderProviderHealth(snapshot);
  renderProgressLog({
    events: snapshot.events,
    startIndex: snapshot.events_start_index,
    nextIndex: snapshot.next_event_index,
  });
  progressEventCursor = Number(snapshot.next_event_index) || progressEventCursor;

  if (snapshot.status === "running" || snapshot.status === "queued") {
    setStatus(`Searching: ${roundedPercent}% - ${phaseLabel}. ${etaLabel}.`);
    setSearchButtonBusy(true, `Searching... ${Math.round(percent)}%`);
  }
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
}

async function pollSearchJob(jobId) {
  while (true) {
    const response = await fetch(
      `/api/search-jobs/${encodeURIComponent(jobId)}?since_event_index=${encodeURIComponent(progressEventCursor)}`,
      {
      cache: "no-store",
      },
    );
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.error || "Search job lookup failed");
    }
    if (payload.progress) {
      renderProgress(payload.progress);
    }
    if (payload.status === "completed") {
      return payload.result;
    }
    if (payload.status === "failed") {
      throw new Error(payload.error || payload.progress?.error || "Search failed");
    }
    await sleep(900);
  }
}

function cloneJson(value) {
  if (value == null) return null;
  return JSON.parse(JSON.stringify(value));
}

function hasExportData() {
  return Boolean(lastSearchPayload && lastSearchResponse);
}

function setExportEnabled(enabled) {
  if (exportBtn) {
    exportBtn.disabled = !enabled;
  }
  if (exportPdfBtn) {
    exportPdfBtn.disabled = !enabled;
  }
}

function buildExportDocument() {
  if (!hasExportData()) return null;
  return {
    export_version: 1,
    exported_at_utc: new Date().toISOString(),
    source: "FlightFinder Engine",
    search_criteria: cloneJson(lastSearchPayload),
    summary_text: summaryEl.textContent || "",
    meta: cloneJson(lastSearchResponse?.meta || {}),
    warnings: cloneJson(lastSearchResponse?.warnings || []),
    results: cloneJson(lastSearchResponse?.results || []),
    notes: [
      "Provider booking links are in results[].legs[].booking_url and result-level booking_url fields when present.",
      "Comparison links are in results[].comparison_links.",
    ],
  };
}

function buildExportFilename() {
  const payload = lastSearchPayload || {};
  const destinations = (payload.destinations || []).slice(0, 3).join("-");
  const periodStart = String(payload.period_start || "").replace(/[^0-9-]/g, "");
  const periodEnd = String(payload.period_end || "").replace(/[^0-9-]/g, "");
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const destPart = destinations || "results";
  const periodPart = periodStart && periodEnd ? `${periodStart}_to_${periodEnd}` : "period";
  return `flight-search-${destPart}-${periodPart}-${stamp}.json`;
}

function downloadTextFile(filename, text, mimeType) {
  const blob = new Blob([text], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 3000);
}

function exportLastSearch() {
  const documentPayload = buildExportDocument();
  if (!documentPayload) {
    setStatus("No completed search to export yet.", "warn");
    return;
  }
  const filename = buildExportFilename();
  const json = `${JSON.stringify(documentPayload, null, 2)}\n`;
  downloadTextFile(filename, json, "application/json;charset=utf-8");
  setStatus(`Exported ${filename}`, "ok");
}

function escapeHtml(value) {
  const raw = String(value ?? "");
  return raw
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function buildPdfReportHtml() {
  const payload = lastSearchPayload || {};
  const response = lastSearchResponse || {};
  const activeProviders = response.meta?.engine?.providers_active || [];
  const usedProviders = response.meta?.engine?.providers_used || [];
  const enabledProviders = activeProviders.length > 0 ? activeProviders : payload.providers || ["kiwi"];
  const grouped = groupResultsByDestination(
    response.results || [],
    payload.destinations || [],
    payload.currency || "RON",
  );
  const generatedAt = new Date().toLocaleString();
  const maxConnLayover =
    payload.max_connection_layover_hours && Number(payload.max_connection_layover_hours) > 0
      ? `${payload.max_connection_layover_hours}h`
      : "No cap";
  const criteria = [
    ["Origins", (payload.origins || []).join(", ") || "N/A"],
    ["Destinations", (payload.destinations || []).join(", ") || "N/A"],
    ["Period", `${payload.period_start || "N/A"} -> ${payload.period_end || "N/A"}`],
    ["Main stay", `${payload.min_stay_days ?? "?"}-${payload.max_stay_days ?? "?"} day(s)`],
    ["Stopover stay", `${payload.min_stopover_days ?? "?"}-${payload.max_stopover_days ?? "?"} day(s)`],
    ["Currency", payload.currency || "RON"],
    ["Rank by", objectiveLabels[payload.objective] || payload.objective || "best"],
    ["Max transfers/direction", payload.max_transfers_per_direction ?? "N/A"],
    ["Max connection layover", maxConnLayover],
    [
      "Baggage profile",
      `${payload.passengers?.adults || 1} adult(s), cabin ${payload.passengers?.hand_bags || 0}, hold ${
        payload.passengers?.hold_bags || 0
      }`,
    ],
    ["Providers (enabled)", enabledProviders.join(", ") || "kiwi"],
    ["Providers (used in results)", usedProviders.join(", ") || enabledProviders.join(", ") || "kiwi"],
  ];

  const criteriaRows = criteria
    .map(
      ([label, value]) =>
        `<tr><th>${escapeHtml(label)}</th><td>${escapeHtml(value)}</td></tr>`,
    )
    .join("");

  const destinationSections = grouped
    .map((group) => {
      const bestPrice = group.bestItem
        ? formatMoney(group.bestItem.total_price, group.currency)
        : "No valid itinerary";
      if (!group.items.length) {
        return `
          <section class="destination-section">
            <h2>${escapeHtml(group.name)} (${escapeHtml(group.code)})</h2>
            <p class="section-subtitle">Best option: ${escapeHtml(bestPrice)}</p>
            <div class="empty-box">No valid itineraries for current constraints in this search run.</div>
          </section>
        `;
      }

      const cards = group.items
        .map((item, index) => {
          const compare = item.comparison_links || {};
          const compareOrder = [
            ["google_flights", "Google Flights"],
            ["skyscanner", "Skyscanner"],
            ["kayak", "Kayak"],
            ["momondo", "Momondo"],
            ["kiwi_search", "Kiwi"],
          ];
          const compareLinks = compareOrder
            .filter(([key]) => compare[key])
            .map(
              ([key, label]) =>
                `<a href="${escapeHtml(compare[key])}">${escapeHtml(label)}</a>`,
            )
            .join(" | ");
          const wholeTripUrl = resolveWholeTripUrl(item, item.legs || [], compare);
          const wholeTripLabel = bookingCtaLabel(item.provider, wholeTripUrl, true);
          const adults = Number.parseInt(
            item.passengers_adults ?? payload.passengers?.adults ?? 1,
            10,
          );
          const safeAdults = Number.isFinite(adults) && adults > 0 ? adults : 1;
          const perAdultValue =
            safeAdults > 1 && Number.isFinite(Number(item.total_price))
              ? ` (~${formatMoney(Math.round((Number(item.total_price) / safeAdults) * 100) / 100, item.currency)} / adult)`
              : "";
          const priceModes = Array.isArray(item.price_modes)
            ? item.price_modes.map((mode) => formatPriceModeLabel(mode)).filter(Boolean)
            : [];
          const priceModeLine = priceModes.length > 0
            ? `Price basis: ${priceModes.join(", ")}`
            : "";

          const legsHtml = (item.legs || [])
            .map((leg) => {
              const isRoundtripTicket = String(leg.ticket_type || "").toLowerCase() === "roundtrip";
              const layovers = isRoundtripTicket
                ? [
                    `Out: ${computeLayoverSummaries(leg.outbound_segments || []).join(", ") || "-"}`,
                    `In: ${computeLayoverSummaries(leg.inbound_segments || []).join(", ") || "-"}`,
                  ].join(" | ")
                : computeLayoverSummaries(leg.segments || []).join(", ") || "-";
              const duration = isRoundtripTicket
                ? `Out ${formatDuration(leg.outbound_duration_seconds) || "N/A"} / In ${
                    formatDuration(leg.inbound_duration_seconds) || "N/A"
                  }`
                : formatDuration(leg.duration_seconds) || "N/A";
              const inboundSegments = leg.inbound_segments || [];
              const lastInboundSegment =
                inboundSegments.length > 0 ? inboundSegments[inboundSegments.length - 1] : null;
              const dep = isRoundtripTicket
                ? `${leg.outbound_segments?.[0]?.depart_local ? formatDateTimeLabel(leg.outbound_segments[0].depart_local) : "N/A"}`
                : leg.departure_local
                  ? formatDateTimeLabel(leg.departure_local)
                  : "N/A";
              const arr = isRoundtripTicket
                ? `${lastInboundSegment?.arrive_local ? formatDateTimeLabel(lastInboundSegment.arrive_local) : "N/A"}`
                : leg.arrival_local
                  ? formatDateTimeLabel(leg.arrival_local)
                  : "N/A";
              const dateLabel = isRoundtripTicket
                ? `${formatDateLabel(leg.date)} -> ${formatDateLabel(leg.return_date || leg.date)}`
                : formatDateLabel(leg.date);
              const stopLabel = isRoundtripTicket
                ? `out ${leg.outbound_stops ?? 0} / in ${leg.inbound_stops ?? 0}`
                : String(leg.stops);
              const legLabel = isRoundtripTicket
                ? `${leg.source} <-> ${leg.destination}`
                : `${leg.source} -> ${leg.destination}`;
              const linkLabel = bookingCtaLabel(leg.provider, leg.booking_url || "", false);
              return `
                <tr>
                  <td>${escapeHtml(legLabel)}</td>
                  <td>${escapeHtml(dateLabel)}</td>
                  <td>${escapeHtml(stopLabel)}</td>
                  <td>${escapeHtml(String(leg.provider || "").toUpperCase())}</td>
                  <td>${escapeHtml(duration)}</td>
                  <td>${escapeHtml(dep)} -> ${escapeHtml(arr)}</td>
                  <td>${escapeHtml(layovers)}</td>
                  <td>${
                    leg.booking_url
                      ? `<a href="${escapeHtml(leg.booking_url)}">${escapeHtml(linkLabel)}</a>`
                      : "-"
                  }</td>
                </tr>
              `;
            })
            .join("");

          return `
            <article class="itinerary-card">
              <div class="itinerary-head">
                <h3>Option ${index + 1}: ${escapeHtml(formatMoney(item.total_price, item.currency))}</h3>
                <p>${escapeHtml(item.destination_code)} | ${escapeHtml(item.itinerary_type || "itinerary")} | ${
                  escapeHtml(item.objective || payload.objective || "best")
                }</p>
              </div>
              <p class="route-line">
                Outbound: ${escapeHtml(item.outbound?.origin || "N/A")} -> ${escapeHtml(item.destination_code || "N/A")}
                (${escapeHtml(formatDateLabel(item.outbound?.date_from_origin || ""))}) |
                Inbound: ${escapeHtml(item.destination_code || "N/A")} -> ${escapeHtml(item.inbound?.arrival_origin || "N/A")}
                (${escapeHtml(formatDateLabel(item.inbound?.date_from_destination || ""))})
              </p>
              <p class="route-line">
                Main stay: ${escapeHtml(String(item.main_destination_stay_days || 0))} day(s) |
                Transfers/direction: out ${escapeHtml(String(item.outbound?.layovers_count || 0))}, in ${escapeHtml(
                  String(item.inbound?.layovers_count || 0),
                )}
              </p>
              <p class="route-line">
                Price shown is total for ${escapeHtml(String(safeAdults))} adult(s)${escapeHtml(perAdultValue)}.
                ${escapeHtml(priceModeLine)}
              </p>
              <table>
                <thead>
                  <tr>
                    <th>Leg</th>
                    <th>Date</th>
                    <th>Stops</th>
                    <th>Provider</th>
                    <th>Duration</th>
                    <th>Departure -> Arrival</th>
                    <th>Layovers</th>
                    <th>Link</th>
                  </tr>
                </thead>
                <tbody>${legsHtml}</tbody>
              </table>
              <p class="links">
                ${
                  wholeTripUrl
                    ? `<a href="${escapeHtml(wholeTripUrl)}">${escapeHtml(wholeTripLabel)}</a>`
                    : ""
                }
                ${compareLinks ? ` | Compare: ${compareLinks}` : ""}
              </p>
            </article>
          `;
        })
        .join("");

      return `
        <section class="destination-section">
          <h2>${escapeHtml(group.name)} (${escapeHtml(group.code)})</h2>
          <p class="section-subtitle">Best option: ${escapeHtml(bestPrice)} | ${group.items.length} option(s)</p>
          ${cards}
        </section>
      `;
    })
    .join("");

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Flight Search Report</title>
    <style>
      :root { color-scheme: light; }
      body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #102235; margin: 24px; }
      h1 { margin: 0 0 8px; font-size: 24px; }
      h2 { margin: 0 0 6px; font-size: 20px; color: #0f3f67; }
      h3 { margin: 0; font-size: 16px; color: #0d3e63; }
      .subtitle { margin: 0 0 14px; color: #496178; }
      .meta-grid { width: 100%; border-collapse: collapse; margin-bottom: 18px; }
      .meta-grid th, .meta-grid td { border: 1px solid #d2dfeb; text-align: left; padding: 8px; font-size: 13px; }
      .meta-grid th { width: 230px; background: #f1f7fd; }
      .destination-section { page-break-inside: avoid; margin-bottom: 20px; }
      .section-subtitle { margin: 0 0 10px; color: #4f667e; font-size: 13px; }
      .itinerary-card { border: 1px solid #d9e4ef; border-radius: 10px; padding: 10px; margin-bottom: 10px; page-break-inside: avoid; }
      .itinerary-head p, .route-line { margin: 5px 0; font-size: 12px; color: #3f566c; }
      table { width: 100%; border-collapse: collapse; margin-top: 8px; }
      th, td { border: 1px solid #d7e2ee; padding: 5px; font-size: 11px; vertical-align: top; }
      th { background: #f6fbff; }
      a { color: #0b5da9; text-decoration: none; }
      .links { margin: 8px 0 0; font-size: 12px; color: #3d576f; }
      .empty-box { border: 1px dashed #c0d2e3; padding: 10px; border-radius: 8px; color: #4d647a; background: #f7fbff; font-size: 13px; }
      @media print { body { margin: 12mm; } }
    </style>
  </head>
  <body>
    <h1>Flight Search Report</h1>
    <p class="subtitle">Generated ${escapeHtml(generatedAt)} | ${
    escapeHtml(`${response.meta?.results_count || 0}`)
  } total result(s)</p>
    <table class="meta-grid"><tbody>${criteriaRows}</tbody></table>
    ${destinationSections}
  </body>
</html>`;
}

function exportLastSearchPdf() {
  if (!hasExportData()) {
    setStatus("No completed search to export yet.", "warn");
    return;
  }
  const popup = window.open("", "_blank");
  if (!popup) {
    setStatus("Popup blocked. Allow popups to export PDF report.", "error");
    return;
  }
  popup.document.open();
  popup.document.write(buildPdfReportHtml());
  popup.document.close();
  popup.focus();
  setTimeout(() => {
    popup.print();
  }, 250);
  setStatus("Opened printable report. Choose 'Save as PDF' in print dialog.", "ok");
}

function storefrontFromUrl(url) {
  if (!url) return "";
  try {
    const parsed = new URL(String(url));
    const host = parsed.hostname.toLowerCase();
    const path = parsed.pathname.toLowerCase();
    if (host.includes("kiwi.com")) return "Kiwi";
    if (host.includes("google.") && path.includes("/travel/flights")) return "Google Flights";
    if (host.includes("skyscanner.")) return "Skyscanner";
    if (host.includes("kayak.")) return "Kayak";
    if (host.includes("momondo.")) return "Momondo";
    return "";
  } catch (_error) {
    return "";
  }
}

function defaultStorefrontForProvider(provider) {
  const id = String(provider || "").trim().toLowerCase();
  if (id === "kiwi") return "Kiwi";
  if (id === "kayak") return "Kayak";
  if (id === "momondo") return "Momondo";
  if (id === "googleflights") return "Google Flights";
  if (id === "skyscanner") return "Skyscanner";
  if (id === "serpapi") return "Google Flights";
  if (id === "amadeus") return "Google Flights";
  return id ? id.toUpperCase() : "Provider";
}

function bookingCtaLabel(provider, bookingUrl, wholeTrip = false) {
  const id = String(provider || "").trim().toLowerCase();
  const storefront = storefrontFromUrl(bookingUrl) || defaultStorefrontForProvider(id);
  const prefix = wholeTrip ? "Open whole trip on" : "Open on";
  if (id === "amadeus" && storefront !== "Amadeus") {
    return `${prefix} ${storefront} (Amadeus-matched route)`;
  }
  return `${prefix} ${storefront}`;
}

function resolveWholeTripUrl(item, legs, comparisonLinks) {
  const providerId = String(item?.provider || legs?.[0]?.provider || "").toLowerCase();
  const legUrl = (legs || []).find((leg) => leg?.booking_url)?.booking_url || "";
  const googleUrl = comparisonLinks?.google_flights || "";
  const kiwiUrl = comparisonLinks?.kiwi_search || "";
  const skyscannerUrl = comparisonLinks?.skyscanner || "";
  const kayakUrl = comparisonLinks?.kayak || "";
  const momondoUrl = comparisonLinks?.momondo || "";

  if (providerId === "amadeus") {
    return googleUrl || kayakUrl || skyscannerUrl || momondoUrl || legUrl || kiwiUrl;
  }
  if (providerId === "serpapi") {
    return legUrl || googleUrl || kiwiUrl || kayakUrl || skyscannerUrl || momondoUrl;
  }
  if (providerId === "googleflights") {
    return legUrl || googleUrl || skyscannerUrl || kayakUrl || momondoUrl || kiwiUrl;
  }
  if (providerId === "skyscanner") {
    return legUrl || skyscannerUrl || googleUrl || kayakUrl || momondoUrl || kiwiUrl;
  }
  if (providerId === "kiwi") {
    return legUrl || kiwiUrl || googleUrl || skyscannerUrl || kayakUrl || momondoUrl;
  }
  if (providerId === "kayak") {
    return legUrl || kayakUrl || googleUrl || skyscannerUrl || kiwiUrl || momondoUrl;
  }
  if (providerId === "momondo") {
    return legUrl || momondoUrl || googleUrl || skyscannerUrl || kayakUrl || kiwiUrl;
  }
  return legUrl || kiwiUrl || googleUrl || skyscannerUrl || kayakUrl || momondoUrl;
}

function renderLeg(leg, options = {}) {
  const showLink = options.showLink !== false;
  const bookingUrl = leg.booking_url || options.fallbackUrl || "https://www.kiwi.com/en/search/results/";
  const linkLabel = options.linkLabel || bookingCtaLabel(leg.provider, bookingUrl, options.wholeTrip === true);
  const node = legTemplate.content.firstElementChild.cloneNode(true);
  const isRoundtripTicket = String(leg.ticket_type || "").toLowerCase() === "roundtrip";

  if (isRoundtripTicket) {
    const route =
      `${leg.source} <-> ${leg.destination} (` +
      `${formatDateLabel(leg.date)} -> ${formatDateLabel(leg.return_date || leg.date)})`;
    node.querySelector(".leg-title").textContent = route;
    node.querySelector(".leg-price").textContent = leg.formatted_price || `${leg.price}`;

    const metaBits = ["Round-trip ticket"];
    metaBits.push(`Outbound ${leg.outbound_stops ?? 0} stop(s)`);
    metaBits.push(`Inbound ${leg.inbound_stops ?? 0} stop(s)`);
    if (leg.provider) {
      metaBits.push(`Provider ${String(leg.provider).toUpperCase()}`);
    }
    const outboundDuration = formatDuration(leg.outbound_duration_seconds);
    const inboundDuration = formatDuration(leg.inbound_duration_seconds);
    if (outboundDuration || inboundDuration) {
      metaBits.push(`Duration out ${outboundDuration || "N/A"} / in ${inboundDuration || "N/A"}`);
    }
    node.querySelector(".leg-meta").textContent = metaBits.join(" | ");

    const outboundSegments = leg.outbound_segments || [];
    const inboundSegments = leg.inbound_segments || [];
    const outboundLayovers = computeLayoverSummaries(outboundSegments);
    const inboundLayovers = computeLayoverSummaries(inboundSegments);
    const legDetails = [
      `Outbound: ${formatSegmentsInline(outboundSegments) || "No outbound segment details"}`,
      `Inbound: ${formatSegmentsInline(inboundSegments) || "No inbound segment details"}`,
    ];
    if (outboundLayovers.length > 0) {
      legDetails.push(`Outbound layovers: ${outboundLayovers.join(", ")}`);
    }
    if (inboundLayovers.length > 0) {
      legDetails.push(`Inbound layovers: ${inboundLayovers.join(", ")}`);
    }
    node.querySelector(".leg-segments").textContent = legDetails.join("\n");
  } else {
    const route = `${leg.source} -> ${leg.destination} (${formatDateLabel(leg.date)})`;
    node.querySelector(".leg-title").textContent = route;
    node.querySelector(".leg-price").textContent = leg.formatted_price || `${leg.price}`;

    const metaBits = [`${leg.stops} stop(s)`];
    if (leg.provider) {
      metaBits.push(`Provider ${String(leg.provider).toUpperCase()}`);
    }
    const legDuration = formatDuration(leg.duration_seconds);
    if (legDuration) {
      metaBits.push(`Duration ${legDuration}`);
    }
    const departureLabel = leg.departure_local ? formatDateTimeLabel(leg.departure_local) : "";
    const arrivalLabel = leg.arrival_local ? formatDateTimeLabel(leg.arrival_local) : "";
    if (departureLabel || arrivalLabel) {
      metaBits.push(`${departureLabel || "N/A"} -> ${arrivalLabel || "N/A"}`);
    }
    node.querySelector(".leg-meta").textContent = metaBits.join(" | ");

    const segments = leg.segments || [];
    const segmentText = formatSegmentsInline(segments);
    const layovers = computeLayoverSummaries(segments);
    const legDetails = [segmentText || "No segment details"];
    if (layovers.length > 0) {
      legDetails.push(`Layovers: ${layovers.join(", ")}`);
    }
    node.querySelector(".leg-segments").textContent = legDetails.join("\n");
  }

  const link = node.querySelector(".leg-link");
  if (!showLink) {
    link.remove();
  } else {
    link.href = bookingUrl;
    link.textContent = linkLabel;
  }
  return node;
}

function renderResultCard(item) {
  const node = cardTemplate.content.firstElementChild.cloneNode(true);
  const itineraryType = item.itinerary_type || "split_stopover";

  node.querySelector(".result-destination").textContent =
    `${item.destination_name} (${item.destination_code})`;
  node.querySelector(".result-note").textContent = item.destination_note || "";

  node.querySelector(".result-total").textContent =
    formatMoney(item.total_price, item.currency);

  const outboundHours = item.outbound_time_to_destination_seconds == null
    ? "N/A"
    : `${Math.round((item.outbound_time_to_destination_seconds / 3600) * 10) / 10}h`;

  let score = "";
  if (item.objective === "best") {
    score = `Best value score: ${item.best_value_score ?? "N/A"} (lower is better)`;
  } else if (item.objective === "cheapest") {
    score = "Cheapest ranking by total trip price.";
  } else if (item.objective === "fastest") {
    score = `Fastest ranking by outbound travel time: ${outboundHours}.`;
  } else {
    score = item.price_per_1000_km == null
      ? "Distance unavailable"
      : `${item.price_per_1000_km} ${item.currency} / 1000 km (direct origin -> destination)`;
  }
  node.querySelector(".result-score").textContent = score;

  const outboundTransfers = item.outbound?.transfer_airports || [];
  const inboundTransfers = item.inbound?.transfer_airports || [];
  let outboundLine = "";
  let inboundLine = "";

  if (itineraryType === "direct_roundtrip") {
    const outVia = outboundTransfers.length ? ` via ${outboundTransfers.join("/")}` : "";
    const inVia = inboundTransfers.length ? ` via ${inboundTransfers.join("/")}` : "";
    outboundLine =
      `Outbound: ${item.outbound.origin} -> ${item.destination_code} ` +
      `(${formatDateLabel(item.outbound.date_from_origin)})${outVia}.`;
    inboundLine =
      `Inbound: ${item.destination_code} -> ${item.inbound.arrival_origin} ` +
      `(${formatDateLabel(item.inbound.date_from_destination)})${inVia}.`;
  } else {
    outboundLine =
      `Outbound: ${item.outbound.origin} -> ${item.outbound.hub} ` +
      `(${formatDateLabel(item.outbound.date_from_origin)}), stopover ${item.outbound.stopover_days} day(s)` +
      ` (~${item.outbound.stopover_days * 24}h), then ${item.outbound.hub} -> ${item.destination_code} ` +
      `(${formatDateLabel(item.outbound.date_to_destination)}).`;

    inboundLine =
      `Inbound: ${item.destination_code} -> ${item.inbound.hub} ` +
      `(${formatDateLabel(item.inbound.date_from_destination)}), stopover ${item.inbound.stopover_days} day(s)` +
      ` (~${item.inbound.stopover_days * 24}h), then ${item.inbound.hub} -> ${item.inbound.arrival_origin} ` +
      `(${formatDateLabel(item.inbound.date_to_origin)}).`;
  }

  const stayLine =
    `Main destination stay: ${item.main_destination_stay_days} day(s) (~${item.main_destination_stay_days * 24}h). ` +
    `Layovers per direction: outbound ${item.outbound.layovers_count}, inbound ${item.inbound.layovers_count}. ` +
    `Time to destination: ${outboundHours}. ` +
    `Distance basis: ${item.distance_km || "N/A"} km direct.`;

  const usesBaseFare =
    item.fare_mode === "base_no_bags" ||
    item.outbound?.fare_mode === "base_no_bags" ||
    item.inbound?.fare_mode === "base_no_bags" ||
    (item.legs || []).some((leg) => leg.fare_mode === "base_no_bags");
  const fareModeLine = usesBaseFare
    ? "Fare mode: market base fare (no extra bags) selected for lower price."
    : "Fare mode: selected baggage profile.";
  const fareProviders = Array.from(
    new Set(
      [
        item.provider,
        item.outbound?.provider,
        item.inbound?.provider,
        ...(item.legs || []).map((leg) => leg.provider),
      ].filter(Boolean),
    ),
  );
  const providerLine = fareProviders.length > 0
    ? `Fare provider(s): ${fareProviders.join(", ")}.`
    : "Fare provider(s): unknown.";
  const adults = Number.parseInt(
    item.passengers_adults ?? lastSearchPayload?.passengers?.adults ?? 1,
    10,
  );
  const safeAdults = Number.isFinite(adults) && adults > 0 ? adults : 1;
  const perAdultLine =
    safeAdults > 1 && Number.isFinite(Number(item.total_price))
      ? `Price shown is total for ${safeAdults} adults (~${formatMoney(
          Math.round((Number(item.total_price) / safeAdults) * 100) / 100,
          item.currency,
        )} / adult).`
      : `Price shown is total for ${safeAdults} adult.`;
  const priceModes = Array.isArray(item.price_modes)
    ? item.price_modes.map((mode) => formatPriceModeLabel(mode)).filter(Boolean)
    : [];
  const priceModeLine = priceModes.length > 0
    ? `Provider price basis: ${priceModes.join(", ")}.`
    : "";
  const pricingStrategyLine = item.pricing_strategy_note
    ? `Pricing strategy: ${item.pricing_strategy_note}`
    : "";

  node.querySelector(".route-meta").textContent = [
    outboundLine,
    inboundLine,
    stayLine,
    fareModeLine,
    providerLine,
    perAdultLine,
    priceModeLine,
    pricingStrategyLine,
  ]
    .filter(Boolean)
    .join("\n");

  const comparisonLinks = item.comparison_links || {};
  const compareOrder = [
    ["google_flights", "Google Flights"],
    ["skyscanner", "Skyscanner"],
    ["kayak", "Kayak"],
    ["momondo", "Momondo"],
  ];
  const compareItems = compareOrder.filter(([key]) => comparisonLinks[key]);
  if (compareItems.length > 0) {
    const compareLine = document.createElement("p");
    compareLine.className = "compare-links";
    compareLine.append("Compare on: ");
    compareItems.forEach(([key, label], index) => {
      const anchor = document.createElement("a");
      anchor.href = comparisonLinks[key];
      anchor.target = "_blank";
      anchor.rel = "noopener noreferrer";
      anchor.textContent = label;
      compareLine.appendChild(anchor);
      if (index < compareItems.length - 1) {
        compareLine.append(" | ");
      }
    });
    node.querySelector(".route-meta").insertAdjacentElement("afterend", compareLine);
  }

  const legsNode = node.querySelector(".legs");
  const legs = item.legs || [];
  const wholeTripUrl = resolveWholeTripUrl(item, legs, comparisonLinks);
  const sharedBooking =
    itineraryType === "direct_roundtrip" &&
    legs.length > 1 &&
    Boolean(wholeTripUrl);
  const wholeTripProvider = item.provider || legs[0]?.provider;
  const wholeTripLabel = bookingCtaLabel(wholeTripProvider, wholeTripUrl, true);

  for (const [index, leg] of legs.entries()) {
    const legWithLink = { ...leg };
    if (sharedBooking && index === 0) {
      legWithLink.booking_url = wholeTripUrl;
    }
    legsNode.appendChild(
      renderLeg(legWithLink, {
        showLink: !sharedBooking || index === 0,
        linkLabel: sharedBooking ? wholeTripLabel : undefined,
        wholeTrip: sharedBooking && index === 0,
        fallbackUrl: comparisonLinks.kiwi_search || comparisonLinks.google_flights,
      }),
    );
  }

  const riskList = node.querySelector(".risk-list");
  for (const note of item.risk_notes || []) {
    const li = document.createElement("li");
    li.textContent = note;
    riskList.appendChild(li);
  }

  return node;
}

function groupResultsByDestination(results, requestedDestinations = [], fallbackCurrency = "RON") {
  const grouped = new Map();
  const orderedCodes = [];

  for (const code of requestedDestinations || []) {
    const normalized = String(code || "").trim().toUpperCase();
    if (!normalized || grouped.has(normalized)) continue;
    grouped.set(normalized, {
      code: normalized,
      name: destinationNameCatalog.get(normalized) || normalized,
      currency: fallbackCurrency || "RON",
      bestItem: null,
      items: [],
    });
    orderedCodes.push(normalized);
  }

  for (const item of results || []) {
    const code = String(item.destination_code || "UNKNOWN").toUpperCase();
    if (!grouped.has(code)) {
      grouped.set(code, {
        code,
        name: item.destination_name || destinationNameCatalog.get(code) || code,
        currency: item.currency || fallbackCurrency || "RON",
        bestItem: null,
        items: [],
      });
      orderedCodes.push(code);
    }
    const entry = grouped.get(code);
    entry.items.push(item);
    entry.name = entry.name || item.destination_name || code;
    entry.currency = item.currency || entry.currency || fallbackCurrency || "RON";
    if (!entry.bestItem || Number(item.total_price) < Number(entry.bestItem.total_price)) {
      entry.bestItem = item;
    }
  }

  return orderedCodes.map((code) => grouped.get(code)).filter(Boolean);
}

function sortItemsByMode(items, mode) {
  const ranked = [...(items || [])];
  if (mode === "price") {
    ranked.sort((a, b) => {
      const aPrice = Number(a?.total_price ?? Number.POSITIVE_INFINITY);
      const bPrice = Number(b?.total_price ?? Number.POSITIVE_INFINITY);
      if (aPrice !== bPrice) return aPrice - bPrice;
      const aTime = Number(a?.outbound_time_to_destination_seconds ?? Number.POSITIVE_INFINITY);
      const bTime = Number(b?.outbound_time_to_destination_seconds ?? Number.POSITIVE_INFINITY);
      return aTime - bTime;
    });
    return ranked;
  }
  if (mode === "travel_time") {
    ranked.sort((a, b) => {
      const aTime = Number(a?.outbound_time_to_destination_seconds ?? Number.POSITIVE_INFINITY);
      const bTime = Number(b?.outbound_time_to_destination_seconds ?? Number.POSITIVE_INFINITY);
      if (aTime !== bTime) return aTime - bTime;
      const aPrice = Number(a?.total_price ?? Number.POSITIVE_INFINITY);
      const bPrice = Number(b?.total_price ?? Number.POSITIVE_INFINITY);
      return aPrice - bPrice;
    });
    return ranked;
  }
  return ranked;
}

function renderDestinationGroup(group, open = false) {
  const details = document.createElement("details");
  details.className = "destination-group";
  details.open = open;

  const summary = document.createElement("summary");
  summary.className = "destination-group-summary";

  const left = document.createElement("div");
  left.className = "destination-group-left";
  const title = document.createElement("span");
  title.className = "destination-group-title";
  title.textContent = `${group.name} (${group.code})`;
  const subtitle = document.createElement("span");
  subtitle.className = "destination-group-subtitle";
  subtitle.textContent = `${group.items.length} option(s) in current results`;
  left.append(title, subtitle);

  const right = document.createElement("div");
  right.className = "destination-group-right";
  const bestLabel = document.createElement("span");
  bestLabel.className = "destination-group-best-label";
  bestLabel.textContent = "Best option";
  const bestValue = document.createElement("span");
  bestValue.className = "destination-group-best-price";
  bestValue.textContent = group.bestItem
    ? formatMoney(group.bestItem.total_price, group.currency)
    : "No valid itinerary";
  right.append(bestLabel, bestValue);

  summary.append(left, right);
  details.appendChild(summary);

  const body = document.createElement("div");
  body.className = "destination-group-body";
  if (group.items.length > 0) {
    const controls = document.createElement("div");
    controls.className = "destination-group-controls";

    const sortLabel = document.createElement("label");
    sortLabel.className = "destination-sort-label";
    sortLabel.textContent = "Order results by";

    const sortSelect = document.createElement("select");
    sortSelect.className = "destination-sort-select";
    sortSelect.innerHTML = [
      '<option value="default">Engine ranking</option>',
      '<option value="price">Price (lowest first)</option>',
      '<option value="travel_time">Travel time (fastest first)</option>',
    ].join("");
    sortSelect.value = destinationSortModes.get(group.code) || "default";
    sortLabel.appendChild(sortSelect);
    controls.appendChild(sortLabel);
    body.appendChild(controls);

    const list = document.createElement("div");
    list.className = "destination-group-results";
    body.appendChild(list);

    const renderSortedCards = () => {
      const mode = sortSelect.value || "default";
      destinationSortModes.set(group.code, mode);
      list.innerHTML = "";
      const items = sortItemsByMode(group.items, mode);
      for (const item of items) {
        list.appendChild(renderResultCard(item));
      }
    };
    sortSelect.addEventListener("change", renderSortedCards);
    renderSortedCards();
  } else {
    const empty = document.createElement("div");
    empty.className = "destination-group-empty";
    const transferCap = lastSearchPayload?.max_transfers_per_direction;
    if (Number.isFinite(Number(transferCap))) {
      empty.textContent =
        `No valid itineraries for current constraints in this destination. ` +
        `Current max transfers/direction: ${transferCap}.`;
    } else {
      empty.textContent = "No valid itineraries for current constraints in this destination.";
    }
    body.appendChild(empty);
  }
  details.appendChild(body);

  return details;
}

function collectPayload() {
  const maxTransfers = asInt("max-transfers-direction");
  return {
    origins: parseCodes(document.getElementById("origins").value),
    destinations: parseCodes(document.getElementById("destinations").value),
    period_start: document.getElementById("period-start").value,
    period_end: document.getElementById("period-end").value,
    min_stay_days: asInt("min-stay"),
    max_stay_days: asInt("max-stay"),
    min_stopover_days: asInt("min-stopover"),
    max_stopover_days: asInt("max-stopover"),
    max_connection_layover_hours: asInt("max-connection-layover-hours"),
    max_transfers_per_direction: maxTransfers,
    // Backward-compatible keys consumed by older server builds:
    max_stops_per_leg: maxTransfers,
    max_layovers_per_direction: maxTransfers,
    currency: document.getElementById("currency").value,
    objective: document.getElementById("objective").value,
    market_compare_fares: document.getElementById("market-compare-fares").checked,
    top_results: asInt("top-results"),
    validate_top_per_destination: asInt("validate-top"),
    auto_hubs_per_direction: asInt("auto-hubs"),
    exhaustive_hub_scan: document.getElementById("exhaustive-hubs").checked,
    hub_candidates: parseCodes(document.getElementById("hub-candidates").value),
    io_workers: asInt("io-workers"),
    estimated_pool_multiplier: asInt("pool-multiplier"),
    calendar_hubs_prefetch: asInt("calendar-hubs-prefetch"),
    max_validate_oneway_keys_per_destination: asInt("max-validate-oneway-keys"),
    max_validate_return_keys_per_destination: asInt("max-validate-return-keys"),
    max_total_provider_calls: asInt("max-total-provider-calls"),
    max_calls_kiwi: asInt("max-calls-kiwi"),
    max_calls_amadeus: asInt("max-calls-amadeus"),
    max_calls_serpapi: asInt("max-calls-serpapi"),
    serpapi_probe_oneway_keys: asInt("serpapi-probe-oneway-keys"),
    serpapi_probe_return_keys: asInt("serpapi-probe-return-keys"),
    providers: getSelectedProviderIds(),
    passengers: {
      adults: asInt("adults"),
      hand_bags: asInt("hand-bags"),
      hold_bags: asInt("hold-bags"),
    },
  };
}

async function loadPresets() {
  try {
    const response = await fetch("/api/presets");
    if (!response.ok) {
      return;
    }
    const presets = await response.json();
    chipsEl.innerHTML = "";
    if (presets.auto_hub_candidates?.length) {
      const hubInput = document.getElementById("hub-candidates");
      if (!hubInput.value.trim()) {
        hubInput.value = presets.auto_hub_candidates.join(",");
      }
    }
    if (presets.providers?.length) {
      providerCatalog = presets.providers;
      if (!providersInputEl?.value.trim()) {
        const allIds = presets.providers
          .filter((provider) => provider?.default_enabled !== false)
          .map((provider) => String(provider.id || "").toLowerCase())
          .filter(Boolean);
        if (providersInputEl) {
          providersInputEl.value = allIds.join(",") || "kiwi";
        }
      }
      renderProviderPicker(parseProviderIds(providersInputEl?.value || ""));
    }
    await refreshProviderConfig();

    const currentCodes = new Set(parseCodes(document.getElementById("destinations").value));

    for (const item of presets.destinations || []) {
      destinationNameCatalog.set(String(item.code || "").toUpperCase(), item.name || item.code);
      const button = document.createElement("button");
      button.type = "button";
      button.className = "chip";
      button.textContent = `${item.code} - ${item.name}`;
      if (currentCodes.has(item.code)) {
        button.classList.add("active");
      }

      button.addEventListener("click", () => {
        const codes = new Set(parseCodes(document.getElementById("destinations").value));
        if (codes.has(item.code)) {
          codes.delete(item.code);
          button.classList.remove("active");
        } else {
          codes.add(item.code);
          button.classList.add("active");
        }
        document.getElementById("destinations").value = Array.from(codes).join(",");
      });

      chipsEl.appendChild(button);
    }
    syncAutoHubControls();
  } catch (_error) {
    // Presets are optional. Do not block search.
  }
}

async function runSearch(event) {
  event.preventDefault();
  const hadExportData = hasExportData();

  resultsEl.innerHTML = "";
  summaryEl.textContent = "";

  setSearchButtonBusy(true, "Preparing...");
  setExportEnabled(false);
  resetProgressDisplay();
  setProgressVisible(true);
  setStatus("Preparing search...");

  try {
    const payload = collectPayload();

    setSearchButtonBusy(true, "Searching...");
    setStatus("Queueing search...");

    const response = await fetch("/api/search-jobs", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.error || "Search failed");
    }
    if (job.progress) {
      renderProgress(job.progress);
    }
    const data =
      job.status === "completed" && job.result ? job.result : await pollSearchJob(job.job_id);

    const { meta, results, warnings } = data;
    for (const [code, name] of Object.entries(meta?.destination_display_names || {})) {
      const normalizedCode = String(code || "").trim().toUpperCase();
      const normalizedName = String(name || "").trim();
      if (!normalizedCode || !normalizedName) continue;
      destinationNameCatalog.set(normalizedCode, normalizedName);
    }
    lastSearchPayload = cloneJson(payload);
    lastSearchResponse = cloneJson(data);
    setExportEnabled(true);

    if (!results || results.length === 0) {
      setSearchButtonBusy(false);
      setStatus("No valid itineraries found for current constraints.", "warn");
      const hints = [];
      const engine = meta?.engine || {};
      const maxConnHours = engine.max_connection_layover_hours;
      const filteredByConnection = engine.filtered_by_connection_layover || 0;
      if (filteredByConnection > 0 && maxConnHours != null) {
        hints.push(
          `${filteredByConnection} itineraries were removed by max connection layover ${maxConnHours}h. ` +
          "Increase this cap or set 0 (no cap).",
        );
      }
      if (meta?.period_start && meta?.period_end) {
        hints.push(
          `Current period: ${meta.period_start} -> ${meta.period_end}. ` +
          `Current stay window: ${asInt("min-stay")}-${asInt("max-stay")} day(s).`,
        );
      }
      if (warnings && warnings.length > 0) {
        hints.push(`Engine notes: ${warnings.slice(-3).join(" | ")}`);
      }
      if (hints.length === 0) {
        hints.push("Try widening period, stay window, or layover constraints.");
      }
      summaryEl.textContent = hints.join(" ");
      const emptyGroups = groupResultsByDestination(
        [],
        payload.destinations || [],
        payload.currency || "RON",
      );
      if (emptyGroups.length > 0) {
        emptyGroups.forEach((group, index) => {
          resultsEl.appendChild(renderDestinationGroup(group, index === 0));
        });
      }
      return;
    }

    setStatus("Search complete.", "ok");

    const maxConnectionLayoverLabel = meta.engine?.max_connection_layover_hours == null
      ? "unlimited"
      : `${meta.engine.max_connection_layover_hours}h`;
    const cpuWorkersLabel = meta.engine?.cpu_workers_auto
      ? `${meta.engine.cpu_workers} (auto max / ${meta.engine.cpu_workers_available} available)`
      : `${meta.engine?.cpu_workers}`;
    const providersRequestedLabel = (meta.engine?.providers_requested || ["kiwi"]).join("/");
    const providersActiveLabel = (meta.engine?.providers_active || ["kiwi"]).join("/");
    const providersUsedLabel = (meta.engine?.providers_used || ["kiwi"]).join("/");
    const hubPoolLabel = meta.engine?.hub_candidates_graph_count
      ? `${meta.engine.hub_candidates_count} total (${meta.engine.hub_candidates_graph_count} graph-derived)`
      : `${meta.engine?.hub_candidates_count ?? "N/A"}`;
    const budgetMeta = meta.engine?.provider_stats?.budget || {};
    const budgetTotal = budgetMeta.max_total_calls == null
      ? "no cap"
      : `${budgetMeta.used_total_calls || 0}/${budgetMeta.max_total_calls}`;
    const marketCompareLabel = meta.engine?.market_compare_fares ? "ON" : "OFF";
    const engineInfo = meta.engine
      ? `Engine: IO workers ${meta.engine.io_workers}, CPU workers ${cpuWorkersLabel}, ` +
        `hub pool ${hubPoolLabel}, ` +
        `providers requested ${providersRequestedLabel}, active ${providersActiveLabel}, used ${providersUsedLabel}, ` +
        `provider API budget ${budgetTotal}, ` +
        `market compare ${marketCompareLabel}, ` +
        `exhaustive hubs ${meta.engine.exhaustive_hub_scan ? "ON" : "OFF"}, ` +
        `calendar hubs ${meta.engine.calendar_hubs_prefetched ?? "N/A"}, ` +
        `calendar routes ${meta.engine.calendar_routes_prefetched}, ` +
        `one-way legs ${meta.engine.oneway_legs_requested}, ` +
        `round-trip itineraries ${meta.engine.roundtrip_itineraries_requested ?? 0}, ` +
        `base-fare selections ${meta.engine.base_fare_selected_returns ?? 0} RT / ${meta.engine.base_fare_selected_oneways ?? 0} OW, ` +
        `max connection layover ${maxConnectionLayoverLabel}, ` +
        `filtered by connection cap ${meta.engine.filtered_by_connection_layover ?? 0}, ` +
        `long-stopover results ${meta.engine.long_stopover_results ?? 0}.`
      : "";
    const searchIdHint = meta.search_id
      ? ` Search id: ${meta.search_id} (debug: logs/engine.log).`
      : "";

    summaryEl.textContent =
      `Found ${meta.results_count} results in ${meta.elapsed_seconds}s. ` +
      `Ranking: ${objectiveLabels[meta.objective] || meta.objective} ` +
      `(${meta.ranking_mode || meta.price_per_km_basis || "custom"}). ${engineInfo}` +
      searchIdHint;

    if (meta.auto_discovered_hubs) {
      const hubInfo = document.createElement("p");
      hubInfo.className = "summary";
      const chunks = Object.entries(meta.auto_discovered_hubs)
        .slice(0, 4)
        .map(([dest, hubs]) => {
          const out = (hubs.outbound || []).join("/");
          const inn = (hubs.inbound || []).join("/");
          return `${dest}: out [${out}] in [${inn}]`;
        });
      hubInfo.textContent = `Auto hubs picked: ${chunks.join(" | ")}`;
      summaryEl.appendChild(document.createElement("br"));
      summaryEl.appendChild(hubInfo);
    }

    const groupedResults = groupResultsByDestination(
      results,
      payload.destinations || [],
      payload.currency || "RON",
    );
    const requestedDestinationsCount = new Set(payload.destinations || []).size;
    const shouldGroupByDestination = requestedDestinationsCount > 1 || groupedResults.length > 1;

    if (shouldGroupByDestination) {
      const firstNonEmptyIndex = groupedResults.findIndex((group) => group.items.length > 0);
      const defaultOpenIndex = firstNonEmptyIndex >= 0 ? firstNonEmptyIndex : 0;
      groupedResults.forEach((group, index) => {
        resultsEl.appendChild(renderDestinationGroup(group, index === defaultOpenIndex));
      });
    } else {
      for (const item of results) {
        resultsEl.appendChild(renderResultCard(item));
      }
    }

    if (warnings && warnings.length > 0) {
      const warningBox = document.createElement("p");
      warningBox.className = "summary";
      const lastNotes = warnings.slice(-3).join(" | ");
      warningBox.textContent = `Engine notes: ${lastNotes}`;
      summaryEl.appendChild(document.createElement("br"));
      summaryEl.appendChild(warningBox);
    }
  } catch (error) {
    const message =
      error instanceof Error && error.message
        ? error.message
        : "Unexpected frontend error before search started.";
    setStatus(`Error: ${message}`, "error");
    setExportEnabled(hadExportData);
  } finally {
    setSearchButtonBusy(false);
  }
}

function seedDates() {
  const today = new Date();
  const start = new Date(today);
  start.setDate(start.getDate() + 45);
  const end = new Date(start);
  end.setDate(end.getDate() + 60);

  const toIsoDate = (d) => d.toISOString().slice(0, 10);
  document.getElementById("period-start").value = toIsoDate(start);
  document.getElementById("period-end").value = toIsoDate(end);
}

function syncAutoHubControls() {
  const exhaustive = document.getElementById("exhaustive-hubs");
  const hubCandidatesInput = document.getElementById("hub-candidates");
  const autoHubsInput = document.getElementById("auto-hubs");
  const hubCount = Math.max(1, parseCodes(hubCandidatesInput.value).length);
  const maxAllowed = Math.max(12, hubCount);
  autoHubsInput.max = String(maxAllowed);

  if (exhaustive.checked) {
    autoHubsInput.value = String(maxAllowed);
    autoHubsInput.disabled = true;
  } else {
    autoHubsInput.disabled = false;
    const current = Number.parseInt(autoHubsInput.value, 10);
    if (!Number.isFinite(current) || current > maxAllowed) {
      autoHubsInput.value = String(Math.min(5, maxAllowed));
    }
  }
}

attachFieldTooltips();
seedDates();
resetProgressDisplay();
loadPresets();
syncAutoHubControls();
document.getElementById("exhaustive-hubs").addEventListener("change", syncAutoHubControls);
document.getElementById("hub-candidates").addEventListener("input", syncAutoHubControls);
if (applyProviderKeysBtn) {
  applyProviderKeysBtn.addEventListener("click", applyProviderKeys);
}
if (applyBudgetPresetBtn) {
  applyBudgetPresetBtn.addEventListener("click", applyBudgetAwarePreset);
}
if (exportBtn) {
  exportBtn.addEventListener("click", exportLastSearch);
  setExportEnabled(hasExportData());
}
if (exportPdfBtn) {
  exportPdfBtn.addEventListener("click", exportLastSearchPdf);
  setExportEnabled(hasExportData());
}
form.addEventListener("submit", runSearch);
