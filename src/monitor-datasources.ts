import { DataSourceConfig } from "./types.js";
import { sanitizeForJson } from "./utils.js";

interface SkyWalkingService {
  id: string;
  name: string;
  shortName?: string;
  layers?: string[];
  normal?: boolean | null;
  group?: string;
}

interface SkyWalkingBasicTrace {
  segmentId?: string;
  traceIds?: string[];
  endpointNames?: string[];
  duration?: number;
  start?: string;
  isError?: boolean | null;
}

interface SkyWalkingSpan {
  traceId?: string;
  segmentId?: string;
  spanId?: number;
  parentSpanId?: number;
  serviceCode?: string;
  serviceInstanceName?: string;
  endpointName?: string | null;
  type?: string;
  peer?: string | null;
  startTime?: number;
  endTime?: number;
  isError?: boolean | null;
  layer?: string | null;
  component?: string | null;
  tags?: Array<{ key?: string; value?: string }>;
  logs?: Array<{ time?: number; data?: Array<{ key?: string; value?: string }> }>;
}

interface GrafanaDatasource {
  id?: number;
  uid?: string;
  name?: string;
  type?: string;
  url?: string;
  database?: string;
  access?: string;
  isDefault?: boolean;
  readOnly?: boolean;
}

interface GrafanaDashboardEnvelope {
  dashboard?: Record<string, unknown>;
  meta?: Record<string, unknown>;
}

interface DashboardVariableSummary {
  name: string;
  type: string;
  current: unknown;
}

interface ResolvedTimeRange {
  fromMs: number;
  toMs: number;
  fromIso: string;
  toIso: string;
}

const DEFAULT_LAST = "1h";
const DEFAULT_LIMIT = 20;
const DEFAULT_MAX_PANELS = 20;
const DEFAULT_MAX_POINTS = 20;
const DEFAULT_MAX_DATA_POINTS = 200;
const MAX_LIMIT = 50;

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function truncateText(text: string, maxLength: number): string {
  return text.length > maxLength ? `${text.slice(0, maxLength).trimEnd()}...` : text;
}

function clampMonitorLimit(value: number | undefined, fallback = DEFAULT_LIMIT): number {
  if (!Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(1, Math.min(MAX_LIMIT, Math.floor(value as number)));
}

function normalizeMonitorBaseUrl(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new Error(`${label} is required`);
  }
  const candidate = /^https?:\/\//i.test(normalized) ? normalized : `https://${normalized}`;
  let parsed: URL;
  try {
    parsed = new URL(candidate);
  } catch (error) {
    throw new Error(`${label} is not a valid URL: ${error instanceof Error ? error.message : String(error)}`);
  }
  const pathname = parsed.pathname.replace(/\/+$/, "");
  return `${parsed.origin}${pathname === "/" ? "" : pathname}`;
}

function resolveMonitorBaseUrl(datasource: DataSourceConfig): string {
  const candidate = datasource.connection.uri?.trim() || datasource.connection.host?.trim();
  if (!candidate) {
    throw new Error(`Datasource ${datasource.id} requires URI or HOST`);
  }
  return normalizeMonitorBaseUrl(candidate, `Datasource ${datasource.id} base URL`);
}

function buildMonitorUrl(baseUrl: string, pathName: string): URL {
  const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const normalizedPathName = pathName.replace(/^\/+/, "");
  return new URL(normalizedPathName, normalizedBaseUrl);
}

function buildMonitorHeaders(datasource: DataSourceConfig, init?: RequestInit): Record<string, string> {
  const headers = new Headers(init?.headers ?? {});
  headers.set("Accept", "application/json");
  const username = datasource.auth.username ?? "";
  const secret = datasource.auth.secret ?? "";
  if ((username || secret) && !headers.has("Authorization")) {
    headers.set("Authorization", `Basic ${Buffer.from(`${username}:${secret}`).toString("base64")}`);
  }
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  return Object.fromEntries(headers.entries());
}

async function fetchMonitorJson<TData>(
  datasource: DataSourceConfig,
  pathName: string,
  init?: RequestInit,
): Promise<TData> {
  const baseUrl = resolveMonitorBaseUrl(datasource);
  const response = await fetch(buildMonitorUrl(baseUrl, pathName), {
    ...init,
    headers: buildMonitorHeaders(datasource, init),
  });
  const rawText = await response.text();
  if (!response.ok) {
    throw new Error(`Monitor request failed ${response.status}: ${truncateText(rawText || response.statusText, 240)}`);
  }
  if (/<!doctype html>/i.test(rawText)) {
    throw new Error("Monitor returned HTML instead of JSON; authentication may have expired");
  }
  try {
    return JSON.parse(rawText) as TData;
  } catch (error) {
    throw new Error(`Monitor returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
}

function isSkywalkingDatasource(datasource: DataSourceConfig): boolean {
  return datasource.type === "skywalking";
}

async function skywalkingQuery<TData>(
  datasource: DataSourceConfig,
  query: string,
  variables?: Record<string, unknown>,
): Promise<TData> {
  const payload = await fetchMonitorJson<{ data?: TData; errors?: Array<{ message?: string }> }>(
    datasource,
    "/graphql",
    {
      method: "POST",
      body: JSON.stringify({ query, variables }),
    },
  );
  if (Array.isArray(payload.errors) && payload.errors.length > 0) {
    throw new Error(
      payload.errors
        .map((item) => item.message)
        .filter(Boolean)
        .join("; ") || "SkyWalking query failed",
    );
  }
  if (!payload.data) {
    throw new Error("SkyWalking query returned empty payload");
  }
  return payload.data;
}

function pad2(value: number): string {
  return String(value).padStart(2, "0");
}

function resolveDurationMs(value: string): number {
  const normalized = value.trim().toLowerCase();
  const matched = normalized.match(/^(\d+)(ms|s|m|h|d|w)$/);
  if (!matched) {
    throw new Error(`Unsupported duration: ${value}`);
  }
  const amount = Number(matched[1]);
  const unit = matched[2];
  const multiplier =
    unit === "ms"
      ? 1
      : unit === "s"
        ? 1_000
        : unit === "m"
          ? 60_000
          : unit === "h"
            ? 3_600_000
            : unit === "d"
              ? 86_400_000
              : 604_800_000;
  return amount * multiplier;
}

function resolveTimeValue(value: string | undefined, nowMs: number): number | null {
  const normalized = value?.trim();
  if (!normalized) {
    return null;
  }
  if (normalized === "now") {
    return nowMs;
  }
  const relative = normalized.match(/^now-(.+)$/i);
  if (relative) {
    return nowMs - resolveDurationMs(relative[1]);
  }
  if (/^\d{13}$/.test(normalized)) {
    return Number(normalized);
  }
  if (/^\d{10}$/.test(normalized)) {
    return Number(normalized) * 1_000;
  }
  const parsed = Date.parse(normalized);
  if (Number.isFinite(parsed)) {
    return parsed;
  }
  if (/^\d+(ms|s|m|h|d|w)$/i.test(normalized)) {
    return nowMs - resolveDurationMs(normalized);
  }
  throw new Error(`Unsupported time value: ${value}`);
}

function resolveTimeRange(
  input: { from?: string; to?: string; last?: string },
  defaultLast = DEFAULT_LAST,
): ResolvedTimeRange {
  const nowMs = Date.now();
  const toMs = resolveTimeValue(input.to, nowMs) ?? nowMs;
  const fromMs = resolveTimeValue(input.from, nowMs) ?? (toMs - resolveDurationMs(input.last?.trim() || defaultLast));
  const normalizedFromMs = Math.min(fromMs, toMs);
  const normalizedToMs = Math.max(fromMs, toMs);
  return {
    fromMs: normalizedFromMs,
    toMs: normalizedToMs,
    fromIso: new Date(normalizedFromMs).toISOString(),
    toIso: new Date(normalizedToMs).toISOString(),
  };
}

function formatSkywalkingDurationValue(epochMs: number): string {
  const date = new Date(epochMs);
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())} ${pad2(date.getHours())}${pad2(date.getMinutes())}`;
}

function buildSkywalkingDuration(input: { from?: string; to?: string; last?: string }) {
  const timeRange = resolveTimeRange(input, DEFAULT_LAST);
  return {
    timeRange,
    duration: {
      start: formatSkywalkingDurationValue(timeRange.fromMs),
      end: formatSkywalkingDurationValue(timeRange.toMs),
      step: "MINUTE" as const,
    },
  };
}

async function listMonitorDatasources(datasource: DataSourceConfig): Promise<GrafanaDatasource[]> {
  const payload = await fetchMonitorJson<unknown[]>(datasource, "/api/datasources");
  return Array.isArray(payload) ? (payload as GrafanaDatasource[]) : [];
}

function summarizeGrafanaDatasource(datasource: GrafanaDatasource): Record<string, unknown> {
  return {
    id: datasource.id ?? null,
    uid: datasource.uid ?? null,
    name: datasource.name ?? null,
    type: datasource.type ?? null,
    url: datasource.url ?? null,
    database: datasource.database ?? null,
    access: datasource.access ?? null,
    isDefault: datasource.isDefault ?? false,
    readOnly: datasource.readOnly ?? false,
  };
}

function summarizeDatasourceRef(value: unknown): string | null {
  const record = asRecord(value);
  if (record) {
    return asString(record.uid) ?? asString(record.name) ?? asString(record.type);
  }
  return asString(value);
}

function summarizeTarget(target: Record<string, unknown>): string {
  const source = asString(target.expr)
    ?? asString(target.query)
    ?? asString(target.queryText)
    ?? asString(target.rawSql)
    ?? JSON.stringify(target);
  const refId = asString(target.refId);
  return truncateText([refId, source].filter(Boolean).join(": "), 160);
}

function flattenPanels(panels: unknown[], parentTitle?: string): Array<{ panel: Record<string, unknown>; parentTitle?: string }> {
  const results: Array<{ panel: Record<string, unknown>; parentTitle?: string }> = [];
  for (const entry of panels) {
    const panel = asRecord(entry);
    if (!panel) {
      continue;
    }
    const title = asString(panel.title) ?? parentTitle;
    if (Array.isArray(panel.panels)) {
      results.push(...flattenPanels(panel.panels, title));
      continue;
    }
    if (asNumber(panel.id) != null) {
      results.push({ panel, parentTitle });
    }
  }
  return results;
}

function summarizeGrafanaPanel(panel: Record<string, unknown>): Record<string, unknown> {
  const targets = Array.isArray(panel.targets)
    ? panel.targets
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
    : [];
  return {
    id: asNumber(panel.id),
    title: asString(panel.title) ?? `Panel ${asNumber(panel.id) ?? "unknown"}`,
    type: asString(panel.type) ?? "unknown",
    datasource: summarizeDatasourceRef(panel.datasource),
    description: truncateText(asString(panel.description) ?? "", 240) || undefined,
    queryCount: targets.filter((target) => target.hide !== true).length,
    queries: targets.filter((target) => target.hide !== true).slice(0, 4).map(summarizeTarget),
  };
}

function extractVariables(dashboard: Record<string, unknown>): DashboardVariableSummary[] {
  const templating = asRecord(dashboard.templating);
  const list = Array.isArray(templating?.list) ? templating.list : [];
  const results: DashboardVariableSummary[] = [];
  for (const entry of list) {
    const record = asRecord(entry);
    if (!record) {
      continue;
    }
    const current = asRecord(record.current);
    const name = asString(record.name);
    if (!name) {
      continue;
    }
    results.push({
      name,
      type: asString(record.type) ?? "unknown",
      current: current?.value ?? current?.text ?? null,
    });
  }
  return results;
}

function buildDashboardSummary(envelope: GrafanaDashboardEnvelope, maxPanels = DEFAULT_MAX_PANELS): Record<string, unknown> {
  const dashboard = asRecord(envelope.dashboard) ?? {};
  const meta = asRecord(envelope.meta) ?? {};
  const uid = asString(dashboard.uid) ?? "unknown";
  const panels = flattenPanels(Array.isArray(dashboard.panels) ? dashboard.panels : []);
  return {
    kind: "dashboard",
    ref: `dashboard:${uid}`,
    uid,
    title: asString(dashboard.title) ?? uid,
    folderTitle: asString(meta.folderTitle) ?? null,
    url: asString(meta.url) ?? null,
    tags: Array.isArray(dashboard.tags) ? dashboard.tags.slice(0, 10) : [],
    variableCount: extractVariables(dashboard).length,
    variables: extractVariables(dashboard).slice(0, 10),
    panelCount: panels.length,
    panels: panels.slice(0, maxPanels).map(({ panel, parentTitle }) => ({
      ...summarizeGrafanaPanel(panel),
      ...(parentTitle ? { parentTitle } : {}),
    })),
    truncated: panels.length > maxPanels,
  };
}

function parseMonitorRef(ref: string): { kind: "dashboard" | "folder" | "panel" | "datasource"; uid: string; panelId?: number } {
  const dashboardMatch = ref.match(/^dashboard:(.+)$/);
  if (dashboardMatch) {
    return { kind: "dashboard", uid: dashboardMatch[1] };
  }
  const folderMatch = ref.match(/^folder:(.+)$/);
  if (folderMatch) {
    return { kind: "folder", uid: folderMatch[1] };
  }
  const panelMatch = ref.match(/^panel:([^:]+):(\d+)$/);
  if (panelMatch) {
    return { kind: "panel", uid: panelMatch[1], panelId: Number(panelMatch[2]) };
  }
  const datasourceMatch = ref.match(/^datasource:(.+)$/);
  if (datasourceMatch) {
    return { kind: "datasource", uid: datasourceMatch[1] };
  }
  throw new Error(`Unsupported monitor ref "${ref}"`);
}

function resolveDatasourceByUidOrName(datasources: GrafanaDatasource[], value: string): GrafanaDatasource | null {
  const normalized = value.trim().toLowerCase();
  if (!normalized) {
    return null;
  }
  return datasources.find((datasource) => (
    datasource.uid?.trim().toLowerCase() === normalized
    || datasource.name?.trim().toLowerCase() === normalized
  )) ?? null;
}

function buildVariableMap(
  dashboard: Record<string, unknown>,
  explicit: Record<string, unknown> | undefined,
): Record<string, unknown> {
  const values: Record<string, unknown> = {};
  for (const variable of extractVariables(dashboard)) {
    values[variable.name] = variable.current;
  }
  for (const [key, value] of Object.entries(explicit ?? {})) {
    values[key] = value;
  }
  return values;
}

function escapeRegexLiteral(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function normalizeVariableValue(value: unknown, format: string | undefined, datasourceType?: string): string | null {
  const isPrometheus = datasourceType?.toLowerCase().includes("prometheus") ?? false;
  if (Array.isArray(value)) {
    const items = value.map((entry) => String(entry));
    if (format === "regex") {
      return items.map(escapeRegexLiteral).join("|");
    }
    return items.join(",");
  }
  if (value == null) {
    return null;
  }
  const text = String(value);
  if (text === "All" || text === "$__all") {
    return isPrometheus ? ".*" : text;
  }
  if (format === "regex") {
    return escapeRegexLiteral(text);
  }
  return text;
}

function substituteTemplateString(input: string, variables: Record<string, unknown>, datasourceType?: string): string {
  return input.replace(/\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)/g, (matched, braced: string | undefined, bare: string | undefined) => {
    const token = braced ?? bare;
    if (!token) {
      return matched;
    }
    const [name, format] = token.split(":", 2);
    const replacement = normalizeVariableValue(variables[name], format, datasourceType);
    return replacement ?? matched;
  });
}

function substituteTemplateValues(value: unknown, variables: Record<string, unknown>, datasourceType?: string): unknown {
  if (typeof value === "string") {
    return substituteTemplateString(value, variables, datasourceType);
  }
  if (Array.isArray(value)) {
    return value.map((entry) => substituteTemplateValues(entry, variables, datasourceType));
  }
  const record = asRecord(value);
  if (!record) {
    return value;
  }
  return Object.fromEntries(
    Object.entries(record).map(([key, entryValue]) => [
      key,
      substituteTemplateValues(entryValue, variables, datasourceType),
    ]),
  );
}

function extractVariableName(value: string): string | null {
  const bareMatch = value.match(/^\$([A-Za-z_][A-Za-z0-9_]*)$/);
  if (bareMatch) {
    return bareMatch[1];
  }
  const bracedMatch = value.match(/^\$\{([^}:]+)(?::[^}]+)?\}$/);
  return bracedMatch?.[1] ?? null;
}

function resolveDatasourceReference(
  value: unknown,
  params: {
    datasources: GrafanaDatasource[];
    variables: Record<string, unknown>;
    explicitDatasource?: string;
    fallbackDatasource?: unknown;
  },
): GrafanaDatasource | null {
  const record = asRecord(value);
  if (record) {
    const identifier = asString(record.uid) ?? asString(record.name);
    if (identifier) {
      return resolveDatasourceByUidOrName(params.datasources, identifier);
    }
  }

  const text = asString(value);
  if (text) {
    const variableName = extractVariableName(text);
    if (variableName) {
      const variableValue = normalizeVariableValue(params.variables[variableName], undefined);
      if (variableValue && variableValue !== "All" && variableValue !== "$__all") {
        return resolveDatasourceByUidOrName(params.datasources, variableValue);
      }
      if (params.explicitDatasource) {
        return resolveDatasourceByUidOrName(params.datasources, params.explicitDatasource);
      }
      if (params.fallbackDatasource && params.fallbackDatasource !== value) {
        return resolveDatasourceReference(params.fallbackDatasource, { ...params, fallbackDatasource: undefined });
      }
      return null;
    }
    return resolveDatasourceByUidOrName(params.datasources, text);
  }

  if (params.explicitDatasource) {
    return resolveDatasourceByUidOrName(params.datasources, params.explicitDatasource);
  }
  if (params.fallbackDatasource && params.fallbackDatasource !== value) {
    return resolveDatasourceReference(params.fallbackDatasource, { ...params, fallbackDatasource: undefined });
  }
  return null;
}

async function readGrafanaDashboard(datasource: DataSourceConfig, uid: string): Promise<GrafanaDashboardEnvelope> {
  return await fetchMonitorJson<GrafanaDashboardEnvelope>(
    datasource,
    `/api/dashboards/uid/${encodeURIComponent(uid)}`,
  );
}

function findPanelOrThrow(
  envelope: GrafanaDashboardEnvelope,
  panelId: number,
): { panel: Record<string, unknown>; parentTitle?: string } {
  const dashboard = asRecord(envelope.dashboard) ?? {};
  const panelRef = flattenPanels(Array.isArray(dashboard.panels) ? dashboard.panels : [])
    .find(({ panel }) => asNumber(panel.id) === panelId);
  if (!panelRef) {
    throw new Error(`Panel not found: ${panelId}`);
  }
  return panelRef;
}

function toNumberOrNull(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function normalizeTimestamp(value: unknown): string | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    const millis = value > 1_000_000_000_000 ? value : value * 1_000;
    return new Date(millis).toISOString();
  }
  if (typeof value === "string" && value.trim()) {
    if (/^\d+(\.\d+)?$/.test(value)) {
      return new Date(Number(value) * 1_000).toISOString();
    }
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) {
      return new Date(parsed).toISOString();
    }
  }
  return null;
}

function buildFrameRows(frame: Record<string, unknown>): {
  fields: Array<{ name: string; type?: string; labels?: Record<string, string> }>;
  rows: unknown[][];
} {
  const schema = asRecord(frame.schema) ?? {};
  const data = asRecord(frame.data) ?? {};
  const fields = Array.isArray(schema.fields)
    ? schema.fields.map((entry) => {
        const record = asRecord(entry) ?? {};
        return {
          name: asString(record.name) ?? "value",
          type: asString(record.type) ?? undefined,
          labels: (asRecord(record.labels) as Record<string, string> | null) ?? undefined,
        };
      })
    : [];
  const columns = Array.isArray(data.values) ? data.values : [];
  const rowCount = columns.reduce((max, column) => Math.max(max, Array.isArray(column) ? column.length : 0), 0);
  const rows = Array.from({ length: rowCount }, (_, rowIndex) => (
    columns.map((column) => (Array.isArray(column) ? column[rowIndex] : undefined))
  ));
  return { fields, rows };
}

function buildSeriesName(field: { name: string; labels?: Record<string, string> }): string {
  const labels = field.labels && Object.keys(field.labels).length > 0
    ? ` {${Object.entries(field.labels).map(([key, value]) => `${key}=${value}`).join(", ")}}`
    : "";
  return `${field.name}${labels}`;
}

function normalizeQueryFrames(panelType: string, response: Record<string, unknown>, maxPoints = DEFAULT_MAX_POINTS): Record<string, unknown> {
  const results = asRecord(response.results) ?? {};
  const frames = Object.values(results)
    .flatMap((entry) => {
      const record = asRecord(entry);
      return Array.isArray(record?.frames) ? record.frames : [];
    })
    .map((entry) => asRecord(entry))
    .filter((entry): entry is Record<string, unknown> => Boolean(entry))
    .map(buildFrameRows);

  if (["stat", "gauge", "bargauge"].includes(panelType)) {
    for (const frame of frames) {
      const numericIndex = frame.fields.findIndex((field) => field.type === "number");
      if (numericIndex < 0) {
        continue;
      }
      const latest = [...frame.rows]
        .reverse()
        .map((row) => toNumberOrNull(row[numericIndex]))
        .find((value) => value != null);
      if (latest != null) {
        return {
          view: "stat",
          field: frame.fields[numericIndex]?.name ?? "value",
          value: latest,
        };
      }
    }
    return { view: "stat", value: null };
  }

  const series: Array<Record<string, unknown>> = [];
  for (const frame of frames) {
    const timeIndex = frame.fields.findIndex((field) => field.type === "time");
    const numericFields = frame.fields
      .map((field, index) => ({ field, index }))
      .filter(({ field, index }) => index !== timeIndex && field.type === "number");
    if (timeIndex < 0 || numericFields.length === 0) {
      continue;
    }
    for (const numericField of numericFields) {
      const points = frame.rows
        .map((row) => ({
          timestamp: normalizeTimestamp(row[timeIndex]),
          value: toNumberOrNull(row[numericField.index]),
        }))
        .filter((point) => point.timestamp != null)
        .slice(-maxPoints);
      if (points.length === 0) {
        continue;
      }
      series.push({
        name: buildSeriesName(numericField.field),
        latestValue: [...points].reverse().find((point) => point.value != null)?.value ?? null,
        points,
      });
    }
  }

  if (series.length > 0) {
    return {
      view: "timeseries",
      seriesCount: series.length,
      series: series.slice(0, 10),
    };
  }

  const firstFrame = frames[0];
  if (!firstFrame) {
    return { view: "empty", rows: [] };
  }
  const columns = firstFrame.fields.map((field) => field.name);
  return {
    view: "table",
    columns,
    rowCount: firstFrame.rows.length,
    rows: firstFrame.rows.slice(0, maxPoints).map((row) => (
      Object.fromEntries(row.map((cell, index) => [columns[index], cell]))
    )),
  };
}

function buildQueryTargets(params: {
  panel: Record<string, unknown>;
  datasources: GrafanaDatasource[];
  variables: Record<string, unknown>;
  datasource?: string;
  maxDataPoints: number;
}): Array<Record<string, unknown>> {
  const targets = Array.isArray(params.panel.targets)
    ? params.panel.targets
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
    : [];
  const queries: Array<Record<string, unknown>> = [];

  for (const target of targets) {
    if (target.hide === true) {
      continue;
    }
    const resolvedDatasource = resolveDatasourceReference(target.datasource, {
      datasources: params.datasources,
      variables: params.variables,
      explicitDatasource: params.datasource,
      fallbackDatasource: params.panel.datasource,
    });
    if (!resolvedDatasource?.uid || !resolvedDatasource.type) {
      throw new Error(`Cannot resolve panel datasource (panel=${asNumber(params.panel.id) ?? "unknown"})`);
    }

    const query = substituteTemplateValues(
      {
        ...target,
        datasource: { uid: resolvedDatasource.uid, type: resolvedDatasource.type },
        datasourceId: resolvedDatasource.id,
        maxDataPoints: params.maxDataPoints,
        intervalMs: asNumber(target.intervalMs) ?? 1_000,
      },
      params.variables,
      resolvedDatasource.type,
    ) as Record<string, unknown>;

    queries.push({
      ...query,
      refId: asString(query.refId) ?? String.fromCharCode(65 + queries.length),
      datasource: { uid: resolvedDatasource.uid, type: resolvedDatasource.type },
      datasourceId: resolvedDatasource.id,
    });
  }

  return queries;
}

function scoreSkywalkingService(service: SkyWalkingService, query: string): number {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) {
    return 0;
  }
  const name = service.name.trim().toLowerCase();
  const shortName = service.shortName?.trim().toLowerCase() ?? "";
  const group = service.group?.trim().toLowerCase() ?? "";
  let score = 0;
  if (name === normalizedQuery) {
    score += 100;
  } else if (shortName === normalizedQuery) {
    score += 90;
  } else if (`${group}::${shortName}` === normalizedQuery) {
    score += 80;
  } else if (name.startsWith(normalizedQuery)) {
    score += 60;
  } else if (shortName.startsWith(normalizedQuery)) {
    score += 50;
  } else if (name.includes(normalizedQuery)) {
    score += 30;
  } else if (shortName.includes(normalizedQuery)) {
    score += 20;
  }
  if (service.normal) {
    score += 5;
  }
  if (service.layers?.includes("GENERAL")) {
    score += 3;
  }
  return score;
}

function summarizeSkywalkingService(service: SkyWalkingService): Record<string, unknown> {
  return {
    id: service.id,
    name: service.name,
    shortName: service.shortName ?? null,
    group: service.group ?? null,
    layers: service.layers ?? [],
    normal: service.normal ?? null,
  };
}

function summarizeSkywalkingTrace(trace: SkyWalkingBasicTrace): Record<string, unknown> {
  return {
    traceId: trace.traceIds?.[0] ?? null,
    segmentId: trace.segmentId ?? null,
    endpointNames: trace.endpointNames ?? [],
    durationMs: trace.duration ?? null,
    startTime: normalizeTimestamp(trace.start) ?? trace.start ?? null,
    isError: trace.isError ?? false,
  };
}

function summarizeSkywalkingTraceDetail(traceId: string, spans: SkyWalkingSpan[], format: "summary" | "raw") {
  const normalizedSpans = spans.map((span) => {
    const durationMs = typeof span.startTime === "number" && typeof span.endTime === "number"
      ? Math.max(0, span.endTime - span.startTime)
      : null;
    return {
      traceId: span.traceId ?? traceId,
      segmentId: span.segmentId ?? null,
      spanId: span.spanId ?? null,
      parentSpanId: span.parentSpanId ?? null,
      serviceCode: span.serviceCode ?? null,
      serviceInstanceName: span.serviceInstanceName ?? null,
      endpointName: span.endpointName ?? null,
      type: span.type ?? null,
      peer: span.peer ?? null,
      component: span.component ?? null,
      layer: span.layer ?? null,
      isError: span.isError ?? false,
      startTime: normalizeTimestamp(span.startTime) ?? span.startTime ?? null,
      endTime: normalizeTimestamp(span.endTime) ?? span.endTime ?? null,
      durationMs,
      tags: Array.isArray(span.tags)
        ? span.tags
            .map((tag) => ({ key: tag.key ?? null, value: tag.value ?? null }))
            .filter((tag) => tag.key)
        : [],
      logCount: Array.isArray(span.logs) ? span.logs.length : 0,
    };
  });
  const rootSpans = normalizedSpans.filter((span) => span.parentSpanId === -1);
  const slowestSpans = [...normalizedSpans]
    .sort((left, right) => Number(right.durationMs ?? -1) - Number(left.durationMs ?? -1))
    .slice(0, 5);
  const errorSpans = normalizedSpans.filter((span) => span.isError).slice(0, 5);
  const unique = (values: Array<string | null>) => [...new Set(values.filter((value): value is string => Boolean(value)))];

  return {
    kind: "trace",
    traceId,
    spanCount: normalizedSpans.length,
    services: unique(normalizedSpans.map((span) => span.serviceCode)),
    instances: unique(normalizedSpans.map((span) => span.serviceInstanceName)),
    endpoints: unique(normalizedSpans.map((span) => span.endpointName)),
    components: unique(normalizedSpans.map((span) => span.component)),
    peers: unique(normalizedSpans.map((span) => span.peer)),
    rootSpans,
    slowestSpans,
    errorSpans,
    ...(format === "raw" ? { raw: normalizedSpans } : {}),
  };
}

async function skywalkingGetService(datasource: DataSourceConfig, serviceId: string): Promise<SkyWalkingService | null> {
  const data = await skywalkingQuery<{ getService?: SkyWalkingService | null }>(
    datasource,
    `query($serviceId: String!) {
      getService(serviceId: $serviceId) { id name shortName layers normal group }
    }`,
    { serviceId },
  );
  return data.getService ?? null;
}

async function skywalkingFindService(datasource: DataSourceConfig, serviceName: string): Promise<SkyWalkingService | null> {
  const data = await skywalkingQuery<{ findService?: SkyWalkingService | null }>(
    datasource,
    `query($serviceName: String!) {
      findService(serviceName: $serviceName) { id name shortName layers normal group }
    }`,
    { serviceName },
  );
  return data.findService ?? null;
}

async function skywalkingSearchServices(
  datasource: DataSourceConfig,
  query: string,
  input: { from?: string; to?: string; last?: string },
): Promise<SkyWalkingService[]> {
  const { duration } = buildSkywalkingDuration(input);
  const data = await skywalkingQuery<{ searchServices?: SkyWalkingService[] }>(
    datasource,
    `query($duration: Duration!, $keyword: String!) {
      searchServices(duration: $duration, keyword: $keyword) { id name shortName layers normal group }
    }`,
    { duration, keyword: query },
  );
  return Array.isArray(data.searchServices) ? data.searchServices : [];
}

async function skywalkingGetAllServices(
  datasource: DataSourceConfig,
  input: { from?: string; to?: string; last?: string },
): Promise<SkyWalkingService[]> {
  const { duration } = buildSkywalkingDuration(input);
  const data = await skywalkingQuery<{ getAllServices?: SkyWalkingService[] }>(
    datasource,
    `query($duration: Duration!) {
      getAllServices(duration: $duration) { id name shortName layers normal group }
    }`,
    { duration },
  );
  return Array.isArray(data.getAllServices) ? data.getAllServices : [];
}

async function resolveSkywalkingService(
  datasource: DataSourceConfig,
  params: {
    service?: string;
    serviceId?: string;
    from?: string;
    to?: string;
    last?: string;
  },
): Promise<SkyWalkingService | null> {
  if (params.serviceId) {
    return await skywalkingGetService(datasource, params.serviceId);
  }
  const serviceName = params.service?.trim();
  if (!serviceName) {
    return null;
  }
  const exact = await skywalkingFindService(datasource, serviceName);
  if (exact) {
    return exact;
  }
  const candidates = await skywalkingSearchServices(datasource, serviceName, params);
  if (candidates.length === 0) {
    return null;
  }
  return [...candidates].sort((left, right) => (
    scoreSkywalkingService(right, serviceName) - scoreSkywalkingService(left, serviceName)
  ))[0] ?? null;
}

async function skywalkingQueryBasicTraces(
  datasource: DataSourceConfig,
  params: {
    serviceId?: string;
    serviceInstanceId?: string;
    endpointId?: string;
    traceId?: string;
    from?: string;
    to?: string;
    last?: string;
    status?: "ALL" | "SUCCESS" | "ERROR";
    pageNum?: number;
    pageSize?: number;
    minDurationMs?: number;
    maxDurationMs?: number;
  },
): Promise<SkyWalkingBasicTrace[]> {
  const condition: Record<string, unknown> = {
    traceState: params.status ?? "ALL",
    queryOrder: "BY_START_TIME",
    paging: {
      pageNum: params.pageNum ?? 1,
      pageSize: clampMonitorLimit(params.pageSize, DEFAULT_LIMIT),
    },
  };
  if (params.serviceId) {
    condition.serviceId = params.serviceId;
  }
  if (params.serviceInstanceId) {
    condition.serviceInstanceId = params.serviceInstanceId;
  }
  if (params.endpointId) {
    condition.endpointId = params.endpointId;
  }
  if (params.traceId) {
    condition.traceId = params.traceId;
  }
  if (typeof params.minDurationMs === "number") {
    condition.minTraceDuration = Math.max(0, Math.floor(params.minDurationMs));
  }
  if (typeof params.maxDurationMs === "number") {
    condition.maxTraceDuration = Math.max(0, Math.floor(params.maxDurationMs));
  }
  if (!params.traceId || params.from || params.to || params.last) {
    condition.queryDuration = buildSkywalkingDuration({ from: params.from, to: params.to, last: params.last }).duration;
  }
  const data = await skywalkingQuery<{ queryBasicTraces?: { traces?: SkyWalkingBasicTrace[] } }>(
    datasource,
    `query($condition: TraceQueryCondition!) {
      queryBasicTraces(condition: $condition) {
        traces { segmentId traceIds endpointNames duration start isError }
      }
    }`,
    { condition },
  );
  return Array.isArray(data.queryBasicTraces?.traces) ? data.queryBasicTraces.traces : [];
}

async function skywalkingQueryTrace(datasource: DataSourceConfig, traceId: string): Promise<SkyWalkingSpan[]> {
  const data = await skywalkingQuery<{ queryTrace?: { spans?: SkyWalkingSpan[] } }>(
    datasource,
    `query($traceId: ID!) {
      queryTrace(traceId: $traceId) {
        spans {
          traceId
          segmentId
          spanId
          parentSpanId
          serviceCode
          serviceInstanceName
          endpointName
          type
          peer
          startTime
          endTime
          isError
          layer
          component
          tags { key value }
          logs { time data { key value } }
        }
      }
    }`,
    { traceId },
  );
  return Array.isArray(data.queryTrace?.spans) ? data.queryTrace.spans : [];
}

export async function testMonitorDatasource(
  datasource: DataSourceConfig,
): Promise<{ message: string; details?: Record<string, unknown> }> {
  const baseUrl = resolveMonitorBaseUrl(datasource);
  if (isSkywalkingDatasource(datasource)) {
    const data = await skywalkingQuery<{ listLayers?: string[] }>(
      datasource,
      "query { listLayers }",
    );
    const layers = Array.isArray(data.listLayers) ? data.listLayers : [];
    return {
      message: `SkyWalking connection ok (${layers.length} layers)`,
      details: {
        baseUrl,
        layers,
      },
    };
  }

  const health = await fetchMonitorJson<Record<string, unknown>>(datasource, "/api/health");
  const datasources = await listMonitorDatasources(datasource).catch(() => []);
  return {
    message: `Monitor connection ok (${datasources.length} datasources)`,
    details: {
      baseUrl,
      health,
      datasourceCount: datasources.length,
    },
  };
}

export async function monitorSearch(params: {
  datasource: DataSourceConfig;
  query?: string;
  kinds?: Array<"dashboard" | "folder" | "datasource" | "service">;
  maxResults?: number;
  from?: string;
  to?: string;
  last?: string;
}): Promise<Record<string, unknown>> {
  const { datasource } = params;
  const query = params.query?.trim() ?? "";
  const limit = clampMonitorLimit(params.maxResults);

  if (isSkywalkingDatasource(datasource)) {
    const kinds = params.kinds?.length ? params.kinds : ["service"];
    if (!kinds.includes("service")) {
      return {
        datasourceId: datasource.id,
        datasourceType: datasource.type,
        baseUrl: resolveMonitorBaseUrl(datasource),
        query,
        hitCount: 0,
        hits: [],
      };
    }
    const services = query
      ? await skywalkingSearchServices(datasource, query, { from: params.from, to: params.to, last: params.last ?? DEFAULT_LAST })
      : await skywalkingGetAllServices(datasource, { from: params.from, to: params.to, last: params.last ?? DEFAULT_LAST });
    const hits = [...services]
      .sort((left, right) => scoreSkywalkingService(right, query) - scoreSkywalkingService(left, query))
      .slice(0, limit)
      .map((service) => ({
        kind: "service",
        ref: `service:${service.id}`,
        title: service.name,
        preview: [service.group, service.shortName, ...(service.layers ?? [])].filter(Boolean).join(" · ") || undefined,
        metadata: summarizeSkywalkingService(service),
      }));
    return {
      datasourceId: datasource.id,
      datasourceType: datasource.type,
      baseUrl: resolveMonitorBaseUrl(datasource),
      query,
      hitCount: hits.length,
      hits,
    };
  }

  const requestedKinds = new Set(params.kinds?.length ? params.kinds : ["dashboard", "folder", "datasource"]);
  const hits: Array<Record<string, unknown>> = [];

  if (requestedKinds.has("dashboard")) {
    const dashboards = await fetchMonitorJson<unknown[]>(
      datasource,
      `/api/search?${new URLSearchParams({ query, type: "dash-db", limit: String(limit) }).toString()}`,
    );
    hits.push(
      ...dashboards
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
        .map((entry) => ({
          kind: "dashboard",
          ref: `dashboard:${asString(entry.uid) ?? asNumber(entry.id) ?? "unknown"}`,
          title: asString(entry.title) ?? "unknown dashboard",
          preview: [asString(entry.folderTitle), asString(entry.url)].filter(Boolean).join(" · ") || undefined,
          metadata: {
            uid: asString(entry.uid) ?? null,
            id: asNumber(entry.id),
            folderUid: asString(entry.folderUid) ?? null,
            folderId: asNumber(entry.folderId),
            url: asString(entry.url) ?? null,
          },
        })),
    );
  }

  if (requestedKinds.has("folder")) {
    const folders = await fetchMonitorJson<unknown[]>(
      datasource,
      `/api/search?${new URLSearchParams({ query, type: "dash-folder", limit: String(limit) }).toString()}`,
    );
    hits.push(
      ...folders
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
        .map((entry) => ({
          kind: "folder",
          ref: `folder:${asString(entry.uid) ?? asNumber(entry.id) ?? "unknown"}`,
          title: asString(entry.title) ?? "unknown folder",
          preview: asString(entry.url) ?? undefined,
          metadata: {
            uid: asString(entry.uid) ?? null,
            id: asNumber(entry.id),
            url: asString(entry.url) ?? null,
          },
        })),
    );
  }

  if (requestedKinds.has("datasource")) {
    const normalizedQuery = query.toLowerCase();
    const datasources = await listMonitorDatasources(datasource);
    hits.push(
      ...datasources
        .filter((entry) => {
          if (!normalizedQuery) {
            return true;
          }
          const haystack = [
            entry.uid,
            entry.name,
            entry.type,
            entry.database,
            entry.url,
          ].filter(Boolean).join(" ").toLowerCase();
          return haystack.includes(normalizedQuery);
        })
        .slice(0, limit)
        .map((entry) => ({
          kind: "datasource",
          ref: `datasource:${entry.uid ?? entry.name ?? "unknown"}`,
          title: entry.name ?? entry.uid ?? "unknown datasource",
          preview: [entry.type, entry.database].filter(Boolean).join(" · ") || undefined,
          metadata: summarizeGrafanaDatasource(entry),
        })),
    );
  }

  return {
    datasourceId: datasource.id,
    datasourceType: datasource.type,
    baseUrl: resolveMonitorBaseUrl(datasource),
    query,
    hitCount: hits.slice(0, limit).length,
    hits: hits.slice(0, limit),
  };
}

export async function monitorRead(params: {
  datasource: DataSourceConfig;
  ref: string;
  maxPanels?: number;
  from?: string;
  to?: string;
  last?: string;
}): Promise<Record<string, unknown>> {
  const { datasource, ref } = params;
  const basePayload = {
    datasourceId: datasource.id,
    datasourceType: datasource.type,
    baseUrl: resolveMonitorBaseUrl(datasource),
    ref,
  };

  if (isSkywalkingDatasource(datasource)) {
    if (ref.startsWith("service:")) {
      const serviceId = ref.slice("service:".length);
      const service = await skywalkingGetService(datasource, serviceId);
      if (!service) {
        throw new Error(`SkyWalking service not found: ${serviceId}`);
      }
      const recentTraces = await skywalkingQueryBasicTraces(datasource, {
        serviceId: service.id,
        from: params.from,
        to: params.to,
        last: params.last ?? DEFAULT_LAST,
        pageNum: 1,
        pageSize: 5,
      });
      return {
        ...basePayload,
        kind: "service",
        service: summarizeSkywalkingService(service),
        recentTraces: recentTraces.map(summarizeSkywalkingTrace),
        suggestedNextTool: "monitor_query_traces",
      };
    }

    if (ref.startsWith("trace:")) {
      const traceId = ref.slice("trace:".length);
      const spans = await skywalkingQueryTrace(datasource, traceId);
      if (spans.length === 0) {
        throw new Error(`SkyWalking trace not found: ${traceId}`);
      }
      return {
        ...basePayload,
        ...summarizeSkywalkingTraceDetail(traceId, spans, "summary"),
      };
    }

    throw new Error("SkyWalking ref only supports service:<id> or trace:<id>");
  }

  const parsedRef = parseMonitorRef(ref);
  if (parsedRef.kind === "dashboard") {
    const dashboard = await readGrafanaDashboard(datasource, parsedRef.uid);
    return {
      ...basePayload,
      ...buildDashboardSummary(dashboard, params.maxPanels ?? DEFAULT_MAX_PANELS),
    };
  }

  if (parsedRef.kind === "panel") {
    const dashboard = await readGrafanaDashboard(datasource, parsedRef.uid);
    const dashboardData = asRecord(dashboard.dashboard) ?? {};
    const panelRef = findPanelOrThrow(dashboard, parsedRef.panelId ?? -1);
    return {
      ...basePayload,
      kind: "panel",
      dashboard: {
        uid: parsedRef.uid,
        title: asString(dashboardData.title) ?? parsedRef.uid,
      },
      panel: {
        ...summarizeGrafanaPanel(panelRef.panel),
        ...(panelRef.parentTitle ? { parentTitle: panelRef.parentTitle } : {}),
      },
      variables: extractVariables(dashboardData).slice(0, 10),
      suggestedNextTool: "monitor_query_panel",
    };
  }

  if (parsedRef.kind === "folder") {
    const dashboards = await fetchMonitorJson<unknown[]>(
      datasource,
      `/api/search?${new URLSearchParams({
        query: "",
        type: "dash-db",
        folderUIDs: parsedRef.uid,
        limit: String(clampMonitorLimit(params.maxPanels, DEFAULT_MAX_PANELS)),
      }).toString()}`,
    );
    return {
      ...basePayload,
      kind: "folder",
      folderUid: parsedRef.uid,
      dashboards: dashboards
        .map((entry) => asRecord(entry))
        .filter((entry): entry is Record<string, unknown> => Boolean(entry))
        .map((entry) => ({
          ref: `dashboard:${asString(entry.uid) ?? asNumber(entry.id) ?? "unknown"}`,
          title: asString(entry.title) ?? "unknown dashboard",
          url: asString(entry.url) ?? null,
        })),
    };
  }

  const datasources = await listMonitorDatasources(datasource);
  const matchedDatasource = resolveDatasourceByUidOrName(datasources, parsedRef.uid);
  if (!matchedDatasource?.uid) {
    throw new Error(`Grafana datasource not found: ${parsedRef.uid}`);
  }
  let health: Record<string, unknown> | null = null;
  try {
    health = await fetchMonitorJson<Record<string, unknown>>(
      datasource,
      `/api/datasources/uid/${encodeURIComponent(matchedDatasource.uid)}/health`,
    );
  } catch (error) {
    health = {
      status: "unknown",
      message: error instanceof Error ? error.message : String(error),
    };
  }
  return {
    ...basePayload,
    kind: "datasource",
    title: matchedDatasource.name ?? matchedDatasource.uid,
    datasource: summarizeGrafanaDatasource(matchedDatasource),
    health,
  };
}

export async function monitorQueryPanel(params: {
  datasource: DataSourceConfig;
  ref: string;
  datasourceRef?: string;
  variables?: Record<string, unknown>;
  from?: string;
  to?: string;
  last?: string;
  format?: "summary" | "raw";
  maxPoints?: number;
  maxDataPoints?: number;
}): Promise<Record<string, unknown>> {
  const { datasource } = params;
  if (isSkywalkingDatasource(datasource)) {
    throw new Error("monitor_query_panel only supports Grafana monitor datasources");
  }
  const parsedRef = parseMonitorRef(params.ref);
  if (parsedRef.kind !== "panel" || parsedRef.panelId == null) {
    throw new Error("monitor_query_panel only supports panel:<dashboardUid>:<panelId>");
  }

  const dashboard = await readGrafanaDashboard(datasource, parsedRef.uid);
  const dashboardData = asRecord(dashboard.dashboard) ?? {};
  const meta = asRecord(dashboard.meta) ?? {};
  const panelRef = findPanelOrThrow(dashboard, parsedRef.panelId);
  const datasources = await listMonitorDatasources(datasource);
  const variableMap = buildVariableMap(dashboardData, params.variables);
  const timeRange = resolveTimeRange({ from: params.from, to: params.to, last: params.last ?? DEFAULT_LAST }, DEFAULT_LAST);
  const queries = buildQueryTargets({
    panel: panelRef.panel,
    datasources,
    variables: variableMap,
    datasource: params.datasourceRef,
    maxDataPoints: params.maxDataPoints ?? DEFAULT_MAX_DATA_POINTS,
  });

  const response = await fetchMonitorJson<Record<string, unknown>>(
    datasource,
    "/api/ds/query",
    {
      method: "POST",
      body: JSON.stringify({
        from: String(timeRange.fromMs),
        to: String(timeRange.toMs),
        queries,
      }),
    },
  );

  return {
    datasourceId: datasource.id,
    datasourceType: datasource.type,
    baseUrl: resolveMonitorBaseUrl(datasource),
    kind: "query_panel",
    ref: params.ref,
    dashboard: {
      uid: parsedRef.uid,
      title: asString(dashboardData.title) ?? parsedRef.uid,
      folderTitle: asString(meta.folderTitle) ?? null,
    },
    panel: {
      ...summarizeGrafanaPanel(panelRef.panel),
      ...(panelRef.parentTitle ? { parentTitle: panelRef.parentTitle } : {}),
    },
    timeRange: {
      from: timeRange.fromIso,
      to: timeRange.toIso,
    },
    queryCount: queries.length,
    queries: queries.map(summarizeTarget),
    result: normalizeQueryFrames(asString(panelRef.panel.type) ?? "unknown", response, params.maxPoints ?? DEFAULT_MAX_POINTS),
    ...(params.format === "raw" ? { raw: sanitizeForJson(response) } : {}),
  };
}

export async function monitorQueryTraces(params: {
  datasource: DataSourceConfig;
  service?: string;
  serviceId?: string;
  serviceInstanceId?: string;
  endpointId?: string;
  traceId?: string;
  status?: "all" | "success" | "error";
  from?: string;
  to?: string;
  last?: string;
  minDurationMs?: number;
  maxDurationMs?: number;
  pageNum?: number;
  pageSize?: number;
  format?: "summary" | "raw";
}): Promise<Record<string, unknown>> {
  const { datasource } = params;
  if (!isSkywalkingDatasource(datasource)) {
    throw new Error("monitor_query_traces only supports SkyWalking datasources");
  }

  const resolvedService = await resolveSkywalkingService(datasource, {
    service: params.service,
    serviceId: params.serviceId,
    from: params.from,
    to: params.to,
    last: params.last ?? DEFAULT_LAST,
  });
  if (!params.traceId && !resolvedService && !params.serviceInstanceId && !params.endpointId) {
    throw new Error("traceId, service, serviceId, serviceInstanceId, or endpointId is required");
  }

  const traces = await skywalkingQueryBasicTraces(datasource, {
    traceId: params.traceId,
    serviceId: resolvedService?.id ?? params.serviceId,
    serviceInstanceId: params.serviceInstanceId,
    endpointId: params.endpointId,
    from: params.from,
    to: params.to,
    last: params.last ?? DEFAULT_LAST,
    status: params.status === "error" ? "ERROR" : params.status === "success" ? "SUCCESS" : "ALL",
    pageNum: params.pageNum,
    pageSize: params.pageSize,
    minDurationMs: params.minDurationMs,
    maxDurationMs: params.maxDurationMs,
  });

  return {
    datasourceId: datasource.id,
    datasourceType: datasource.type,
    baseUrl: resolveMonitorBaseUrl(datasource),
    kind: "query_traces",
    service: resolvedService ? summarizeSkywalkingService(resolvedService) : null,
    filters: {
      traceId: params.traceId ?? null,
      serviceId: resolvedService?.id ?? params.serviceId ?? null,
      serviceInstanceId: params.serviceInstanceId ?? null,
      endpointId: params.endpointId ?? null,
      status: params.status ?? "all",
      minDurationMs: params.minDurationMs ?? null,
      maxDurationMs: params.maxDurationMs ?? null,
    },
    timeRange: buildSkywalkingDuration({ from: params.from, to: params.to, last: params.last ?? DEFAULT_LAST }).timeRange,
    traceCount: traces.length,
    traces: traces.map(summarizeSkywalkingTrace),
    ...(params.format === "raw" ? { raw: sanitizeForJson(traces) } : {}),
  };
}
