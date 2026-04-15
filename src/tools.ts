import { existsSync, readdirSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod";
import { registerWorkspaceResources } from "./resources.js";
import { createToolRuntime, ToolRuntime } from "./runtime/index.js";
import { buildCatalogToolDefinitions } from "./tooling/catalog-tools.js";
import {
  LoadedToolModule,
  ToolModuleLoader,
  ToolModuleWatchStatus,
  ToolStorePackageRecord,
} from "./tooling/module-loader.js";
import {
  addRemoteSource,
  installRemotePackage,
  listRemotePackages,
  listRemoteSources,
  removeRemoteSource,
} from "./tooling/remote-store.js";
import {
  kafkaConsumerLag,
  kafkaTopicOffsets,
  logcenterSearch,
  mongoAggregateReadonly,
  mongoFindReadonly,
  sqlDescribeSchemaReadonly,
  sqlQueryReadonly,
} from "./datasources.js";
import {
  monitorQueryPanel,
  monitorQueryTraces,
  monitorRead,
  monitorSearch,
} from "./monitor-datasources.js";
import { ConfigStore, DataSourceConfig, ProjectConfig } from "./types.js";
import { emptyStore, readStoreSync } from "./store.js";
import { resolvePathUnderRoot, runLocalCommand, safeJsonStringify } from "./utils.js";

type ToolArgs = Record<string, unknown>;
type ToolSchema = Record<string, z.ZodTypeAny>;

interface ToolContext {
  getStore: () => ConfigStore;
  getStorePath: () => string;
  getRuntime: () => ToolRuntime;
  listTools: (options?: { includeHubTools?: boolean; category?: string }) => Array<Record<string, unknown>>;
  inspectTool: (toolName: string) => Record<string, unknown>;
  callTool: (toolName: string, args?: ToolArgs) => Promise<unknown>;
  stack: string[];
}

interface ToolDefinition {
  name: string;
  category: string;
  description: string;
  schema: ToolSchema;
  cacheTtlMs?: number;
  handler: (context: ToolContext, args: any) => Promise<unknown>;
}

interface ToolRegistry {
  definitions: ToolDefinition[];
  listTools: (options?: { includeHubTools?: boolean; category?: string }) => Array<Record<string, unknown>>;
  inspectTool: (toolName: string) => Record<string, unknown>;
  execute: (toolName: string, args?: ToolArgs, stack?: string[]) => Promise<unknown>;
  reloadModules: () => Promise<Record<string, unknown>>;
  startModuleWatch: (onReloaded?: (summary: Record<string, unknown>) => Promise<void> | void) => ToolModuleWatchStatus;
  getModuleWatchStatus: () => ToolModuleWatchStatus;
  listStorePackages: () => ToolStorePackageRecord[];
  listRemoteSources: () => ReturnType<typeof listRemoteSources>;
  listRemotePackages: () => Promise<Array<Record<string, unknown>>>;
  installModuleFromStore: (packageId: string, options?: { overwrite?: boolean }) => Promise<Record<string, unknown>>;
  installModuleFromRemote: (
    sourceId: string,
    packageId: string,
    options?: { overwrite?: boolean },
  ) => Promise<Record<string, unknown>>;
  setModuleEnabled: (moduleId: string, enabled: boolean) => Promise<Record<string, unknown>>;
  uninstallModule: (moduleId: string) => Promise<Record<string, unknown>>;
}

interface SyncEntityPreset {
  entityType: string;
  tableNames: string[];
  topicKeywords: string[];
  recommendedSqlHints: string[];
  recommendedCodeQueries: string[];
  recommendedLogQueries: string[];
}

function toTextResult(data: unknown) {
  return {
    content: [{ type: "text" as const, text: safeJsonStringify(data) }],
  };
}

function summarizeDatasource(datasource: DataSourceConfig) {
  const expiresAt = datasource.auth.expiresAt ?? null;
  const expired = expiresAt ? Date.parse(expiresAt) <= Date.now() : false;
  return {
    id: datasource.id,
    label: datasource.label,
    type: datasource.type,
    role: datasource.role,
    usageNotes: datasource.usageNotes,
    description: datasource.description,
    enabled: datasource.enabled,
    database: datasource.connection.database ?? null,
    host: datasource.connection.host ?? null,
    uri: datasource.connection.uri ?? null,
    dataView: datasource.connection.dataView ?? null,
    brokers: datasource.connection.brokers ?? [],
    projectIds: datasource.projectIds,
    expiresAt,
    expired,
  };
}

function workspaceKnowledgeBaseRoot(storePath: string): string {
  return path.join(storePath, "knowledge-base");
}

function workspaceMemoryRoot(storePath: string): string {
  return path.join(storePath, "memory");
}

function projectKnowledgeBaseRoot(storePath: string, projectId: string): string {
  return path.join(storePath, "projects", projectId, "knowledge-base");
}

function projectMemoryRoot(storePath: string, projectId: string): string {
  return path.join(storePath, "projects", projectId, "memory");
}

function getEnabledProjects(store: ConfigStore): ProjectConfig[] {
  return store.projects.filter((project) => project.enabled);
}

function resolveProject(store: ConfigStore, projectId?: string): ProjectConfig {
  const enabledProjects = getEnabledProjects(store);
  if (projectId) {
    const matched = enabledProjects.find((project) => project.id === projectId);
    if (!matched) {
      throw new Error(`Project not found or disabled: ${projectId}`);
    }
    return matched;
  }

  if (store.activeProjectId) {
    const active = enabledProjects.find((project) => project.id === store.activeProjectId);
    if (active) {
      return active;
    }
  }

  if (enabledProjects.length === 1) {
    return enabledProjects[0];
  }

  throw new Error("projectId is required when multiple enabled projects exist and no active project is set");
}

function parseOptionalStringArrayJson(value: string | undefined, label: string): string[] {
  if (!value?.trim()) {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (error) {
    throw new Error(`${label} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON array`);
  }
  return parsed.map((item) => String(item ?? "").trim()).filter(Boolean);
}

function resolveProjectScope(
  store: ConfigStore,
  options?: {
    projectId?: string;
    projectIdsJson?: string;
    allWhenOmitted?: boolean;
  },
): ProjectConfig[] {
  const explicitIds = parseOptionalStringArrayJson(options?.projectIdsJson, "projectIdsJson");
  if (explicitIds.length > 0) {
    return explicitIds.map((projectId) => resolveProject(store, projectId));
  }
  if (options?.projectId?.trim()) {
    return [resolveProject(store, options.projectId)];
  }
  if (options?.allWhenOmitted) {
    return getEnabledProjects(store);
  }
  return [resolveProject(store)];
}

function listMarkdownFilesRecursive(root: string, relPrefix = ""): Array<Record<string, unknown>> {
  if (!path.isAbsolute(root) || !existsSyncSafe(root)) {
    return [];
  }
  const entries = new Array<Record<string, unknown>>();
  for (const entry of safeReadDir(root)) {
    const absPath = path.join(root, entry.name);
    const relPath = relPrefix ? path.posix.join(relPrefix, entry.name) : entry.name;
    if (entry.isDirectory()) {
      entries.push(...listMarkdownFilesRecursive(absPath, relPath));
      continue;
    }
    if (!entry.isFile() || !entry.name.endsWith(".md")) {
      continue;
    }
    entries.push({
      relPath,
      absPath,
      name: entry.name,
    });
  }
  return entries;
}

function existsSyncSafe(targetPath: string): boolean {
  try {
    return existsSync(targetPath);
  } catch {
    return false;
  }
}

function safeReadDir(dirPath: string): Array<{ name: string; isDirectory: () => boolean; isFile: () => boolean }> {
  try {
    return readdirSync(dirPath, { withFileTypes: true }).map((entry) => ({
      name: entry.name,
      isDirectory: () => entry.isDirectory(),
      isFile: () => entry.isFile(),
    }));
  } catch {
    return [];
  }
}

function getProjectDatasources(store: ConfigStore, project: ProjectConfig): DataSourceConfig[] {
  const boundIds = new Set(project.datasourceIds);
  return store.datasources.filter((datasource) => datasource.enabled && boundIds.has(datasource.id));
}

function getDatasourcesForProjects(store: ConfigStore, projects: ProjectConfig[]): DataSourceConfig[] {
  const datasourceIds = new Set(projects.flatMap((project) => project.datasourceIds));
  return store.datasources.filter((datasource) => datasource.enabled && datasourceIds.has(datasource.id));
}

function uniqueByKey<T>(items: T[], getKey: (item: T) => string): T[] {
  const seen = new Set<string>();
  const results: T[] = [];
  for (const item of items) {
    const key = getKey(item);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    results.push(item);
  }
  return results;
}

function resolveKnowledgeBaseRoots(
  storePath: string,
  store: ConfigStore,
  options?: { projectId?: string; projectIdsJson?: string },
): Array<{ scope: "workspace" | "project"; projectId?: string; root: string }> {
  const projects = options?.projectId || options?.projectIdsJson
    ? resolveProjectScope(store, { projectId: options?.projectId, projectIdsJson: options?.projectIdsJson })
    : [];
  const roots: Array<{ scope: "workspace" | "project"; projectId?: string; root: string }> = [
    { scope: "workspace", root: workspaceKnowledgeBaseRoot(storePath) },
  ];
  for (const project of projects) {
    roots.push({ scope: "project", projectId: project.id, root: projectKnowledgeBaseRoot(storePath, project.id) });
  }
  return roots;
}

function resolveMemoryRoots(
  storePath: string,
  store: ConfigStore,
  options?: { projectId?: string; projectIdsJson?: string },
): Array<{ scope: "workspace" | "project"; projectId?: string; root: string }> {
  const projects = options?.projectId || options?.projectIdsJson
    ? resolveProjectScope(store, { projectId: options?.projectId, projectIdsJson: options?.projectIdsJson })
    : [];
  const roots: Array<{ scope: "workspace" | "project"; projectId?: string; root: string }> = [
    { scope: "workspace", root: workspaceMemoryRoot(storePath) },
  ];
  for (const project of projects) {
    roots.push({ scope: "project", projectId: project.id, root: projectMemoryRoot(storePath, project.id) });
  }
  return roots;
}

async function readMarkdownDoc(absPath: string): Promise<Record<string, unknown>> {
  const content = await fs.readFile(absPath, "utf8");
  return {
    absPath,
    name: path.basename(absPath),
    content,
  };
}

function caseFileName(title: string): string {
  const base = title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64) || "case";
  return `${new Date().toISOString().replace(/[:.]/g, "-")}-${base}.md`;
}

function resolveDatasource(
  store: ConfigStore,
  datasourceId: string,
  projectId?: string,
): DataSourceConfig {
  if (projectId) {
    const project = resolveProject(store, projectId);
    const datasource = getProjectDatasources(store, project).find((item) => item.id === datasourceId);
    if (!datasource) {
      throw new Error(`Datasource ${datasourceId} is not bound to project ${project.id}`);
    }
    return datasource;
  }

  const datasource = store.datasources.find((item) => item.id === datasourceId && item.enabled);
  if (!datasource) {
    throw new Error(`Datasource not found or disabled: ${datasourceId}`);
  }
  return datasource;
}

function tokenizeIntentText(text: string): string[] {
  return Array.from(
    new Set(
      text
        .toLowerCase()
        .split(/[^a-z0-9_\u4e00-\u9fa5-]+/i)
        .map((item) => item.trim())
        .filter((item) => item.length >= 2),
    ),
  );
}

function scoreDatasourceForIntent(
  datasource: DataSourceConfig,
  project: ProjectConfig | null,
  datasourceType: string | undefined,
  intent: string,
  databaseHint: string | undefined,
): { score: number; reasons: string[] } {
  let score = 0;
  const reasons: string[] = [];
  const corpus = [
    datasource.id,
    datasource.label,
    datasource.type,
    datasource.role,
    datasource.usageNotes,
    datasource.description,
    datasource.connection.database ?? "",
    datasource.connection.host ?? "",
    ...(datasource.connection.brokers ?? []),
  ]
    .join(" ")
    .toLowerCase();

  if (project && datasource.projectIds.includes(project.id)) {
    score += 50;
    reasons.push(`绑定在项目 ${project.id}`);
  }

  if (datasourceType && datasource.type === datasourceType) {
    score += 120;
    reasons.push(`类型匹配 ${datasourceType}`);
  }

  if (databaseHint?.trim()) {
    const normalizedDbHint = databaseHint.trim().toLowerCase();
    if ((datasource.connection.database ?? "").toLowerCase().includes(normalizedDbHint)) {
      score += 45;
      reasons.push(`数据库提示命中 ${databaseHint}`);
    }
  }

  for (const term of tokenizeIntentText(intent)) {
    if (corpus.includes(term)) {
      const weight = term.length >= 6 ? 16 : 10;
      score += weight;
      reasons.push(`意图命中 ${term}`);
    }
  }

  if (/已确认|confirmed/i.test(`${datasource.usageNotes} ${datasource.description}`)) {
    score += 20;
    reasons.push("标记为已确认");
  }

  if (/候选|candidate/i.test(`${datasource.usageNotes} ${datasource.description}`)) {
    score -= 5;
    reasons.push("标记为候选");
  }

  if (datasource.enabled) {
    score += 10;
    reasons.push("数据源启用中");
  }

  return { score, reasons };
}

function resolveDatasourceCandidates(
  store: ConfigStore,
  options: {
    projectId?: string;
    datasourceType?: string;
    intent: string;
    databaseHint?: string;
    limit?: number;
  },
): Array<Record<string, unknown>> {
  const project = options.projectId ? resolveProject(store, options.projectId) : null;
  const pool = project ? getProjectDatasources(store, project) : store.datasources.filter((item) => item.enabled);

  return pool
    .map((datasource) => {
      const scored = scoreDatasourceForIntent(
        datasource,
        project,
        options.datasourceType,
        options.intent,
        options.databaseHint,
      );
      return {
        ...summarizeDatasource(datasource),
        score: scored.score,
        reasons: scored.reasons,
      };
    })
    .filter((item) => Number(item.score) > 0)
    .sort((left, right) => Number(right.score) - Number(left.score))
    .slice(0, options.limit ?? 5);
}

const syncEntityPresets: Record<string, SyncEntityPreset> = {
  lp: {
    entityType: "lp",
    tableNames: ["lp"],
    topicKeywords: ["doc_lp", "inventory_service.doc_lp", "lp"],
    recommendedSqlHints: ["inventory_service.doc_lp", "inventory_service.doc_location", "lp", "location", "inventory"],
    recommendedCodeQueries: ["wmsLpSync", "doc_lp", "sync/lp", "Not allowed to move ILP", "CANNOT_MOVE_LP"],
    recommendedLogQueries: ["doc_lp", "LP update failed", "Not allowed to move ILP", "x-request-id", "traceId"],
  },
  order: {
    entityType: "order",
    tableNames: ["order"],
    topicKeywords: ["doc_order", "wms.doc_order", "order"],
    recommendedSqlHints: ["doc_order", "doc_order_itemline", "order", "order_itemline", "inventory_lock"],
    recommendedCodeQueries: ["wmsOrderSync", "doc_order", "sync/order", "SyncFromWmsService order", "orderItemLine"],
    recommendedLogQueries: ["doc_order", "order sync", "sync-from-wms order", "x-request-id", "traceId"],
  },
  inventory: {
    entityType: "inventory",
    tableNames: ["inventory"],
    topicKeywords: ["doc_inventory", "inventory_service.doc_inventory", "inventory"],
    recommendedSqlHints: ["doc_inventory", "doc_inventory_lock", "inventory", "inventory_lock", "doc_lp"],
    recommendedCodeQueries: ["wmsInventorySync", "doc_inventory", "sync/inventory", "inventory lock", "inventory_service"],
    recommendedLogQueries: ["doc_inventory", "inventory sync", "inventory lock", "x-request-id", "traceId"],
  },
  receipt: {
    entityType: "receipt",
    tableNames: ["receipt"],
    topicKeywords: ["doc_receipt", "wms.doc_receipt", "receipt"],
    recommendedSqlHints: ["doc_receipt", "doc_receipt_itemline", "receipt", "receipt_itemline", "inventory"],
    recommendedCodeQueries: ["wmsReceiptSync", "doc_receipt", "sync/receipt", "receiptItemLine", "receipt"],
    recommendedLogQueries: ["doc_receipt", "receipt sync", "receipt itemline", "x-request-id", "traceId"],
  },
  item: {
    entityType: "item",
    tableNames: ["item", "def_item"],
    topicKeywords: ["def_item", "item"],
    recommendedSqlHints: ["def_item", "def_item_uom", "item", "item_uom"],
    recommendedCodeQueries: ["def_item", "item sync", "sync/item", "itemIntegration", "item master"],
    recommendedLogQueries: ["def_item", "item sync", "item master", "x-request-id", "traceId"],
  },
  customer: {
    entityType: "customer",
    tableNames: ["customer"],
    topicKeywords: ["customer", "org", "organization"],
    recommendedSqlHints: ["customer", "organization", "org"],
    recommendedCodeQueries: ["customer sync", "organization", "CustomerService", "no Customer found"],
    recommendedLogQueries: ["customer sync", "organization", "no Customer found", "x-request-id", "traceId"],
  },
  adjustment: {
    entityType: "adjustment",
    tableNames: ["adjustment"],
    topicKeywords: ["adjustment", "inventory_adjustment"],
    recommendedSqlHints: ["adjustment", "doc_inventory", "doc_lp", "location"],
    recommendedCodeQueries: ["adjustment", "AdjustmentService", "AdjustmentValidator", "sync/adjustment"],
    recommendedLogQueries: ["adjustment", "inventory adjustment", "AdjustmentService", "x-request-id", "traceId"],
  },
};

function getSyncEntityPreset(entityType: string | undefined): SyncEntityPreset {
  const normalized = String(entityType ?? "").trim().toLowerCase();
  return syncEntityPresets[normalized] ?? {
    entityType: normalized || "generic",
    tableNames: normalized ? [normalized] : [],
    topicKeywords: normalized ? [normalized] : [],
    recommendedSqlHints: normalized ? [normalized] : [],
    recommendedCodeQueries: normalized ? [normalized] : [],
    recommendedLogQueries: normalized ? [normalized] : [],
  };
}

function uniqueStrings(values: Array<string | undefined | null>): string[] {
  return Array.from(
    new Set(
      values
        .map((item) => String(item ?? "").trim())
        .filter(Boolean),
    ),
  );
}

function buildDataEvidenceSummary(result: {
  datasource: Record<string, unknown>;
  database: string;
  collection: string;
  appliedFilter: Record<string, unknown>;
  samples: unknown;
  errorSummary: unknown;
  tableTopicSummary: unknown;
}): Record<string, unknown> {
  const samples = Array.isArray(result.samples) ? result.samples : [];
  const errorSummary = Array.isArray(result.errorSummary) ? result.errorSummary : [];
  const tableTopicSummary = Array.isArray(result.tableTopicSummary) ? result.tableTopicSummary : [];
  const firstSample =
    samples.length > 0 && samples[0] && typeof samples[0] === "object"
      ? (samples[0] as Record<string, unknown>)
      : null;
  const topError =
    errorSummary.length > 0 && errorSummary[0] && typeof errorSummary[0] === "object"
      ? (errorSummary[0] as Record<string, unknown>)
      : null;
  const topTableTopic =
    tableTopicSummary.length > 0 && tableTopicSummary[0] && typeof tableTopicSummary[0] === "object"
      ? (tableTopicSummary[0] as Record<string, unknown>)
      : null;

  return {
    source: {
      datasourceId: result.datasource.id ?? null,
      database: result.database,
      collection: result.collection,
    },
    appliedFilter: result.appliedFilter,
    sampleIds: samples
      .slice(0, 5)
      .map((item) => (item && typeof item === "object" ? (item as Record<string, unknown>)._id ?? null : null))
      .filter(Boolean),
    samplePrimaryIds: samples
      .slice(0, 5)
      .map((item) =>
        item && typeof item === "object" ? (item as Record<string, unknown>).primaryId ?? null : null,
      )
      .filter(Boolean),
    dominantErrorMessage: topError?._id ?? null,
    dominantErrorCount: topError?.count ?? null,
    latestUpdatedWhen: topError?.latestUpdatedWhen ?? firstSample?.updatedWhen ?? null,
    dominantTableTopic: topTableTopic?._id ?? null,
    firstSample,
    citationGuidance: [
      "回复时优先引用 source.datasourceId、database、collection。",
      "至少引用 appliedFilter 和一条 firstSample 或 sampleIds。",
      "如果下结论是‘同一类报错’，必须引用 dominantErrorMessage 和 dominantErrorCount。",
    ],
  };
}

async function traceSyncFailureCore(
  store: ConfigStore,
  args: {
    projectId?: string;
    datasourceId?: string;
    database?: string;
    collection?: string;
    tableName?: string;
    tableNames?: string[];
    primaryId?: string;
    topic?: string;
    errorPattern?: string;
    retryGte?: number;
    limit?: number;
  },
): Promise<Record<string, unknown>> {
  const project = resolveProject(store, args.projectId);
  const datasource =
    typeof args.datasourceId === "string" && args.datasourceId.trim()
      ? resolveDatasource(store, args.datasourceId, project.id)
      : (() => {
          const candidates = resolveDatasourceCandidates(store, {
            projectId: project.id,
            datasourceType: "mongo",
            intent: [
              "sync_failed_info",
              "sync failure",
              args.tableName,
              ...(args.tableNames ?? []),
              args.topic,
              args.errorPattern,
              "retry",
              "mongo",
            ]
              .filter(Boolean)
              .join(" "),
            databaseHint: args.database,
            limit: 1,
          });
          const resolvedId = String(candidates[0]?.id ?? "").trim();
          if (!resolvedId) {
            throw new Error(`No suitable Mongo datasource found for project ${project.id}`);
          }
          return resolveDatasource(store, resolvedId, project.id);
        })();

  const database = args.database?.trim() || datasource.connection.database || undefined;
  if (!database) {
    throw new Error(`Mongo datasource ${datasource.id} has no default database; provide database explicitly`);
  }

  const collection = String(args.collection ?? "sync_failed_info");
  const tableNames = uniqueStrings([args.tableName, ...(args.tableNames ?? [])]);
  const matchFilter: Record<string, unknown> = {};
  if (tableNames.length === 1) {
    matchFilter.tableName = tableNames[0];
  } else if (tableNames.length > 1) {
    matchFilter.tableName = { $in: tableNames };
  }
  if (typeof args.primaryId === "string" && args.primaryId.trim()) {
    matchFilter.primaryId = args.primaryId.trim();
  }
  if (typeof args.topic === "string" && args.topic.trim()) {
    matchFilter.topic = args.topic.trim();
  }
  if (typeof args.retryGte === "number" && args.retryGte > 0) {
    matchFilter.retryCount = { $gte: args.retryGte };
  }
  if (typeof args.errorPattern === "string" && args.errorPattern.trim()) {
    matchFilter.errorMessage = { $regex: args.errorPattern.trim(), $options: "i" };
  }

  const findResult = await mongoFindReadonly({
    datasource,
    database,
    collection,
    filterJson: JSON.stringify(matchFilter),
    projectionJson: JSON.stringify({
      _id: 1,
      primaryId: 1,
      tableName: 1,
      topic: 1,
      retryCount: 1,
      errorMessage: 1,
      updatedWhen: 1,
      messageBody: 1,
    }),
    sortJson: JSON.stringify({ updatedWhen: -1 }),
    limit: Number(args.limit ?? 20),
  });

  const errorSummary = await mongoAggregateReadonly({
    datasource,
    database,
    collection,
    pipelineJson: JSON.stringify([
      { $match: matchFilter },
      {
        $group: {
          _id: "$errorMessage",
          count: { $sum: 1 },
          latestUpdatedWhen: { $max: "$updatedWhen" },
        },
      },
      { $sort: { count: -1, latestUpdatedWhen: -1 } },
      { $limit: 10 },
    ]),
  });

  const tableTopicSummary = await mongoAggregateReadonly({
    datasource,
    database,
    collection,
    pipelineJson: JSON.stringify([
      { $match: matchFilter },
      {
        $group: {
          _id: { tableName: "$tableName", topic: "$topic" },
          count: { $sum: 1 },
          latestUpdatedWhen: { $max: "$updatedWhen" },
        },
      },
      { $sort: { count: -1, latestUpdatedWhen: -1 } },
      { $limit: 10 },
    ]),
  });

  return {
    projectId: project.id,
    datasource: summarizeDatasource(datasource),
    database,
    collection,
    appliedFilter: matchFilter,
    sampleCount: findResult.rowCount,
    samples: findResult.rows,
    errorSummary: errorSummary.rows,
    tableTopicSummary: tableTopicSummary.rows,
    dataEvidence: buildDataEvidenceSummary({
      datasource: summarizeDatasource(datasource),
      database,
      collection,
      appliedFilter: matchFilter,
      samples: findResult.rows,
      errorSummary: errorSummary.rows,
      tableTopicSummary: tableTopicSummary.rows,
    }),
  };
}

async function collectCodeEvidenceForSync(project: ProjectConfig, queries: string[]): Promise<Record<string, unknown>> {
  if (project.repoRoots.length === 0) {
    return {
      skipped: true,
      reason: `Project ${project.id} has no repo roots`,
      searchQueries: queries,
      matchCount: 0,
      topMatches: [],
    };
  }
  const normalizedQueries = uniqueStrings(queries).slice(0, 6);
  if (normalizedQueries.length === 0) {
    return {
      skipped: true,
      reason: "No code evidence queries provided",
      searchQueries: [],
      matchCount: 0,
      topMatches: [],
    };
  }
  const matches = await collectStructuredSearchEvidence({
    roots: project.repoRoots,
    queries: normalizedQueries,
    limitPerQuery: 5,
    overallLimit: 20,
    fixedString: true,
  });
  return buildCodeEvidenceSummary({
    project,
    queries: normalizedQueries,
    matches,
  });
}

async function collectLogEvidenceForSync(project: ProjectConfig, queries: string[]): Promise<Record<string, unknown>> {
  if (project.logRoots.length === 0) {
    return {
      skipped: true,
      reason: `Project ${project.id} has no log roots`,
      searchQueries: queries,
      matchCount: 0,
      topMatches: [],
    };
  }
  const normalizedQueries = uniqueStrings(queries).slice(0, 6);
  if (normalizedQueries.length === 0) {
    return {
      skipped: true,
      reason: "No log evidence queries provided",
      searchQueries: [],
      matchCount: 0,
      topMatches: [],
    };
  }
  const matches = await collectStructuredSearchEvidence({
    roots: project.logRoots,
    queries: normalizedQueries,
    limitPerQuery: 5,
    overallLimit: 20,
    fixedString: true,
  });
  return buildLogEvidenceSummary({
    project,
    queries: normalizedQueries,
    matches,
  });
}

async function searchAcrossRoots(
  roots: string[],
  query: string,
  glob: string | undefined,
  limit: number,
  options?: { fixedString?: boolean },
): Promise<Array<Record<string, unknown>>> {
  const results: Array<Record<string, unknown>> = [];
  for (const root of roots) {
    if (/^https?:\/\//i.test(root.trim())) {
      throw new Error(
        `Search roots must be local filesystem paths. Got remote URL: ${root}. Use a datasource-backed tool such as logcenter_search instead.`,
      );
    }
    const rootStat = await fs.stat(root).catch(() => null);
    if (!rootStat) {
      continue;
    }
    const args = ["--line-number", "--no-heading", "--color", "never", "--hidden", "-g", "!.git"];
    if (options?.fixedString) {
      args.push("-F");
    }
    if (glob && rootStat.isDirectory()) {
      args.push("-g", glob);
    }
    args.push(query, ".");
    let cwd = root;
    if (rootStat.isFile()) {
      cwd = path.dirname(root);
      args.splice(args.length - 1, 1, path.basename(root));
    }
    const result = await runLocalCommand("rg", args, cwd, 20_000);
    if (result.exitCode !== 0 && result.exitCode !== 1) {
      throw new Error(result.stderr || `search failed in ${root}`);
    }
    const lines = result.stdout.split(/\r?\n/).filter(Boolean);
    for (const line of lines) {
      const match = line.match(/^(.+?):(\d+):(.*)$/);
      if (!match) {
        continue;
      }
      const resolvedPath = rootStat.isFile()
        ? path.resolve(cwd, match[1])
        : path.resolve(root, match[1]);
      results.push({
        repoRoot: root,
        filePath: resolvedPath,
        lineNumber: Number(match[2]),
        preview: match[3],
      });
      if (results.length >= limit) {
        return results;
      }
    }
  }
  return results;
}

function shortErrorSearchTerm(value: unknown): string | null {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  if (!text) {
    return null;
  }
  const firstLine = text.split("\n")[0].trim();
  return firstLine.slice(0, 120) || null;
}

function summarizeSearchMatches(
  matches: Array<Record<string, unknown>>,
  key: "filePath" | "repoRoot",
  limit: number,
): string[] {
  return Array.from(
    new Set(
      matches
        .map((item) => String(item[key] ?? "").trim())
        .filter(Boolean),
    ),
  ).slice(0, limit);
}

function buildCodeEvidenceSummary(result: {
  project: ProjectConfig;
  queries: string[];
  matches: Array<Record<string, unknown>>;
}): Record<string, unknown> {
  return {
    projectId: result.project.id,
    repoRoots: result.project.repoRoots,
    searchQueries: result.queries,
    matchCount: result.matches.length,
    topMatches: result.matches.slice(0, 8),
    files: summarizeSearchMatches(result.matches, "filePath", 8),
    citationGuidance: [
      "回复时至少引用一个 filePath 和 lineNumber。",
      "说明命中的查询词是哪个 searchQueries 项。",
      "如果代码证据不足以支撑结论，要明确写‘代码证据不足’。",
    ],
  };
}

function buildLogEvidenceSummary(result: {
  project: ProjectConfig;
  queries: string[];
  matches: Array<Record<string, unknown>>;
}): Record<string, unknown> {
  return {
    projectId: result.project.id,
    logRoots: result.project.logRoots,
    searchQueries: result.queries,
    matchCount: result.matches.length,
    topMatches: result.matches.slice(0, 8),
    files: summarizeSearchMatches(result.matches, "filePath", 8),
    citationGuidance: [
      "回复时至少引用一个日志文件路径和一条 preview 命中。",
      "说明使用了哪个 searchQueries 项进行检索。",
      "如果没有命中，明确写‘未找到日志证据’。",
    ],
  };
}

async function collectStructuredSearchEvidence(options: {
  roots: string[];
  queries: string[];
  limitPerQuery: number;
  overallLimit: number;
  fixedString?: boolean;
}): Promise<Array<Record<string, unknown>>> {
  const seen = new Set<string>();
  const results: Array<Record<string, unknown>> = [];
  for (const query of options.queries) {
    if (results.length >= options.overallLimit) {
      break;
    }
    const hits = await searchAcrossRoots(
      options.roots,
      query,
      undefined,
      options.limitPerQuery,
      { fixedString: options.fixedString },
    );
    for (const hit of hits) {
      const fingerprint = [
        String(hit.repoRoot ?? ""),
        String(hit.filePath ?? ""),
        String(hit.lineNumber ?? ""),
        String(hit.preview ?? ""),
      ].join("|");
      if (seen.has(fingerprint)) {
        continue;
      }
      seen.add(fingerprint);
      results.push({
        ...hit,
        matchedQuery: query,
      });
      if (results.length >= options.overallLimit) {
        break;
      }
    }
  }
  return results;
}

async function resolveReadableFile(
  project: ProjectConfig,
  filePath: string,
): Promise<string> {
  const trimmed = filePath.trim();
  if (!trimmed) {
    throw new Error("filePath is empty");
  }

  if (path.isAbsolute(trimmed)) {
    for (const root of project.repoRoots) {
      const resolvedRoot = path.resolve(root);
      if (trimmed === resolvedRoot || trimmed.startsWith(`${resolvedRoot}${path.sep}`)) {
        return trimmed;
      }
    }
    throw new Error(`Absolute path is outside project roots: ${trimmed}`);
  }

  for (const root of project.repoRoots) {
    const resolved = resolvePathUnderRoot(root, trimmed);
    try {
      await fs.access(resolved);
      return resolved;
    } catch {
      continue;
    }
  }

  throw new Error(`File not found in project roots: ${trimmed}`);
}

function parseJsonObject(value: string | undefined, label: string): Record<string, unknown> {
  if (!value?.trim()) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch (error) {
    throw new Error(`${label} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function parseExecSteps(stepsJson: string): Array<{ tool: string; args: ToolArgs; note?: string }> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(stepsJson);
  } catch (error) {
    throw new Error(`stepsJson must be valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!Array.isArray(parsed)) {
    throw new Error("stepsJson must be a JSON array");
  }
  return parsed.map((item, index) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      throw new Error(`stepsJson[${index}] must be an object`);
    }
    const step = item as Record<string, unknown>;
    const tool = String(step.tool ?? "").trim();
    if (!tool) {
      throw new Error(`stepsJson[${index}].tool is required`);
    }
    const args = step.args;
    if (args !== undefined && (!args || typeof args !== "object" || Array.isArray(args))) {
      throw new Error(`stepsJson[${index}].args must be a JSON object when provided`);
    }
    const note = String(step.note ?? "").trim() || undefined;
    return {
      tool,
      args: (args as ToolArgs | undefined) ?? {},
      note,
    };
  });
}

function unwrapZodType(schema: z.ZodTypeAny): { schema: z.ZodTypeAny; required: boolean } {
  let current: z.ZodTypeAny = schema;
  let required = true;
  while (
    current instanceof z.ZodOptional ||
    current instanceof z.ZodNullable ||
    current instanceof z.ZodDefault
  ) {
    if (current instanceof z.ZodOptional || current instanceof z.ZodDefault) {
      required = false;
    }
    current = (current as unknown as { unwrap: () => z.ZodTypeAny }).unwrap();
  }
  return { schema: current, required };
}

function describeZodType(schema: z.ZodTypeAny): Record<string, unknown> {
  const { schema: unwrapped, required } = unwrapZodType(schema);
  if (unwrapped instanceof z.ZodString) {
    return { type: "string", required };
  }
  if (unwrapped instanceof z.ZodNumber) {
    return { type: "number", required };
  }
  if (unwrapped instanceof z.ZodBoolean) {
    return { type: "boolean", required };
  }
  if (unwrapped instanceof z.ZodArray) {
    return {
      type: "array",
      required,
      items: describeZodType(unwrapped.element as z.ZodTypeAny),
    };
  }
  if (unwrapped instanceof z.ZodObject) {
    return {
      type: "object",
      required,
      properties: Object.fromEntries(
        Object.entries(unwrapped.shape).map(([key, value]) => [key, describeZodType(value as z.ZodTypeAny)]),
      ),
    };
  }
  if (unwrapped instanceof z.ZodEnum) {
    return {
      type: "enum",
      required,
      values: [...unwrapped.options],
    };
  }
  return {
    type: unwrapped.constructor.name.replace(/^Zod/, "").toLowerCase() || "unknown",
    required,
  };
}

function buildToolOverview(definition: ToolDefinition) {
  return {
    name: definition.name,
    category: definition.category,
    description: definition.description,
    parameters: Object.entries(definition.schema).map(([name, schema]) => ({
      name,
      ...describeZodType(schema),
    })),
  };
}

function assertHubTargetAllowed(toolName: string) {
  if (toolName === "hub_invoke" || toolName === "hub_exec") {
    throw new Error(`${toolName} cannot be invoked from hub meta tools`);
  }
}

function buildBuiltinCoreToolDefinitions(getStore: () => ConfigStore, getStorePath: () => string): ToolDefinition[] {
  return [
    {
      name: "sql_query_readonly",
      category: "sql",
      description:
        "Run read-only SQL against a configured datasource. Only SELECT/WITH/SHOW/DESCRIBE/EXPLAIN are allowed.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        sql: z.string().min(1),
        paramsJson: z.string().optional(),
      },
      handler: async (_context, { datasourceId, projectId, sql, paramsJson }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        let params: unknown[] = [];
        if (paramsJson?.trim()) {
          const parsed = JSON.parse(paramsJson);
          if (!Array.isArray(parsed)) {
            throw new Error("paramsJson must be a JSON array");
          }
          params = parsed;
        }
        return await sqlQueryReadonly(datasource, sql, params);
      },
    },
    {
      name: "sql_describe_schema_readonly",
      category: "sql",
      description:
        "Inspect SQL schema metadata in a read-only way. Use this to list tables for a schema or inspect columns for a specific table before writing SQL queries.",
      cacheTtlMs: 300_000,
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        schemaName: z.string().optional(),
        tableName: z.string().optional(),
        limit: z.number().int().positive().max(500).default(100),
      },
      handler: async (_context, { datasourceId, projectId, schemaName, tableName, limit }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await sqlDescribeSchemaReadonly({
          datasource,
          schemaName,
          tableName,
          limit,
        });
      },
    },
    {
      name: "mongo_find_readonly",
      category: "mongo",
      description:
        "Run a read-only Mongo find query against a configured datasource. Works with both modern and legacy Mongo deployments.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        database: z.string().optional(),
        collection: z.string().min(1),
        filterJson: z.string().optional(),
        projectionJson: z.string().optional(),
        sortJson: z.string().optional(),
        limit: z.number().int().positive().max(500).optional(),
      },
      handler: async (_context, args) => {
        const store = getStore();
        const datasource = resolveDatasource(store, String(args.datasourceId), args.projectId as string | undefined);
        return await mongoFindReadonly({
          datasource,
          database: args.database as string | undefined,
          collection: String(args.collection),
          filterJson: args.filterJson as string | undefined,
          projectionJson: args.projectionJson as string | undefined,
          sortJson: args.sortJson as string | undefined,
          limit: args.limit as number | undefined,
        });
      },
    },
    {
      name: "mongo_aggregate_readonly",
      category: "mongo",
      description:
        "Run a read-only Mongo aggregation pipeline against a configured datasource. Use for grouped diagnostics and counts.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        database: z.string().optional(),
        collection: z.string().min(1),
        pipelineJson: z.string().min(2),
      },
      handler: async (_context, args) => {
        const store = getStore();
        const datasource = resolveDatasource(store, String(args.datasourceId), args.projectId as string | undefined);
        return await mongoAggregateReadonly({
          datasource,
          database: args.database as string | undefined,
          collection: String(args.collection),
          pipelineJson: String(args.pipelineJson),
        });
      },
    },
    {
      name: "trace_sync_failure",
      category: "mongo",
      description:
        "Trace sync_failed_info-style failures with one higher-level call. Automatically resolves the best Mongo datasource for the project when datasourceId is omitted.",
      schema: {
        projectId: z.string().optional(),
        datasourceId: z.string().optional(),
        database: z.string().optional(),
        collection: z.string().default("sync_failed_info"),
        tableName: z.string().optional(),
        primaryId: z.string().optional(),
        topic: z.string().optional(),
        errorPattern: z.string().optional(),
        retryGte: z.number().int().nonnegative().default(1),
        limit: z.number().int().positive().max(100).default(20),
      },
      handler: async (_context, args) => {
        const store = getStore();
        const project = resolveProject(store, args.projectId as string | undefined);
        const result = await traceSyncFailureCore(store, {
          projectId: project.id,
          datasourceId: args.datasourceId as string | undefined,
          database: args.database as string | undefined,
          collection: String(args.collection),
          tableName: args.tableName as string | undefined,
          primaryId: args.primaryId as string | undefined,
          topic: args.topic as string | undefined,
          errorPattern: args.errorPattern as string | undefined,
          retryGte: Number(args.retryGte),
          limit: Number(args.limit),
        });
        const dominantErrorMessage =
          result.dataEvidence && typeof result.dataEvidence === "object"
            ? shortErrorSearchTerm((result.dataEvidence as Record<string, unknown>).dominantErrorMessage)
            : null;
        const samplePrimaryIds =
          result.dataEvidence && typeof result.dataEvidence === "object"
            ? (((result.dataEvidence as Record<string, unknown>).samplePrimaryIds as unknown[]) ?? [])
                .map((item) => String(item ?? "").trim())
                .filter(Boolean)
            : [];
        const codeEvidenceQueries = uniqueStrings([
          String(args.tableName ?? ""),
          String(args.topic ?? ""),
          dominantErrorMessage,
          "sync_failed_info",
        ]);
        const logEvidenceQueries = uniqueStrings([
          String(args.primaryId ?? ""),
          ...samplePrimaryIds.slice(0, 3),
          dominantErrorMessage,
          String(args.topic ?? ""),
          String(args.tableName ?? ""),
        ]);
        const [codeEvidence, logEvidence] = await Promise.all([
          collectCodeEvidenceForSync(project, codeEvidenceQueries),
          collectLogEvidenceForSync(project, logEvidenceQueries),
        ]);
        return {
          ...result,
          codeEvidence,
          logEvidence,
          recommendedNextTools: [
            "project_playbook",
            "repo_search",
            "repo_read_file",
            "sql_query_readonly",
          ],
        };
      },
    },
    {
      name: "trace_sync_entity_failure",
      category: "mongo",
      description:
        "Trace sync failures by business entity type such as lp, order, inventory, receipt, item, customer, or adjustment. This is the preferred high-level entrypoint for sync_failed_info investigations.",
      schema: {
        projectId: z.string().optional(),
        datasourceId: z.string().optional(),
        entityType: z.enum(["lp", "order", "inventory", "receipt", "item", "customer", "adjustment"]),
        primaryId: z.string().optional(),
        topic: z.string().optional(),
        errorPattern: z.string().optional(),
        database: z.string().optional(),
        retryGte: z.number().int().nonnegative().default(1),
        limit: z.number().int().positive().max(100).default(20),
        extraTableNamesJson: z.string().optional(),
      },
      handler: async (_context, args) => {
        const store = getStore();
        const project = resolveProject(store, args.projectId as string | undefined);
        const preset = getSyncEntityPreset(String(args.entityType));
        let extraTableNames: string[] = [];
        if (typeof args.extraTableNamesJson === "string" && args.extraTableNamesJson.trim()) {
          const parsed = JSON.parse(args.extraTableNamesJson);
          if (!Array.isArray(parsed)) {
            throw new Error("extraTableNamesJson must be a JSON array");
          }
          extraTableNames = parsed.map((item) => String(item ?? "").trim()).filter(Boolean);
        }
        const tableNames = uniqueStrings([...preset.tableNames, ...extraTableNames]);
        const result = await traceSyncFailureCore(store, {
          projectId: project.id,
          datasourceId: args.datasourceId as string | undefined,
          database: args.database as string | undefined,
          collection: "sync_failed_info",
          tableNames,
          primaryId: args.primaryId as string | undefined,
          topic: args.topic as string | undefined,
          errorPattern: args.errorPattern as string | undefined,
          retryGte: Number(args.retryGte),
          limit: Number(args.limit),
        });
        const dominantErrorMessage =
          result.dataEvidence && typeof result.dataEvidence === "object"
            ? shortErrorSearchTerm((result.dataEvidence as Record<string, unknown>).dominantErrorMessage)
            : null;
        const samplePrimaryIds =
          result.dataEvidence && typeof result.dataEvidence === "object"
            ? (((result.dataEvidence as Record<string, unknown>).samplePrimaryIds as unknown[]) ?? [])
                .map((item) => String(item ?? "").trim())
                .filter(Boolean)
            : [];
        const codeEvidenceQueries = uniqueStrings([
          ...preset.recommendedCodeQueries,
          dominantErrorMessage,
          String(args.topic ?? ""),
        ]);
        const logEvidenceQueries = uniqueStrings([
          ...preset.recommendedLogQueries,
          ...preset.topicKeywords,
          String(args.primaryId ?? ""),
          ...samplePrimaryIds.slice(0, 3),
          dominantErrorMessage,
          String(args.topic ?? ""),
        ]);
        const [codeEvidence, logEvidence] = await Promise.all([
          collectCodeEvidenceForSync(project, codeEvidenceQueries),
          collectLogEvidenceForSync(project, logEvidenceQueries),
        ]);

        return {
          ...result,
          entityType: preset.entityType,
          codeEvidence,
          logEvidence,
          entityPreset: {
            tableNames: preset.tableNames,
            topicKeywords: preset.topicKeywords,
            recommendedSqlHints: preset.recommendedSqlHints,
            recommendedCodeQueries: preset.recommendedCodeQueries,
            recommendedLogQueries: preset.recommendedLogQueries,
          },
          recommendedNextTools: [
            "project_playbook",
            "resolve_datasource_for_intent",
            "repo_search",
            "repo_read_file",
            "sql_query_readonly",
            "sql_describe_schema_readonly",
          ],
          recommendedNextInputs: {
            codeQueries: preset.recommendedCodeQueries,
            logQueries: preset.recommendedLogQueries,
            sqlHints: preset.recommendedSqlHints,
            topicKeywords: preset.topicKeywords,
          },
        };
      },
    },
    {
      name: "kafka_topic_offsets",
      category: "kafka",
      description:
        "Fetch topic offsets from a configured Kafka datasource. Use to inspect whether messages exist on a topic and how partitions are moving.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        topic: z.string().min(1),
      },
      handler: async (_context, { datasourceId, projectId, topic }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await kafkaTopicOffsets(datasource, topic);
      },
    },
    {
      name: "kafka_consumer_lag",
      category: "kafka",
      description:
        "Fetch consumer lag from a configured Kafka datasource. Use to confirm whether a sync pipeline is stuck or delayed.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        groupId: z.string().min(1),
        topic: z.string().optional(),
      },
      handler: async (_context, { datasourceId, projectId, groupId, topic }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await kafkaConsumerLag(datasource, groupId, topic);
      },
    },
    {
      name: "logcenter_search",
      category: "log",
      description:
        "Search a remote Kibana/logcenter datasource through its configured data view. Use this when project logs live in logcenter rather than local files.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        dataView: z.string().optional(),
        query: z.string().optional(),
        from: z.string().optional(),
        to: z.string().optional(),
        limit: z.number().int().positive().max(200).default(50),
      },
      handler: async (_context, { datasourceId, projectId, dataView, query, from, to, limit }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await logcenterSearch({
          datasource,
          dataView: dataView as string | undefined,
          query: query as string | undefined,
          from: from as string | undefined,
          to: to as string | undefined,
          limit,
        });
      },
    },
    {
      name: "monitor_search",
      category: "monitor",
      description:
        "Search monitor resources. Grafana returns dashboards/folders/datasources; SkyWalking returns service candidates.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        query: z.string().optional(),
        kinds: z.array(z.enum(["dashboard", "folder", "datasource", "service"])).optional(),
        maxResults: z.number().int().positive().max(50).default(20),
      },
      handler: async (_context, { datasourceId, projectId, query, kinds, maxResults }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await monitorSearch({
          datasource,
          query: query as string | undefined,
          kinds: kinds as Array<"dashboard" | "folder" | "datasource" | "service"> | undefined,
          maxResults,
        });
      },
    },
    {
      name: "monitor_read",
      category: "monitor",
      description:
        "Read monitor resource details. Grafana supports dashboard/panel/folder/datasource; SkyWalking supports service/trace.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        ref: z.string().min(1),
        maxPanels: z.number().int().positive().max(50).default(20),
      },
      handler: async (_context, { datasourceId, projectId, ref, maxPanels }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await monitorRead({
          datasource,
          ref,
          maxPanels,
        });
      },
    },
    {
      name: "monitor_query_panel",
      category: "monitor",
      description:
        "Execute an existing Grafana panel query through the configured monitor datasource and return a normalized summary.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        ref: z.string().min(1),
        datasource: z.string().optional(),
        variablesJson: z.string().optional(),
        from: z.string().optional(),
        to: z.string().optional(),
        last: z.string().optional(),
        format: z.enum(["summary", "raw"]).default("summary"),
        maxPoints: z.number().int().positive().max(100).default(20),
        maxDataPoints: z.number().int().positive().max(2000).default(200),
      },
      handler: async (_context, { datasourceId, projectId, ref, datasource: datasourceRef, variablesJson, from, to, last, format, maxPoints, maxDataPoints }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await monitorQueryPanel({
          datasource,
          ref,
          datasourceRef: datasourceRef as string | undefined,
          variables: parseJsonObject(variablesJson as string | undefined, "variablesJson"),
          from: from as string | undefined,
          to: to as string | undefined,
          last: last as string | undefined,
          format,
          maxPoints,
          maxDataPoints,
        });
      },
    },
    {
      name: "monitor_query_traces",
      category: "monitor",
      description:
        "Run structured SkyWalking trace queries by service/trace filters through the configured skywalking datasource.",
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
        service: z.string().optional(),
        serviceId: z.string().optional(),
        serviceInstanceId: z.string().optional(),
        endpointId: z.string().optional(),
        traceId: z.string().optional(),
        status: z.enum(["all", "success", "error"]).default("all"),
        from: z.string().optional(),
        to: z.string().optional(),
        last: z.string().optional(),
        minDurationMs: z.number().int().nonnegative().optional(),
        maxDurationMs: z.number().int().nonnegative().optional(),
        pageNum: z.number().int().positive().default(1),
        pageSize: z.number().int().positive().max(50).default(20),
        format: z.enum(["summary", "raw"]).default("summary"),
      },
      handler: async (_context, args) => {
        const store = getStore();
        const datasource = resolveDatasource(store, String(args.datasourceId), args.projectId as string | undefined);
        return await monitorQueryTraces({
          datasource,
          service: args.service as string | undefined,
          serviceId: args.serviceId as string | undefined,
          serviceInstanceId: args.serviceInstanceId as string | undefined,
          endpointId: args.endpointId as string | undefined,
          traceId: args.traceId as string | undefined,
          status: args.status as "all" | "success" | "error",
          from: args.from as string | undefined,
          to: args.to as string | undefined,
          last: args.last as string | undefined,
          minDurationMs: args.minDurationMs as number | undefined,
          maxDurationMs: args.maxDurationMs as number | undefined,
          pageNum: args.pageNum as number | undefined,
          pageSize: args.pageSize as number | undefined,
          format: args.format as "summary" | "raw",
        });
      },
    },
    {
      name: "repo_search",
      category: "repo",
      description:
        "Search across repo roots bound to one or more projects. Use this to locate sync logic, mapping code, and error messages in source code.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        query: z.string().min(1),
        glob: z.string().optional(),
        limit: z.number().int().positive().max(1000).default(200),
      },
      handler: async (_context, { projectId, projectIdsJson, query, glob, limit }) => {
        const store = getStore();
        const projects = resolveProjectScope(store, { projectId, projectIdsJson, allWhenOmitted: false });
        const repoRoots = Array.from(new Set(projects.flatMap((project) => project.repoRoots)));
        if (repoRoots.length === 0) {
          const fallbackProject = projects[0];
          throw new Error(
            fallbackProject
              ? `Projects ${projects.map((item) => item.id).join(", ")} have no repo roots`
              : "No repo roots available for the selected project scope",
          );
        }
        return {
          projectId: projects.length === 1 ? projects[0].id : null,
          projectIds: projects.map((item) => item.id),
          matches: await searchAcrossRoots(repoRoots, query, glob, limit),
        };
      },
    },
    {
      name: "repo_read_file",
      category: "repo",
      description:
        "Read a file from the selected project's repo roots. Accepts absolute paths returned by repo_search or paths relative to repo roots.",
      schema: {
        projectId: z.string().optional(),
        filePath: z.string().min(1),
        startLine: z.number().int().positive().default(1),
        endLine: z.number().int().positive().default(220),
      },
      handler: async (_context, { projectId, filePath, startLine, endLine }) => {
        const store = getStore();
        const project = resolveProject(store, projectId);
        const absPath = await resolveReadableFile(project, filePath);
        const raw = await fs.readFile(absPath, "utf8");
        const lines = raw.split(/\r?\n/);
        const startIndex = Math.max(0, startLine - 1);
        const endIndex = Math.min(lines.length, endLine);
        const content = lines
          .slice(startIndex, endIndex)
          .map((line, index) => `${startIndex + index + 1}: ${line}`)
          .join("\n");

        return {
          projectId: project.id,
          filePath: absPath,
          startLine: startIndex + 1,
          endLine: endIndex,
          content,
        };
      },
    },
    {
      name: "log_search",
      category: "log",
      description:
        "Search local log roots bound to one or more projects using ripgrep. Use this for runtime evidence when project scope is known.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        query: z.string().min(1),
        glob: z.string().optional(),
        limit: z.number().int().positive().max(1000).default(200),
      },
      handler: async (_context, { projectId, projectIdsJson, query, glob, limit }) => {
        const store = getStore();
        const projects = resolveProjectScope(store, { projectId, projectIdsJson, allWhenOmitted: false });
        const logRoots = Array.from(new Set(projects.flatMap((project) => project.logRoots)));
        if (logRoots.length === 0) {
          const fallbackProject = projects[0];
          throw new Error(
            fallbackProject
              ? `Projects ${projects.map((item) => item.id).join(", ")} have no log roots`
              : "No log roots available for the selected project scope",
          );
        }
        return {
          projectId: projects.length === 1 ? projects[0].id : null,
          projectIds: projects.map((item) => item.id),
          matches: await searchAcrossRoots(logRoots, query, glob, limit),
        };
      },
    },
    {
      name: "runtime_recent_calls",
      category: "runtime",
      description:
        "List recent MCP tool calls recorded by the current server process. Use this to understand which tools were used, whether cache was hit, and which calls failed.",
      schema: {
        limit: z.number().int().positive().max(200).default(30),
        toolName: z.string().optional(),
      },
      handler: async (context, { limit, toolName }) => {
        return {
          calls: context.getRuntime().logs.list(Number(limit), toolName as string | undefined),
        };
      },
    },
    {
      name: "runtime_cache_stats",
      category: "runtime",
      description:
        "Show runtime cache statistics for the current MCP server process.",
      schema: {},
      handler: async (context) => {
        context.getRuntime().cache.pruneExpired();
        return {
          cache: context.getRuntime().cache.stats(),
        };
      },
    },
    {
      name: "hub_list",
      category: "hub",
      description:
        "List the MCP tools exposed by this server, grouped by category. Use when you want the tool catalog from within Codex without re-reading external docs.",
      schema: {
        includeHubTools: z.boolean().default(false),
        category: z.string().optional(),
      },
      handler: async (context, { includeHubTools, category }) => {
        return {
          tools: context.listTools({ includeHubTools, category }),
        };
      },
    },
    {
      name: "hub_inspect",
      category: "hub",
      description:
        "Inspect one MCP tool and return its parameter schema. Use before hub_invoke or when you need to know how a tool expects its inputs.",
      schema: {
        toolName: z.string().min(1),
      },
      handler: async (context, { toolName }) => {
        return context.inspectTool(toolName);
      },
    },
    {
      name: "hub_invoke",
      category: "hub",
      description:
        "Invoke another tool by name using a JSON object of arguments. Use this when you want a meta-tool entrypoint or when a higher-level orchestrator only knows tool names at runtime.",
      schema: {
        toolName: z.string().min(1),
        argsJson: z.string().optional(),
      },
      handler: async (context, { toolName, argsJson }) => {
        assertHubTargetAllowed(toolName);
        const args = parseJsonObject(argsJson, "argsJson");
        const startedAt = Date.now();
        const result = await context.callTool(toolName, args);
        return {
          toolName,
          durationMs: Date.now() - startedAt,
          args,
          result,
        };
      },
    },
    {
      name: "hub_exec",
      category: "hub",
      description:
        "Execute a sequential multi-step tool plan. Each step is a JSON object with {tool, args, note}. Use this for short deterministic playbooks while keeping the server side read-only.",
      schema: {
        stepsJson: z.string().min(2),
        continueOnError: z.boolean().default(false),
      },
      handler: async (context, { stepsJson, continueOnError }) => {
        const steps = parseExecSteps(stepsJson);
        const results: Array<Record<string, unknown>> = [];
        let failedCount = 0;

        for (let index = 0; index < steps.length; index += 1) {
          const step = steps[index];
          assertHubTargetAllowed(step.tool);
          const startedAt = Date.now();
          try {
            const result = await context.callTool(step.tool, step.args);
            results.push({
              index,
              tool: step.tool,
              note: step.note ?? null,
              durationMs: Date.now() - startedAt,
              ok: true,
              args: step.args,
              result,
            });
          } catch (error) {
            failedCount += 1;
            results.push({
              index,
              tool: step.tool,
              note: step.note ?? null,
              durationMs: Date.now() - startedAt,
              ok: false,
              args: step.args,
              error: error instanceof Error ? error.message : String(error),
            });
            if (!continueOnError) {
              break;
            }
          }
        }

        return {
          ok: failedCount === 0,
          stepCount: steps.length,
          completedCount: results.filter((item) => item.ok === true).length,
          failedCount,
          results,
        };
      },
    },
  ];
}

function buildBuiltinToolModules(
  getStore: () => ConfigStore,
  getStorePath: () => string,
): LoadedToolModule[] {
  return [
    {
      manifest: {
        id: "builtin.catalog",
        label: "Built-in Catalog Tools",
        version: "1.0.0",
        description: "Workspace, project, knowledge-base, memory, and datasource catalog tools.",
        source: "builtin",
        entry: "src/tooling/catalog-tools.ts",
        enabled: true,
        tags: ["builtin", "catalog", "workspace", "kb", "memory"],
        hotReloadable: false,
      },
      definitions: buildCatalogToolDefinitions(getStore, getStorePath),
    },
    {
      manifest: {
        id: "builtin.core",
        label: "Built-in Core Tools",
        version: "1.0.0",
        description: "SQL, Mongo, Kafka, repo, log, sync, runtime, and hub tools.",
        source: "builtin",
        entry: "src/tools.ts",
        enabled: true,
        tags: ["builtin", "core", "sync", "runtime", "hub"],
        hotReloadable: false,
      },
      definitions: buildBuiltinCoreToolDefinitions(getStore, getStorePath),
    },
  ];
}

async function createToolRegistry(
  getStore: () => ConfigStore,
  getStorePath: () => string,
  runtime: ToolRuntime = createToolRuntime(),
): Promise<ToolRegistry> {
  const moduleLoader = new ToolModuleLoader({
    storePath: getStorePath(),
    getStore,
    getStorePath,
    buildBuiltinModules: () => buildBuiltinToolModules(getStore, getStorePath),
  });
  let definitions: ToolDefinition[] = [];
  let definitionMap = new Map<string, ToolDefinition>();

  const listTools = (options?: { includeHubTools?: boolean; category?: string }) => {
    return definitions
      .filter((definition) => (options?.includeHubTools ? true : definition.category !== "hub"))
      .filter((definition) => (options?.category ? definition.category === options.category : true))
      .map((definition) => buildToolOverview(definition));
  };

  const inspectTool = (toolName: string) => {
    const definition = definitionMap.get(toolName);
    if (!definition) {
      throw new Error(`Unknown tool: ${toolName}`);
    }
    return buildToolOverview(definition);
  };

  const ensureUniqueToolNames = (items: ToolDefinition[]) => {
    const seen = new Set<string>();
    for (const definition of items) {
      if (seen.has(definition.name)) {
        throw new Error(`Duplicate tool name detected: ${definition.name}`);
      }
      seen.add(definition.name);
    }
  };

  const listStorePackages = () => moduleLoader.listStorePackages();
  const listInstalledModuleIds = () => moduleLoader.getInstalledModuleIds();
  const listRemoteSourceRecords = () => listRemoteSources();
  const listRemotePackageRecords = async () => {
    const records = await listRemotePackages(listInstalledModuleIds());
    return records.map((item) => ({
      ...item,
      sourceType: "remote",
    }));
  };

  const reloadDefinitions = async () => {
    const summary = await moduleLoader.reload();
    const managementDefinitions: ToolDefinition[] = [
      {
        name: "tool_module_catalog",
        category: "runtime",
        description:
          "List all built-in and externally installed tool modules, their source, version, status, and contained tools.",
        cacheTtlMs: 30_000,
        schema: {
          includeDisabled: z.boolean().default(true),
        },
        handler: async (_context, { includeDisabled }) => {
          return {
            modules: moduleLoader.listModules({ includeDisabled }),
            failures: moduleLoader.listFailures(),
            watch: moduleLoader.getWatchStatus(),
          };
        },
      },
      {
        name: "tool_module_reload",
        category: "runtime",
        description:
          "Reload tool modules from disk. Newly added external tool manifests become available after this call without restarting the MCP process.",
        schema: {},
        handler: async () => {
          const nextSummary = await reloadDefinitions();
          return nextSummary;
        },
      },
      {
        name: "tool_module_watch_status",
        category: "runtime",
        description:
          "Show automatic tool module watch status, watched directories, and recent reload activity.",
        cacheTtlMs: 5_000,
        schema: {},
        handler: async () => moduleLoader.getWatchStatus(),
      },
      {
        name: "tool_module_set_enabled",
        category: "runtime",
        description:
          "Enable or disable an installed external tool module, then reload the registry. Built-in modules cannot be disabled.",
        schema: {
          moduleId: z.string().min(1),
          enabled: z.boolean(),
        },
        handler: async (_context, { moduleId, enabled }) => {
          const updated = await moduleLoader.setModuleEnabled(moduleId, Boolean(enabled));
          const reloaded = await reloadDefinitions();
          return {
            updated,
            reloaded,
          };
        },
      },
      {
        name: "tool_module_uninstall",
        category: "runtime",
        description:
          "Uninstall an installed external tool module from the current workspace tool-modules directory, then reload modules.",
        schema: {
          moduleId: z.string().min(1),
        },
        handler: async (_context, { moduleId }) => {
          const removed = await moduleLoader.uninstallModule(moduleId);
          const reloaded = await reloadDefinitions();
          return {
            removed,
            reloaded,
          };
        },
      },
      {
        name: "tool_store_catalog",
        category: "runtime",
        description:
          "List local tool-store packages that can be installed into this workspace as external MCP tool modules.",
        cacheTtlMs: 15_000,
        schema: {},
        handler: async () => ({
          packages: listStorePackages(),
        }),
      },
      {
        name: "tool_store_remote_source_catalog",
        category: "runtime",
        description:
          "List configured remote tool-store sources. Remote sources are stored outside the workspace and do not affect datasource credentials.",
        cacheTtlMs: 15_000,
        schema: {},
        handler: async () => ({
          sources: listRemoteSourceRecords(),
        }),
      },
      {
        name: "tool_store_remote_source_add",
        category: "runtime",
        description:
          "Add or update a remote tool-store source. The URL must point to a JSON catalog. Sources are stored outside the workspace.",
        schema: {
          sourceId: z.string().min(1),
          label: z.string().optional(),
          url: z.string().min(1),
          description: z.string().optional(),
          overwrite: z.boolean().default(false),
        },
        handler: async (_context, { sourceId, label, url, description, overwrite }) => ({
          source: await addRemoteSource({
            id: sourceId,
            label,
            url,
            description,
            overwrite,
          }),
        }),
      },
      {
        name: "tool_store_remote_source_remove",
        category: "runtime",
        description:
          "Remove a configured remote tool-store source. This only removes the source config and does not uninstall already installed modules.",
        schema: {
          sourceId: z.string().min(1),
        },
        handler: async (_context, { sourceId }) => {
          await removeRemoteSource(sourceId);
          return {
            removed: sourceId,
          };
        },
      },
      {
        name: "tool_store_remote_catalog",
        category: "runtime",
        description:
          "List installable tool packages from configured remote sources. Remote packages can then be installed into the current workspace.",
        cacheTtlMs: 20_000,
        schema: {},
        handler: async () => ({
          packages: await listRemotePackageRecords(),
        }),
      },
      {
        name: "tool_store_install_local",
        category: "runtime",
        description:
          "Install a local tool-store package into the current workspace tool-modules directory and reload modules.",
        schema: {
          packageId: z.string().min(1),
          overwrite: z.boolean().default(false),
        },
        handler: async (_context, { packageId, overwrite }) => {
          const installed = await moduleLoader.installFromStore(packageId, { overwrite });
          const reloaded = await reloadDefinitions();
          return {
            installed,
            reloaded,
          };
        },
      },
      {
        name: "tool_store_install_remote",
        category: "runtime",
        description:
          "Install a package from a configured remote tool-store source into the current workspace tool-modules directory and reload modules.",
        schema: {
          sourceId: z.string().min(1),
          packageId: z.string().min(1),
          overwrite: z.boolean().default(false),
        },
        handler: async (_context, { sourceId, packageId, overwrite }) => {
          const installed = await installRemotePackage(getStorePath(), sourceId, packageId, { overwrite });
          const reloaded = await reloadDefinitions();
          return {
            installed,
            reloaded,
          };
        },
      },
    ];
    const nextDefinitions = [...moduleLoader.getDefinitions(), ...managementDefinitions];
    ensureUniqueToolNames(nextDefinitions);
    definitions = nextDefinitions;
    definitionMap = new Map(definitions.map((definition) => [definition.name, definition]));
    return {
      ...summary,
      modules: moduleLoader.listModules({ includeDisabled: true }),
      failures: moduleLoader.listFailures(),
    };
  };

  await reloadDefinitions();

  const startModuleWatch = (onReloaded?: (summary: Record<string, unknown>) => Promise<void> | void) => {
    return moduleLoader.startWatching(async () => {
      const summary = await reloadDefinitions();
      await onReloaded?.(summary);
    });
  };

  const installModuleFromStore = async (packageId: string, options?: { overwrite?: boolean }) => {
    const installed = await moduleLoader.installFromStore(packageId, options);
    const reloaded = await reloadDefinitions();
    return {
      installed,
      reloaded,
    };
  };

  const installModuleFromRemote = async (
    sourceId: string,
    packageId: string,
    options?: { overwrite?: boolean },
  ) => {
    const installed = await installRemotePackage(getStorePath(), sourceId, packageId, options);
    const reloaded = await reloadDefinitions();
    return {
      installed,
      reloaded,
    };
  };

  const setModuleEnabled = async (moduleId: string, enabled: boolean) => {
    const updated = await moduleLoader.setModuleEnabled(moduleId, enabled);
    const reloaded = await reloadDefinitions();
    return {
      updated,
      reloaded,
    };
  };

  const uninstallModule = async (moduleId: string) => {
    const removed = await moduleLoader.uninstallModule(moduleId);
    const reloaded = await reloadDefinitions();
    return {
      removed,
      reloaded,
    };
  };

  const execute = async (toolName: string, args: ToolArgs = {}, stack: string[] = []): Promise<unknown> => {
    const definition = definitionMap.get(toolName);
    if (!definition) {
      throw new Error(`Unknown tool: ${toolName}`);
    }
    if (stack.length >= 8) {
      throw new Error(`Tool call depth exceeded while invoking ${toolName}`);
    }
    if (stack.includes(toolName)) {
      throw new Error(`Recursive tool invocation detected for ${toolName}`);
    }
    const schema = z.object(definition.schema).passthrough();
    const parsedArgs = schema.parse(args) as ToolArgs;
    const cacheKey = definition.cacheTtlMs ? runtime.buildCacheKey(toolName, parsedArgs) : null;
    if (cacheKey) {
      const cached = runtime.cache.get(cacheKey);
      if (cached.hit) {
        const now = new Date().toISOString();
        runtime.logs.record({
          id: `${Date.now()}-${toolName}`,
          toolName,
          startedAt: now,
          completedAt: now,
          durationMs: 0,
          ok: true,
          cached: true,
          stackDepth: stack.length,
          args: parsedArgs,
        });
        return cached.value;
      }
    }
    const startedAtMs = Date.now();
    const context: ToolContext = {
      getStore,
      getStorePath,
      getRuntime: () => runtime,
      stack,
      listTools,
      inspectTool,
      callTool: async (nextToolName: string, nextArgs: ToolArgs = {}) =>
        await execute(nextToolName, nextArgs, [...stack, toolName]),
    };
    try {
      const result = await definition.handler(context, parsedArgs);
      if (cacheKey && definition.cacheTtlMs) {
        runtime.cache.set(cacheKey, result, definition.cacheTtlMs);
      }
      runtime.logs.record({
        id: `${startedAtMs}-${toolName}`,
        toolName,
        startedAt: new Date(startedAtMs).toISOString(),
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAtMs,
        ok: true,
        cached: false,
        stackDepth: stack.length,
        args: parsedArgs,
      });
      return result;
    } catch (error) {
      runtime.logs.record({
        id: `${startedAtMs}-${toolName}`,
        toolName,
        startedAt: new Date(startedAtMs).toISOString(),
        completedAt: new Date().toISOString(),
        durationMs: Date.now() - startedAtMs,
        ok: false,
        cached: false,
        stackDepth: stack.length,
        args: parsedArgs,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  };

  return {
    get definitions() {
      return definitions;
    },
    listTools,
    inspectTool,
    execute,
    reloadModules: reloadDefinitions,
    startModuleWatch,
    getModuleWatchStatus: () => moduleLoader.getWatchStatus(),
    listStorePackages,
    listRemoteSources: listRemoteSourceRecords,
    listRemotePackages: listRemotePackageRecords,
    installModuleFromStore,
    installModuleFromRemote,
    setModuleEnabled,
    uninstallModule,
  };
}

export async function getToolManifest(): Promise<Array<Record<string, unknown>>> {
  return (await createToolRegistry(() => emptyStore(), () => "")).listTools({ includeHubTools: true });
}

export async function runToolLocally(
  storePath: string,
  toolName: string,
  args: ToolArgs = {},
): Promise<unknown> {
  const registry = await createToolRegistry(() => readStoreSync(storePath), () => storePath);
  return await registry.execute(toolName, args);
}

export async function createServer(storePath: string): Promise<McpServer> {
  const server = new McpServer({
    name: "wms-ai-agent",
    version: "0.4.0",
  });

  const runtime = createToolRuntime();
  const registry = await createToolRegistry(() => readStoreSync(storePath), () => storePath, runtime);
  registerWorkspaceResources(server, storePath);

  const registeredTools = new Map<string, ReturnType<typeof server.tool>>();
  const syncRegisteredTools = () => {
    const latest = new Map(registry.definitions.map((definition) => [definition.name, definition]));
    for (const [toolName, registered] of registeredTools.entries()) {
      if (!latest.has(toolName)) {
        registered.remove();
        registeredTools.delete(toolName);
      }
    }
    for (const definition of registry.definitions) {
      const existing = registeredTools.get(definition.name);
      const callback = async (args: unknown) => toTextResult(await registry.execute(definition.name, args as ToolArgs));
      if (existing) {
        existing.update({
          description: definition.description,
          paramsSchema: definition.schema,
          callback,
          enabled: true,
        });
        continue;
      }
      registeredTools.set(
        definition.name,
        server.tool(definition.name, definition.description, definition.schema, callback),
      );
    }
    server.sendToolListChanged();
  };

  syncRegisteredTools();
  registry.startModuleWatch(async () => {
    syncRegisteredTools();
  });
  const originalExecute = registry.execute;
  registry.execute = async (toolName: string, args?: ToolArgs, stack?: string[]) => {
    const result = await originalExecute(toolName, args, stack);
    if (
      toolName === "tool_module_reload" ||
      toolName === "tool_module_set_enabled" ||
      toolName === "tool_module_uninstall" ||
      toolName === "tool_store_install_local" ||
      toolName === "tool_store_install_remote"
    ) {
      syncRegisteredTools();
    }
    return result;
  };

  return server;
}
