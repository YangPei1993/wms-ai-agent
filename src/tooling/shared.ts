import { existsSync, readdirSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import { ConfigStore, DataSourceConfig, ProjectConfig } from "../types.js";
import { resolvePathUnderRoot } from "../utils.js";
import { ToolRuntime } from "../runtime/index.js";

export type ToolArgs = Record<string, unknown>;
export type ToolSchema = Record<string, z.ZodTypeAny>;

export interface ToolContext {
  getStore: () => ConfigStore;
  getStorePath: () => string;
  getRuntime: () => ToolRuntime;
  listTools: (options?: { includeHubTools?: boolean; category?: string }) => Array<Record<string, unknown>>;
  inspectTool: (toolName: string) => Record<string, unknown>;
  callTool: (toolName: string, args?: ToolArgs) => Promise<unknown>;
  stack: string[];
}

export interface ToolDefinition {
  name: string;
  category: string;
  description: string;
  schema: ToolSchema;
  cacheTtlMs?: number;
  handler: (context: ToolContext, args: any) => Promise<unknown>;
}

export interface ToolRegistry {
  definitions: ToolDefinition[];
  listTools: (options?: { includeHubTools?: boolean; category?: string }) => Array<Record<string, unknown>>;
  inspectTool: (toolName: string) => Record<string, unknown>;
  execute: (toolName: string, args?: ToolArgs, stack?: string[]) => Promise<unknown>;
}

export function summarizeDatasource(datasource: DataSourceConfig) {
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

export function workspaceKnowledgeBaseRoot(storePath: string): string {
  return path.join(storePath, "knowledge-base");
}

export function workspaceMemoryRoot(storePath: string): string {
  return path.join(storePath, "memory");
}

export function projectKnowledgeBaseRoot(storePath: string, projectId: string): string {
  return path.join(storePath, "projects", projectId, "knowledge-base");
}

export function projectMemoryRoot(storePath: string, projectId: string): string {
  return path.join(storePath, "projects", projectId, "memory");
}

export function getEnabledProjects(store: ConfigStore): ProjectConfig[] {
  return store.projects.filter((project) => project.enabled);
}

export function resolveProject(store: ConfigStore, projectId?: string): ProjectConfig {
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

export function parseOptionalStringArrayJson(value: string | undefined, label: string): string[] {
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

export function resolveProjectScope(
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

export function listMarkdownFilesRecursive(root: string, relPrefix = ""): Array<Record<string, unknown>> {
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

export function existsSyncSafe(targetPath: string): boolean {
  try {
    return existsSync(targetPath);
  } catch {
    return false;
  }
}

export function safeReadDir(
  dirPath: string,
): Array<{ name: string; isDirectory: () => boolean; isFile: () => boolean }> {
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

export function getProjectDatasources(store: ConfigStore, project: ProjectConfig): DataSourceConfig[] {
  const boundIds = new Set(project.datasourceIds);
  return store.datasources.filter((datasource) => datasource.enabled && boundIds.has(datasource.id));
}

export function getDatasourcesForProjects(store: ConfigStore, projects: ProjectConfig[]): DataSourceConfig[] {
  const datasourceIds = new Set(projects.flatMap((project) => project.datasourceIds));
  return store.datasources.filter((datasource) => datasource.enabled && datasourceIds.has(datasource.id));
}

export function uniqueByKey<T>(items: T[], getKey: (item: T) => string): T[] {
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

export function resolveKnowledgeBaseRoots(
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

export function resolveMemoryRoots(
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

export async function readMarkdownDoc(absPath: string): Promise<Record<string, unknown>> {
  const content = await fs.readFile(absPath, "utf8");
  return {
    absPath,
    name: path.basename(absPath),
    content,
  };
}

export function caseFileName(title: string): string {
  const base = title
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 64) || "case";
  return `${new Date().toISOString().replace(/[:.]/g, "-")}-${base}.md`;
}

export function resolveDatasource(
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

export function tokenizeIntentText(text: string): string[] {
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

export function scoreDatasourceForIntent(
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

export function resolveDatasourceCandidates(
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

export function uniqueStrings(values: Array<string | undefined | null>): string[] {
  return Array.from(
    new Set(
      values
        .map((item) => String(item ?? "").trim())
        .filter(Boolean),
    ),
  );
}

export function resolveKnowledgeOrMemoryPath(
  storePath: string,
  relPath: string | undefined,
  absPath: string | undefined,
): string {
  const targetPath = String(absPath ?? "").trim()
    || (String(relPath ?? "").trim() ? resolvePathUnderRoot(storePath, String(relPath).trim()) : "");
  if (!targetPath) {
    throw new Error("absPath or relPath is required");
  }
  return path.resolve(targetPath);
}
