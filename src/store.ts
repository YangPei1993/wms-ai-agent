import {
  existsSync,
  readFileSync,
  readdirSync,
  statSync,
} from "node:fs";
import { promises as fs } from "node:fs";
import os from "node:os";
import path from "node:path";
import { ConfigStore, DataSourceConfig, ProjectConfig } from "./types.js";
import { ensureArray, ensureBoolean, ensureNumber, nowIso, slugify } from "./utils.js";

export function defaultStorePath(): string {
  return path.join(os.homedir(), ".wms-ai-agent", "workspace");
}

export function resolveStorePath(explicitPath?: string): string {
  if (explicitPath && explicitPath.trim()) {
    return path.resolve(explicitPath.trim());
  }
  return defaultStorePath();
}

export function emptyStore(): ConfigStore {
  return {
    version: 1,
    activeProjectId: null,
    globalInstructions: "",
    toolUsageGuidelines: "",
    projects: [],
    datasources: [],
  };
}

function parseEnvText(raw: string): Record<string, string> {
  const result: Record<string, string> = {};
  for (const line of raw.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const separator = trimmed.indexOf("=");
    if (separator <= 0) {
      continue;
    }
    const key = trimmed.slice(0, separator).trim();
    let value = trimmed.slice(separator + 1).trim();
    if (
      (value.startsWith('"') && value.endsWith('"')) ||
      (value.startsWith("'") && value.endsWith("'"))
    ) {
      value = value.slice(1, -1);
    }
    if (key) {
      result[key] = value;
    }
  }
  return result;
}

function parseCsv(value: string | undefined): string[] {
  if (!value?.trim()) {
    return [];
  }
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function readTextIfExists(filePath: string): string {
  try {
    if (!existsSync(filePath)) {
      return "";
    }
    return readFileSync(filePath, "utf8");
  } catch {
    return "";
  }
}

function readLinesIfExists(filePath: string): string[] {
  return readTextIfExists(filePath)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith("#"));
}

function readDirNamesIfExists(dirPath: string): string[] {
  try {
    if (!existsSync(dirPath)) {
      return [];
    }
    return readdirSync(dirPath).filter((name) => {
      try {
        return statSync(path.join(dirPath, name)).isDirectory();
      } catch {
        return false;
      }
    });
  } catch {
    return [];
  }
}

function normalizeProject(input: unknown): ProjectConfig | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  const item = input as Record<string, unknown>;
  const label = String(item.label ?? "").trim();
  const id = String(item.id ?? "").trim() || slugify(label);
  if (!id || !label) {
    return null;
  }
  const createdAt = String(item.createdAt ?? nowIso());
  const updatedAt = String(item.updatedAt ?? nowIso());
  return {
    id,
    label,
    enabled: ensureBoolean(item.enabled, true),
    description: String(item.description ?? ""),
    matchHints: Array.isArray(item.matchHints) ? ensureArray(item.matchHints) : parseCsv(String(item.matchHints ?? "")),
    instructions: String(item.instructions ?? ""),
    investigationChecklist: String(item.investigationChecklist ?? ""),
    repoRoots: Array.isArray(item.repoRoots) ? ensureArray(item.repoRoots) : parseCsv(String(item.repoRoots ?? "")),
    logRoots: Array.isArray(item.logRoots) ? ensureArray(item.logRoots) : parseCsv(String(item.logRoots ?? "")),
    datasourceIds: Array.isArray(item.datasourceIds)
      ? ensureArray(item.datasourceIds)
      : parseCsv(String(item.datasourceIds ?? "")),
    createdAt,
    updatedAt,
  };
}

function normalizeDatasource(input: unknown): DataSourceConfig | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  const item = input as Record<string, unknown>;
  const label = String(item.label ?? "").trim();
  const id = String(item.id ?? "").trim() || slugify(label);
  const type = String(item.type ?? "").trim() as DataSourceConfig["type"];
  if (!id || !label || !["mysql", "postgres", "mongo", "kafka", "logcenter", "monitor", "skywalking", "wms_agent"].includes(type)) {
    return null;
  }
  const connectionRaw =
    item.connection && typeof item.connection === "object" && !Array.isArray(item.connection)
      ? (item.connection as Record<string, unknown>)
      : {};
  const authRaw =
    item.auth && typeof item.auth === "object" && !Array.isArray(item.auth)
      ? (item.auth as Record<string, unknown>)
      : {};
  const createdAt = String(item.createdAt ?? nowIso());
  const updatedAt = String(item.updatedAt ?? nowIso());

  return {
    id,
    label,
    type,
    enabled: ensureBoolean(item.enabled, true),
    description: String(item.description ?? ""),
    role: String(item.role ?? ""),
    usageNotes: String(item.usageNotes ?? ""),
    projectIds: Array.isArray(item.projectIds) ? ensureArray(item.projectIds) : parseCsv(String(item.projectIds ?? "")),
    connection: {
      host: String(connectionRaw.host ?? "").trim() || undefined,
      port: ensureNumber(connectionRaw.port),
      database: String(connectionRaw.database ?? "").trim() || undefined,
      uri: String(connectionRaw.uri ?? "").trim() || undefined,
      authSource: String(connectionRaw.authSource ?? "").trim() || undefined,
      brokers: Array.isArray(connectionRaw.brokers)
        ? ensureArray(connectionRaw.brokers)
        : parseCsv(String(connectionRaw.brokers ?? "")),
      clientId: String(connectionRaw.clientId ?? "").trim() || undefined,
      ssl: ensureBoolean(connectionRaw.ssl, false),
      saslMechanism: String(connectionRaw.saslMechanism ?? "").trim() || undefined,
      mongoMode:
        String(connectionRaw.mongoMode ?? "").trim() === "legacy-shell"
          ? "legacy-shell"
          : String(connectionRaw.mongoMode ?? "").trim() === "native"
            ? "native"
            : "auto",
      authMode:
        String(connectionRaw.authMode ?? "").trim() === "form"
          ? "form"
          : String(connectionRaw.authMode ?? "").trim() === "basic"
            ? "basic"
            : undefined,
      loginPath: String(connectionRaw.loginPath ?? "").trim() || undefined,
      dataView: String(connectionRaw.dataView ?? "").trim() || undefined,
      optionsJson: String(connectionRaw.optionsJson ?? "").trim() || undefined,
    },
    auth: {
      mode: "manual",
      username: String(authRaw.username ?? "").trim() || undefined,
      secret: String(authRaw.secret ?? "") || undefined,
      expiresAt: String(authRaw.expiresAt ?? "").trim() || undefined,
      updatedAt: String(authRaw.updatedAt ?? "").trim() || undefined,
    },
    createdAt,
    updatedAt,
  };
}

export function normalizeStore(input: unknown): ConfigStore {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return emptyStore();
  }
  const item = input as Record<string, unknown>;
  const projects = (Array.isArray(item.projects) ? item.projects : [])
    .map((value) => normalizeProject(value))
    .filter((value): value is ProjectConfig => value !== null);
  const datasources = (Array.isArray(item.datasources) ? item.datasources : [])
    .map((value) => normalizeDatasource(value))
    .filter((value): value is DataSourceConfig => value !== null);
  const activeProjectId = String(item.activeProjectId ?? "").trim() || null;

  return {
    version: 1,
    activeProjectId: projects.some((project) => project.id === activeProjectId) ? activeProjectId : null,
    globalInstructions: String(item.globalInstructions ?? ""),
    toolUsageGuidelines: String(item.toolUsageGuidelines ?? ""),
    projects,
    datasources,
  };
}

function normalizeRelationships(store: ConfigStore): ConfigStore {
  const projectMap = new Map(store.projects.map((project) => [project.id, project]));
  const datasourceMap = new Map(store.datasources.map((datasource) => [datasource.id, datasource]));

  for (const project of store.projects) {
    project.datasourceIds = [...new Set(project.datasourceIds)].filter((datasourceId) =>
      datasourceMap.has(datasourceId),
    );
  }

  for (const datasource of store.datasources) {
    datasource.projectIds = [...new Set(datasource.projectIds)].filter((projectId) => projectMap.has(projectId));
  }

  for (const project of store.projects) {
    for (const datasourceId of project.datasourceIds) {
      const datasource = datasourceMap.get(datasourceId);
      if (datasource && !datasource.projectIds.includes(project.id)) {
        datasource.projectIds = [...datasource.projectIds, project.id];
      }
    }
  }

  return store;
}

function readLegacyJsonSync(storePath: string): ConfigStore {
  try {
    return normalizeStore(JSON.parse(readFileSync(storePath, "utf8")));
  } catch {
    return emptyStore();
  }
}

function loadProjectFromWorkspace(projectId: string, projectDir: string): ProjectConfig | null {
  const meta = parseEnvText(readTextIfExists(path.join(projectDir, "project.env")));
  const playbook = readTextIfExists(path.join(projectDir, "playbook.md")).trim();
  const checklist = readTextIfExists(path.join(projectDir, "checklist.md")).trim();
  return normalizeProject({
    id: projectId,
    label: meta.LABEL ?? projectId,
    enabled: meta.ENABLED ?? "true",
    description: meta.DESCRIPTION ?? "",
    matchHints: parseCsv(meta.MATCH_HINTS),
    instructions: playbook || meta.INSTRUCTIONS || "",
    investigationChecklist: checklist || meta.INVESTIGATION_CHECKLIST || "",
    repoRoots: readLinesIfExists(path.join(projectDir, "repos.txt")),
    logRoots: readLinesIfExists(path.join(projectDir, "logs.txt")),
    datasourceIds: readLinesIfExists(path.join(projectDir, "datasources.txt")),
    createdAt: meta.CREATED_AT ?? nowIso(),
    updatedAt: meta.UPDATED_AT ?? nowIso(),
  });
}

function loadDatasourceFromWorkspace(datasourceId: string, datasourceDir: string): DataSourceConfig | null {
  const meta = parseEnvText(readTextIfExists(path.join(datasourceDir, "datasource.env")));
  const secret = parseEnvText(readTextIfExists(path.join(datasourceDir, "secret.env")));
  return normalizeDatasource({
    id: datasourceId,
    label: meta.LABEL ?? datasourceId,
    type: meta.TYPE,
    enabled: meta.ENABLED ?? "true",
    description: meta.DESCRIPTION ?? "",
    role: meta.ROLE ?? "",
    usageNotes: meta.USAGE_NOTES ?? "",
    projectIds: parseCsv(meta.PROJECT_IDS),
    connection: {
      host: meta.HOST,
      port: meta.PORT,
      database: meta.DATABASE,
      uri: meta.URI,
      authSource: meta.AUTH_SOURCE,
      brokers: parseCsv(meta.BROKERS),
      clientId: meta.CLIENT_ID,
      ssl: meta.SSL ?? "false",
      saslMechanism: meta.SASL_MECHANISM,
      mongoMode: meta.MONGO_MODE,
      authMode: meta.AUTH_MODE,
      loginPath: meta.LOGIN_PATH,
      dataView: meta.DATA_VIEW,
      optionsJson: meta.OPTIONS_JSON,
    },
    auth: {
      mode: "manual",
      username: secret.USERNAME,
      secret: secret.SECRET,
      expiresAt: secret.EXPIRES_AT,
      updatedAt: secret.UPDATED_AT,
    },
    createdAt: meta.CREATED_AT ?? nowIso(),
    updatedAt: meta.UPDATED_AT ?? nowIso(),
  });
}

function readWorkspaceTreeSync(workspaceRoot: string): ConfigStore {
  if (!existsSync(workspaceRoot)) {
    return emptyStore();
  }
  const stat = statSync(workspaceRoot);
  if (!stat.isDirectory()) {
    return readLegacyJsonSync(workspaceRoot);
  }

  const workspaceMeta = parseEnvText(readTextIfExists(path.join(workspaceRoot, "workspace.env")));
  const projectsRoot = path.join(workspaceRoot, "projects");
  const datasourcesRoot = path.join(workspaceRoot, "datasources");

  const projects = readDirNamesIfExists(projectsRoot)
    .map((projectId) => loadProjectFromWorkspace(projectId, path.join(projectsRoot, projectId)))
    .filter((value): value is ProjectConfig => value !== null);

  const datasources = readDirNamesIfExists(datasourcesRoot)
    .map((datasourceId) => loadDatasourceFromWorkspace(datasourceId, path.join(datasourcesRoot, datasourceId)))
    .filter((value): value is DataSourceConfig => value !== null);

  const store = normalizeRelationships({
    version: 1,
    activeProjectId: workspaceMeta.ACTIVE_PROJECT_ID || null,
    globalInstructions:
      readTextIfExists(path.join(workspaceRoot, "instructions.md")).trim() ||
      workspaceMeta.GLOBAL_INSTRUCTIONS ||
      "",
    toolUsageGuidelines:
      readTextIfExists(path.join(workspaceRoot, "tool-guidelines.md")).trim() ||
      workspaceMeta.TOOL_USAGE_GUIDELINES ||
      "",
    projects,
    datasources,
  });

  if (!store.projects.some((project) => project.id === store.activeProjectId)) {
    store.activeProjectId = null;
  }
  return store;
}

export function readStoreSync(storePath: string): ConfigStore {
  if (!existsSync(storePath)) {
    return emptyStore();
  }
  return readWorkspaceTreeSync(storePath);
}

export async function readStore(storePath: string): Promise<ConfigStore> {
  return readStoreSync(storePath);
}

export async function writeStore(storePath: string, store: ConfigStore): Promise<void> {
  await fs.mkdir(path.dirname(storePath), { recursive: true });
  await fs.writeFile(storePath, `${JSON.stringify(normalizeStore(store), null, 2)}\n`, "utf8");
}

export function upsertProject(store: ConfigStore, input: Partial<ProjectConfig> & { label: string }): ConfigStore {
  const normalized = normalizeProject({
    ...input,
    updatedAt: nowIso(),
    createdAt: input.createdAt ?? nowIso(),
  });
  if (!normalized) {
    throw new Error("Invalid project");
  }
  const existing = store.projects.find((project) => project.id === normalized.id);
  return {
    ...store,
    projects: existing
      ? store.projects.map((project) => (project.id === normalized.id ? { ...existing, ...normalized } : project))
      : [...store.projects, normalized],
  };
}

export function upsertDatasource(
  store: ConfigStore,
  input: Partial<DataSourceConfig> & { label: string; type: DataSourceConfig["type"] },
): ConfigStore {
  const normalized = normalizeDatasource({
    ...input,
    updatedAt: nowIso(),
    createdAt: input.createdAt ?? nowIso(),
  });
  if (!normalized) {
    throw new Error("Invalid datasource");
  }
  const existing = store.datasources.find((datasource) => datasource.id === normalized.id);
  return {
    ...store,
    datasources: existing
      ? store.datasources.map((datasource) =>
          datasource.id === normalized.id ? { ...existing, ...normalized } : datasource,
        )
      : [...store.datasources, normalized],
  };
}
