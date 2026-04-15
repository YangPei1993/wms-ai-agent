export type DataSourceType = "mysql" | "postgres" | "mongo" | "kafka" | "logcenter" | "monitor" | "skywalking" | "wms_agent";
export type MongoCompatMode = "auto" | "native" | "legacy-shell";
export type LogcenterAuthMode = "basic" | "form";

export interface ProjectConfig {
  id: string;
  label: string;
  enabled: boolean;
  description: string;
  matchHints: string[];
  instructions: string;
  investigationChecklist: string;
  repoRoots: string[];
  logRoots: string[];
  datasourceIds: string[];
  createdAt: string;
  updatedAt: string;
}

export interface DataSourceConnection {
  host?: string;
  port?: number;
  database?: string;
  uri?: string;
  authSource?: string;
  brokers?: string[];
  clientId?: string;
  ssl?: boolean;
  saslMechanism?: string;
  mongoMode?: MongoCompatMode;
  authMode?: LogcenterAuthMode;
  loginPath?: string;
  dataView?: string;
  optionsJson?: string;
}

export interface DataSourceAuth {
  mode: "manual";
  username?: string;
  secret?: string;
  expiresAt?: string;
  updatedAt?: string;
}

export interface DataSourceConfig {
  id: string;
  label: string;
  type: DataSourceType;
  enabled: boolean;
  description: string;
  role: string;
  usageNotes: string;
  projectIds: string[];
  connection: DataSourceConnection;
  auth: DataSourceAuth;
  createdAt: string;
  updatedAt: string;
}

export interface ConfigStore {
  version: 1;
  activeProjectId: string | null;
  globalInstructions: string;
  toolUsageGuidelines: string;
  projects: ProjectConfig[];
  datasources: DataSourceConfig[];
}

export interface DataSourceTestResult {
  ok: boolean;
  message: string;
  durationMs: number;
  details?: unknown;
}

export interface ResolvedProjectContext {
  project: ProjectConfig;
  datasources: DataSourceConfig[];
}
