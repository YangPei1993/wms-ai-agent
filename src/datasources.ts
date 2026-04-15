import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { MongoClient, ObjectId } from "mongodb";
import mysql from "mysql2/promise";
import { Kafka } from "kafkajs";
import { Client as PgClient } from "pg";
import { DataSourceConfig, DataSourceTestResult, LogcenterAuthMode, MongoCompatMode } from "./types.js";
import { testMonitorDatasource } from "./monitor-datasources.js";
import { commandExists, runLocalCommand, sanitizeForJson } from "./utils.js";

type DbKind = "mysql" | "postgres";

interface DbClient {
  kind: DbKind;
  query: (sql: string, params?: unknown[]) => Promise<Record<string, unknown>[]>;
  close: () => Promise<void>;
}

let mongoAutoDetectedMode: MongoCompatMode | null = null;
let legacyMongoRunnerPromise: Promise<"python" | "mongosh" | "mongo"> | null = null;
let legacyPythonCommandPromise: Promise<string | null> | null = null;
const agentBaseDir = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

export function assertReadOnlySql(sql: string): void {
  const normalized = sql.trim().replace(/^[;(]+/, "").trim().toLowerCase();
  if (!normalized) {
    throw new Error("SQL is empty");
  }
  const allowed = ["select", "with", "show", "describe", "desc", "explain"];
  if (!allowed.some((prefix) => normalized.startsWith(prefix))) {
    throw new Error("Only read-only SQL is allowed");
  }
  const denied = /\b(insert|update|delete|replace|truncate|drop|alter|create|grant|revoke|merge|rename)\b/i;
  if (denied.test(normalized)) {
    throw new Error("DML/DDL keywords are not allowed");
  }
}

function parseJsonObject(json: string | undefined, label: string): Record<string, unknown> {
  if (!json || !json.trim()) {
    return {};
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(json);
  } catch (error) {
    throw new Error(`${label} must be valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`);
  }
  return parsed as Record<string, unknown>;
}

function encodeCredential(value: string | undefined): string {
  return encodeURIComponent(value ?? "");
}

function buildSqlUrl(datasource: DataSourceConfig): string {
  if (datasource.connection.uri) {
    return datasource.connection.uri;
  }
  const host = datasource.connection.host?.trim();
  if (!host) {
    throw new Error(`Datasource ${datasource.id} host is required`);
  }
  const port =
    datasource.connection.port ??
    (datasource.type === "postgres" ? 5432 : 3306);
  const username = datasource.auth.username ?? "";
  const secret = datasource.auth.secret ?? "";
  const database = datasource.connection.database?.trim();
  const authSegment =
    username || secret ? `${encodeCredential(username)}:${encodeCredential(secret)}@` : "";
  const pathSegment = database ? `/${encodeURIComponent(database)}` : "";
  const protocol = datasource.type === "postgres" ? "postgres" : "mysql";
  return `${protocol}://${authSegment}${host}:${port}${pathSegment}`;
}

function buildMongoUrl(datasource: DataSourceConfig): string {
  if (datasource.connection.uri) {
    return datasource.connection.uri;
  }
  const host = datasource.connection.host?.trim();
  if (!host) {
    throw new Error(`Datasource ${datasource.id} host is required`);
  }
  const port = datasource.connection.port ?? 27017;
  const username = datasource.auth.username ?? "";
  const secret = datasource.auth.secret ?? "";
  const authSegment =
    username || secret ? `${encodeCredential(username)}:${encodeCredential(secret)}@` : "";
  const dbSegment = datasource.connection.database?.trim()
    ? `/${encodeURIComponent(datasource.connection.database.trim())}`
    : "/";
  const query = datasource.connection.authSource?.trim()
    ? `?authSource=${encodeURIComponent(datasource.connection.authSource.trim())}`
    : "";
  return `mongodb://${authSegment}${host}:${port}${dbSegment}${query}`;
}

function getKafkaClient(datasource: DataSourceConfig): Kafka {
  const brokers = datasource.connection.brokers?.filter(Boolean) ?? [];
  if (brokers.length === 0) {
    throw new Error(`Datasource ${datasource.id} brokers are empty`);
  }
  const username = datasource.auth.username;
  const password = datasource.auth.secret;
  const mechanism = (datasource.connection.saslMechanism ?? "plain").toLowerCase();
  const config: ConstructorParameters<typeof Kafka>[0] = {
    clientId: datasource.connection.clientId ?? "wms-ai-agent",
    brokers,
  };

  if (datasource.connection.ssl) {
    config.ssl = true;
  }

  if (username && password) {
    if (!["plain", "scram-sha-256", "scram-sha-512"].includes(mechanism)) {
      throw new Error(`Unsupported SASL mechanism: ${mechanism}`);
    }
    switch (mechanism) {
      case "plain":
        config.sasl = { mechanism: "plain", username, password };
        break;
      case "scram-sha-256":
        config.sasl = { mechanism: "scram-sha-256", username, password };
        break;
      case "scram-sha-512":
        config.sasl = { mechanism: "scram-sha-512", username, password };
        break;
      default:
        throw new Error(`Unsupported SASL mechanism: ${mechanism}`);
    }
  }

  return new Kafka(config);
}

function normalizeHttpBaseUrl(value: string, label: string): string {
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
  return parsed.origin;
}

function resolveLogcenterBaseUrl(datasource: DataSourceConfig): string {
  const candidate = datasource.connection.uri?.trim() || datasource.connection.host?.trim();
  if (!candidate) {
    throw new Error(`Logcenter datasource ${datasource.id} requires URI or HOST`);
  }
  return normalizeHttpBaseUrl(candidate, `Logcenter datasource ${datasource.id} base URL`);
}

function resolveLogcenterAuthMode(datasource: DataSourceConfig): LogcenterAuthMode {
  return datasource.connection.authMode === "form" ? "form" : "basic";
}

function resolveLogcenterLoginPath(datasource: DataSourceConfig): string {
  const raw = datasource.connection.loginPath?.trim() || "/login";
  return raw.startsWith("/") ? raw : `/${raw}`;
}

function getSetCookies(response: Response): string[] {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] };
  if (typeof headers.getSetCookie === "function") {
    return headers.getSetCookie();
  }
  const single = response.headers.get("set-cookie");
  return single ? [single] : [];
}

function buildCookieHeader(setCookies: string[]): string {
  return Array.from(
    new Set(
      setCookies
        .map((item) => item.split(";")[0]?.trim() ?? "")
        .filter(Boolean),
    ),
  ).join("; ");
}

function buildLogcenterPreviewSource(value: unknown, depth = 0): unknown {
  if (depth >= 4) {
    return "[truncated]";
  }
  if (typeof value === "string") {
    return value.length > 400 ? `${value.slice(0, 397)}...` : value;
  }
  if (Array.isArray(value)) {
    return value.slice(0, 12).map((item) => buildLogcenterPreviewSource(item, depth + 1));
  }
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .slice(0, 30)
        .map(([key, item]) => [key, buildLogcenterPreviewSource(item, depth + 1)]),
    );
  }
  return value;
}

function getNestedValue(record: Record<string, unknown>, dottedPath: string): unknown {
  const segments = dottedPath.split(".");
  let current: unknown = record;
  for (const segment of segments) {
    if (!current || typeof current !== "object" || Array.isArray(current)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function pickFirstTruthyString(record: Record<string, unknown>, candidates: string[]): string | null {
  for (const candidate of candidates) {
    const value = getNestedValue(record, candidate);
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return null;
}

function pickLogcenterSummaryFields(source: Record<string, unknown>): Record<string, unknown> {
  return {
    timestamp: pickFirstTruthyString(source, ["@timestamp", "timestamp"]),
    level: pickFirstTruthyString(source, ["log.level", "level", "severity"]),
    service: pickFirstTruthyString(source, ["service.name", "service_name", "application"]),
    namespace: pickFirstTruthyString(source, ["data_stream.namespace"]),
    host: pickFirstTruthyString(source, ["host.name", "computername"]),
    requestId: pickFirstTruthyString(source, ["xrequestid", "trace.id", "traceId", "requestId", "httpRequest.requestId"]),
    request: pickFirstTruthyString(source, ["request", "url.path", "uri_stem", "httpRequest.uri"]),
    response: pickFirstTruthyString(source, ["response", "response_code", "status"]),
    message: pickFirstTruthyString(source, ["message", "event.original"]),
  };
}

interface LogcenterSession {
  baseUrl: string;
  defaultHeaders: Record<string, string>;
}

async function createLogcenterSession(datasource: DataSourceConfig): Promise<LogcenterSession> {
  const baseUrl = resolveLogcenterBaseUrl(datasource);
  const authMode = resolveLogcenterAuthMode(datasource);
  if (authMode === "basic") {
    const username = datasource.auth.username ?? "";
    const secret = datasource.auth.secret ?? "";
    if (!username && !secret) {
      throw new Error(`Logcenter datasource ${datasource.id} requires USERNAME/SECRET for basic auth`);
    }
    const token = Buffer.from(`${username}:${secret}`).toString("base64");
    return {
      baseUrl,
      defaultHeaders: {
        Authorization: `Basic ${token}`,
      },
    };
  }

  const username = datasource.auth.username?.trim();
  const secret = datasource.auth.secret ?? "";
  if (!username || !secret) {
    throw new Error(`Logcenter datasource ${datasource.id} requires USERNAME/SECRET for form login`);
  }

  const loginUrl = new URL(resolveLogcenterLoginPath(datasource), `${baseUrl}/`);
  const loginResponse = await fetch(loginUrl, {
    method: "POST",
    redirect: "manual",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body: new URLSearchParams({
      username,
      password: secret,
    }),
  });

  const cookieHeader = buildCookieHeader(getSetCookies(loginResponse));
  if (!cookieHeader) {
    const responseText = await loginResponse.text().catch(() => "");
    throw new Error(
      `Logcenter form login did not return a session cookie (${loginResponse.status}): ${responseText.slice(0, 200)}`,
    );
  }

  const statusResponse = await fetch(new URL("/api/status", `${baseUrl}/`), {
    headers: {
      Cookie: cookieHeader,
    },
  });
  if (!statusResponse.ok) {
    const responseText = await statusResponse.text().catch(() => "");
    throw new Error(
      `Logcenter session verification failed (${statusResponse.status}): ${responseText.slice(0, 200)}`,
    );
  }

  return {
    baseUrl,
    defaultHeaders: {
      Cookie: cookieHeader,
    },
  };
}

async function fetchLogcenterJson(
  session: LogcenterSession,
  pathName: string,
  init?: RequestInit,
): Promise<Record<string, unknown>> {
  const response = await fetch(new URL(pathName, `${session.baseUrl}/`), {
    ...init,
    headers: {
      Accept: "application/json",
      ...(session.defaultHeaders ?? {}),
      ...((init?.headers ?? {}) as Record<string, string>),
    },
  });
  const rawText = await response.text();
  if (!response.ok) {
    throw new Error(`Logcenter request failed ${response.status}: ${rawText.slice(0, 300)}`);
  }
  if (/<!doctype html>/i.test(rawText)) {
    throw new Error("Logcenter returned HTML instead of JSON; authentication may have expired");
  }
  try {
    return JSON.parse(rawText) as Record<string, unknown>;
  } catch (error) {
    throw new Error(`Logcenter returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function resolveLogcenterDataView(
  session: LogcenterSession,
  datasource: DataSourceConfig,
  overrideDataView?: string,
): Promise<{ id: string; title: string; timeFieldName: string | null }> {
  const requested = overrideDataView?.trim() || datasource.connection.dataView?.trim();
  if (!requested) {
    throw new Error(`Logcenter datasource ${datasource.id} requires DATA_VIEW or a dataView override`);
  }
  let resolvedRef = requested;
  let dataViewResponse: Record<string, unknown>;
  try {
    dataViewResponse = await fetchLogcenterJson(
      session,
      `/api/data_views/data_view/${encodeURIComponent(resolvedRef)}`,
    );
  } catch (error) {
    const shouldFallback = error instanceof Error && /saved object .* not found|request failed 404/i.test(error.message);
    if (!shouldFallback) {
      throw error;
    }
    const listResponse = await fetchLogcenterJson(session, "/api/data_views");
    const dataViews = Array.isArray(listResponse.data_view) ? listResponse.data_view : [];
    const matched = dataViews.find((item) => {
      if (!item || typeof item !== "object") {
        return false;
      }
      const candidate = item as Record<string, unknown>;
      return [candidate.id, candidate.title, candidate.name]
        .map((value) => String(value ?? "").trim())
        .includes(requested);
    });
    if (!matched || typeof matched !== "object") {
      throw error;
    }
    resolvedRef = String((matched as Record<string, unknown>).id ?? "").trim() || requested;
    dataViewResponse = await fetchLogcenterJson(
      session,
      `/api/data_views/data_view/${encodeURIComponent(resolvedRef)}`,
    );
  }
  const dataViewRaw =
    dataViewResponse.data_view && typeof dataViewResponse.data_view === "object"
      ? (dataViewResponse.data_view as Record<string, unknown>)
      : null;
  if (!dataViewRaw) {
    throw new Error(`Logcenter data view not found: ${requested}`);
  }
  const title = String(dataViewRaw.title ?? "").trim();
  if (!title) {
    throw new Error(`Logcenter data view ${requested} has no title`);
  }
  return {
    id: String(dataViewRaw.id ?? requested).trim() || requested,
    title,
    timeFieldName: String(dataViewRaw.timeFieldName ?? "").trim() || null,
  };
}

function buildLogcenterSearchBody(params: {
  query?: string;
  timeFieldName: string | null;
  from: string;
  to: string;
  limit: number;
}): Record<string, unknown> {
  const filters: Record<string, unknown>[] = [];
  if (params.timeFieldName) {
    filters.push({
      range: {
        [params.timeFieldName]: {
          gte: params.from,
          lte: params.to,
        },
      },
    });
  }
  if (params.query?.trim()) {
    filters.push({
      query_string: {
        query: params.query.trim(),
        default_operator: "AND",
        analyze_wildcard: true,
        lenient: true,
      },
    });
  }
  return {
    size: Math.max(1, Math.min(200, params.limit)),
    track_total_hits: true,
    sort: params.timeFieldName
      ? [{ [params.timeFieldName]: { order: "desc", unmapped_type: "date" } }]
      : undefined,
    query: filters.length > 0 ? { bool: { filter: filters } } : { match_all: {} },
  };
}

async function createDbClient(datasource: DataSourceConfig): Promise<DbClient> {
  const connectionUrl = buildSqlUrl(datasource);
  if (datasource.type === "postgres") {
    const client = new PgClient({ connectionString: connectionUrl });
    await client.connect();
    return {
      kind: "postgres",
      query: async (sql, params = []) => {
        const result = await client.query(sql, params);
        return result.rows as Record<string, unknown>[];
      },
      close: async () => {
        await client.end();
      },
    };
  }

  if (datasource.type === "mysql") {
    const conn = await mysql.createConnection(connectionUrl);
    return {
      kind: "mysql",
      query: async (sql, params = []) => {
        const [rows] = await conn.query(sql, params);
        return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
      },
      close: async () => {
        await conn.end();
      },
    };
  }

  throw new Error(`Datasource ${datasource.id} is not SQL`);
}

function containsMongoCompatibilityMessage(message: string): boolean {
  const normalized = message.toLowerCase();
  return normalized.includes("wire version") && normalized.includes("requires at least");
}

function isMongoCompatibilityError(error: unknown): boolean {
  if (!(error instanceof Error)) {
    return false;
  }
  if (containsMongoCompatibilityMessage(error.message)) {
    return true;
  }
  let current: unknown = error;
  let depth = 0;
  while (current && depth < 5) {
    depth += 1;
    if (current instanceof Error && containsMongoCompatibilityMessage(current.message)) {
      return true;
    }
    current = (current as { cause?: unknown }).cause;
  }
  return false;
}

async function resolveLegacyPythonCommand(cwd: string): Promise<string | null> {
  if (legacyPythonCommandPromise) {
    return legacyPythonCommandPromise;
  }
  legacyPythonCommandPromise = (async () => {
    const explicit = process.env.WMS_AI_AGENT_LEGACY_PYTHON?.trim();
    if (explicit) {
      return explicit;
    }
    const venvPython = path.join(cwd, ".venv", "bin", "python3");
    if (existsSync(venvPython)) {
      return venvPython;
    }
    if (await commandExists("python3", cwd)) {
      return "python3";
    }
    return null;
  })();
  return legacyPythonCommandPromise;
}

async function canUseLegacyPyMongo(cwd: string): Promise<boolean> {
  const pythonCommand = await resolveLegacyPythonCommand(cwd);
  if (!pythonCommand) {
    return false;
  }
  const probe = await runLocalCommand(
    pythonCommand,
    [
      "-c",
      "import sys\ntry:\n import pymongo\n sys.exit(0 if int(pymongo.version.split('.')[0]) < 4 else 1)\nexcept Exception:\n sys.exit(1)\n",
    ],
    cwd,
    5_000,
  );
  return probe.exitCode === 0;
}

async function getLegacyMongoRunner(): Promise<"python" | "mongosh" | "mongo"> {
  if (legacyMongoRunnerPromise) {
    return legacyMongoRunnerPromise;
  }
  const cwd = agentBaseDir;
  legacyMongoRunnerPromise = (async () => {
    const explicitPython = process.env.WMS_AI_AGENT_LEGACY_PYTHON?.trim();
    if (explicitPython) {
      return "python";
    }
    const bundledVenvPython = path.join(agentBaseDir, ".venv", "bin", "python3");
    if (existsSync(bundledVenvPython)) {
      return "python";
    }
    if (await canUseLegacyPyMongo(cwd)) {
      return "python";
    }
    if (await commandExists("mongosh", cwd)) {
      return "mongosh";
    }
    if (await commandExists("mongo", cwd)) {
      return "mongo";
    }
    throw new Error("Legacy Mongo mode requires local `python3+pymongo<4` or `mongosh` or `mongo` shell");
  })();
  return legacyMongoRunnerPromise;
}

function buildLegacyMongoScript(body: string): string {
  return `(function(){try{${body}}catch(e){print(JSON.stringify({ok:0,error:(e&&e.stack)?e.stack:String(e)}));quit(2);}})();`;
}

function parseLastJsonLine(raw: string): unknown | null {
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    try {
      return JSON.parse(lines[index]);
    } catch {
      continue;
    }
  }
  return null;
}

async function runLegacyMongoScript(uri: string, script: string, timeoutMs = 60_000): Promise<unknown> {
  const runner = await getLegacyMongoRunner();
  if (runner === "python") {
    throw new Error("Python legacy runner should use runLegacyMongoPythonOperation");
  }
  const args = [uri, "--quiet", "--eval", script];
  const result = await runLocalCommand(runner, args, agentBaseDir, timeoutMs);
  if (result.timedOut) {
    throw new Error("Legacy Mongo shell command timed out");
  }
  const payload = parseLastJsonLine(result.stdout);
  if (payload && typeof payload === "object") {
    const parsed = payload as { ok?: number; data?: unknown; error?: string };
    if (parsed.ok === 1) {
      return parsed.data;
    }
    if (parsed.ok === 0) {
      throw new Error(parsed.error ?? "Legacy Mongo shell returned error");
    }
  }
  throw new Error(result.stderr || result.stdout || `Legacy Mongo shell failed: ${result.exitCode}`);
}

async function runLegacyMongoPythonOperation(payload: Record<string, unknown>, timeoutMs = 20_000): Promise<unknown> {
  const runner = await getLegacyMongoRunner();
  if (runner !== "python") {
    throw new Error("Python legacy runner is unavailable");
  }
  const pythonCommand = await resolveLegacyPythonCommand(agentBaseDir);
  if (!pythonCommand) {
    throw new Error("Legacy Python command is unavailable");
  }

  const encodedPayload = JSON.stringify(JSON.stringify(payload));
  const pythonScript = `
import json, sys
from pymongo import MongoClient
from bson import json_util

payload = json.loads(${encodedPayload})
client = MongoClient(payload["uri"], serverSelectionTimeoutMS=5000, connectTimeoutMS=5000, socketTimeoutMS=5000)
try:
    db = client[payload["database"]]
    op = payload["op"]
    if op == "ping":
        data = db.command("ping")
    elif op == "find":
        coll = db[payload["collection"]]
        projection = payload.get("projection") or None
        cursor = coll.find(payload.get("filter") or {}, projection)
        sort = payload.get("sort") or {}
        if sort:
            cursor = cursor.sort(list(sort.items()))
        data = list(cursor.limit(int(payload.get("limit") or 100)))
    elif op == "aggregate":
        coll = db[payload["collection"]]
        data = list(coll.aggregate(payload.get("pipeline") or []))
    else:
        raise Exception(f"Unsupported op: {op}")
    print(json.dumps({"ok": 1, "data": json.loads(json_util.dumps(data))}))
except Exception as e:
    print(json.dumps({"ok": 0, "error": str(e)}))
    sys.exit(2)
finally:
    client.close()
`.trim();

  const result = await runLocalCommand(pythonCommand, ["-c", pythonScript], agentBaseDir, timeoutMs);
  if (result.timedOut) {
    throw new Error("Legacy Mongo python command timed out");
  }
  const payloadResult = parseLastJsonLine(result.stdout);
  if (payloadResult && typeof payloadResult === "object") {
    const parsed = payloadResult as { ok?: number; data?: unknown; error?: string };
    if (parsed.ok === 1) {
      return parsed.data;
    }
    if (parsed.ok === 0) {
      throw new Error(parsed.error ?? "Legacy Mongo python runner returned error");
    }
  }
  throw new Error(result.stderr || result.stdout || `Legacy Mongo python runner failed: ${result.exitCode}`);
}

async function runWithMongoMode<T>(
  datasource: DataSourceConfig,
  nativeOperation: () => Promise<T>,
  legacyOperation: () => Promise<T>,
): Promise<{ mode: "native" | "legacy-shell"; data: T }> {
  const configuredMode = datasource.connection.mongoMode ?? "auto";
  if (configuredMode === "legacy-shell") {
    return { mode: "legacy-shell", data: await legacyOperation() };
  }
  if (configuredMode === "native") {
    return { mode: "native", data: await nativeOperation() };
  }
  if (mongoAutoDetectedMode === "legacy-shell") {
    return { mode: "legacy-shell", data: await legacyOperation() };
  }
  try {
    const data = await nativeOperation();
    mongoAutoDetectedMode = "native";
    return { mode: "native", data };
  } catch (error) {
    if (!isMongoCompatibilityError(error)) {
      throw error;
    }
    mongoAutoDetectedMode = "legacy-shell";
    return { mode: "legacy-shell", data: await legacyOperation() };
  }
}

function validateMongoName(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new Error(`${label} is empty`);
  }
  if (!/^[A-Za-z0-9_.-]+$/.test(normalized)) {
    throw new Error(`${label} contains unsupported characters`);
  }
  return normalized;
}

function validateSqlIdentifier(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new Error(`${label} is empty`);
  }
  if (!/^[A-Za-z0-9_$.-]+$/.test(normalized)) {
    throw new Error(`${label} contains unsupported characters`);
  }
  return normalized;
}

function toMongoKeyValue(keyField: string, keyValue: string | number): unknown {
  if (typeof keyValue === "number") {
    return keyValue;
  }
  const value = keyValue.trim();
  if (keyField === "_id" && ObjectId.isValid(value) && value.length === 24) {
    return new ObjectId(value);
  }
  return value;
}

export async function testDatasource(datasource: DataSourceConfig): Promise<DataSourceTestResult> {
  const startedAt = Date.now();
  try {
    if (datasource.type === "mysql" || datasource.type === "postgres") {
      const client = await createDbClient(datasource);
      try {
        await client.query("SELECT 1 AS ok");
      } finally {
        await client.close();
      }
      return { ok: true, message: "SQL connection ok", durationMs: Date.now() - startedAt };
    }

    if (datasource.type === "mongo") {
      const uri = buildMongoUrl(datasource);
      const { mode } = await runWithMongoMode(
        datasource,
        async () => {
          const client = new MongoClient(uri);
          await client.connect();
          try {
            await client.db(datasource.connection.database || "admin").command({ ping: 1 });
          } finally {
            await client.close();
          }
          return true;
        },
        async () => {
          const runner = await getLegacyMongoRunner();
          if (runner === "python") {
            await runLegacyMongoPythonOperation({
              op: "ping",
              uri,
              database: datasource.connection.database || "admin",
            });
          } else {
            const script = buildLegacyMongoScript(`
              var targetDb=${JSON.stringify(datasource.connection.database || "admin")};
              var result=db.getSiblingDB(targetDb).runCommand({ping:1});
              print(JSON.stringify({ok:1,data:result}));
            `);
            await runLegacyMongoScript(uri, script);
          }
          return true;
        },
      );
      return {
        ok: true,
        message: `Mongo connection ok (${mode})`,
        durationMs: Date.now() - startedAt,
      };
    }

    if (datasource.type === "kafka") {
      const kafka = getKafkaClient(datasource);
      const admin = kafka.admin();
      await admin.connect();
      try {
        await admin.listTopics();
      } finally {
        await admin.disconnect();
      }
      return { ok: true, message: "Kafka connection ok", durationMs: Date.now() - startedAt };
    }

    if (datasource.type === "logcenter") {
      const session = await createLogcenterSession(datasource);
      const status = await fetchLogcenterJson(session, "/api/status");
      const dataViewRef = datasource.connection.dataView?.trim();
      const dataView = dataViewRef
        ? await resolveLogcenterDataView(session, datasource)
        : null;
      return {
        ok: true,
        message: dataView
          ? `Logcenter connection ok (${dataView.title})`
          : "Logcenter connection ok",
        durationMs: Date.now() - startedAt,
        details: {
          baseUrl: session.baseUrl,
          dataView,
          statusVersion: (status.version && typeof status.version === "object")
            ? (status.version as Record<string, unknown>).number ?? null
            : null,
        },
      };
    }

    if (datasource.type === "monitor" || datasource.type === "skywalking") {
      const result = await testMonitorDatasource(datasource);
      return {
        ok: true,
        message: result.message,
        durationMs: Date.now() - startedAt,
        details: result.details,
      };
    }

    throw new Error(`Unsupported datasource type: ${datasource.type}`);
  } catch (error) {
    return {
      ok: false,
      message: error instanceof Error ? error.message : String(error),
      durationMs: Date.now() - startedAt,
    };
  }
}

export async function logcenterSearch(params: {
  datasource: DataSourceConfig;
  dataView?: string;
  query?: string;
  from?: string;
  to?: string;
  limit?: number;
}): Promise<Record<string, unknown>> {
  const datasource = params.datasource;
  const session = await createLogcenterSession(datasource);
  const dataView = await resolveLogcenterDataView(session, datasource, params.dataView);
  const from = params.from?.trim() || "now-15m";
  const to = params.to?.trim() || "now";
  const limit = Math.max(1, Math.min(200, params.limit ?? 50));
  const searchResponse = await fetchLogcenterJson(
    session,
    "/internal/search/es",
    {
      method: "POST",
      headers: {
        "kbn-xsrf": "true",
        "elastic-api-version": "1",
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        params: {
          index: dataView.title,
          body: buildLogcenterSearchBody({
            query: params.query,
            timeFieldName: dataView.timeFieldName,
            from,
            to,
            limit,
          }),
        },
      }),
    },
  );

  const rawResponse =
    searchResponse.rawResponse && typeof searchResponse.rawResponse === "object"
      ? (searchResponse.rawResponse as Record<string, unknown>)
      : {};
  const hitsContainer =
    rawResponse.hits && typeof rawResponse.hits === "object"
      ? (rawResponse.hits as Record<string, unknown>)
      : {};
  const hits = Array.isArray(hitsContainer.hits) ? hitsContainer.hits : [];
  const totalRaw = hitsContainer.total;
  const totalHits =
    typeof totalRaw === "number"
      ? totalRaw
      : totalRaw && typeof totalRaw === "object"
        ? Number((totalRaw as Record<string, unknown>).value ?? hits.length)
        : hits.length;
  const totalRelation =
    totalRaw && typeof totalRaw === "object"
      ? String((totalRaw as Record<string, unknown>).relation ?? "eq")
      : "eq";

  return {
    datasourceId: datasource.id,
    datasourceType: datasource.type,
    baseUrl: session.baseUrl,
    dataView,
    query: params.query?.trim() || "",
    timeRange: {
      from,
      to,
      timeFieldName: dataView.timeFieldName,
    },
    totalHits,
    totalRelation,
    hitCount: hits.length,
    hits: sanitizeForJson(
      hits.map((item) => {
        const hit = item && typeof item === "object" ? (item as Record<string, unknown>) : {};
        const source =
          hit._source && typeof hit._source === "object" && !Array.isArray(hit._source)
            ? (hit._source as Record<string, unknown>)
            : {};
        const summary = pickLogcenterSummaryFields(source);
        return {
          index: String(hit._index ?? "").trim() || null,
          id: String(hit._id ?? "").trim() || null,
          timestamp: summary.timestamp ?? null,
          preview: summary.message ?? summary.request ?? summary.response ?? null,
          summary,
          source: buildLogcenterPreviewSource(source),
          sort: Array.isArray(hit.sort) ? hit.sort : [],
        };
      }),
    ),
  };
}

export async function sqlQueryReadonly(
  datasource: DataSourceConfig,
  sql: string,
  params: unknown[] = [],
): Promise<Record<string, unknown>> {
  assertReadOnlySql(sql);
  const client = await createDbClient(datasource);
  try {
    const rows = await client.query(sql, params);
    return {
      datasourceId: datasource.id,
      datasourceType: datasource.type,
      rowCount: rows.length,
      rows: sanitizeForJson(rows),
    };
  } finally {
    await client.close();
  }
}

async function resolveSqlSchemaName(
  client: DbClient,
  datasource: DataSourceConfig,
  explicitSchemaName?: string,
): Promise<string> {
  if (explicitSchemaName?.trim()) {
    return validateSqlIdentifier(explicitSchemaName, "schemaName");
  }
  if (datasource.connection.database?.trim()) {
    return validateSqlIdentifier(datasource.connection.database, "schemaName");
  }

  const rows =
    client.kind === "postgres"
      ? await client.query("SELECT current_schema() AS schema_name")
      : await client.query("SELECT DATABASE() AS schema_name");
  const schemaName = String(rows[0]?.schema_name ?? "").trim();
  if (!schemaName) {
    throw new Error("schemaName is required when datasource has no default database/schema");
  }
  return validateSqlIdentifier(schemaName, "schemaName");
}

export async function sqlDescribeSchemaReadonly(params: {
  datasource: DataSourceConfig;
  schemaName?: string;
  tableName?: string;
  limit?: number;
}): Promise<Record<string, unknown>> {
  const datasource = params.datasource;
  const client = await createDbClient(datasource);
  const limit = Math.max(1, Math.min(500, params.limit ?? 100));
  try {
    const schemaName = await resolveSqlSchemaName(client, datasource, params.schemaName);
    const tableName = params.tableName?.trim()
      ? validateSqlIdentifier(params.tableName, "tableName")
      : null;

    if (tableName) {
      if (client.kind === "postgres") {
        const tableRows = await client.query(
          `
            SELECT
              table_schema AS schema_name,
              table_name,
              table_type
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_name = $2
            ORDER BY table_name
          `,
          [schemaName, tableName],
        );
        const columnRows = await client.query(
          `
            SELECT
              table_schema AS schema_name,
              table_name,
              column_name,
              ordinal_position,
              data_type,
              udt_name,
              is_nullable,
              column_default
            FROM information_schema.columns
            WHERE table_schema = $1
              AND table_name = $2
            ORDER BY ordinal_position
          `,
          [schemaName, tableName],
        );
        return {
          datasourceId: datasource.id,
          datasourceType: datasource.type,
          schemaName,
          tableName,
          table: sanitizeForJson(tableRows[0] ?? null),
          columnCount: columnRows.length,
          columns: sanitizeForJson(columnRows),
        };
      }

      const tableRows = await client.query(
        `
          SELECT
            table_schema AS schema_name,
            table_name,
            table_type,
            engine,
            table_rows
          FROM information_schema.tables
          WHERE table_schema = ?
            AND table_name = ?
          ORDER BY table_name
        `,
        [schemaName, tableName],
      );
      const columnRows = await client.query(
        `
          SELECT
            table_schema AS schema_name,
            table_name,
            column_name,
            ordinal_position,
            column_type,
            data_type,
            is_nullable,
            column_default,
            column_key,
            extra
          FROM information_schema.columns
          WHERE table_schema = ?
            AND table_name = ?
          ORDER BY ordinal_position
        `,
        [schemaName, tableName],
      );
      return {
        datasourceId: datasource.id,
        datasourceType: datasource.type,
        schemaName,
        tableName,
        table: sanitizeForJson(tableRows[0] ?? null),
        columnCount: columnRows.length,
        columns: sanitizeForJson(columnRows),
      };
    }

    if (client.kind === "postgres") {
      const tableRows = await client.query(
        `
          SELECT
            table_schema AS schema_name,
            table_name,
            table_type
          FROM information_schema.tables
          WHERE table_schema = $1
          ORDER BY table_name
          LIMIT $2
        `,
        [schemaName, limit],
      );
      return {
        datasourceId: datasource.id,
        datasourceType: datasource.type,
        schemaName,
        tableCount: tableRows.length,
        tables: sanitizeForJson(tableRows),
      };
    }

    const tableRows = await client.query(
      `
        SELECT
          table_schema AS schema_name,
          table_name,
          table_type,
          engine,
          table_rows
        FROM information_schema.tables
        WHERE table_schema = ?
        ORDER BY table_name
        LIMIT ?
      `,
      [schemaName, limit],
    );
    return {
      datasourceId: datasource.id,
      datasourceType: datasource.type,
      schemaName,
      tableCount: tableRows.length,
      tables: sanitizeForJson(tableRows),
    };
  } finally {
    await client.close();
  }
}

export async function mongoFindReadonly(params: {
  datasource: DataSourceConfig;
  database?: string;
  collection: string;
  filterJson?: string;
  projectionJson?: string;
  sortJson?: string;
  limit?: number;
}): Promise<Record<string, unknown>> {
  const datasource = params.datasource;
  const database = validateMongoName(params.database ?? datasource.connection.database ?? "", "database");
  const collection = validateMongoName(params.collection, "collection");
  const filter = parseJsonObject(params.filterJson, "filterJson");
  const projection = parseJsonObject(params.projectionJson, "projectionJson");
  const sort = parseJsonObject(params.sortJson, "sortJson");
  const limit = Math.max(1, Math.min(500, params.limit ?? 100));
  const uri = buildMongoUrl(datasource);

  const { mode, data } = await runWithMongoMode(
    datasource,
    async () => {
      const client = new MongoClient(uri);
      await client.connect();
      try {
        const coll = client.db(database).collection(collection);
        let cursor = coll.find(filter, Object.keys(projection).length ? { projection } : {});
        if (Object.keys(sort).length) {
          cursor = cursor.sort(sort as Record<string, 1 | -1>);
        }
        return await cursor.limit(limit).toArray();
      } finally {
        await client.close();
      }
    },
    async () => {
      const runner = await getLegacyMongoRunner();
      if (runner === "python") {
        return (await runLegacyMongoPythonOperation({
          op: "find",
          uri,
          database,
          collection,
          filter,
          projection,
          sort,
          limit: Math.trunc(limit),
        })) as unknown[];
      }
      const script = buildLegacyMongoScript(`
        var database=${JSON.stringify(database)};
        var collection=${JSON.stringify(collection)};
        var filter=${JSON.stringify(filter)};
        var projection=${JSON.stringify(projection)};
        var sort=${JSON.stringify(sort)};
        var limit=${Math.trunc(limit)};
        var coll=db.getSiblingDB(database).getCollection(collection);
        var cursor=(Object.keys(projection).length>0)?coll.find(filter, projection):coll.find(filter);
        if (Object.keys(sort).length>0) cursor=cursor.sort(sort);
        var docs=cursor.limit(limit).toArray();
        print(JSON.stringify({ok:1,data:docs}));
      `);
      return await runLegacyMongoScript(uri, script) as unknown[];
    },
  );

  return {
    datasourceId: datasource.id,
    mongoMode: mode,
    database,
    collection,
    rowCount: Array.isArray(data) ? data.length : 0,
    rows: sanitizeForJson(data),
  };
}

export async function mongoAggregateReadonly(params: {
  datasource: DataSourceConfig;
  database?: string;
  collection: string;
  pipelineJson: string;
}): Promise<Record<string, unknown>> {
  const datasource = params.datasource;
  const database = validateMongoName(params.database ?? datasource.connection.database ?? "", "database");
  const collection = validateMongoName(params.collection, "collection");
  const uri = buildMongoUrl(datasource);
  let pipeline: unknown;
  try {
    pipeline = JSON.parse(params.pipelineJson);
  } catch (error) {
    throw new Error(`pipelineJson must be valid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!Array.isArray(pipeline)) {
    throw new Error("pipelineJson must be a JSON array");
  }

  const { mode, data } = await runWithMongoMode(
    datasource,
    async () => {
      const client = new MongoClient(uri);
      await client.connect();
      try {
        return await client.db(database).collection(collection).aggregate(pipeline as Record<string, unknown>[]).toArray();
      } finally {
        await client.close();
      }
    },
    async () => {
      const runner = await getLegacyMongoRunner();
      if (runner === "python") {
        return (await runLegacyMongoPythonOperation({
          op: "aggregate",
          uri,
          database,
          collection,
          pipeline,
        })) as unknown[];
      }
      const script = buildLegacyMongoScript(`
        var database=${JSON.stringify(database)};
        var collection=${JSON.stringify(collection)};
        var pipeline=${JSON.stringify(pipeline)};
        var rows=db.getSiblingDB(database).getCollection(collection).aggregate(pipeline).toArray();
        print(JSON.stringify({ok:1,data:rows}));
      `);
      return await runLegacyMongoScript(uri, script) as unknown[];
    },
  );

  return {
    datasourceId: datasource.id,
    mongoMode: mode,
    database,
    collection,
    rowCount: Array.isArray(data) ? data.length : 0,
    rows: sanitizeForJson(data),
  };
}

export async function kafkaTopicOffsets(
  datasource: DataSourceConfig,
  topic: string,
): Promise<Record<string, unknown>> {
  const kafka = getKafkaClient(datasource);
  const admin = kafka.admin();
  await admin.connect();
  try {
    const offsets = await admin.fetchTopicOffsets(topic);
    return {
      datasourceId: datasource.id,
      topic,
      offsets: sanitizeForJson(offsets),
    };
  } finally {
    await admin.disconnect();
  }
}

function parseOffset(raw: string | undefined): bigint {
  if (!raw || raw === "-1") {
    return 0n;
  }
  try {
    return BigInt(raw);
  } catch {
    return 0n;
  }
}

export async function kafkaConsumerLag(
  datasource: DataSourceConfig,
  groupId: string,
  topic?: string,
): Promise<Record<string, unknown>> {
  const kafka = getKafkaClient(datasource);
  const admin = kafka.admin();
  await admin.connect();
  try {
    const offsets = await admin.fetchOffsets({
      groupId,
      ...(topic ? { topics: [topic] } : {}),
    });
    const partitions: Array<Record<string, unknown>> = [];
    let totalLag = 0n;

    for (const topicOffsets of offsets) {
      const endOffsets = await admin.fetchTopicOffsets(topicOffsets.topic);
      const endByPartition = new Map<number, string>(endOffsets.map((item) => [item.partition, item.offset]));
      for (const partition of topicOffsets.partitions) {
        const committed = parseOffset(partition.offset);
        const end = parseOffset(endByPartition.get(partition.partition));
        const lag = end > committed ? end - committed : 0n;
        totalLag += lag;
        partitions.push({
          topic: topicOffsets.topic,
          partition: partition.partition,
          committed: committed.toString(),
          end: end.toString(),
          lag: lag.toString(),
        });
      }
    }

    return {
      datasourceId: datasource.id,
      groupId,
      topic: topic ?? null,
      totalLag: totalLag.toString(),
      partitions,
    };
  } finally {
    await admin.disconnect();
  }
}

export function valueToMongoKey(keyField: string, keyValue: string | number): unknown {
  return toMongoKeyValue(keyField, keyValue);
}
