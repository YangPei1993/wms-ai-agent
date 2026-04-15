import { DataSourceConfig } from "./types.js";
import { sanitizeForJson } from "./utils.js";

const REMOTE_WMS_AGENT_SESSION_TTL_MS = 30 * 60 * 1_000;
const DEFAULT_STREAM_TIMEOUT_MS = 60_000;
const STREAM_RECONNECT_DELAY_MS = 250;
const MAX_STREAM_RECONNECT_ATTEMPTS = 3;

type RemoteWmsAgentSession = {
  baseUrl: string;
  cookieHeader: string;
  createdAt: number;
};

const remoteWmsAgentSessionCache = new Map<string, RemoteWmsAgentSession>();

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function truncateText(text: string, maxLength: number): string {
  return text.length > maxLength ? `${text.slice(0, maxLength).trimEnd()}...` : text;
}

function normalizeRemoteWmsAgentBaseUrl(value: string, label: string): string {
  const normalized = value.trim();
  if (!normalized) {
    throw new Error(`${label} is required`);
  }
  const candidate = /^https?:\/\//i.test(normalized) ? normalized : `http://${normalized}`;
  let parsed: URL;
  try {
    parsed = new URL(candidate);
  } catch (error) {
    throw new Error(`${label} is not a valid URL: ${error instanceof Error ? error.message : String(error)}`);
  }
  const pathname = parsed.pathname.replace(/\/+$/, "");
  return `${parsed.origin}${pathname === "/" ? "" : pathname}`;
}

function resolveRemoteWmsAgentBaseUrl(datasource: DataSourceConfig): string {
  const candidate = datasource.connection.uri?.trim() || datasource.connection.host?.trim();
  if (!candidate) {
    throw new Error(`Datasource ${datasource.id} requires URI or HOST`);
  }
  return normalizeRemoteWmsAgentBaseUrl(candidate, `Datasource ${datasource.id} base URL`);
}

function buildRemoteUrl(baseUrl: string, pathName: string): URL {
  const normalizedBaseUrl = baseUrl.endsWith("/") ? baseUrl : `${baseUrl}/`;
  const normalizedPathName = pathName.replace(/^\/+/, "");
  return new URL(normalizedPathName, normalizedBaseUrl);
}

function getSetCookies(response: Response): string[] {
  const headers = response.headers as Headers & { getSetCookie?: () => string[] };
  if (typeof headers.getSetCookie === "function") {
    return headers.getSetCookie();
  }
  const single = response.headers.get("set-cookie");
  return single ? [single] : [];
}

function parseCookieHeader(value: string | undefined): Map<string, string> {
  const cookies = new Map<string, string>();
  for (const item of String(value ?? "").split(/;\s*/)) {
    const normalized = item.trim();
    if (!normalized) {
      continue;
    }
    const index = normalized.indexOf("=");
    if (index <= 0) {
      continue;
    }
    cookies.set(normalized.slice(0, index), normalized.slice(index + 1));
  }
  return cookies;
}

function mergeCookieHeaders(currentCookieHeader: string | undefined, setCookies: string[]): string {
  const cookies = parseCookieHeader(currentCookieHeader);
  for (const item of setCookies) {
    const first = item.split(";")[0]?.trim() ?? "";
    if (!first) {
      continue;
    }
    const index = first.indexOf("=");
    if (index <= 0) {
      continue;
    }
    cookies.set(first.slice(0, index), first.slice(index + 1));
  }
  return Array.from(cookies.entries()).map(([name, value]) => `${name}=${value}`).join("; ");
}

function requireRemoteCredentials(datasource: DataSourceConfig): { username: string; secret: string } {
  const username = datasource.auth.username?.trim() ?? "";
  const secret = datasource.auth.secret ?? "";
  if (!username || !secret) {
    throw new Error(`Datasource ${datasource.id} requires USERNAME and SECRET for remote WMS Agent login`);
  }
  return { username, secret };
}

async function parseJsonResponse<TData>(response: Response, label: string): Promise<TData> {
  const rawText = await response.text();
  if (!response.ok) {
    throw new Error(`${label} failed ${response.status}: ${truncateText(rawText || response.statusText, 240)}`);
  }
  try {
    return JSON.parse(rawText) as TData;
  } catch (error) {
    throw new Error(`${label} returned invalid JSON: ${error instanceof Error ? error.message : String(error)}`);
  }
}

async function loginRemoteWmsAgent(datasource: DataSourceConfig): Promise<RemoteWmsAgentSession> {
  const baseUrl = resolveRemoteWmsAgentBaseUrl(datasource);
  const { username, secret } = requireRemoteCredentials(datasource);

  const csrfResponse = await fetch(buildRemoteUrl(baseUrl, "/api/auth/csrf"), {
    headers: {
      Accept: "application/json",
    },
  });
  let cookieHeader = mergeCookieHeaders("", getSetCookies(csrfResponse));
  const csrfPayload = await parseJsonResponse<{ csrfToken?: string }>(csrfResponse, "WMS Agent csrf");
  const csrfToken = csrfPayload.csrfToken?.trim();
  if (!csrfToken) {
    throw new Error("WMS Agent csrf token is empty");
  }

  const callbackResponse = await fetch(buildRemoteUrl(baseUrl, "/api/auth/callback/credentials"), {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/x-www-form-urlencoded",
      "X-Auth-Return-Redirect": "1",
      ...(cookieHeader ? { Cookie: cookieHeader } : {}),
    },
    body: new URLSearchParams({
      email: username,
      password: secret,
      csrfToken,
      callbackUrl: `${baseUrl}/`,
    }),
  });
  cookieHeader = mergeCookieHeaders(cookieHeader, getSetCookies(callbackResponse));
  const callbackPayload = await parseJsonResponse<{ url?: string; error?: string }>(
    callbackResponse,
    "WMS Agent credential login",
  );

  const redirectUrl = callbackPayload.url ? new URL(callbackPayload.url, `${baseUrl}/`) : null;
  const error = callbackPayload.error ?? redirectUrl?.searchParams.get("error") ?? null;
  if (error) {
    throw new Error(`WMS Agent login failed: ${error}`);
  }
  if (!cookieHeader) {
    throw new Error("WMS Agent login succeeded but did not return a session cookie");
  }

  const session: RemoteWmsAgentSession = {
    baseUrl,
    cookieHeader,
    createdAt: Date.now(),
  };
  remoteWmsAgentSessionCache.set(datasource.id, session);
  return session;
}

async function resolveRemoteSession(
  datasource: DataSourceConfig,
  forceRefresh = false,
): Promise<RemoteWmsAgentSession> {
  const baseUrl = resolveRemoteWmsAgentBaseUrl(datasource);
  const cached = remoteWmsAgentSessionCache.get(datasource.id);
  if (
    !forceRefresh &&
    cached &&
    cached.baseUrl === baseUrl &&
    Date.now() - cached.createdAt < REMOTE_WMS_AGENT_SESSION_TTL_MS
  ) {
    return cached;
  }
  return await loginRemoteWmsAgent(datasource);
}

function storeUpdatedSession(datasource: DataSourceConfig, session: RemoteWmsAgentSession, response: Response): RemoteWmsAgentSession {
  const mergedCookieHeader = mergeCookieHeaders(session.cookieHeader, getSetCookies(response));
  if (mergedCookieHeader === session.cookieHeader) {
    return session;
  }
  const updated = {
    ...session,
    cookieHeader: mergedCookieHeader,
    createdAt: Date.now(),
  };
  remoteWmsAgentSessionCache.set(datasource.id, updated);
  return updated;
}

async function fetchRemoteJson<TData>(
  datasource: DataSourceConfig,
  pathName: string,
  init?: RequestInit,
  retryOnUnauthorized = true,
): Promise<TData> {
  let session = await resolveRemoteSession(datasource);
  const doFetch = async (activeSession: RemoteWmsAgentSession) => {
    const response = await fetch(buildRemoteUrl(activeSession.baseUrl, pathName), {
      ...init,
      headers: {
        Accept: "application/json",
        ...(activeSession.cookieHeader ? { Cookie: activeSession.cookieHeader } : {}),
        ...((init?.headers ?? {}) as Record<string, string>),
      },
    });
    session = storeUpdatedSession(datasource, activeSession, response);
    return response;
  };

  let response = await doFetch(session);
  if (response.status === 401 && retryOnUnauthorized) {
    session = await resolveRemoteSession(datasource, true);
    response = await doFetch(session);
  }
  return await parseJsonResponse<TData>(response, `WMS Agent ${pathName}`);
}

async function openRemoteStream(
  datasource: DataSourceConfig,
  pathName: string,
): Promise<Response> {
  let session = await resolveRemoteSession(datasource);
  const doFetch = async (activeSession: RemoteWmsAgentSession) => {
    const response = await fetch(buildRemoteUrl(activeSession.baseUrl, pathName), {
      headers: {
        Accept: "text/event-stream",
        ...(activeSession.cookieHeader ? { Cookie: activeSession.cookieHeader } : {}),
      },
    });
    session = storeUpdatedSession(datasource, activeSession, response);
    return response;
  };

  let response = await doFetch(session);
  if (response.status === 401) {
    session = await resolveRemoteSession(datasource, true);
    response = await doFetch(session);
  }
  if (!response.ok || !response.body) {
    const rawText = await response.text().catch(() => "");
    throw new Error(`WMS Agent stream failed ${response.status}: ${truncateText(rawText || response.statusText, 240)}`);
  }
  return response;
}

function isTerminalTurnEvent(event: Record<string, unknown>): boolean {
  return event.type === "turn.completed"
    || event.type === "turn.failed"
    || event.type === "turn.paused";
}

async function readSseStream(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  onEvent: (event: Record<string, unknown>) => Promise<boolean> | boolean,
): Promise<boolean> {
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }

    buffer += decoder.decode(value, { stream: true });
    const chunks = buffer.split("\n\n");
    buffer = chunks.pop() ?? "";

    for (const chunk of chunks) {
      if (!chunk.startsWith("data: ")) {
        continue;
      }
      try {
        const payload = JSON.parse(chunk.slice(6)) as Record<string, unknown>;
        const terminal = await onEvent(payload);
        if (terminal) {
          return true;
        }
      } catch {
        // ignore malformed payload
      }
    }
  }

  return false;
}

function summarizeStreamEvent(event: Record<string, unknown>): Record<string, unknown> {
  const snapshot = asRecord(event.snapshot);
  return {
    seq: asNumber(event.seq),
    type: asString(event.type) ?? null,
    agent: asString(event.agent) ?? asString(event.agentId) ?? null,
    tool: asString(event.tool) ?? null,
    status: asString(event.status) ?? null,
    message: asString(event.message) ?? null,
    reason: asString(event.reason) ?? null,
    summary: asString(event.summary) ?? null,
    runtimeEvent: asString(event.event) ?? null,
    runtimeStage: asString(snapshot?.stage) ?? null,
    timestamp: asNumber(event.timestamp),
  };
}

async function waitForTurnTerminal(params: {
  datasource: DataSourceConfig;
  threadId: string;
  turnId: string;
  afterSeq: number;
  timeoutMs?: number;
}): Promise<{
  afterSeq: number;
  eventCount: number;
  events: Array<Record<string, unknown>>;
  terminalEvent: Record<string, unknown> | null;
}> {
  const timeoutMs = Math.max(5_000, Math.min(5 * 60_000, params.timeoutMs ?? DEFAULT_STREAM_TIMEOUT_MS));
  const deadline = Date.now() + timeoutMs;
  const events: Array<Record<string, unknown>> = [];
  let afterSeq = params.afterSeq;
  let reconnectAttempts = 0;
  let terminalEvent: Record<string, unknown> | null = null;

  while (Date.now() < deadline) {
    const response = await openRemoteStream(
      params.datasource,
      `/api/threads/${encodeURIComponent(params.threadId)}/stream?${new URLSearchParams({
        turnId: params.turnId,
        afterSeq: String(afterSeq),
      }).toString()}`,
    );

    const done = await readSseStream(response.body!.getReader(), async (event) => {
      const seq = asNumber(event.seq);
      if (seq != null && seq > afterSeq) {
        afterSeq = seq;
      }
      events.push(summarizeStreamEvent(event));
      if (isTerminalTurnEvent(event)) {
        terminalEvent = sanitizeForJson(event) as Record<string, unknown>;
        return true;
      }
      return false;
    });

    if (done || terminalEvent) {
      return {
        afterSeq,
        eventCount: events.length,
        events,
        terminalEvent,
      };
    }

    reconnectAttempts += 1;
    if (reconnectAttempts > MAX_STREAM_RECONNECT_ATTEMPTS) {
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, STREAM_RECONNECT_DELAY_MS * reconnectAttempts));
  }

  throw new Error(`Timed out waiting for remote WMS Agent turn ${params.turnId}`);
}

function summarizeTurn(turn: unknown): Record<string, unknown> {
  const record = asRecord(turn) ?? {};
  const items = Array.isArray(record.items) ? record.items : [];
  const events = Array.isArray(record.events) ? record.events : [];
  return {
    id: asString(record.id) ?? null,
    status: asString(record.status) ?? null,
    intentKind: asString(record.intentKind) ?? null,
    overlay: asString(record.overlay) ?? null,
    createdAt: asString(record.createdAt) ?? null,
    itemCount: items.length,
    eventCount: events.length,
  };
}

function summarizeSession(session: unknown): Record<string, unknown> | null {
  const record = asRecord(session);
  if (!record) {
    return null;
  }
  const runtimeInspector = asRecord(record.runtimeInspector);
  const pendingDecision = asRecord(record.pendingDecision);
  const activeProfile = asRecord(record.activeProfile);
  return {
    currentTurnId: asString(record.currentTurnId) ?? null,
    turnStatus: asString(record.turnStatus) ?? null,
    activeProfile: activeProfile
      ? {
          id: asString(activeProfile.id) ?? null,
          name: asString(activeProfile.name) ?? null,
        }
      : null,
    pendingDecisionKind: asString(pendingDecision?.kind) ?? null,
    attentionRequired: runtimeInspector?.attentionRequired === true,
    suggestedAction: asString(runtimeInspector?.suggestedAction) ?? null,
    blockingReason: asString(runtimeInspector?.blockingReason) ?? null,
  };
}

function extractAssistantMessage(detail: unknown, turnId: string): Record<string, unknown> | null {
  const detailRecord = asRecord(detail) ?? {};
  const transcript = asRecord(detailRecord.transcript);
  const entries = Array.isArray(transcript?.entries) ? transcript.entries : [];
  const matched = entries
    .map((entry) => asRecord(entry))
    .filter((entry): entry is Record<string, unknown> => Boolean(entry))
    .filter((entry) => entry.type === "message" && entry.role === "assistant")
    .filter((entry) => entry.turnId === turnId);
  const latest = matched.at(-1)
    ?? entries
      .map((entry) => asRecord(entry))
      .filter((entry): entry is Record<string, unknown> => Boolean(entry))
      .filter((entry) => entry.type === "message" && entry.role === "assistant")
      .at(-1)
    ?? null;
  if (!latest) {
    return null;
  }
  return {
    id: asString(latest.id) ?? null,
    turnId: asString(latest.turnId) ?? null,
    content: asString(latest.content) ?? null,
    toolCallCount: Array.isArray(latest.toolCalls) ? latest.toolCalls.length : 0,
    imageCount: Array.isArray(latest.images) ? latest.images.length : 0,
  };
}

export async function testRemoteWmsAgentDatasource(
  datasource: DataSourceConfig,
): Promise<{ message: string; details?: Record<string, unknown> }> {
  const session = await resolveRemoteSession(datasource, true);
  const threads = await fetchRemoteJson<Record<string, unknown>>(
    datasource,
    `/api/threads?${new URLSearchParams({ limit: "1", offset: "0" }).toString()}`,
    undefined,
    false,
  );
  return {
    message: "Remote WMS Agent connection ok",
    details: {
      baseUrl: session.baseUrl,
      cookieActive: Boolean(session.cookieHeader),
      sampleThreadCount: Array.isArray((threads as Record<string, unknown>).threads)
        ? ((threads as Record<string, unknown>).threads as unknown[]).length
        : null,
    },
  };
}

export async function listRemoteWmsAgentThreads(params: {
  datasource: DataSourceConfig;
  limit?: number;
  offset?: number;
}): Promise<Record<string, unknown>> {
  const payload = await fetchRemoteJson<Record<string, unknown>>(
    params.datasource,
    `/api/threads?${new URLSearchParams({
      limit: String(Math.max(1, Math.min(100, params.limit ?? 20))),
      offset: String(Math.max(0, params.offset ?? 0)),
    }).toString()}`,
  );
  return {
    datasourceId: params.datasource.id,
    datasourceType: params.datasource.type,
    baseUrl: resolveRemoteWmsAgentBaseUrl(params.datasource),
    ...sanitizeForJson(payload) as Record<string, unknown>,
  };
}

export async function createRemoteWmsAgentThread(params: {
  datasource: DataSourceConfig;
  title?: string;
  activeProfileId?: string;
  mode?: "direct" | "scheduled";
}): Promise<Record<string, unknown>> {
  const payload = await fetchRemoteJson<Record<string, unknown>>(
    params.datasource,
    "/api/threads",
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        mode: params.mode ?? "direct",
        ...(params.title?.trim() ? { title: params.title.trim() } : {}),
        ...(params.activeProfileId?.trim() ? { activeProfileId: params.activeProfileId.trim() } : {}),
      }),
    },
  );
  return {
    datasourceId: params.datasource.id,
    datasourceType: params.datasource.type,
    baseUrl: resolveRemoteWmsAgentBaseUrl(params.datasource),
    ...sanitizeForJson(payload) as Record<string, unknown>,
  };
}

export async function getRemoteWmsAgentThreadDetail(params: {
  datasource: DataSourceConfig;
  threadId: string;
}): Promise<Record<string, unknown>> {
  const payload = await fetchRemoteJson<Record<string, unknown>>(
    params.datasource,
    `/api/threads/${encodeURIComponent(params.threadId)}`,
  );
  const turns = Array.isArray(payload.turns) ? payload.turns : [];
  return {
    datasourceId: params.datasource.id,
    datasourceType: params.datasource.type,
    baseUrl: resolveRemoteWmsAgentBaseUrl(params.datasource),
    thread: {
      id: asString(payload.id) ?? params.threadId,
      title: asString(payload.title) ?? null,
      mode: asString(payload.mode) ?? null,
      updatedAt: asString(payload.updatedAt) ?? null,
    },
    latestRunEventSeq: asNumber(payload.latestRunEventSeq),
    turnCount: turns.length,
    turns: turns.slice(-10).map(summarizeTurn),
    session: summarizeSession(payload.session),
    latestAssistantMessage: extractAssistantMessage(payload, asString(turns.at(-1) && asRecord(turns.at(-1))?.id) ?? ""),
    detail: sanitizeForJson(payload),
  };
}

export async function runRemoteWmsAgentTurn(params: {
  datasource: DataSourceConfig;
  threadId?: string;
  title?: string;
  activeProfileId?: string;
  content: string;
  waitTimeoutMs?: number;
  includeThreadDetail?: boolean;
}): Promise<Record<string, unknown>> {
  const threadId = params.threadId?.trim()
    ? params.threadId.trim()
    : String((await createRemoteWmsAgentThread({
        datasource: params.datasource,
        title: params.title,
        activeProfileId: params.activeProfileId,
        mode: "direct",
      })).id ?? "");
  if (!threadId) {
    throw new Error("Failed to resolve remote WMS Agent threadId");
  }

  const accepted = await fetchRemoteJson<{
    ok: true;
    threadId: string;
    turnId: string;
    status: "accepted" | "claimed" | "running";
    afterSeq: number;
  }>(
    params.datasource,
    `/api/threads/${encodeURIComponent(threadId)}/turns`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-Idempotency-Key": `wms-ai-agent:${crypto.randomUUID()}`,
      },
      body: JSON.stringify({
        message: {
          content: params.content,
          ...(params.activeProfileId?.trim() ? { profileId: params.activeProfileId.trim() } : {}),
        },
      }),
    },
  );

  const streamResult = await waitForTurnTerminal({
    datasource: params.datasource,
    threadId,
    turnId: accepted.turnId,
    afterSeq: accepted.afterSeq,
    timeoutMs: params.waitTimeoutMs,
  });
  const detail = await fetchRemoteJson<Record<string, unknown>>(
    params.datasource,
    `/api/threads/${encodeURIComponent(threadId)}`,
  );
  const turns = Array.isArray(detail.turns) ? detail.turns : [];
  const currentTurn = turns
    .map((turn) => asRecord(turn))
    .filter((turn): turn is Record<string, unknown> => Boolean(turn))
    .find((turn) => turn.id === accepted.turnId)
    ?? null;

  return {
    datasourceId: params.datasource.id,
    datasourceType: params.datasource.type,
    baseUrl: resolveRemoteWmsAgentBaseUrl(params.datasource),
    threadId,
    turnId: accepted.turnId,
    acceptedStatus: accepted.status,
    afterSeq: streamResult.afterSeq,
    terminalEventType: asString(streamResult.terminalEvent?.type) ?? null,
    terminalEvent: streamResult.terminalEvent,
    eventCount: streamResult.eventCount,
    events: streamResult.events,
    turn: currentTurn ? summarizeTurn(currentTurn) : null,
    session: summarizeSession(detail.session),
    assistantMessage: extractAssistantMessage(detail, accepted.turnId),
    ...(params.includeThreadDetail ? { detail: sanitizeForJson(detail) } : {}),
  };
}
