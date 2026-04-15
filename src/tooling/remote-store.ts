import { existsSync, readFileSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { nowIso } from "../utils.js";

export interface RemoteToolStoreSource {
  id: string;
  label: string;
  url: string;
  enabled: boolean;
  description: string;
  createdAt: string;
  updatedAt: string;
}

export interface RemoteToolStorePackageRecord {
  id: string;
  label: string;
  version: string;
  description: string;
  tags: string[];
  manifestUrl: string;
  homepage?: string;
  sourceId: string;
  sourceLabel: string;
  installed: boolean;
}

interface RemoteCatalogPackageRecord {
  id?: unknown;
  label?: unknown;
  version?: unknown;
  description?: unknown;
  tags?: unknown;
  manifestUrl?: unknown;
  homepage?: unknown;
}

interface RemoteCatalogFile {
  packages?: unknown;
}

function defaultRemoteSourcesPath(): string {
  return path.join(os.homedir(), ".wms-ai-agent", "tool-store", "remote-sources.json");
}

export function resolveRemoteSourcesPath(): string {
  const configured = process.env.WMS_AI_AGENT_REMOTE_SOURCES_PATH?.trim();
  if (configured) {
    return path.resolve(configured);
  }
  return defaultRemoteSourcesPath();
}

function normalizeRemoteSource(input: unknown): RemoteToolStoreSource | null {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    return null;
  }
  const item = input as Record<string, unknown>;
  const id = String(item.id ?? "").trim();
  const label = String(item.label ?? "").trim() || id;
  const url = String(item.url ?? "").trim();
  if (!id || !url) {
    return null;
  }
  return {
    id,
    label,
    url,
    enabled: item.enabled !== false,
    description: String(item.description ?? "").trim(),
    createdAt: String(item.createdAt ?? nowIso()),
    updatedAt: String(item.updatedAt ?? nowIso()),
  };
}

function readRemoteSourceFileSync(filePath: string): RemoteToolStoreSource[] {
  if (!existsSync(filePath)) {
    return [];
  }
  try {
    const parsed = JSON.parse(readFileSync(filePath, "utf8")) as unknown;
    if (!Array.isArray(parsed)) {
      return [];
    }
    return parsed
      .map((item) => normalizeRemoteSource(item))
      .filter((item): item is RemoteToolStoreSource => item !== null)
      .sort((left, right) => left.id.localeCompare(right.id));
  } catch {
    return [];
  }
}

async function writeRemoteSourceFile(filePath: string, items: RemoteToolStoreSource[]): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, JSON.stringify(items, null, 2), "utf8");
}

function assertHttpUrl(value: string, label: string): URL {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch (error) {
    throw new Error(`${label} must be a valid URL: ${error instanceof Error ? error.message : String(error)}`);
  }
  if (!["http:", "https:"].includes(parsed.protocol)) {
    throw new Error(`${label} must use http or https`);
  }
  return parsed;
}

function normalizeRemotePackage(
  item: RemoteCatalogPackageRecord,
  source: RemoteToolStoreSource,
  installedIds: Set<string>,
): RemoteToolStorePackageRecord | null {
  const id = String(item.id ?? "").trim();
  const manifestUrl = String(item.manifestUrl ?? "").trim();
  if (!id || !manifestUrl) {
    return null;
  }
  assertHttpUrl(manifestUrl, `manifestUrl for package ${id}`);
  return {
    id,
    label: String(item.label ?? id).trim(),
    version: String(item.version ?? "0.1.0").trim(),
    description: String(item.description ?? "").trim(),
    tags: Array.isArray(item.tags)
      ? item.tags.map((tag) => String(tag ?? "").trim()).filter(Boolean)
      : [],
    manifestUrl,
    homepage: String(item.homepage ?? "").trim() || undefined,
    sourceId: source.id,
    sourceLabel: source.label,
    installed: installedIds.has(id),
  };
}

async function fetchJson(url: string): Promise<unknown> {
  const response = await fetch(url, {
    headers: {
      accept: "application/json",
      "user-agent": "wms-ai-investigator/0.4.0",
    },
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status} ${response.statusText}`);
  }
  return await response.json();
}

async function fetchText(url: string): Promise<string> {
  const response = await fetch(url, {
    headers: {
      accept: "application/javascript, text/plain;q=0.9, */*;q=0.8",
      "user-agent": "wms-ai-investigator/0.4.0",
    },
  });
  if (!response.ok) {
    throw new Error(`Request failed for ${url}: ${response.status} ${response.statusText}`);
  }
  return await response.text();
}

function parseRemoteCatalog(raw: unknown): RemoteCatalogPackageRecord[] {
  if (Array.isArray(raw)) {
    return raw.filter((item): item is RemoteCatalogPackageRecord => Boolean(item) && typeof item === "object");
  }
  if (raw && typeof raw === "object" && !Array.isArray(raw)) {
    const parsed = raw as RemoteCatalogFile;
    if (Array.isArray(parsed.packages)) {
      return parsed.packages.filter((item): item is RemoteCatalogPackageRecord => Boolean(item) && typeof item === "object");
    }
  }
  throw new Error("Remote catalog must be a JSON array or { packages: [] } object");
}

export function listRemoteSources(): RemoteToolStoreSource[] {
  return readRemoteSourceFileSync(resolveRemoteSourcesPath());
}

export async function addRemoteSource(input: {
  id: string;
  label?: string;
  url: string;
  description?: string;
  enabled?: boolean;
  overwrite?: boolean;
}): Promise<RemoteToolStoreSource> {
  const id = String(input.id ?? "").trim();
  const url = String(input.url ?? "").trim();
  if (!id) {
    throw new Error("Remote source id is required");
  }
  assertHttpUrl(url, "Remote source url");
  const filePath = resolveRemoteSourcesPath();
  const current = readRemoteSourceFileSync(filePath);
  const now = nowIso();
  const nextItem: RemoteToolStoreSource = {
    id,
    label: String(input.label ?? id).trim() || id,
    url,
    enabled: input.enabled !== false,
    description: String(input.description ?? "").trim(),
    createdAt: now,
    updatedAt: now,
  };
  const existingIndex = current.findIndex((item) => item.id === id);
  if (existingIndex >= 0) {
    if (!input.overwrite) {
      throw new Error(`Remote source already exists: ${id}`);
    }
    nextItem.createdAt = current[existingIndex].createdAt;
    current[existingIndex] = nextItem;
  } else {
    current.push(nextItem);
  }
  await writeRemoteSourceFile(filePath, current.sort((left, right) => left.id.localeCompare(right.id)));
  return nextItem;
}

export async function removeRemoteSource(sourceId: string): Promise<void> {
  const id = String(sourceId ?? "").trim();
  if (!id) {
    throw new Error("Remote source id is required");
  }
  const filePath = resolveRemoteSourcesPath();
  const current = readRemoteSourceFileSync(filePath);
  const nextItems = current.filter((item) => item.id !== id);
  if (nextItems.length === current.length) {
    throw new Error(`Remote source not found: ${id}`);
  }
  if (nextItems.length === 0) {
    await rm(filePath, { force: true });
    return;
  }
  await writeRemoteSourceFile(filePath, nextItems);
}

export async function listRemotePackages(installedIds: Set<string>): Promise<RemoteToolStorePackageRecord[]> {
  const sources = listRemoteSources().filter((item) => item.enabled);
  const results: RemoteToolStorePackageRecord[] = [];
  for (const source of sources) {
    let payload: unknown;
    try {
      payload = await fetchJson(source.url);
    } catch {
      continue;
    }
    for (const item of parseRemoteCatalog(payload)) {
      const normalized = normalizeRemotePackage(item, source, installedIds);
      if (normalized) {
        results.push(normalized);
      }
    }
  }
  return results.sort((left, right) => {
    const sourceCompare = left.sourceId.localeCompare(right.sourceId);
    return sourceCompare !== 0 ? sourceCompare : left.id.localeCompare(right.id);
  });
}

export async function installRemotePackage(
  storePath: string,
  sourceId: string,
  packageId: string,
  options?: { overwrite?: boolean },
): Promise<{
  sourceId: string;
  packageId: string;
  destinationDir: string;
  manifestPath: string;
  modulePath: string;
  overwritten: boolean;
}> {
  const source = listRemoteSources().find((item) => item.id === sourceId && item.enabled);
  if (!source) {
    throw new Error(`Remote source not found or disabled: ${sourceId}`);
  }
  const packages = await listRemotePackages(new Set());
  const pkg = packages.find((item) => item.sourceId === sourceId && item.id === packageId);
  if (!pkg) {
    throw new Error(`Remote package not found: ${packageId} from ${sourceId}`);
  }

  const manifestPayload = await fetchJson(pkg.manifestUrl);
  if (!manifestPayload || typeof manifestPayload !== "object" || Array.isArray(manifestPayload)) {
    throw new Error(`Remote manifest must be a JSON object: ${pkg.manifestUrl}`);
  }
  const manifest = manifestPayload as Record<string, unknown>;
  const rawEntry = String(manifest.entry ?? "").trim();
  if (!rawEntry) {
    throw new Error(`Remote manifest entry is required: ${pkg.manifestUrl}`);
  }
  const manifestUrl = assertHttpUrl(pkg.manifestUrl, "manifestUrl");
  const entryUrl = new URL(rawEntry, manifestUrl);
  const moduleCode = await fetchText(entryUrl.href);

  const destinationDir = path.join(storePath, "tool-modules", pkg.id);
  const manifestPath = path.join(destinationDir, "manifest.json");
  const modulePath = path.join(destinationDir, "module.js");
  const destinationExists = existsSync(destinationDir);
  if (destinationExists && !options?.overwrite) {
    throw new Error(`Remote package already installed: ${pkg.id}`);
  }

  await mkdir(destinationDir, { recursive: true });
  const localManifest = {
    id: pkg.id,
    label: pkg.label,
    version: pkg.version,
    description: pkg.description,
    entry: "./module.js",
    enabled: manifest.enabled !== false,
    tags: Array.isArray(manifest.tags) ? manifest.tags.map((item) => String(item ?? "").trim()).filter(Boolean) : pkg.tags,
    hotReloadable: manifest.hotReloadable !== false,
    source: "external",
    remoteSourceId: source.id,
    remoteSourceUrl: source.url,
    remoteManifestUrl: pkg.manifestUrl,
    homepage: pkg.homepage ?? null,
  };
  await writeFile(manifestPath, JSON.stringify(localManifest, null, 2), "utf8");
  await writeFile(modulePath, moduleCode, "utf8");

  return {
    sourceId: source.id,
    packageId: pkg.id,
    destinationDir,
    manifestPath,
    modulePath,
    overwritten: destinationExists,
  };
}
