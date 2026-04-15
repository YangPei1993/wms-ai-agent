import { existsSync, FSWatcher, readFileSync, readdirSync, statSync, watch } from "node:fs";
import { cp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { ConfigStore } from "../types.js";
import { ToolDefinition } from "./shared.js";

export interface ToolModuleManifest {
  id: string;
  label: string;
  version: string;
  description: string;
  source: "builtin" | "external";
  entry: string;
  enabled: boolean;
  tags: string[];
  hotReloadable: boolean;
  manifestPath?: string;
}

export interface LoadedToolModule {
  manifest: ToolModuleManifest;
  definitions: ToolDefinition[];
}

interface ToolModuleOverrideRecord {
  enabled?: boolean;
  updatedAt?: string;
}

interface ExternalModuleManifestFile {
  id: string;
  label?: string;
  version?: string;
  description?: string;
  entry: string;
  enabled?: boolean;
  tags?: string[];
  hotReloadable?: boolean;
}

interface ToolModuleLoaderOptions {
  storePath: string;
  getStore: () => ConfigStore;
  getStorePath: () => string;
  buildBuiltinModules: () => LoadedToolModule[] | Promise<LoadedToolModule[]>;
}

interface ExternalModuleLoadFailure {
  manifestPath: string;
  error: string;
}

export interface ToolStorePackageRecord {
  id: string;
  label: string;
  version: string;
  description: string;
  tags: string[];
  manifestPath: string;
  sourceDir: string;
  entry: string;
  installed: boolean;
}

export interface ToolModuleWatchStatus {
  active: boolean;
  watchedPaths: string[];
  reloadCount: number;
  lastEventAt: string | null;
  lastEventPath: string | null;
}

interface ToolModuleExport {
  manifest?: Partial<ToolModuleManifest>;
  buildToolDefinitions?: (
    getStore: () => ConfigStore,
    getStorePath: () => string,
  ) => ToolDefinition[] | Promise<ToolDefinition[]>;
  default?: (
    getStore: () => ConfigStore,
    getStorePath: () => string,
  ) => ToolDefinition[] | Promise<ToolDefinition[]>;
}

function safeReadDir(dirPath: string): string[] {
  try {
    if (!existsSync(dirPath) || !statSync(dirPath).isDirectory()) {
      return [];
    }
    return readdirSync(dirPath).map((item) => path.join(dirPath, item));
  } catch {
    return [];
  }
}

function defaultExternalModuleDirs(storePath: string): string[] {
  const dirs = [path.join(storePath, "tool-modules")];
  const extra = process.env.WMS_AI_AGENT_TOOL_MODULES_PATH?.trim();
  if (extra) {
    for (const entry of extra.split(path.delimiter).map((item) => item.trim()).filter(Boolean)) {
      dirs.push(entry);
    }
  }
  return Array.from(new Set(dirs.map((item) => path.resolve(item))));
}

function defaultToolStorePackagesDir(): string {
  const currentFile = fileURLToPath(import.meta.url);
  const repoRoot = path.resolve(path.dirname(currentFile), "..", "..");
  const configured = process.env.WMS_AI_AGENT_TOOL_STORE_PATH?.trim();
  if (configured) {
    return path.resolve(configured);
  }
  return path.join(repoRoot, "tool-store", "packages");
}

function resolveManagedModulesDir(storePath: string): string {
  return path.join(storePath, "tool-modules");
}

function resolveModuleOverridesPath(): string {
  const configured = process.env.WMS_AI_AGENT_TOOL_MODULE_OVERRIDES_PATH?.trim();
  if (configured) {
    return path.resolve(configured);
  }
  return path.join(path.dirname(defaultToolStorePackagesDir()), "module-overrides.json");
}

function readModuleOverridesSync(filePath: string): Record<string, ToolModuleOverrideRecord> {
  if (!existsSync(filePath)) {
    return {};
  }
  try {
    const parsed = JSON.parse(readFileSync(filePath, "utf8")) as unknown;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return {};
    }
    const result: Record<string, ToolModuleOverrideRecord> = {};
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      if (!value || typeof value !== "object" || Array.isArray(value)) {
        continue;
      }
      const record = value as Record<string, unknown>;
      result[key] = {
        enabled: typeof record.enabled === "boolean" ? record.enabled : undefined,
        updatedAt: typeof record.updatedAt === "string" ? record.updatedAt : undefined,
      };
    }
    return result;
  } catch {
    return {};
  }
}

async function writeModuleOverrides(filePath: string, value: Record<string, ToolModuleOverrideRecord>): Promise<void> {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, JSON.stringify(value, null, 2), "utf8");
}

function candidateManifestPaths(dir: string): string[] {
  const manifestPaths: string[] = [];
  for (const item of safeReadDir(dir)) {
    const itemStat = safeStat(item);
    if (!itemStat) {
      continue;
    }
    if (itemStat.isFile() && item.endsWith(".json")) {
      manifestPaths.push(item);
      continue;
    }
    if (itemStat.isDirectory()) {
      const manifestPath = path.join(item, "manifest.json");
      if (existsSync(manifestPath)) {
        manifestPaths.push(manifestPath);
      }
    }
  }
  return manifestPaths;
}

function safeStat(targetPath: string) {
  try {
    return statSync(targetPath);
  } catch {
    return null;
  }
}

async function importExternalModule(entryPath: string): Promise<ToolModuleExport> {
  const moduleUrl = pathToFileURL(entryPath).href;
  return await import(moduleUrl);
}

export class ToolModuleLoader {
  private modules: LoadedToolModule[] = [];
  private failures: ExternalModuleLoadFailure[] = [];
  private watchers: FSWatcher[] = [];
  private debounceTimer: NodeJS.Timeout | null = null;
  private watchCallback: (() => Promise<void> | void) | null = null;
  private watchStatus: ToolModuleWatchStatus = {
    active: false,
    watchedPaths: [],
    reloadCount: 0,
    lastEventAt: null,
    lastEventPath: null,
  };

  constructor(private readonly options: ToolModuleLoaderOptions) {}

  async reload(): Promise<{
    moduleCount: number;
    toolCount: number;
    failureCount: number;
  }> {
    const builtinModules = await Promise.resolve(this.options.buildBuiltinModules());
    const externalResult = await this.loadExternalModules();
    const combined = [...builtinModules, ...externalResult.modules];
    this.assertNoDuplicateToolNames(combined);
    this.modules = combined;
    this.failures = externalResult.failures;
    this.refreshWatchers();
    return {
      moduleCount: this.modules.length,
      toolCount: this.modules.reduce((sum, item) => sum + item.definitions.length, 0),
      failureCount: this.failures.length,
    };
  }

  getDefinitions(): ToolDefinition[] {
    return this.modules
      .filter((module) => module.manifest.enabled)
      .flatMap((module) => module.definitions);
  }

  listModules(options?: { includeDisabled?: boolean }): Array<Record<string, unknown>> {
    return this.modules
      .filter((module) => (options?.includeDisabled ? true : module.manifest.enabled))
      .map((module) => ({
        ...module.manifest,
        toolCount: module.definitions.length,
        toolNames: module.definitions.map((definition) => definition.name),
        canToggle: module.manifest.source === "external",
        canUninstall: this.isManagedExternalModule(module.manifest),
      }));
  }

  listFailures(): ExternalModuleLoadFailure[] {
    return [...this.failures];
  }

  getInstalledModuleIds(): Set<string> {
    return new Set(this.listInstalledModuleRecords().map((item) => item.id));
  }

  listStorePackages(): ToolStorePackageRecord[] {
    const packagesDir = defaultToolStorePackagesDir();
    const installedIds = this.getInstalledModuleIds();

    return candidateManifestPaths(packagesDir)
      .map((manifestPath) => {
        const parsed = this.readManifestSync(manifestPath);
        return {
          id: String(parsed.id).trim(),
          label: String(parsed.label ?? parsed.id).trim(),
          version: String(parsed.version ?? "0.1.0").trim(),
          description: String(parsed.description ?? "").trim(),
          tags: Array.isArray(parsed.tags) ? parsed.tags.map((item) => String(item ?? "").trim()).filter(Boolean) : [],
          manifestPath,
          sourceDir: path.dirname(manifestPath),
          entry: String(parsed.entry ?? "").trim(),
          installed: installedIds.has(String(parsed.id).trim()),
        };
      })
      .filter((item) => item.id && item.entry)
      .sort((left, right) => left.id.localeCompare(right.id));
  }

  async installFromStore(packageId: string, options?: { overwrite?: boolean }): Promise<{
    packageId: string;
    destinationDir: string;
    manifestPath: string;
    overwritten: boolean;
  }> {
    const packageRecord = this.listStorePackages().find((item) => item.id === packageId);
    if (!packageRecord) {
      throw new Error(`Tool store package not found: ${packageId}`);
    }
    const destinationDir = path.join(this.options.storePath, "tool-modules", packageRecord.id);
    const destinationStat = safeStat(destinationDir);
    if (destinationStat && !options?.overwrite) {
      throw new Error(`Tool module already installed: ${packageId}`);
    }
    await mkdir(path.dirname(destinationDir), { recursive: true });
    await cp(packageRecord.sourceDir, destinationDir, {
      recursive: true,
      force: true,
    });
    return {
      packageId,
      destinationDir,
      manifestPath: path.join(destinationDir, "manifest.json"),
      overwritten: Boolean(destinationStat),
    };
  }

  async setModuleEnabled(moduleId: string, enabled: boolean): Promise<{
    moduleId: string;
    enabled: boolean;
    overridesPath: string;
  }> {
    const moduleRecord = this.findInstalledModuleRecord(moduleId);
    if (!moduleRecord) {
      throw new Error(`Installed tool module not found: ${moduleId}`);
    }
    const overridesPath = resolveModuleOverridesPath();
    const overrides = readModuleOverridesSync(overridesPath);
    overrides[moduleId] = {
      ...(overrides[moduleId] ?? {}),
      enabled,
      updatedAt: new Date().toISOString(),
    };
    await writeModuleOverrides(overridesPath, overrides);
    return {
      moduleId,
      enabled,
      overridesPath,
    };
  }

  async uninstallModule(moduleId: string): Promise<{
    moduleId: string;
    removedDir: string;
    overridesPath: string;
  }> {
    const moduleRecord = this.findInstalledModuleRecord(moduleId);
    if (!moduleRecord) {
      throw new Error(`Installed tool module not found: ${moduleId}`);
    }
    const removedDir = moduleRecord.sourceDir;
    await rm(removedDir, { recursive: true, force: true });

    const overridesPath = resolveModuleOverridesPath();
    const overrides = readModuleOverridesSync(overridesPath);
    if (moduleId in overrides) {
      delete overrides[moduleId];
      await writeModuleOverrides(overridesPath, overrides);
    }

    return {
      moduleId,
      removedDir,
      overridesPath,
    };
  }

  startWatching(onReloadRequired: () => Promise<void> | void): ToolModuleWatchStatus {
    this.watchCallback = onReloadRequired;
    this.watchStatus.active = true;
    this.refreshWatchers();
    return this.getWatchStatus();
  }

  stopWatching(): void {
    this.watchStatus.active = false;
    this.watchCallback = null;
    this.clearWatchers();
  }

  getWatchStatus(): ToolModuleWatchStatus {
    return {
      ...this.watchStatus,
      watchedPaths: [...this.watchStatus.watchedPaths],
    };
  }

  private async loadExternalModules(): Promise<{
    modules: LoadedToolModule[];
    failures: ExternalModuleLoadFailure[];
  }> {
    const modules: LoadedToolModule[] = [];
    const failures: ExternalModuleLoadFailure[] = [];
    const overrides = readModuleOverridesSync(resolveModuleOverridesPath());

    for (const dir of defaultExternalModuleDirs(this.options.storePath)) {
      for (const manifestPath of candidateManifestPaths(dir)) {
        try {
          const module = await this.loadExternalModuleFromManifest(manifestPath, overrides);
          modules.push(module);
        } catch (error) {
          failures.push({
            manifestPath,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }

    return { modules, failures };
  }

  private async loadExternalModuleFromManifest(
    manifestPath: string,
    overrides: Record<string, ToolModuleOverrideRecord>,
  ): Promise<LoadedToolModule> {
    const parsed = await this.readManifest(manifestPath);

    const entryPath = path.resolve(path.dirname(manifestPath), String(parsed.entry).trim());
    const imported = await importExternalModule(entryPath);
    const buildToolDefinitions =
      imported.buildToolDefinitions ?? imported.default;
    if (typeof buildToolDefinitions !== "function") {
      throw new Error(`Module ${entryPath} must export buildToolDefinitions or default function`);
    }

    const definitions = await Promise.resolve(buildToolDefinitions(this.options.getStore, this.options.getStorePath));
    if (!Array.isArray(definitions) || definitions.length === 0) {
      throw new Error(`Module ${entryPath} returned no tool definitions`);
    }

    const moduleId = String(parsed.id).trim();
    const override = overrides[moduleId];
    const effectiveEnabled = typeof override?.enabled === "boolean" ? override.enabled : parsed.enabled !== false;

    return {
      manifest: {
        id: moduleId,
        label: String(parsed.label ?? parsed.id).trim(),
        version: String(parsed.version ?? "0.1.0").trim(),
        description: String(parsed.description ?? imported.manifest?.description ?? "").trim(),
        source: "external",
        entry: entryPath,
        enabled: effectiveEnabled,
        tags: Array.isArray(parsed.tags) ? parsed.tags.map((item) => String(item ?? "").trim()).filter(Boolean) : [],
        hotReloadable: parsed.hotReloadable !== false,
        manifestPath,
      },
      definitions,
    };
  }

  private assertNoDuplicateToolNames(modules: LoadedToolModule[]): void {
    const seen = new Map<string, string>();
    for (const module of modules) {
      for (const definition of module.definitions) {
        const previous = seen.get(definition.name);
        if (previous) {
          throw new Error(`Duplicate tool name "${definition.name}" from modules ${previous} and ${module.manifest.id}`);
        }
        seen.set(definition.name, module.manifest.id);
      }
    }
  }

  private async readManifest(manifestPath: string): Promise<ExternalModuleManifestFile> {
    const manifestText = await readFile(manifestPath, "utf8");
    return this.parseManifest(manifestText);
  }

  private readManifestSync(manifestPath: string): ExternalModuleManifestFile {
    return this.parseManifest(readFileSync(manifestPath, "utf8"));
  }

  private parseManifest(manifestText: string): ExternalModuleManifestFile {
    const parsed = JSON.parse(manifestText) as ExternalModuleManifestFile;
    if (!parsed || typeof parsed !== "object") {
      throw new Error("Manifest must be a JSON object");
    }
    if (!String(parsed.id ?? "").trim()) {
      throw new Error("Manifest id is required");
    }
    if (!String(parsed.entry ?? "").trim()) {
      throw new Error("Manifest entry is required");
    }
    return parsed;
  }

  private refreshWatchers(): void {
    this.clearWatchers();
    if (!this.watchStatus.active || !this.watchCallback) {
      return;
    }

    const watchTargets = new Set<string>(defaultExternalModuleDirs(this.options.storePath));
    for (const module of this.modules) {
      if (module.manifest.source !== "external" || !module.manifest.manifestPath) {
        continue;
      }
      watchTargets.add(path.dirname(module.manifest.manifestPath));
    }

    const watchedPaths: string[] = [];
    for (const targetPath of watchTargets) {
      const targetStat = safeStat(targetPath);
      if (!targetStat?.isDirectory()) {
        continue;
      }
      try {
        const watcher = watch(targetPath, { persistent: false }, (_eventType, filename) => {
          this.watchStatus.lastEventAt = new Date().toISOString();
          this.watchStatus.lastEventPath = filename ? path.join(targetPath, String(filename)) : targetPath;
          this.scheduleReload();
        });
        this.watchers.push(watcher);
        watchedPaths.push(targetPath);
      } catch {
        // Ignore individual watcher failures; manual reload still works.
      }
    }
    this.watchStatus.watchedPaths = watchedPaths;
  }

  private clearWatchers(): void {
    for (const watcher of this.watchers) {
      watcher.close();
    }
    this.watchers = [];
    this.watchStatus.watchedPaths = [];
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
  }

  private scheduleReload(): void {
    if (!this.watchCallback) {
      return;
    }
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
    }
    this.debounceTimer = setTimeout(async () => {
      this.debounceTimer = null;
      this.watchStatus.reloadCount += 1;
      try {
        await this.watchCallback?.();
      } catch {
        // Keep watcher alive; errors surface through manual reload/catalog tools.
      }
    }, 400);
  }

  private listInstalledModuleRecords(): ToolStorePackageRecord[] {
    const managedDir = resolveManagedModulesDir(this.options.storePath);
    return candidateManifestPaths(managedDir)
      .map((manifestPath) => {
        const parsed = this.readManifestSync(manifestPath);
        return {
          id: String(parsed.id ?? "").trim(),
          label: String(parsed.label ?? parsed.id).trim(),
          version: String(parsed.version ?? "0.1.0").trim(),
          description: String(parsed.description ?? "").trim(),
          tags: Array.isArray(parsed.tags) ? parsed.tags.map((item) => String(item ?? "").trim()).filter(Boolean) : [],
          manifestPath,
          sourceDir: path.dirname(manifestPath),
          entry: String(parsed.entry ?? "").trim(),
          installed: true,
        };
      })
      .filter((item) => item.id && item.entry)
      .sort((left, right) => left.id.localeCompare(right.id));
  }

  private findInstalledModuleRecord(moduleId: string): ToolStorePackageRecord | null {
    return this.listInstalledModuleRecords().find((item) => item.id === moduleId) ?? null;
  }

  private isManagedExternalModule(manifest: ToolModuleManifest): boolean {
    if (manifest.source !== "external" || !manifest.manifestPath) {
      return false;
    }
    const managedDir = resolveManagedModulesDir(this.options.storePath);
    const manifestDir = path.dirname(manifest.manifestPath);
    return manifestDir === path.join(managedDir, manifest.id);
  }
}
