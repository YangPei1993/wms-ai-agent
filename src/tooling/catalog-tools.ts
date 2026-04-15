import { promises as fs } from "node:fs";
import path from "node:path";
import { z } from "zod";
import { testDatasource } from "../datasources.js";
import { ConfigStore } from "../types.js";
import { nowIso, safeJsonStringify, slugify } from "../utils.js";
import {
  caseFileName,
  getDatasourcesForProjects,
  getEnabledProjects,
  getProjectDatasources,
  listMarkdownFilesRecursive,
  parseOptionalStringArrayJson,
  readMarkdownDoc,
  resolveDatasource,
  resolveDatasourceCandidates,
  resolveKnowledgeBaseRoots,
  resolveKnowledgeOrMemoryPath,
  resolveMemoryRoots,
  resolveProjectScope,
  summarizeDatasource,
  ToolDefinition,
  ToolSchema,
  uniqueByKey,
  workspaceMemoryRoot,
  projectMemoryRoot,
} from "./shared.js";
import { searchAcrossRoots } from "./search-helpers.js";

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

function memoryBaseRoot(storePath: string, projectId?: string | null): string {
  return projectId ? projectMemoryRoot(storePath, projectId) : workspaceMemoryRoot(storePath);
}

function memoryCasesRoot(storePath: string, projectId?: string | null): string {
  return path.join(memoryBaseRoot(storePath, projectId), "cases");
}

function memoryPatternsRoot(storePath: string, projectId?: string | null): string {
  return path.join(memoryBaseRoot(storePath, projectId), "patterns");
}

function classifyMemoryDocument(relPath: string): "case" | "pattern" | "memory" {
  const normalized = String(relPath ?? "");
  if (
    normalized.includes("/memory/cases/") ||
    normalized.startsWith("memory/cases/") ||
    normalized.startsWith("cases/") ||
    normalized.includes("/cases/")
  ) {
    return "case";
  }
  if (
    normalized.includes("/memory/patterns/") ||
    normalized.startsWith("memory/patterns/") ||
    normalized.startsWith("patterns/") ||
    normalized.includes("/patterns/")
  ) {
    return "pattern";
  }
  return "memory";
}

function parseMetadataLine(content: string, key: string): string | null {
  const pattern = new RegExp(`^-\\s+${key}:\\s*(.*)$`, "mi");
  const matched = pattern.exec(content);
  return matched ? matched[1].trim() : null;
}

function extractCaseMetadata(content: string): Record<string, string | number | null> {
  const occurrenceRaw = parseMetadataLine(content, "Occurrence Count");
  return {
    dedupeKey: parseMetadataLine(content, "Dedupe Key"),
    patternKey: parseMetadataLine(content, "Pattern Key"),
    title: /^#\s+(.+)$/m.exec(content)?.[1]?.trim() ?? null,
    occurrenceCount: occurrenceRaw ? Number(occurrenceRaw) || 1 : 1,
    lastSeenAt: parseMetadataLine(content, "Last Seen At"),
  };
}

function buildCaseDedupeKey(input: {
  projectId?: string | null;
  title: string;
  tags: string[];
  patternKey?: string | null;
}): string {
  if (input.patternKey?.trim()) {
    return `${input.projectId ?? "workspace"}::pattern::${slugify(input.patternKey)}`;
  }
  const normalizedTitle = slugify(input.title);
  const primaryTag = input.tags[0] ? slugify(input.tags[0]) : "generic";
  return `${input.projectId ?? "workspace"}::${primaryTag}::${normalizedTitle}`;
}

async function listMarkdownFilesSafe(root: string): Promise<string[]> {
  try {
    const items = await fs.readdir(root);
    return items.filter((item) => item.endsWith(".md")).sort((left, right) => left.localeCompare(right));
  } catch {
    return [];
  }
}

async function findExistingCaseByDedupeKey(root: string, dedupeKey: string): Promise<{ absPath: string; content: string } | null> {
  for (const fileName of await listMarkdownFilesSafe(root)) {
    const absPath = path.join(root, fileName);
    const content = await fs.readFile(absPath, "utf8");
    if (parseMetadataLine(content, "Dedupe Key") === dedupeKey) {
      return { absPath, content };
    }
  }
  return null;
}

async function writePatternFile(params: {
  absPath: string;
  title: string;
  projectId?: string | null;
  patternKey: string;
  tags: string[];
  trigger: string;
  rootCause: string;
  method: string;
  recommendations: string;
  relatedCaseRelPath?: string | null;
}): Promise<void> {
  const existing = await fs.readFile(params.absPath, "utf8").catch(() => "");
  const existingCases = new Set(
    existing
      .split(/\r?\n/)
      .map((line) => /^- \[(.+?)\]\((.+?)\)$/.exec(line))
      .filter(Boolean)
      .map((match) => (match as RegExpExecArray)[2]),
  );
  if (params.relatedCaseRelPath) {
    existingCases.add(params.relatedCaseRelPath);
  }
  const relatedCaseLines = Array.from(existingCases).sort().map((relPath) => {
    const label = path.basename(relPath, ".md");
    return `- [${label}](${relPath})`;
  });
  const content = [
    `# ${params.title}`,
    "",
    `- Pattern Key: ${params.patternKey}`,
    `- Project: ${params.projectId ?? "workspace"}`,
    `- Tags: ${params.tags.join(", ")}`,
    `- Updated At: ${nowIso()}`,
    "",
    "## Trigger / Symptoms",
    "",
    params.trigger || "未填写",
    "",
    "## Root Cause",
    "",
    params.rootCause || "未填写",
    "",
    "## Investigation Method",
    "",
    params.method || "未填写",
    "",
    "## Recommendations",
    "",
    params.recommendations || "未填写",
    "",
    "## Related Cases",
    "",
    ...(relatedCaseLines.length ? relatedCaseLines : ["- 暂无"]),
    "",
  ].join("\n");
  await fs.writeFile(params.absPath, content, "utf8");
}

function memoryDocumentSummary(root: string, item: Record<string, unknown>) {
  const relPath = String(item.relPath ?? "");
  const absPath = String(item.absPath ?? "");
  const kind = classifyMemoryDocument(relPath || path.relative(root, absPath));
  return {
    ...item,
    kind,
  };
}

export function buildCatalogToolDefinitions(
  getStore: () => ConfigStore,
  getStorePath: () => string,
): ToolDefinition[] {
  return [
    {
      name: "workspace_playbook",
      category: "workspace",
      description:
        "Load workspace-level instructions, tool usage rules, and enabled project summaries. Recommended as the first call when the user question does not explicitly say which project to inspect.",
      schema: {
        includeDatasources: z.boolean().default(false),
      } satisfies ToolSchema,
      handler: async (_context, { includeDatasources }) => {
        const store = getStore();
        const projects = getEnabledProjects(store).map((project) => ({
          id: project.id,
          label: project.label,
          description: project.description,
          matchHints: project.matchHints,
          repoRoots: project.repoRoots,
          logRoots: project.logRoots,
          datasourceIds: project.datasourceIds,
          datasources: includeDatasources ? getProjectDatasources(store, project).map(summarizeDatasource) : undefined,
        }));
        return {
          activeProjectId: store.activeProjectId,
          globalInstructions: store.globalInstructions,
          toolUsageGuidelines: store.toolUsageGuidelines,
          enabledProjectCount: projects.length,
          projects,
          recommendedStartTools: [
            "workspace_playbook",
            "project_catalog",
            "project_playbook",
            "datasource_overview",
          ],
        };
      },
    },
    {
      name: "project_catalog",
      category: "project",
      description:
        "List all enabled projects and their bound datasources. Use this or project_playbook before deep investigation if project context is unclear.",
      cacheTtlMs: 60_000,
      schema: {
        includeDisabled: z.boolean().optional(),
        includeDatasources: z.boolean().default(true),
      } satisfies ToolSchema,
      handler: async (_context, { includeDisabled, includeDatasources }) => {
        const store = getStore();
        const projects = (includeDisabled ? store.projects : getEnabledProjects(store)).map((project) => ({
          id: project.id,
          label: project.label,
          enabled: project.enabled,
          matchHints: project.matchHints,
          repoRoots: project.repoRoots,
          logRoots: project.logRoots,
          datasourceIds: project.datasourceIds,
          instructionsSummary: project.instructions.slice(0, 240),
          datasources: includeDatasources ? getProjectDatasources(store, project).map(summarizeDatasource) : undefined,
        }));
        return {
          activeProjectId: store.activeProjectId,
          globalInstructions: store.globalInstructions,
          toolUsageGuidelines: store.toolUsageGuidelines,
          projects,
        };
      },
    },
    {
      name: "project_playbook",
      category: "project",
      description:
        "Load the global guidance, project instructions, project checklist, and bound datasources for the current investigation. Supports single-project and multi-project scope.",
      cacheTtlMs: 60_000,
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (_context, { projectId, projectIdsJson }) => {
        const store = getStore();
        const projects = resolveProjectScope(store, { projectId, projectIdsJson });
        const datasources = getDatasourcesForProjects(store, projects).map(summarizeDatasource);
        const project = projects.length === 1 ? projects[0] : null;
        return {
          activeProjectId: store.activeProjectId,
          globalInstructions: store.globalInstructions,
          toolUsageGuidelines: store.toolUsageGuidelines,
          projectIds: projects.map((item) => item.id),
          projectCount: projects.length,
          project: project
            ? {
                id: project.id,
                label: project.label,
                description: project.description,
                matchHints: project.matchHints,
                repoRoots: project.repoRoots,
                logRoots: project.logRoots,
                instructions: project.instructions,
                investigationChecklist: project.investigationChecklist,
              }
            : null,
          projects: projects.map((item) => ({
            id: item.id,
            label: item.label,
            description: item.description,
            matchHints: item.matchHints,
            repoRoots: item.repoRoots,
            logRoots: item.logRoots,
            instructions: item.instructions,
            investigationChecklist: item.investigationChecklist,
          })),
          datasources,
        };
      },
    },
    {
      name: "kb_catalog",
      category: "knowledge-base",
      description:
        "List knowledge-base markdown documents from workspace scope and optional project scopes. Use this to see what static knowledge is available before reading details.",
      cacheTtlMs: 60_000,
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson }) => {
        const store = getStore();
        const roots = resolveKnowledgeBaseRoots(context.getStorePath(), store, { projectId, projectIdsJson });
        const documents = uniqueByKey(
          roots.flatMap((rootInfo) =>
            listMarkdownFilesRecursive(rootInfo.root).map((item) => ({
              scope: rootInfo.scope,
              projectId: rootInfo.projectId ?? null,
              root: rootInfo.root,
              ...item,
            })),
          ),
          (item) => String((item as Record<string, unknown>).absPath ?? ""),
        );
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          documentCount: documents.length,
          documents,
        };
      },
    },
    {
      name: "kb_read",
      category: "knowledge-base",
      description:
        "Read one knowledge-base markdown document by absolute path or workspace-relative path.",
      schema: {
        relPath: z.string().optional(),
        absPath: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { relPath, absPath }) => {
        const storePath = context.getStorePath();
        const normalized = resolveKnowledgeOrMemoryPath(storePath, relPath as string | undefined, absPath as string | undefined);
        const allowedRoots = [
          path.resolve(path.join(storePath, "knowledge-base")),
          ...getEnabledProjects(getStore()).map((project) =>
            path.resolve(path.join(storePath, "projects", project.id, "knowledge-base"))
          ),
        ];
        if (!allowedRoots.some((root) => normalized === root || normalized.startsWith(`${root}${path.sep}`))) {
          throw new Error(`Knowledge-base path is outside allowed roots: ${normalized}`);
        }
        return await readMarkdownDoc(normalized);
      },
    },
    {
      name: "kb_search",
      category: "knowledge-base",
      description:
        "Search across knowledge-base markdown documents at workspace scope and optional project scopes.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        query: z.string().min(1),
        limit: z.number().int().positive().max(500).default(100),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson, query, limit }) => {
        const store = getStore();
        const roots = resolveKnowledgeBaseRoots(context.getStorePath(), store, { projectId, projectIdsJson }).map(
          (item) => item.root,
        );
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          matches: await searchAcrossRoots(roots, query, "*.md", limit, { fixedString: true }),
        };
      },
    },
    {
      name: "memory_catalog",
      category: "memory",
      description:
        "List recorded memory markdown documents from workspace scope and optional project scopes. Distinguishes reusable patterns from concrete cases.",
      cacheTtlMs: 60_000,
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson }) => {
        const store = getStore();
        const roots = resolveMemoryRoots(context.getStorePath(), store, { projectId, projectIdsJson });
        const documents = uniqueByKey(
          roots.flatMap((rootInfo) =>
            listMarkdownFilesRecursive(rootInfo.root).map((item) => ({
              scope: rootInfo.scope,
              projectId: rootInfo.projectId ?? null,
              root: rootInfo.root,
              ...memoryDocumentSummary(rootInfo.root, item),
            })),
          ),
          (item) => String((item as Record<string, unknown>).absPath ?? ""),
        );
        const caseCount = documents.filter((item) => String((item as Record<string, unknown>).kind ?? "") === "case").length;
        const patternCount = documents.filter((item) => String((item as Record<string, unknown>).kind ?? "") === "pattern").length;
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          documentCount: documents.length,
          caseCount,
          patternCount,
          memoryCount: documents.length - caseCount - patternCount,
          documents,
        };
      },
    },
    {
      name: "memory_pattern_catalog",
      category: "memory",
      description:
        "List reusable memory patterns only. Use this before recording a new case to see whether the same root-cause/method already exists.",
      cacheTtlMs: 60_000,
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson }) => {
        const store = getStore();
        const roots = resolveMemoryRoots(context.getStorePath(), store, { projectId, projectIdsJson });
        const documents = uniqueByKey(
          roots.flatMap((rootInfo) =>
            listMarkdownFilesRecursive(path.join(rootInfo.root, "patterns")).map((item) => ({
              scope: rootInfo.scope,
              projectId: rootInfo.projectId ?? null,
              root: path.join(rootInfo.root, "patterns"),
              ...memoryDocumentSummary(path.join(rootInfo.root, "patterns"), item),
            })),
          ),
          (item) => String((item as Record<string, unknown>).absPath ?? ""),
        );
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          patternCount: documents.length,
          documents,
        };
      },
    },
    {
      name: "memory_read",
      category: "memory",
      description:
        "Read one recorded case-memory markdown document by absolute path or workspace-relative path.",
      schema: {
        relPath: z.string().optional(),
        absPath: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { relPath, absPath }) => {
        const storePath = context.getStorePath();
        const normalized = resolveKnowledgeOrMemoryPath(storePath, relPath as string | undefined, absPath as string | undefined);
        const allowedRoots = [
          path.resolve(workspaceMemoryRoot(storePath)),
          ...getEnabledProjects(getStore()).map((project) => path.resolve(projectMemoryRoot(storePath, project.id))),
        ];
        if (!allowedRoots.some((root) => normalized === root || normalized.startsWith(`${root}${path.sep}`))) {
          throw new Error(`Memory path is outside allowed roots: ${normalized}`);
        }
        const doc = await readMarkdownDoc(normalized);
        return {
          ...doc,
          kind: classifyMemoryDocument(path.relative(storePath, normalized)),
          metadata: extractCaseMetadata(String(doc.content ?? "")),
        };
      },
    },
    {
      name: "memory_pattern_read",
      category: "memory",
      description:
        "Read one reusable memory pattern markdown document by absolute path or workspace-relative path.",
      schema: {
        relPath: z.string().optional(),
        absPath: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, { relPath, absPath }) => {
        const storePath = context.getStorePath();
        const normalized = resolveKnowledgeOrMemoryPath(storePath, relPath as string | undefined, absPath as string | undefined);
        const allowedRoots = [
          path.resolve(path.join(workspaceMemoryRoot(storePath), "patterns")),
          ...getEnabledProjects(getStore()).map((project) =>
            path.resolve(path.join(projectMemoryRoot(storePath, project.id), "patterns"))
          ),
        ];
        if (!allowedRoots.some((root) => normalized === root || normalized.startsWith(`${root}${path.sep}`))) {
          throw new Error(`Pattern path is outside allowed roots: ${normalized}`);
        }
        const doc = await readMarkdownDoc(normalized);
        return {
          ...doc,
          kind: "pattern",
        };
      },
    },
    {
      name: "memory_search",
      category: "memory",
      description:
        "Search across recorded case-memory markdown documents at workspace scope and optional project scopes.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        query: z.string().min(1),
        limit: z.number().int().positive().max(500).default(100),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson, query, limit }) => {
        const store = getStore();
        const roots = resolveMemoryRoots(context.getStorePath(), store, { projectId, projectIdsJson }).map(
          (item) => item.root,
        );
        const matches = await searchAcrossRoots(roots, query, "*.md", limit, { fixedString: true });
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          matches: matches.map((item) => {
            const root = String((item as Record<string, unknown>).repoRoot ?? "");
            const filePath = String((item as Record<string, unknown>).filePath ?? "");
            return {
              ...item,
              kind: classifyMemoryDocument(path.relative(root, filePath)),
            };
          }),
        };
      },
    },
    {
      name: "memory_pattern_search",
      category: "memory",
      description:
        "Search reusable memory patterns only. Prefer this before recording a new pattern or case.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        query: z.string().min(1),
        limit: z.number().int().positive().max(500).default(100),
      } satisfies ToolSchema,
      handler: async (context, { projectId, projectIdsJson, query, limit }) => {
        const store = getStore();
        const roots = resolveMemoryRoots(context.getStorePath(), store, { projectId, projectIdsJson }).map(
          (item) => path.join(item.root, "patterns"),
        );
        const matches = await searchAcrossRoots(roots, query, "*.md", limit, { fixedString: true });
        return {
          projectId: projectId ?? null,
          projectIds: parseOptionalStringArrayJson(projectIdsJson, "projectIdsJson"),
          matches: matches.map((item) => {
            const root = String((item as Record<string, unknown>).repoRoot ?? "");
            const filePath = String((item as Record<string, unknown>).filePath ?? "");
            return {
              ...item,
              kind: classifyMemoryDocument(path.relative(root, filePath)),
            };
          }),
        };
      },
    },
    {
      name: "memory_upsert_pattern",
      category: "memory",
      description:
        "Create or update one reusable investigation pattern/method. Use this to summarize repeated cases into a stable root-cause and troubleshooting method.",
      schema: {
        projectId: z.string().optional(),
        patternKey: z.string().optional(),
        title: z.string().min(2),
        tagsJson: z.string().optional(),
        trigger: z.string().optional(),
        rootCause: z.string().optional(),
        methodSummary: z.string().optional(),
        recommendations: z.string().optional(),
        relatedCaseRelPath: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, args) => {
        const storePath = context.getStorePath();
        const projects = args.projectId
          ? resolveProjectScope(getStore(), { projectId: args.projectId as string })
          : [];
        const project = projects[0] ?? null;
        const baseRoot = memoryPatternsRoot(storePath, project?.id);
        await fs.mkdir(baseRoot, { recursive: true });
        const tags = parseOptionalStringArrayJson(args.tagsJson as string | undefined, "tagsJson");
        const patternKey = String(args.patternKey ?? "").trim() || slugify(String(args.title));
        const absPath = path.join(baseRoot, `${slugify(patternKey)}.md`);
        await writePatternFile({
          absPath,
          title: String(args.title).trim(),
          projectId: project?.id ?? null,
          patternKey,
          tags,
          trigger: String(args.trigger ?? "").trim() || "未填写",
          rootCause: String(args.rootCause ?? "").trim() || "未填写",
          method: String(args.methodSummary ?? "").trim() || "未填写",
          recommendations: String(args.recommendations ?? "").trim() || "未填写",
          relatedCaseRelPath: String(args.relatedCaseRelPath ?? "").trim() || null,
        });
        return {
          projectId: project?.id ?? null,
          patternKey,
          absPath,
          relPath: path.relative(storePath, absPath),
          tags,
        };
      },
    },
    {
      name: "memory_record_case",
      category: "memory",
      description:
        "Record or update one investigation case into markdown memory. Repeated cases will deduplicate by dedupeKey/pattern and can also update a reusable pattern summary.",
      schema: {
        projectId: z.string().optional(),
        title: z.string().min(2),
        question: z.string().optional(),
        conclusion: z.string().min(2),
        uncertainty: z.string().optional(),
        recommendations: z.string().optional(),
        tagsJson: z.string().optional(),
        dedupeKey: z.string().optional(),
        patternKey: z.string().optional(),
        patternTitle: z.string().optional(),
        methodSummary: z.string().optional(),
        rootCause: z.string().optional(),
        trigger: z.string().optional(),
        patternTagsJson: z.string().optional(),
        dataEvidenceJson: z.string().optional(),
        codeEvidenceJson: z.string().optional(),
        logEvidenceJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (context, args) => {
        const storePath = context.getStorePath();
        const projects = args.projectId
          ? resolveProjectScope(getStore(), { projectId: args.projectId as string })
          : [];
        const project = projects[0] ?? null;
        const baseRoot = memoryCasesRoot(storePath, project?.id);
        const patternsRoot = memoryPatternsRoot(storePath, project?.id);
        await fs.mkdir(baseRoot, { recursive: true });
        await fs.mkdir(patternsRoot, { recursive: true });
        const tags = parseOptionalStringArrayJson(args.tagsJson as string | undefined, "tagsJson");
        const patternTags = parseOptionalStringArrayJson(args.patternTagsJson as string | undefined, "patternTagsJson");
        const dataEvidence = parseJsonObject(args.dataEvidenceJson as string | undefined, "dataEvidenceJson");
        const codeEvidence = parseJsonObject(args.codeEvidenceJson as string | undefined, "codeEvidenceJson");
        const logEvidence = parseJsonObject(args.logEvidenceJson as string | undefined, "logEvidenceJson");
        const patternKey = String(args.patternKey ?? "").trim() || null;
        const dedupeKey =
          String(args.dedupeKey ?? "").trim() ||
          buildCaseDedupeKey({
            projectId: project?.id ?? null,
            title: String(args.title),
            tags,
            patternKey,
          });
        const existing = await findExistingCaseByDedupeKey(baseRoot, dedupeKey);
        const existingMetadata = existing ? extractCaseMetadata(existing.content) : null;
        const absPath = existing?.absPath ?? path.join(baseRoot, caseFileName(String(args.title)));
        const createdAt = existing ? parseMetadataLine(existing.content, "Created At") || nowIso() : nowIso();
        const occurrenceCount =
          existingMetadata && typeof existingMetadata.occurrenceCount === "number"
            ? Number(existingMetadata.occurrenceCount || 1) + 1
            : 1;
        const lastSeenAt = nowIso();
        const relatedPatternKey = patternKey || String(existingMetadata?.patternKey ?? "").trim() || null;
        const content = [
          `# ${String(args.title).trim()}`,
          "",
          `- Created At: ${createdAt}`,
          `- Last Seen At: ${lastSeenAt}`,
          `- Project: ${project?.id ?? "workspace"}`,
          `- Dedupe Key: ${dedupeKey}`,
          `- Pattern Key: ${relatedPatternKey ?? ""}`,
          `- Occurrence Count: ${occurrenceCount}`,
          `- Tags: ${tags.join(", ")}`,
          "",
          "## Question",
          "",
          String(args.question ?? "").trim() || "未填写",
          "",
          "## Conclusion",
          "",
          String(args.conclusion).trim(),
          "",
          "## Data Evidence",
          "",
          "```json",
          safeJsonStringify(dataEvidence),
          "```",
          "",
          "## Code Evidence",
          "",
          "```json",
          safeJsonStringify(codeEvidence),
          "```",
          "",
          "## Log Evidence",
          "",
          "```json",
          safeJsonStringify(logEvidence),
          "```",
          "",
          "## Uncertainty",
          "",
          String(args.uncertainty ?? "").trim() || "未填写",
          "",
          "## Recommendations",
          "",
          String(args.recommendations ?? "").trim() || "未填写",
          "",
        ].join("\n");
        await fs.writeFile(absPath, content, "utf8");
        const relatedCaseRelPath = path.relative(storePath, absPath);
        const normalizedPatternTitle =
          String(args.patternTitle ?? "").trim() ||
          (relatedPatternKey ? `${String(args.title).trim()} Pattern` : "");
        if (normalizedPatternTitle || relatedPatternKey) {
          const patternId = relatedPatternKey || slugify(String(args.title));
          const patternAbsPath = path.join(patternsRoot, `${slugify(patternId)}.md`);
          await writePatternFile({
            absPath: patternAbsPath,
            title: normalizedPatternTitle || String(args.title).trim(),
            projectId: project?.id ?? null,
            patternKey: patternId,
            tags: patternTags.length > 0 ? patternTags : tags,
            trigger: String(args.trigger ?? args.question ?? args.title).trim(),
            rootCause: String(args.rootCause ?? args.conclusion).trim(),
            method: String(args.methodSummary ?? "").trim() || "复用当前案例中的数据、代码、日志证据继续核实。",
            recommendations: String(args.recommendations ?? "").trim() || "按相关案例和证据继续排查。",
            relatedCaseRelPath,
          });
        }
        return {
          projectId: project?.id ?? null,
          absPath,
          relPath: relatedCaseRelPath,
          dedupeKey,
          occurrenceCount,
          patternKey: relatedPatternKey,
          updatedExisting: Boolean(existing),
          tags,
        };
      },
    },
    {
      name: "datasource_overview",
      category: "datasource",
      description:
        "Show datasource summaries, credential expiry, and binding information. Supports single-project and multi-project scope.",
      cacheTtlMs: 60_000,
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (_context, { projectId, projectIdsJson }) => {
        const store = getStore();
        const projects =
          projectId || projectIdsJson
            ? resolveProjectScope(store, { projectId, projectIdsJson })
            : [];
        const datasources =
          projects.length > 0
            ? getDatasourcesForProjects(store, projects)
            : store.datasources.filter((datasource) => datasource.enabled);
        return {
          projectId: projects.length === 1 ? projects[0].id : null,
          projectIds: projects.map((item) => item.id),
          datasources: datasources.map(summarizeDatasource),
        };
      },
    },
    {
      name: "resolve_datasource_for_intent",
      category: "datasource",
      description:
        "Resolve the most suitable datasource for an investigation intent within one or more projects. Use this instead of guessing datasourceId when multiple datasources exist.",
      schema: {
        projectId: z.string().optional(),
        projectIdsJson: z.string().optional(),
        datasourceType: z.enum(["mysql", "postgres", "mongo", "kafka", "logcenter", "monitor", "skywalking"]).optional(),
        intent: z.string().min(2),
        databaseHint: z.string().optional(),
        limit: z.number().int().positive().max(10).default(5),
      } satisfies ToolSchema,
      handler: async (_context, { projectId, projectIdsJson, datasourceType, intent, databaseHint, limit }) => {
        const store = getStore();
        const projects = resolveProjectScope(store, { projectId, projectIdsJson, allWhenOmitted: false });
        const pool =
          projects.length > 0
            ? getDatasourcesForProjects(store, projects)
            : store.datasources.filter((item) => item.enabled);
        const candidates = pool
          .map((datasource) => {
            const projectMatch = projects.find((item) => datasource.projectIds.includes(item.id)) ?? null;
            const scored = resolveDatasourceCandidates(
              {
                ...store,
                datasources: [datasource],
                projects: projectMatch ? [projectMatch] : store.projects,
              },
              {
                projectId: projectMatch?.id,
                datasourceType,
                intent,
                databaseHint,
                limit: 1,
              },
            )[0];
            return scored;
          })
          .filter(Boolean)
          .sort((left, right) => Number((right as Record<string, unknown>).score ?? 0) - Number((left as Record<string, unknown>).score ?? 0))
          .slice(0, limit);

        return {
          projectId: projects.length === 1 ? projects[0].id : null,
          projectIds: projects.map((item) => item.id),
          datasourceType: datasourceType ?? null,
          intent,
          databaseHint: databaseHint ?? null,
          resolvedDatasourceId: candidates[0] ? String((candidates[0] as Record<string, unknown>).id ?? "") : null,
          candidateCount: candidates.length,
          candidates,
        };
      },
    },
    {
      name: "datasource_test",
      category: "datasource",
      description:
        "Test one configured datasource connection with its saved credentials. Use when failures may be caused by expired credentials or bad connectivity.",
      cacheTtlMs: 300_000,
      schema: {
        datasourceId: z.string(),
        projectId: z.string().optional(),
      } satisfies ToolSchema,
      handler: async (_context, { datasourceId, projectId }) => {
        const store = getStore();
        const datasource = resolveDatasource(store, datasourceId, projectId);
        return await testDatasource(datasource);
      },
    },
  ];
}
