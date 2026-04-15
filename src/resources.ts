import { existsSync, readdirSync, statSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { ReadResourceResult, TextResourceContents } from "@modelcontextprotocol/sdk/types.js";
import { ConfigStore, ProjectConfig } from "./types.js";
import { readStoreSync } from "./store.js";

interface ResourceSpec {
  name: string;
  title: string;
  uri: string;
  absPath: string;
  description: string;
  mimeType: string;
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

function safeIsDirectory(targetPath: string): boolean {
  try {
    return existsSync(targetPath) && statSync(targetPath).isDirectory();
  } catch {
    return false;
  }
}

function listMarkdownFilesRecursive(root: string, relPrefix = ""): Array<{ relPath: string; absPath: string }> {
  if (!safeIsDirectory(root)) {
    return [];
  }
  const items: Array<{ relPath: string; absPath: string }> = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const absPath = path.join(root, entry.name);
    const relPath = relPrefix ? path.posix.join(relPrefix, entry.name) : entry.name;
    if (entry.isDirectory()) {
      items.push(...listMarkdownFilesRecursive(absPath, relPath));
      continue;
    }
    if (entry.isFile() && entry.name.endsWith(".md")) {
      items.push({ relPath, absPath });
    }
  }
  return items;
}

function toWorkspaceUri(relPath: string): string {
  const normalized = relPath
    .split(path.sep)
    .join("/")
    .split("/")
    .map((segment) => encodeURIComponent(segment))
    .join("/");
  return `wms-ai:///workspace/${normalized}`;
}

function addStaticResource(specs: ResourceSpec[], relPath: string, absPath: string, title: string, description: string): void {
  if (!existsSync(absPath)) {
    return;
  }
  specs.push({
    name: relPath,
    title,
    uri: toWorkspaceUri(relPath),
    absPath,
    description,
    mimeType: absPath.endsWith(".md") ? "text/markdown" : "text/plain",
  });
}

function buildWorkspaceResourceSpecs(storePath: string, store: ConfigStore): ResourceSpec[] {
  const specs: ResourceSpec[] = [];

  addStaticResource(
    specs,
    "instructions.md",
    path.join(storePath, "instructions.md"),
    "Workspace Instructions",
    "全局调查说明，约束 Codex 的总体排查行为。",
  );
  addStaticResource(
    specs,
    "tool-guidelines.md",
    path.join(storePath, "tool-guidelines.md"),
    "Tool Guidelines",
    "工具使用规则，约束 SQL/Mongo/Kafka/Repo/Log 的调用顺序。",
  );

  for (const doc of listMarkdownFilesRecursive(workspaceKnowledgeBaseRoot(storePath))) {
    addStaticResource(
      specs,
      path.posix.join("knowledge-base", doc.relPath),
      doc.absPath,
      `Workspace KB: ${doc.relPath}`,
      "工作区级知识库文档。",
    );
  }

  for (const doc of listMarkdownFilesRecursive(workspaceMemoryRoot(storePath))) {
    addStaticResource(
      specs,
      path.posix.join("memory", doc.relPath),
      doc.absPath,
      `Workspace Memory: ${doc.relPath}`,
      "工作区级案例记忆文档。",
    );
  }

  for (const project of store.projects.filter((item) => item.enabled)) {
    addProjectResources(specs, storePath, project);
  }

  return specs;
}

function addProjectResources(specs: ResourceSpec[], storePath: string, project: ProjectConfig): void {
  const projectRoot = path.join(storePath, "projects", project.id);

  addStaticResource(
    specs,
    path.posix.join("projects", project.id, "playbook.md"),
    path.join(projectRoot, "playbook.md"),
    `Project Playbook: ${project.label}`,
    `项目 ${project.id} 的调查手册。`,
  );
  addStaticResource(
    specs,
    path.posix.join("projects", project.id, "checklist.md"),
    path.join(projectRoot, "checklist.md"),
    `Project Checklist: ${project.label}`,
    `项目 ${project.id} 的标准排查清单。`,
  );

  for (const doc of listMarkdownFilesRecursive(projectKnowledgeBaseRoot(storePath, project.id))) {
    addStaticResource(
      specs,
      path.posix.join("projects", project.id, "knowledge-base", doc.relPath),
      doc.absPath,
      `Project KB (${project.id}): ${doc.relPath}`,
      `项目 ${project.id} 的知识库文档。`,
    );
  }

  for (const doc of listMarkdownFilesRecursive(projectMemoryRoot(storePath, project.id))) {
    addStaticResource(
      specs,
      path.posix.join("projects", project.id, "memory", doc.relPath),
      doc.absPath,
      `Project Memory (${project.id}): ${doc.relPath}`,
      `项目 ${project.id} 的历史案例文档。`,
    );
  }
}

async function readTextResource(spec: ResourceSpec): Promise<ReadResourceResult> {
  const text = await fs.readFile(spec.absPath, "utf8");
  const contents: TextResourceContents[] = [
    {
      uri: spec.uri,
      mimeType: spec.mimeType,
      text,
    },
  ];
  return {
    contents,
  };
}

export function registerWorkspaceResources(server: McpServer, storePath: string): void {
  const store = readStoreSync(storePath);
  const specs = buildWorkspaceResourceSpecs(storePath, store);
  for (const spec of specs) {
    server.registerResource(
      spec.name,
      spec.uri,
      {
        title: spec.title,
        description: spec.description,
        mimeType: spec.mimeType,
      },
      async () => await readTextResource(spec),
    );
  }
}
