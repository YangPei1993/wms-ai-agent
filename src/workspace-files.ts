import { existsSync, readdirSync, readFileSync, statSync } from "node:fs";
import { promises as fs } from "node:fs";
import path from "node:path";
import { readStoreSync } from "./store.js";
import { nowIso, resolvePathUnderRoot, slugify } from "./utils.js";

export type WorkspaceNodeKind =
  | "workspace"
  | "section"
  | "project"
  | "datasource"
  | "file";

export interface WorkspaceNode {
  id: string;
  name: string;
  kind: WorkspaceNodeKind;
  relPath: string;
  absPath: string;
  description: string;
  children: WorkspaceNode[];
}

export interface FileFieldGuide {
  key: string;
  required: boolean;
  description: string;
  example?: string;
}

export interface WorkspaceGuide {
  title: string;
  summary: string;
  howToFill: string[];
  fields: FileFieldGuide[];
  nodeActions: string[];
}

export interface DatasourceFormInput {
  datasourceId: string;
  type: string;
  label?: string;
  enabled?: boolean;
  description?: string;
  role?: string;
  usageNotes?: string;
  host?: string;
  port?: string;
  database?: string;
  uri?: string;
  authSource?: string;
  brokers?: string;
  clientId?: string;
  ssl?: boolean;
  saslMechanism?: string;
  mongoMode?: string;
  authMode?: string;
  loginPath?: string;
  dataView?: string;
  optionsJson?: string;
  username?: string;
  secret?: string;
  expiresAt?: string;
}

const PROJECT_FILE_ORDER = [
  "project.env",
  "repos.txt",
  "logs.txt",
  "datasources.txt",
  "playbook.md",
  "checklist.md",
];

const DATASOURCE_FILE_ORDER = ["datasource.env", "secret.env"];
const WORKSPACE_KB_FILE_ORDER = ["index.md", "sync-architecture.md", "common-patterns.md"];
const PROJECT_KB_FILE_ORDER = ["architecture.md", "data-model.md", "sync-flow.md", "common-failures.md"];
const MEMORY_FILE_ORDER = ["index.md"];
const MEMORY_SUBDIR_FILE_ORDER: Record<string, string[]> = {
  patterns: ["index.md"],
  cases: ["index.md"],
};

function readTextIfExists(filePath: string): string {
  try {
    return existsSync(filePath) ? readFileSync(filePath, "utf8") : "";
  } catch {
    return "";
  }
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
    const value = trimmed.slice(separator + 1).trim();
    if (key) {
      result[key] = value;
    }
  }
  return result;
}

function listDirNames(dirPath: string): string[] {
  try {
    if (!existsSync(dirPath)) {
      return [];
    }
    return readdirSync(dirPath)
      .filter((entry) => {
        try {
          return statSync(path.join(dirPath, entry)).isDirectory();
        } catch {
          return false;
        }
      })
      .sort((left, right) => left.localeCompare(right));
  } catch {
    return [];
  }
}

function listFileNames(dirPath: string, preferredOrder: string[]): string[] {
  try {
    if (!existsSync(dirPath)) {
      return [];
    }
    const existing = readdirSync(dirPath)
      .filter((entry) => {
        try {
          return statSync(path.join(dirPath, entry)).isFile();
        } catch {
          return false;
        }
      })
      .sort((left, right) => left.localeCompare(right));
    const priority = preferredOrder.filter((name) => existing.includes(name));
    const rest = existing.filter((name) => !priority.includes(name));
    return [...priority, ...rest];
  } catch {
    return [];
  }
}

function fileNode(root: string, relPath: string, description: string): WorkspaceNode {
  return {
    id: relPath,
    name: path.basename(relPath),
    kind: "file",
    relPath,
    absPath: resolvePathUnderRoot(root, relPath),
    description,
    children: [],
  };
}

function guideSummaryForFile(fileName: string): string {
  switch (fileName) {
    case "workspace.env":
      return "工作区级别的轻量环境配置，主要放默认项目等总开关。";
    case "instructions.md":
      return "给 Codex 的全局调查说明，定义排查总原则。";
    case "tool-guidelines.md":
      return "给 Codex 的工具使用规则，约束 SQL/Mongo/Kafka/代码搜索的使用顺序。";
    case "project.env":
      return "项目元信息，描述项目定位、提示词匹配、启用状态。";
    case "repos.txt":
      return "项目绑定的仓库目录清单，每行一个绝对路径。";
    case "logs.txt":
      return "项目绑定的日志目录或日志文件清单，每行一个绝对路径。";
    case "datasources.txt":
      return "项目绑定的数据源 ID 清单，每行一个。这里是项目实际生效的数据源绑定，Codex 工具看到的就是这里列出的 ID。";
    case "playbook.md":
      return "项目调查手册，告诉 Codex 这个项目的链路、关键模块和排查顺序。";
    case "checklist.md":
      return "项目调查 checklist，列出固定的排查步骤和常用核对项。";
    case "datasource.env":
      return "数据源公开配置，放连接目标、用途、项目绑定和驱动参数。";
    case "secret.env":
      return "数据源临时凭证，放 username/password 或 token 以及过期时间。";
    case "index.md":
      return "索引或总览文档，概览当前目录下的知识或案例。";
    case "sync-architecture.md":
      return "工作区级同步架构说明，描述 2.0 / middleware / 3.0 的总链路。";
    case "common-patterns.md":
      return "工作区级常见问题模式库，记录可复用的故障模式。";
    case "architecture.md":
      return "项目级架构知识文档，描述模块职责和系统边界。";
    case "data-model.md":
      return "项目级数据模型文档，描述关键表、集合、主键和业务键。";
    case "sync-flow.md":
      return "项目级同步流文档，描述入口、topic、路由和落库路径。";
    case "common-failures.md":
      return "项目级常见失败模式文档，记录典型根因和处理经验。";
    default:
      return "工作区文件。";
  }
}

function genericSectionNode(
  root: string,
  relPath: string,
  name: string,
  description: string,
  preferredOrder: string[] = [],
): WorkspaceNode {
  const absPath = resolvePathUnderRoot(root, relPath);
  const childFiles = listFileNames(absPath, preferredOrder).map((fileName) =>
    fileNode(root, path.posix.join(relPath, fileName), guideSummaryForFile(fileName)),
  );
  const childSections = listDirNames(absPath).map((dirName) =>
    genericSectionNode(
      root,
      path.posix.join(relPath, dirName),
      dirName,
      dirName === "cases"
        ? "案例目录，按文件沉淀历史排查。"
        : dirName === "patterns"
          ? "方法/模式目录，沉淀可复用的排查方法和根因模式。"
          : "子目录。",
      MEMORY_SUBDIR_FILE_ORDER[dirName] ?? [],
    ),
  );
  return {
    id: relPath,
    name,
    kind: "section",
    relPath,
    absPath,
    description,
    children: [...childFiles, ...childSections],
  };
}

function projectNode(root: string, projectId: string): WorkspaceNode {
  const relPath = path.posix.join("projects", projectId);
  const absPath = resolvePathUnderRoot(root, relPath);
  const children = listFileNames(absPath, PROJECT_FILE_ORDER).map((fileName) =>
    fileNode(root, path.posix.join(relPath, fileName), guideSummaryForFile(fileName)),
  );
  if (existsSync(path.join(absPath, "knowledge-base"))) {
    children.push(
      genericSectionNode(
        root,
        path.posix.join(relPath, "knowledge-base"),
        "knowledge-base",
        "项目知识库，维护架构、数据模型、同步流和常见失败模式。",
        PROJECT_KB_FILE_ORDER,
      ),
    );
  }
  if (existsSync(path.join(absPath, "memory"))) {
    children.push(
      genericSectionNode(
        root,
        path.posix.join(relPath, "memory"),
        "memory",
        "项目案例记忆，沉淀历史问题、证据和结论。",
        MEMORY_FILE_ORDER,
      ),
    );
  }
  return {
    id: relPath,
    name: projectId,
    kind: "project",
    relPath,
    absPath,
    description: "项目节点，管理仓库目录、日志目录、数据源绑定和调查手册。",
    children,
  };
}

function datasourceNode(root: string, datasourceId: string): WorkspaceNode {
  const relPath = path.posix.join("datasources", datasourceId);
  const absPath = resolvePathUnderRoot(root, relPath);
  const children = listFileNames(absPath, DATASOURCE_FILE_ORDER).map((fileName) =>
    fileNode(root, path.posix.join(relPath, fileName), guideSummaryForFile(fileName)),
  );
  return {
    id: relPath,
    name: datasourceId,
    kind: "datasource",
    relPath,
    absPath,
    description: "数据源节点，管理连接信息、用途说明和临时凭证。",
    children,
  };
}

export async function ensureWorkspaceRoot(root: string): Promise<void> {
  await fs.mkdir(path.join(root, "projects"), { recursive: true });
  await fs.mkdir(path.join(root, "datasources"), { recursive: true });
  await fs.mkdir(path.join(root, "knowledge-base"), { recursive: true });
  await fs.mkdir(path.join(root, "memory", "cases"), { recursive: true });
  await fs.mkdir(path.join(root, "memory", "patterns"), { recursive: true });
  const files: Array<[string, string]> = [
    [
      path.join(root, "workspace.env"),
      [
        "# 工作区默认配置",
        "# ACTIVE_PROJECT_ID 填你希望 Codex 默认优先排查的项目 ID",
        "ACTIVE_PROJECT_ID=",
        "",
      ].join("\n"),
    ],
    [
      path.join(root, "instructions.md"),
      [
        "# 全局调查说明",
        "",
        "- 这里写给 Codex 的全局排查规则。",
        "- 建议说明项目间关系、优先排查顺序和安全边界。",
        "",
      ].join("\n"),
    ],
    [
      path.join(root, "tool-guidelines.md"),
      [
        "# 工具使用规则",
        "",
        "- 这里写给 Codex 的工具调用规则。",
        "- 例如先查项目手册，再查数据源，再读代码。",
        "",
      ].join("\n"),
    ],
    [
      path.join(root, "knowledge-base", "index.md"),
      ["# 工作区知识库", "", "- 这里维护跨项目的结构化知识。", ""].join("\n"),
    ],
    [
      path.join(root, "knowledge-base", "sync-architecture.md"),
      ["# 同步总架构", "", "- 这里说明 2.0 / middleware / 3.0 的总链路。", ""].join("\n"),
    ],
    [
      path.join(root, "knowledge-base", "common-patterns.md"),
      ["# 常见问题模式", "", "- 这里沉淀跨项目复用的故障模式。", ""].join("\n"),
    ],
    [
      path.join(root, "memory", "index.md"),
      ["# 工作区记忆", "", "- `patterns/` 用来沉淀可复用的方法、根因模式和排查步骤。", "- `cases/` 用来记录具体案例，但同类问题应优先归并到 pattern。", ""].join("\n"),
    ],
    [
      path.join(root, "memory", "patterns", "index.md"),
      ["# 工作区方法与模式", "", "- 这里沉淀跨项目可复用的调查方法和根因模式。", "- 同类问题优先更新既有 pattern，而不是重复新增 case。", ""].join("\n"),
    ],
    [
      path.join(root, "memory", "cases", "index.md"),
      ["# 工作区案例", "", "- 这里记录具体问题案例。", "- 如果问题已经有稳定模式，应更新 pattern 并只维护少量代表性 case。", ""].join("\n"),
    ],
  ];
  for (const [filePath, content] of files) {
    if (!existsSync(filePath)) {
      await fs.writeFile(filePath, content, "utf8");
    }
  }

  for (const projectId of listDirNames(path.join(root, "projects"))) {
    const projectRoot = path.join(root, "projects", projectId);
    await fs.mkdir(path.join(projectRoot, "knowledge-base"), { recursive: true });
    await fs.mkdir(path.join(projectRoot, "memory", "cases"), { recursive: true });
    await fs.mkdir(path.join(projectRoot, "memory", "patterns"), { recursive: true });
    const projectLabel = parseEnvText(readTextIfExists(path.join(projectRoot, "project.env"))).LABEL || projectId;
    const projectFiles: Array<[string, string]> = [
      [path.join(projectRoot, "knowledge-base", "architecture.md"), `# ${projectLabel} 架构说明\n\n## 模块职责\n\n- \n`],
      [path.join(projectRoot, "knowledge-base", "data-model.md"), `# ${projectLabel} 数据模型\n\n## 关键表 / 集合\n\n- \n`],
      [path.join(projectRoot, "knowledge-base", "sync-flow.md"), `# ${projectLabel} 同步流\n\n## 入口与出口\n\n- \n`],
      [path.join(projectRoot, "knowledge-base", "common-failures.md"), `# ${projectLabel} 常见失败模式\n\n- \n`],
      [path.join(projectRoot, "memory", "index.md"), `# ${projectLabel} 记忆\n\n- \`patterns/\` 用来沉淀本项目可复用的方法、根因模式和排查步骤。\n- \`cases/\` 用来记录具体案例，但同类问题应优先归并到 pattern。\n`],
      [path.join(projectRoot, "memory", "patterns", "index.md"), `# ${projectLabel} 方法与模式\n\n- 这里沉淀本项目可复用的调查方法和根因模式。\n- 同类问题优先更新既有 pattern，而不是重复新增 case。\n`],
      [path.join(projectRoot, "memory", "cases", "index.md"), `# ${projectLabel} 案例\n\n- 这里记录本项目具体案例。\n- 如果问题已经形成稳定模式，应更新 pattern 并只保留代表性 case。\n`],
    ];
    for (const [filePath, content] of projectFiles) {
      if (!existsSync(filePath)) {
        await fs.writeFile(filePath, content, "utf8");
      }
    }
  }
}

export function buildWorkspaceTree(root: string): WorkspaceNode {
  const projectsRoot = path.join(root, "projects");
  const datasourcesRoot = path.join(root, "datasources");
  const projectChildren = listDirNames(projectsRoot).map((projectId) => projectNode(root, projectId));
  const datasourceChildren = listDirNames(datasourcesRoot).map((datasourceId) => datasourceNode(root, datasourceId));

  return {
    id: "",
    name: "workspace",
    kind: "workspace",
    relPath: "",
    absPath: root,
    description: "工作区根目录，定义默认项目、全局说明、项目树和数据源池。",
    children: [
      fileNode(root, "workspace.env", guideSummaryForFile("workspace.env")),
      fileNode(root, "instructions.md", guideSummaryForFile("instructions.md")),
      fileNode(root, "tool-guidelines.md", guideSummaryForFile("tool-guidelines.md")),
      genericSectionNode(
        root,
        "knowledge-base",
        "knowledge-base",
        "工作区知识库，维护跨项目的架构、规则和共性问题模式。",
        WORKSPACE_KB_FILE_ORDER,
      ),
      genericSectionNode(
        root,
        "memory",
        "memory",
        "工作区案例记忆，维护跨项目历史排查案例。",
        MEMORY_FILE_ORDER,
      ),
      {
        id: "projects",
        name: "projects",
        kind: "section",
        relPath: "projects",
        absPath: resolvePathUnderRoot(root, "projects"),
        description: "项目目录树。每个子目录代表一个可独立调查的项目。",
        children: projectChildren,
      },
      {
        id: "datasources",
        name: "datasources",
        kind: "section",
        relPath: "datasources",
        absPath: resolvePathUnderRoot(root, "datasources"),
        description: "数据源资源池。项目通过 datasources.txt 绑定这些数据源。",
        children: datasourceChildren,
      },
    ],
  };
}

function markdownGuide(title: string, summary: string, howToFill: string[], nodeActions: string[] = []): WorkspaceGuide {
  return { title, summary, howToFill, nodeActions, fields: [] };
}

export function getGuideForNode(relPath: string, kind: WorkspaceNodeKind): WorkspaceGuide {
  if (!relPath) {
    return {
      title: "工作区根目录",
      summary: "整个配置的入口。工作区下面只放全局说明、项目目录树和共享数据源池。",
      howToFill: [
        "先维护 workspace.env、instructions.md、tool-guidelines.md 三个全局文件。",
        "项目放在 projects/<project-id>/ 下。",
        "数据源放在 datasources/<datasource-id>/ 下。",
        "项目通过 datasources.txt 绑定共享数据源，不要把连接信息直接写进项目目录。",
      ],
      fields: [],
      nodeActions: ["新增项目", "新增数据源", "设置默认项目"],
    };
  }

  if (relPath === "projects") {
    return {
      title: "项目目录树",
      summary: "这里管理每一个可独立调查的系统项目，例如 inventory-sync、fulfillment-api、ops-console。",
      howToFill: [
        "每个项目一个目录，目录名就是 project id。",
        "项目目录下面固定放 project.env、repos.txt、logs.txt、datasources.txt、playbook.md、checklist.md。",
        "项目说明尽量写清楚定位、链路、排查顺序和项目边界。",
      ],
      fields: [],
      nodeActions: ["新增项目"],
    };
  }

  if (relPath === "datasources") {
    return {
      title: "数据源资源池",
      summary: "这里统一维护所有共享数据源。项目只绑定数据源 ID，不重复写连接配置。",
      howToFill: [
        "一个数据源一个目录，目录名就是 datasource id。",
        "datasource.env 放连接目标和用途，secret.env 放敏感凭证。",
        "如果数据源只属于某个项目，也仍然建议放在资源池，通过项目去绑定。",
      ],
      fields: [],
      nodeActions: ["新增数据源"],
    };
  }

  if (relPath === "knowledge-base") {
    return {
      title: "工作区知识库",
      summary: "这里维护跨项目的可审核知识，不写临时结论和敏感信息。",
      howToFill: [
        "放系统总架构、通用规则、共性问题模式。",
        "这里写‘系统是什么’，不要写某次临时排查记录。",
      ],
      fields: [],
      nodeActions: ["保存文件"],
    };
  }

  if (relPath === "memory") {
    return {
      title: "工作区案例记忆",
      summary: "这里维护历史案例和排查结论，和知识库分开。",
      howToFill: [
        "这里写‘以前怎么查过’，不要混入系统真相定义。",
        "每个案例建议独立成一个 markdown 文件。",
      ],
      fields: [],
      nodeActions: ["保存文件"],
    };
  }

  if (kind === "project") {
    return {
      title: "项目节点",
      summary: "项目是 Codex 理解业务边界的最小单元，决定仓库、日志、手册和默认数据源范围。",
      howToFill: [
        "project.env 写项目元信息和匹配提示词。",
        "repos.txt 写项目相关仓库的绝对路径。",
        "logs.txt 写项目相关日志路径。",
        "datasources.txt 只写数据源 ID，不写 host/password。",
        "playbook.md 写项目调查手册，checklist.md 写固定排查顺序。",
      ],
      fields: [],
      nodeActions: ["绑定已有数据源", "设为默认项目", "重命名项目", "删除项目"],
    };
  }

  if (kind === "datasource") {
    return {
      title: "数据源节点",
      summary: "数据源节点负责描述连接目标、使用角色和临时凭证，供多个项目复用。",
      howToFill: [
        "datasource.env 放非敏感配置和用途说明。",
        "secret.env 放当前有效的账号、密码或 token。",
        "secret.env 过期时只需要更新这一份文件，不用改项目文件。",
      ],
      fields: [],
      nodeActions: ["查看连接说明", "更新凭证", "重命名数据源", "删除数据源"],
    };
  }

  if (relPath.endsWith("/knowledge-base")) {
    return {
      title: "项目知识库",
      summary: "项目级静态知识库，维护项目结构、数据模型、同步流和常见规则。",
      howToFill: [
        "优先写稳定、可审核的知识。",
        "不要把一次性排查结论写进这里。",
      ],
      fields: [],
      nodeActions: ["保存文件"],
    };
  }

  if (relPath.endsWith("/memory")) {
    return {
      title: "项目案例记忆",
      summary: "项目级动态案例库，沉淀这个项目的历史故障、证据和结论。",
      howToFill: [
        "案例建议单独成文件，按日期或问题类型命名。",
        "记录问题、证据、结论、不确定项和建议。",
      ],
      fields: [],
      nodeActions: ["保存文件"],
    };
  }

  const fileName = path.basename(relPath);
  switch (fileName) {
    case "workspace.env":
      return {
        title: "workspace.env",
        summary: "工作区级别的环境配置，目前最重要的是默认项目。",
        howToFill: [
          "ACTIVE_PROJECT_ID 填项目目录名，例如 inventory-sync。",
          "如果留空，Codex 会根据问题内容和项目提示词自己判断。",
        ],
        fields: [
          {
            key: "ACTIVE_PROJECT_ID",
            required: false,
            description: "默认项目 ID，对应 projects/<project-id> 的目录名。",
            example: "inventory-sync",
          },
        ],
        nodeActions: ["保存文件"],
      };
    case "instructions.md":
      return markdownGuide("instructions.md", "给 Codex 的全局调查说明。", [
        "写跨项目通用的排查原则。",
        "说明 2.0 -> middleware -> 3.0 的整体链路和默认判断顺序。",
        "不要把敏感密码直接写进这里。",
      ], ["保存文件"]);
    case "tool-guidelines.md":
      return markdownGuide("tool-guidelines.md", "给 Codex 的工具调用约束。", [
        "定义 SQL、Mongo、Kafka、repo、log 各自的调用顺序。",
        "明确先读 playbook，再查数据，再读代码。",
      ], ["保存文件"]);
    case "sync-architecture.md":
      return markdownGuide("sync-architecture.md", "跨项目同步总架构知识。", [
        "写 2.0 / middleware / 3.0 的链路和边界。",
        "这里适合放总览图和统一规则。",
      ], ["保存文件"]);
    case "common-patterns.md":
      return markdownGuide("common-patterns.md", "跨项目常见问题模式库。", [
        "按模式沉淀，不按单次 case 记录。",
        "例如主数据缺失、路由配置错误、traceId 长度不匹配。",
      ], ["保存文件"]);
    case "architecture.md":
      return markdownGuide("architecture.md", "项目架构知识文档。", [
        "写项目结构、模块职责和系统边界。",
      ], ["保存文件"]);
    case "data-model.md":
      return markdownGuide("data-model.md", "项目数据模型知识文档。", [
        "写关键表、集合、主键、业务键和映射关系。",
      ], ["保存文件"]);
    case "sync-flow.md":
      return markdownGuide("sync-flow.md", "项目同步流知识文档。", [
        "写入口、topic、路由和落库链路。",
      ], ["保存文件"]);
    case "common-failures.md":
      return markdownGuide("common-failures.md", "项目常见失败模式文档。", [
        "写稳定、可复用的失败模式和典型处理思路。",
      ], ["保存文件"]);
    case "project.env":
      return {
        title: "project.env",
        summary: "项目元信息。控制项目名称、启用状态、描述和匹配提示词。",
        howToFill: [
          "LABEL 填展示名。",
          "MATCH_HINTS 用逗号分隔，写 Codex 可能会命中的关键词。",
          "DESCRIPTION 用一句话描述这个项目是什么。",
          "ENABLED 推荐填 true/false。",
        ],
        fields: [
          { key: "LABEL", required: true, description: "项目展示名。", example: "Inventory Sync" },
          { key: "ENABLED", required: true, description: "是否启用。", example: "true" },
          { key: "DESCRIPTION", required: false, description: "项目一句话描述。", example: "routes inventory events between source and destination systems" },
          { key: "MATCH_HINTS", required: false, description: "逗号分隔的提示词。", example: "inventory,sync,events,fulfillment" },
          { key: "CREATED_AT", required: false, description: "创建时间。", example: nowIso() },
          { key: "UPDATED_AT", required: false, description: "更新时间。", example: nowIso() },
        ],
        nodeActions: ["保存文件"],
      };
    case "repos.txt":
      return {
        title: "repos.txt",
        summary: "项目绑定的仓库目录列表，每行一个绝对路径。",
        howToFill: [
          "只填和这个项目强相关的仓库根目录。",
          "尽量填绝对路径，避免写太大的公共目录。",
          "注释可以用 # 开头。",
        ],
        fields: [
          {
            key: "每行一个仓库目录",
            required: true,
            description: "Codex 会在这些目录里执行 repo_search / repo_read_file。",
            example: "/workspace/services/inventory-sync",
          },
        ],
        nodeActions: ["保存文件"],
      };
    case "logs.txt":
      return {
        title: "logs.txt",
        summary: "项目绑定的日志路径列表，每行一个绝对路径。",
        howToFill: [
          "可以填目录，也可以填具体文件。",
          "如果当前机器没有这套日志，可以先留空。",
          "日志路径尽量按项目边界拆开，不要混放。",
          "远程 Kibana / logcenter 不要写在这里，改为配置 TYPE=logcenter 的 datasource。",
        ],
        fields: [
          {
            key: "每行一个日志路径",
            required: false,
            description: "Codex 会用 log_search 在这些路径里检索。",
            example: "/var/log/inventory-sync/app.log",
          },
        ],
        nodeActions: ["保存文件"],
      };
    case "datasources.txt":
      return {
        title: "datasources.txt",
        summary: "项目绑定的数据源 ID 列表，每行一个。",
        howToFill: [
          "这里写 datasource id，不写连接字符串。",
          "数据源本体定义在 workspace/datasources/<datasource-id>/ 下。",
          "一个项目可以绑定多个数据源，例如 mysql + mongo + kafka + logcenter + monitor + skywalking + wms_agent。",
        ],
        fields: [
          {
            key: "每行一个 datasource id",
            required: false,
            description: "引用 datasources/ 下的目录名。",
            example: "inventory-postgres",
          },
        ],
        nodeActions: ["保存文件", "绑定已有数据源", "解绑数据源"],
      };
    case "playbook.md":
      return markdownGuide("playbook.md", "项目调查手册，告诉 Codex 这个项目的业务链路和重点。", [
        "先写项目定位，再写关键模块和排查顺序。",
        "尽量说明这个项目在大链路中的位置，例如是 2.0、3.0 还是 middleware。",
        "把容易误判的边界和常见异常模式写清楚。",
      ], ["保存文件"]);
    case "checklist.md":
      return markdownGuide("checklist.md", "项目 checklist，列固定核对项。", [
        "适合写标准操作步骤，例如先查哪个表，再查哪个日志。",
        "适合写可复用的核对顺序，不适合写太长的业务背景。",
      ], ["保存文件"]);
    case "datasource.env":
      return {
        title: "datasource.env",
        summary: "数据源公开配置。放连接目标、用途说明和驱动参数。",
        howToFill: [
          "TYPE 目前支持 mysql / postgres / mongo / kafka / logcenter / monitor / skywalking / wms_agent。",
          "MySQL/Postgres 推荐按“名称、主机、端口、用户名、密码”模板填写。",
          "Mongo 推荐只填 URI；如果 URI 已完整包含认证信息，就不用再单独填用户名密码。",
          "Logcenter 推荐填 URI、AUTH_MODE、LOGIN_PATH、DATA_VIEW；账号密码放在 secret.env。",
          "PROJECT_IDS 会按项目绑定关系自动同步，不需要你手工维护。",
        ],
        fields: [
          { key: "LABEL", required: true, description: "数据源展示名。", example: "Inventory Postgres" },
          { key: "TYPE", required: true, description: "数据源类型。", example: "mongo" },
          { key: "ENABLED", required: true, description: "是否启用。", example: "true" },
          { key: "ROLE", required: false, description: "用途说明。", example: "primary relational store for inventory reconciliation" },
          { key: "PROJECT_IDS", required: false, description: "逗号分隔的项目 ID。", example: "inventory-sync,ops-console" },
          { key: "HOST", required: false, description: "SQL/Kafka 主机名。", example: "db.example.internal" },
          { key: "PORT", required: false, description: "SQL/Kafka 端口。", example: "33061" },
          { key: "DATABASE", required: false, description: "SQL 默认数据库名。", example: "inventory" },
          { key: "URI", required: false, description: "Mongo 完整连接串；Logcenter / Monitor / SkyWalking 填 base URL。", example: "mongodb://readonly_user:***@db.example.internal:27017/inventory?authSource=admin" },
          { key: "AUTH_SOURCE", required: false, description: "Mongo authSource。", example: "admin" },
          { key: "BROKERS", required: false, description: "Kafka broker，逗号分隔。", example: "host1:9092,host2:9092" },
          { key: "CLIENT_ID", required: false, description: "Kafka client id。", example: "wms-ai-agent" },
          { key: "SSL", required: false, description: "Kafka 是否启用 SSL。", example: "true" },
          { key: "SASL_MECHANISM", required: false, description: "Kafka SASL 机制。", example: "plain" },
          { key: "MONGO_MODE", required: false, description: "Mongo 驱动模式。", example: "legacy-shell" },
          { key: "AUTH_MODE", required: false, description: "Logcenter 认证模式。", example: "basic" },
          { key: "LOGIN_PATH", required: false, description: "Logcenter 表单登录路径。", example: "/login" },
          { key: "DATA_VIEW", required: false, description: "Logcenter data view id 或 title。", example: "logs-*" },
          { key: "USAGE_NOTES", required: false, description: "补充说明。", example: "candidate from stage config" },
        ],
        nodeActions: ["保存文件", "测试数据源"],
      };
    case "secret.env":
      return {
        title: "secret.env",
        summary: "数据源敏感凭证。建议只放临时账号、密码或 token。",
        howToFill: [
          "MySQL/Postgres/Kafka/Logcenter 通常在这里填 USERNAME 和 SECRET。",
          "Mongo 如果直接使用 URI，可以让这个文件留空。",
          "EXPIRES_AT 建议填过期时间，方便你知道什么时候需要更新。",
          "这个文件不要放调查手册内容。",
        ],
        fields: [
          { key: "USERNAME", required: false, description: "登录用户名。", example: "readonly_user" },
          { key: "SECRET", required: false, description: "密码或 token。", example: "replace-me" },
          { key: "EXPIRES_AT", required: false, description: "过期时间。", example: "2026-03-20T23:00:00+08:00" },
          { key: "UPDATED_AT", required: false, description: "最近更新时间。", example: nowIso() },
        ],
        nodeActions: ["保存文件"],
      };
    case "index.md":
      if (relPath.includes("/memory/") || relPath === "memory/index.md") {
        return markdownGuide("memory/index.md", "记忆索引文档。", [
          "这里概览案例目录、分类方式和使用约定。",
        ], ["保存文件"]);
      }
      if (relPath.includes("/knowledge-base/") || relPath === "knowledge-base/index.md") {
        return markdownGuide("knowledge-base/index.md", "知识库索引文档。", [
          "这里概览知识文档目录、范围和维护约定。",
        ], ["保存文件"]);
      }
      return markdownGuide("index.md", "索引文档。", ["这里概览当前目录内容。"], ["保存文件"]);
    default:
      return markdownGuide(fileName, "工作区文件。", ["按当前目录语义填写。"], ["保存文件"]);
  }
}

export async function readWorkspaceFile(root: string, relPath: string): Promise<{
  absPath: string;
  content: string;
  guide: WorkspaceGuide;
}> {
  const absPath = resolvePathUnderRoot(root, relPath);
  const content = readTextIfExists(absPath);
  return {
    absPath,
    content,
    guide: getGuideForNode(relPath, "file"),
  };
}

export async function writeWorkspaceFile(root: string, relPath: string, content: string): Promise<void> {
  const absPath = resolvePathUnderRoot(root, relPath);
  await fs.mkdir(path.dirname(absPath), { recursive: true });
  await fs.writeFile(absPath, content, "utf8");
}

function baseProjectEnv(projectId: string, label: string): string {
  const timestamp = nowIso();
  return [
    `LABEL=${label}`,
    "ENABLED=true",
    "DESCRIPTION=",
    "MATCH_HINTS=",
    `CREATED_AT=${timestamp}`,
    `UPDATED_AT=${timestamp}`,
    "",
  ].join("\n");
}

function baseDatasourceEnv(datasourceId: string, label: string, type: string): string {
  const timestamp = nowIso();
  if (type === "mongo") {
    return [
      `LABEL=${label}`,
      "TYPE=mongo",
      "ENABLED=true",
      "DESCRIPTION=",
      "ROLE=",
      "USAGE_NOTES=",
      "PROJECT_IDS=",
      "URI=",
      "AUTH_SOURCE=",
      "MONGO_MODE=auto",
      "OPTIONS_JSON=",
      `CREATED_AT=${timestamp}`,
      `UPDATED_AT=${timestamp}`,
      "",
    ].join("\n");
  }

  if (type === "logcenter") {
    return [
      `LABEL=${label}`,
      "TYPE=logcenter",
      "ENABLED=true",
      "DESCRIPTION=",
      "ROLE=",
      "USAGE_NOTES=",
      "PROJECT_IDS=",
      "URI=",
      "AUTH_MODE=basic",
      "LOGIN_PATH=/login",
      "DATA_VIEW=",
      "OPTIONS_JSON=",
      `CREATED_AT=${timestamp}`,
      `UPDATED_AT=${timestamp}`,
      "",
    ].join("\n");
  }

  if (type === "monitor" || type === "skywalking" || type === "wms_agent") {
    return [
      `LABEL=${label}`,
      `TYPE=${type}`,
      "ENABLED=true",
      "DESCRIPTION=",
      "ROLE=",
      "USAGE_NOTES=",
      "PROJECT_IDS=",
      "URI=",
      "OPTIONS_JSON=",
      `CREATED_AT=${timestamp}`,
      `UPDATED_AT=${timestamp}`,
      "",
    ].join("\n");
  }

  return [
    `LABEL=${label}`,
    `TYPE=${type}`,
    "ENABLED=true",
    "DESCRIPTION=",
    "ROLE=",
    "USAGE_NOTES=",
    "PROJECT_IDS=",
    "HOST=",
    "PORT=",
    "DATABASE=",
    "URI=",
    "BROKERS=",
    "CLIENT_ID=",
    "SSL=false",
    "SASL_MECHANISM=",
    "OPTIONS_JSON=",
    `CREATED_AT=${timestamp}`,
    `UPDATED_AT=${timestamp}`,
    "",
  ].join("\n");
}

function baseSecretEnv(): string {
  return ["USERNAME=", "SECRET=", "EXPIRES_AT=", `UPDATED_AT=${nowIso()}`, ""].join("\n");
}

export async function createProjectSkeleton(root: string, rawId: string, rawLabel?: string): Promise<string> {
  const projectId = slugify(rawId);
  const label = rawLabel?.trim() || rawId.trim() || projectId;
  const projectRoot = resolvePathUnderRoot(root, path.posix.join("projects", projectId));
  if (existsSync(projectRoot)) {
    throw new Error(`Project already exists: ${projectId}`);
  }
  await fs.mkdir(projectRoot, { recursive: true });
  await fs.writeFile(path.join(projectRoot, "project.env"), baseProjectEnv(projectId, label), "utf8");
  await fs.writeFile(path.join(projectRoot, "repos.txt"), "# 每行一个仓库绝对路径\n", "utf8");
  await fs.writeFile(
    path.join(projectRoot, "logs.txt"),
    "# 每行一个日志目录或日志文件绝对路径\n# 远程 Kibana / logcenter 请配置成 datasource，不要写在这里\n",
    "utf8",
  );
  await fs.writeFile(path.join(projectRoot, "datasources.txt"), "# 每行一个 datasource id\n", "utf8");
  await fs.writeFile(
    path.join(projectRoot, "playbook.md"),
    [`# ${label} 调查手册`, "", "## 项目定位", "", "- 这里说明这个项目的职责。", "", "## 排查顺序", "", "1. ", ""].join("\n"),
    "utf8",
  );
  await fs.writeFile(
    path.join(projectRoot, "checklist.md"),
    [`# ${label} Checklist`, "", "- [ ] 明确问题入口", "- [ ] 查业务对象", "- [ ] 查代码链路", ""].join("\n"),
    "utf8",
  );
  await fs.mkdir(path.join(projectRoot, "knowledge-base"), { recursive: true });
  await fs.mkdir(path.join(projectRoot, "memory", "cases"), { recursive: true });
  await fs.writeFile(
    path.join(projectRoot, "knowledge-base", "architecture.md"),
    [`# ${label} 架构说明`, "", "## 模块职责", "", "- ", ""].join("\n"),
    "utf8",
  );
  await fs.writeFile(
    path.join(projectRoot, "knowledge-base", "data-model.md"),
    [`# ${label} 数据模型`, "", "## 关键表 / 集合", "", "- ", ""].join("\n"),
    "utf8",
  );
  await fs.writeFile(
    path.join(projectRoot, "knowledge-base", "sync-flow.md"),
    [`# ${label} 同步流`, "", "## 入口与出口", "", "- ", ""].join("\n"),
    "utf8",
  );
  await fs.writeFile(
    path.join(projectRoot, "knowledge-base", "common-failures.md"),
    [`# ${label} 常见失败模式`, "", "- ", ""].join("\n"),
    "utf8",
  );
  await fs.writeFile(
    path.join(projectRoot, "memory", "index.md"),
    [`# ${label} 案例记忆`, "", "- 这里记录本项目历史案例。", "- 具体案例放在 cases/ 目录下。", ""].join("\n"),
    "utf8",
  );
  return projectId;
}

export async function createDatasourceSkeleton(
  root: string,
  rawId: string,
  rawLabel: string | undefined,
  type: string,
): Promise<string> {
  const datasourceId = slugify(rawId);
  const label = rawLabel?.trim() || rawId.trim() || datasourceId;
  const datasourceRoot = resolvePathUnderRoot(root, path.posix.join("datasources", datasourceId));
  if (existsSync(datasourceRoot)) {
    throw new Error(`Datasource already exists: ${datasourceId}`);
  }
  await fs.mkdir(datasourceRoot, { recursive: true });
  await fs.writeFile(path.join(datasourceRoot, "datasource.env"), baseDatasourceEnv(datasourceId, label, type), "utf8");
  await fs.writeFile(path.join(datasourceRoot, "secret.env"), baseSecretEnv(), "utf8");
  return datasourceId;
}

function normalizeListContent(content: string): string[] {
  return content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"));
}

function updateSimpleEnv(content: string, key: string, value: string): string {
  const lines = content.split(/\r?\n/);
  let found = false;
  const next = lines.map((line) => {
    if (line.trim().startsWith(`${key}=`)) {
      found = true;
      return `${key}=${value}`;
    }
    return line;
  });
  if (!found) {
    next.push(`${key}=${value}`);
  }
  return `${next.join("\n").replace(/\n+$/, "")}\n`;
}

export async function bindDatasourceToProject(root: string, projectId: string, datasourceId: string): Promise<void> {
  const projectFile = resolvePathUnderRoot(root, path.posix.join("projects", projectId, "datasources.txt"));
  const datasourceEnvFile = resolvePathUnderRoot(root, path.posix.join("datasources", datasourceId, "datasource.env"));
  if (!existsSync(projectFile)) {
    throw new Error(`Project not found: ${projectId}`);
  }
  if (!existsSync(datasourceEnvFile)) {
    throw new Error(`Datasource not found: ${datasourceId}`);
  }

  const currentProjectBindings = normalizeListContent(readTextIfExists(projectFile));
  if (!currentProjectBindings.includes(datasourceId)) {
    const nextContent = `${[...currentProjectBindings, datasourceId].join("\n")}\n`;
    await fs.writeFile(projectFile, nextContent, "utf8");
  }

  const datasourceEnv = readTextIfExists(datasourceEnvFile);
  const projectIdsMatch = datasourceEnv.match(/^PROJECT_IDS=(.*)$/m);
  const projectIds = projectIdsMatch
    ? projectIdsMatch[1]
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    : [];
  if (!projectIds.includes(projectId)) {
    const nextEnv = updateSimpleEnv(datasourceEnv, "PROJECT_IDS", [...projectIds, projectId].join(","));
    await fs.writeFile(datasourceEnvFile, nextEnv, "utf8");
  }
}

export async function unbindDatasourceFromProject(root: string, projectId: string, datasourceId: string): Promise<void> {
  const projectFile = resolvePathUnderRoot(root, path.posix.join("projects", projectId, "datasources.txt"));
  const datasourceEnvFile = resolvePathUnderRoot(root, path.posix.join("datasources", datasourceId, "datasource.env"));
  if (!existsSync(projectFile)) {
    throw new Error(`Project not found: ${projectId}`);
  }
  if (!existsSync(datasourceEnvFile)) {
    throw new Error(`Datasource not found: ${datasourceId}`);
  }

  const currentProjectBindings = normalizeListContent(readTextIfExists(projectFile));
  const nextBindings = currentProjectBindings.filter((item) => item !== datasourceId);
  await fs.writeFile(projectFile, `${nextBindings.join("\n")}${nextBindings.length ? "\n" : ""}`, "utf8");

  const datasourceEnv = readTextIfExists(datasourceEnvFile);
  const projectIdsMatch = datasourceEnv.match(/^PROJECT_IDS=(.*)$/m);
  const projectIds = projectIdsMatch
    ? projectIdsMatch[1]
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    : [];
  const nextProjectIds = projectIds.filter((item) => item !== projectId);
  const nextEnv = updateSimpleEnv(datasourceEnv, "PROJECT_IDS", nextProjectIds.join(","));
  await fs.writeFile(datasourceEnvFile, nextEnv, "utf8");
}

export async function setActiveProject(root: string, projectId: string): Promise<void> {
  const store = readStoreSync(root);
  if (!store.projects.some((project) => project.id === projectId)) {
    throw new Error(`Project not found: ${projectId}`);
  }
  const workspaceEnvFile = resolvePathUnderRoot(root, "workspace.env");
  const current = readTextIfExists(workspaceEnvFile);
  const next = updateSimpleEnv(current || "ACTIVE_PROJECT_ID=\n", "ACTIVE_PROJECT_ID", projectId);
  await fs.writeFile(workspaceEnvFile, next, "utf8");
}

export async function renameProject(root: string, oldProjectId: string, nextRawProjectId: string): Promise<string> {
  const nextProjectId = slugify(nextRawProjectId);
  if (!nextProjectId) {
    throw new Error("New project id is empty");
  }
  if (nextProjectId === oldProjectId) {
    return nextProjectId;
  }

  const oldProjectRoot = resolvePathUnderRoot(root, path.posix.join("projects", oldProjectId));
  const nextProjectRoot = resolvePathUnderRoot(root, path.posix.join("projects", nextProjectId));
  if (!existsSync(oldProjectRoot)) {
    throw new Error(`Project not found: ${oldProjectId}`);
  }
  if (existsSync(nextProjectRoot)) {
    throw new Error(`Project already exists: ${nextProjectId}`);
  }

  const store = readStoreSync(root);
  await fs.rename(oldProjectRoot, nextProjectRoot);

  for (const datasource of store.datasources.filter((item) => item.projectIds.includes(oldProjectId))) {
    const datasourceEnvFile = resolvePathUnderRoot(root, path.posix.join("datasources", datasource.id, "datasource.env"));
    const datasourceEnv = readTextIfExists(datasourceEnvFile);
    const projectIdsMatch = datasourceEnv.match(/^PROJECT_IDS=(.*)$/m);
    const projectIds = projectIdsMatch
      ? projectIdsMatch[1]
          .split(",")
          .map((item) => item.trim())
          .filter(Boolean)
      : [];
    const nextProjectIds = projectIds.map((item) => (item === oldProjectId ? nextProjectId : item));
    const nextEnv = updateSimpleEnv(datasourceEnv, "PROJECT_IDS", nextProjectIds.join(","));
    await fs.writeFile(datasourceEnvFile, nextEnv, "utf8");
  }

  if (store.activeProjectId === oldProjectId) {
    const workspaceEnvFile = resolvePathUnderRoot(root, "workspace.env");
    const current = readTextIfExists(workspaceEnvFile);
    const next = updateSimpleEnv(current || "ACTIVE_PROJECT_ID=\n", "ACTIVE_PROJECT_ID", nextProjectId);
    await fs.writeFile(workspaceEnvFile, next, "utf8");
  }

  return nextProjectId;
}

export async function renameDatasource(root: string, oldDatasourceId: string, nextRawDatasourceId: string): Promise<string> {
  const nextDatasourceId = slugify(nextRawDatasourceId);
  if (!nextDatasourceId) {
    throw new Error("New datasource id is empty");
  }
  if (nextDatasourceId === oldDatasourceId) {
    return nextDatasourceId;
  }

  const oldDatasourceRoot = resolvePathUnderRoot(root, path.posix.join("datasources", oldDatasourceId));
  const nextDatasourceRoot = resolvePathUnderRoot(root, path.posix.join("datasources", nextDatasourceId));
  if (!existsSync(oldDatasourceRoot)) {
    throw new Error(`Datasource not found: ${oldDatasourceId}`);
  }
  if (existsSync(nextDatasourceRoot)) {
    throw new Error(`Datasource already exists: ${nextDatasourceId}`);
  }

  const store = readStoreSync(root);

  for (const project of store.projects.filter((item) => item.datasourceIds.includes(oldDatasourceId))) {
    const projectFile = resolvePathUnderRoot(root, path.posix.join("projects", project.id, "datasources.txt"));
    const currentBindings = normalizeListContent(readTextIfExists(projectFile));
    const nextBindings = currentBindings.map((item) => (item === oldDatasourceId ? nextDatasourceId : item));
    await fs.writeFile(projectFile, `${nextBindings.join("\n")}${nextBindings.length ? "\n" : ""}`, "utf8");
  }

  await fs.rename(oldDatasourceRoot, nextDatasourceRoot);
  return nextDatasourceId;
}

export async function deleteProject(root: string, projectId: string): Promise<void> {
  const projectRoot = resolvePathUnderRoot(root, path.posix.join("projects", projectId));
  if (!existsSync(projectRoot)) {
    throw new Error(`Project not found: ${projectId}`);
  }

  const store = readStoreSync(root);
  for (const datasource of store.datasources.filter((item) => item.projectIds.includes(projectId))) {
    await unbindDatasourceFromProject(root, projectId, datasource.id);
  }

  await fs.rm(projectRoot, { recursive: true, force: true });

  if (store.activeProjectId === projectId) {
    const workspaceEnvFile = resolvePathUnderRoot(root, "workspace.env");
    const current = readTextIfExists(workspaceEnvFile);
    const next = updateSimpleEnv(current || "ACTIVE_PROJECT_ID=\n", "ACTIVE_PROJECT_ID", "");
    await fs.writeFile(workspaceEnvFile, next, "utf8");
  }
}

export async function deleteDatasource(root: string, datasourceId: string): Promise<void> {
  const datasourceRoot = resolvePathUnderRoot(root, path.posix.join("datasources", datasourceId));
  if (!existsSync(datasourceRoot)) {
    throw new Error(`Datasource not found: ${datasourceId}`);
  }

  const store = readStoreSync(root);
  for (const project of store.projects.filter((item) => item.datasourceIds.includes(datasourceId))) {
    await unbindDatasourceFromProject(root, project.id, datasourceId);
  }

  await fs.rm(datasourceRoot, { recursive: true, force: true });
}

export async function writeDatasourceConfig(root: string, input: DatasourceFormInput): Promise<void> {
  const datasourceId = input.datasourceId.trim();
  if (!datasourceId) {
    throw new Error("datasourceId is required");
  }

  const datasourceRoot = resolvePathUnderRoot(root, path.posix.join("datasources", datasourceId));
  const datasourceEnvFile = path.join(datasourceRoot, "datasource.env");
  const secretEnvFile = path.join(datasourceRoot, "secret.env");
  if (!existsSync(datasourceEnvFile)) {
    throw new Error(`Datasource not found: ${datasourceId}`);
  }

  const store = readStoreSync(root);
  const boundProjectIds = store.projects
    .filter((project) => project.datasourceIds.includes(datasourceId))
    .map((project) => project.id);

  const datasourceEnv = readTextIfExists(datasourceEnvFile);
  const secretEnv = readTextIfExists(secretEnvFile);
  const datasourceMeta = parseEnvText(datasourceEnv);
  const nextType = (input.type || datasourceMeta.TYPE || "").trim();
  if (!nextType) {
    throw new Error("Datasource type is required");
  }

  let nextDatasourceEnv = datasourceEnv || baseDatasourceEnv(datasourceId, datasourceId, nextType);
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "LABEL", input.label?.trim() || datasourceMeta.LABEL || datasourceId);
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "TYPE", nextType);
  nextDatasourceEnv = updateSimpleEnv(
    nextDatasourceEnv,
    "ENABLED",
    String(input.enabled ?? (datasourceMeta.ENABLED || "true")).toLowerCase() === "false" ? "false" : "true",
  );
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "DESCRIPTION", input.description?.trim() ?? datasourceMeta.DESCRIPTION ?? "");
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "ROLE", input.role?.trim() ?? datasourceMeta.ROLE ?? "");
  nextDatasourceEnv = updateSimpleEnv(
    nextDatasourceEnv,
    "USAGE_NOTES",
    input.usageNotes?.trim() ?? datasourceMeta.USAGE_NOTES ?? "",
  );
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "PROJECT_IDS", boundProjectIds.join(","));
  nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "UPDATED_AT", nowIso());
  if (!datasourceMeta.CREATED_AT) {
    nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "CREATED_AT", nowIso());
  }

  if (nextType === "mongo") {
    if (input.uri !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "URI", input.uri.trim());
    }
    if (input.authSource !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "AUTH_SOURCE", input.authSource.trim());
    }
    if (input.mongoMode !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "MONGO_MODE", input.mongoMode.trim() || "auto");
    }
    if (input.optionsJson !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "OPTIONS_JSON", input.optionsJson.trim());
    }
  } else if (nextType === "logcenter") {
    if (input.uri !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "URI", input.uri.trim());
    }
    if (input.authMode !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(
        nextDatasourceEnv,
        "AUTH_MODE",
        input.authMode.trim() === "form" ? "form" : "basic",
      );
    }
    if (input.loginPath !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "LOGIN_PATH", input.loginPath.trim() || "/login");
    }
    if (input.dataView !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "DATA_VIEW", input.dataView.trim());
    }
    if (input.optionsJson !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "OPTIONS_JSON", input.optionsJson.trim());
    }

    let nextSecretEnv = secretEnv || baseSecretEnv();
    if (input.username !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "USERNAME", input.username.trim());
    }
    if (input.secret !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "SECRET", input.secret);
    }
    if (input.expiresAt !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "EXPIRES_AT", input.expiresAt.trim());
    }
    nextSecretEnv = updateSimpleEnv(nextSecretEnv, "UPDATED_AT", nowIso());
    await fs.writeFile(secretEnvFile, nextSecretEnv, "utf8");
  } else if (nextType === "monitor" || nextType === "skywalking" || nextType === "wms_agent") {
    if (input.uri !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "URI", input.uri.trim());
    }
    if (input.optionsJson !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "OPTIONS_JSON", input.optionsJson.trim());
    }

    let nextSecretEnv = secretEnv || baseSecretEnv();
    if (input.username !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "USERNAME", input.username.trim());
    }
    if (input.secret !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "SECRET", input.secret);
    }
    if (input.expiresAt !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "EXPIRES_AT", input.expiresAt.trim());
    }
    nextSecretEnv = updateSimpleEnv(nextSecretEnv, "UPDATED_AT", nowIso());
    await fs.writeFile(secretEnvFile, nextSecretEnv, "utf8");
  } else {
    if (input.host !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "HOST", input.host.trim());
    }
    if (input.port !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "PORT", input.port.trim());
    }
    if (input.database !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "DATABASE", input.database.trim());
    }
    if (input.uri !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "URI", input.uri.trim());
    }
    if (input.brokers !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "BROKERS", input.brokers.trim());
    }
    if (input.clientId !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "CLIENT_ID", input.clientId.trim());
    }
    if (input.ssl !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "SSL", input.ssl ? "true" : "false");
    }
    if (input.saslMechanism !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "SASL_MECHANISM", input.saslMechanism.trim());
    }
    if (input.optionsJson !== undefined) {
      nextDatasourceEnv = updateSimpleEnv(nextDatasourceEnv, "OPTIONS_JSON", input.optionsJson.trim());
    }

    let nextSecretEnv = secretEnv || baseSecretEnv();
    if (input.username !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "USERNAME", input.username.trim());
    }
    if (input.secret !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "SECRET", input.secret);
    }
    if (input.expiresAt !== undefined) {
      nextSecretEnv = updateSimpleEnv(nextSecretEnv, "EXPIRES_AT", input.expiresAt.trim());
    }
    nextSecretEnv = updateSimpleEnv(nextSecretEnv, "UPDATED_AT", nowIso());
    await fs.writeFile(secretEnvFile, nextSecretEnv, "utf8");
  }

  await fs.writeFile(datasourceEnvFile, nextDatasourceEnv, "utf8");
}
