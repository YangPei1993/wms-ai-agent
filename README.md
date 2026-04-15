# wms-ai-agent

`wms-ai-agent` is a file-based MCP server for investigation workflows.

It keeps project context in plain text files and exposes read-only tools for:

- repository search and file reads
- log search
- SQL schema inspection and queries
- Mongo find and aggregate queries
- Kafka offsets and consumer lag
- monitor / tracing backends
- workspace knowledge-base and case memory
- local tool modules and tool-store packages

The original use case was WMS investigation, but the layout is generic enough for any multi-service backend estate where you want Codex to reason over code, data, logs, and operational playbooks without hiding state in a database.

## Why this shape

- Plain files stay reviewable in Git.
- Project boundaries are explicit.
- Secrets live in dedicated `secret.env` files instead of mixed into playbooks.
- The MCP surface stays read-only for operational evidence gathering.
- A thin local config UI helps edit the workspace without adding another backend.

## Repository layout

```text
wms-ai-agent/
├── src/
├── tool-store/
├── examples/
│   └── workspace-template/
├── docs/
├── scripts/
└── README.md
```

## Quick start

### 1. Install dependencies

```bash
npm install
```

### 2. Build

```bash
npm run build
```

### 3. Create a workspace

By default the server reads from:

```text
~/.wms-ai-agent/workspace
```

You can either use that default path or point the server at any other directory with `WMS_AI_AGENT_STORE_PATH`.

A starter template is included here:

```text
examples/workspace-template
```

Example:

```bash
mkdir -p ~/.wms-ai-agent
cp -R examples/workspace-template ~/.wms-ai-agent/workspace
```

### 4. Run the MCP server over stdio

```bash
WMS_AI_AGENT_STORE_PATH="$HOME/.wms-ai-agent/workspace" npm run start
```

### 5. Run the local config UI

The config UI binds to `127.0.0.1` only.

```bash
WMS_AI_AGENT_STORE_PATH="$HOME/.wms-ai-agent/workspace" npm run start:config
```

Or use the helper script:

```bash
./scripts/start-config-ui.sh
```

The default UI URL is:

```text
http://127.0.0.1:3789
```

## Using it from Codex

Example MCP server entry:

```json
{
  "mcpServers": {
    "wms-ai-agent": {
      "command": "node",
      "args": ["/absolute/path/to/wms-ai-agent/build/index.js"],
      "env": {
        "WMS_AI_AGENT_STORE_PATH": "/absolute/path/to/workspace"
      }
    }
  }
}
```

## Workspace layout

```text
workspace/
├── workspace.env
├── instructions.md
├── tool-guidelines.md
├── knowledge-base/
├── memory/
├── projects/
│   └── <project-id>/
│       ├── project.env
│       ├── repos.txt
│       ├── logs.txt
│       ├── datasources.txt
│       ├── playbook.md
│       ├── checklist.md
│       ├── knowledge-base/
│       └── memory/
└── datasources/
    └── <datasource-id>/
        ├── datasource.env
        └── secret.env
```

## Supported datasource types

- `mysql`
- `postgres`
- `mongo`
- `kafka`
- `logcenter`
- `monitor`
- `skywalking`
- `wms_agent`

## Main tool groups

- workspace and project discovery
- datasource overview and health checks
- read-only SQL / Mongo / Kafka diagnostics
- repository and log inspection
- monitor and trace queries
- knowledge-base and case-memory helpers
- external tool-module loading
- local and remote tool-store installation

## Environment variables

- `WMS_AI_AGENT_STORE_PATH`: override the workspace root
- `WMS_AI_AGENT_CONFIG_PORT`: config UI port, default `3789`
- `WMS_AI_AGENT_LEGACY_PYTHON`: explicit Python executable for legacy Mongo shell helpers
- `WMS_AI_AGENT_TOOL_MODULES_PATH`: extra tool-module directories, `:` separated
- `WMS_AI_AGENT_TOOL_STORE_PATH`: override the local tool-store path
- `WMS_AI_AGENT_TOOL_MODULE_OVERRIDES_PATH`: extra module override path
- `WMS_AI_AGENT_REMOTE_SOURCES_PATH`: override the remote tool-source registry file

## Tool modules

The server can load tools from three places:

- built-in tools
- workspace tool modules
- tool-store packages installed into the workspace

Default workspace module directory:

```text
<workspace>/tool-modules
```

Two supported layouts:

```text
tool-modules/
  demo-module.json
```

or:

```text
tool-modules/
  demo-module/
    manifest.json
    module.js
```

Manifest example:

```json
{
  "id": "project.snapshot",
  "label": "Project Snapshot",
  "version": "1.0.0",
  "description": "Project summary tools",
  "entry": "./module.js",
  "enabled": true,
  "tags": ["project", "summary"],
  "hotReloadable": true
}
```

Module entry example:

```js
export function buildToolDefinitions(getStore, getStorePath) {
  return [
    {
      name: "demo_tool",
      category: "project",
      description: "demo",
      schema: {},
      handler: async (_context, args) => ({ ok: true, args })
    }
  ];
}
```

## Tool store

This repository includes a local tool store under:

```text
tool-store/packages
```

You can list packages with `tool_store_catalog` and install them into the active workspace with `tool_store_install_local`.

Remote catalogs are supported through `tool_store_remote_source_add`, `tool_store_remote_catalog`, and `tool_store_install_remote`.

## Security notes

- `secret.env` is plain text by design. Treat the workspace as sensitive local state.
- Keep real credentials out of Git.
- The built-in data access tools are intentionally read-only.
- Put durable system knowledge in `knowledge-base/` and case-specific findings in `memory/`.

## Development

```bash
npm run check
npm run build
npm run dev:mcp
npm run dev:config
```

## Status

This repository is being cleaned up into a public-ready baseline. The core MCP server and config UI are usable, while packaging, docs, and extension examples are still evolving.

## License

MIT
