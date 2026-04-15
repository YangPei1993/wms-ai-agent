#!/usr/bin/env node

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { createServer } from "./tools.js";
import { resolveStorePath } from "./store.js";

async function main() {
  const storePath = resolveStorePath(process.env.WMS_AI_AGENT_STORE_PATH);
  const server = await createServer(storePath);
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error(`wms-ai-agent running on stdio, workspace=${storePath}`);
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
