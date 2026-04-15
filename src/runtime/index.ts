import { safeJsonStringify } from "../utils.js";
import { RuntimeCache } from "./cache.js";
import { RuntimeLogBuffer } from "./log-buffer.js";

export interface ToolRuntime {
  cache: RuntimeCache;
  logs: RuntimeLogBuffer;
  buildCacheKey: (toolName: string, args: unknown) => string;
}

export function createToolRuntime(): ToolRuntime {
  return {
    cache: new RuntimeCache(),
    logs: new RuntimeLogBuffer(300),
    buildCacheKey: (toolName: string, args: unknown) => `${toolName}::${safeJsonStringify(args)}`,
  };
}
