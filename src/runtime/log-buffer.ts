export interface RuntimeCallLogEntry {
  id: string;
  toolName: string;
  startedAt: string;
  completedAt: string;
  durationMs: number;
  ok: boolean;
  cached: boolean;
  stackDepth: number;
  args: unknown;
  error?: string;
}

export class RuntimeLogBuffer {
  private readonly entries: RuntimeCallLogEntry[] = [];

  constructor(private readonly maxEntries = 200) {}

  record(entry: RuntimeCallLogEntry): void {
    this.entries.push(entry);
    if (this.entries.length > this.maxEntries) {
      this.entries.splice(0, this.entries.length - this.maxEntries);
    }
  }

  list(limit = 20, toolName?: string): RuntimeCallLogEntry[] {
    const scoped = toolName?.trim()
      ? this.entries.filter((entry) => entry.toolName === toolName.trim())
      : this.entries;
    return scoped.slice(-Math.max(1, limit)).reverse();
  }
}
