export interface RuntimeCacheEntry<T = unknown> {
  value: T;
  createdAt: string;
  expiresAt: number;
  hits: number;
}

export interface RuntimeCacheStats {
  size: number;
  activeEntries: number;
  expiredEntries: number;
}

export class RuntimeCache {
  private readonly entries = new Map<string, RuntimeCacheEntry>();

  get<T = unknown>(key: string): { hit: boolean; value?: T } {
    const entry = this.entries.get(key);
    if (!entry) {
      return { hit: false };
    }
    if (entry.expiresAt <= Date.now()) {
      this.entries.delete(key);
      return { hit: false };
    }
    entry.hits += 1;
    return { hit: true, value: entry.value as T };
  }

  set<T = unknown>(key: string, value: T, ttlMs: number): void {
    this.entries.set(key, {
      value,
      createdAt: new Date().toISOString(),
      expiresAt: Date.now() + Math.max(1, ttlMs),
      hits: 0,
    });
  }

  pruneExpired(): void {
    const now = Date.now();
    for (const [key, entry] of this.entries.entries()) {
      if (entry.expiresAt <= now) {
        this.entries.delete(key);
      }
    }
  }

  stats(): RuntimeCacheStats {
    const now = Date.now();
    let activeEntries = 0;
    let expiredEntries = 0;
    for (const entry of this.entries.values()) {
      if (entry.expiresAt > now) {
        activeEntries += 1;
      } else {
        expiredEntries += 1;
      }
    }
    return {
      size: this.entries.size,
      activeEntries,
      expiredEntries,
    };
  }
}
