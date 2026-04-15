import { promises as fs } from "node:fs";
import path from "node:path";
import { runLocalCommand } from "../utils.js";

export async function searchAcrossRoots(
  roots: string[],
  query: string,
  glob: string | undefined,
  limit: number,
  options?: { fixedString?: boolean },
): Promise<Array<Record<string, unknown>>> {
  const results: Array<Record<string, unknown>> = [];
  for (const root of roots) {
    if (/^https?:\/\//i.test(root.trim())) {
      throw new Error(
        `Search roots must be local filesystem paths. Got remote URL: ${root}. Use a datasource-backed tool instead.`,
      );
    }
    const rootStat = await fs.stat(root).catch(() => null);
    if (!rootStat) {
      continue;
    }
    const args = ["--line-number", "--no-heading", "--color", "never", "--hidden", "-g", "!.git"];
    if (options?.fixedString) {
      args.push("-F");
    }
    if (glob && rootStat.isDirectory()) {
      args.push("-g", glob);
    }
    args.push(query, ".");
    let cwd = root;
    if (rootStat.isFile()) {
      cwd = path.dirname(root);
      args.splice(args.length - 1, 1, path.basename(root));
    }
    const result = await runLocalCommand("rg", args, cwd, 20_000);
    if (result.exitCode !== 0 && result.exitCode !== 1) {
      throw new Error(result.stderr || `search failed in ${root}`);
    }
    const lines = result.stdout.split(/\r?\n/).filter(Boolean);
    for (const line of lines) {
      const match = line.match(/^(.+?):(\d+):(.*)$/);
      if (!match) {
        continue;
      }
      const resolvedPath = rootStat.isFile()
        ? path.resolve(cwd, match[1])
        : path.resolve(root, match[1]);
      results.push({
        repoRoot: root,
        filePath: resolvedPath,
        lineNumber: Number(match[2]),
        preview: match[3],
      });
      if (results.length >= limit) {
        return results;
      }
    }
  }
  return results;
}
