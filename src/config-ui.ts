import fs from "node:fs/promises";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { testDatasource } from "./datasources.js";
import { readStoreSync, resolveStorePath } from "./store.js";
import { runToolLocally } from "./tools.js";
import { safeJsonStringify } from "./utils.js";
import {
  bindDatasourceToProject,
  buildWorkspaceTree,
  createDatasourceSkeleton,
  createProjectSkeleton,
  deleteDatasource,
  deleteProject,
  ensureWorkspaceRoot,
  getGuideForNode,
  readWorkspaceFile,
  renameDatasource,
  renameProject,
  setActiveProject,
  unbindDatasourceFromProject,
  writeDatasourceConfig,
  writeWorkspaceFile,
  WorkspaceNode,
} from "./workspace-files.js";

interface JsonResponse {
  ok: boolean;
  [key: string]: unknown;
}

const MODULE_DIR = path.dirname(fileURLToPath(import.meta.url));
const PROJECT_ROOT = path.resolve(MODULE_DIR, "..");

function sendJson(response: http.ServerResponse, statusCode: number, payload: JsonResponse): void {
  response.statusCode = statusCode;
  response.setHeader("content-type", "application/json; charset=utf-8");
  response.setHeader("cache-control", "no-store, max-age=0");
  response.end(JSON.stringify(payload));
}

function sendHtml(response: http.ServerResponse, html: string): void {
  response.statusCode = 200;
  response.setHeader("content-type", "text/html; charset=utf-8");
  response.setHeader("cache-control", "no-store, max-age=0");
  response.end(html);
}

function sendText(response: http.ServerResponse, statusCode: number, contentType: string, body: string): void {
  response.statusCode = statusCode;
  response.setHeader("content-type", contentType);
  response.setHeader("cache-control", "no-store, max-age=0");
  response.end(body);
}

async function readBody(request: http.IncomingMessage): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  const raw = Buffer.concat(chunks).toString("utf8").trim();
  if (!raw) {
    return {};
  }
  return JSON.parse(raw);
}

function htmlPage(): string {
  return `<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>WMS AI Agent 配置中心</title>
    <style>
      :root {
        color-scheme: light;
        --bg: #f5f5f7;
        --bg-deep: #edf1f6;
        --surface: rgba(255, 255, 255, 0.72);
        --surface-strong: rgba(255, 255, 255, 0.9);
        --surface-muted: rgba(248, 250, 252, 0.82);
        --line: rgba(15, 23, 42, 0.08);
        --line-strong: rgba(15, 23, 42, 0.14);
        --text: #0f172a;
        --muted: #667085;
        --muted-2: #98a2b3;
        --accent: #0a84ff;
        --accent-soft: rgba(10, 132, 255, 0.12);
        --accent-soft-strong: rgba(10, 132, 255, 0.2);
        --success: #16a34a;
        --warn: #f59e0b;
        --danger: #ef4444;
        --shadow-soft: 0 18px 48px rgba(15, 23, 42, 0.08);
        --shadow-panel: 0 18px 36px rgba(15, 23, 42, 0.06), inset 0 1px 0 rgba(255, 255, 255, 0.65);
        --layout-height: auto;
        --left-pane-width: 340px;
        --right-pane-width: 380px;
        --resizer-width: 10px;
        --min-left-width: 260px;
        --min-center-width: 460px;
        --min-right-width: 300px;
      }
      * { box-sizing: border-box; }
      html, body {
        height: 100%;
        overflow: hidden;
      }
      body {
        position: relative;
        margin: 0;
        padding: 18px;
        font-family: "SF Pro Display", "SF Pro Text", "PingFang SC", -apple-system, BlinkMacSystemFont, "Helvetica Neue", sans-serif;
        background:
          linear-gradient(180deg, #fbfbfd 0%, #f4f6f9 46%, #edf1f6 100%);
        color: var(--text);
        display: flex;
        flex-direction: column;
        gap: 18px;
        height: 100vh;
        min-height: 100vh;
        overflow: hidden;
      }
      body::before,
      body::after {
        content: "";
        position: fixed;
        border-radius: 999px;
        filter: blur(50px);
        pointer-events: none;
        z-index: 0;
      }
      body::before {
        top: -22vh;
        left: -8vw;
        width: 48vw;
        height: 48vw;
        background: radial-gradient(circle, rgba(10, 132, 255, 0.22), rgba(10, 132, 255, 0));
      }
      body::after {
        right: -12vw;
        bottom: -30vh;
        width: 52vw;
        height: 52vw;
        background: radial-gradient(circle, rgba(94, 234, 212, 0.16), rgba(94, 234, 212, 0));
      }
      header,
      main {
        position: relative;
        z-index: 1;
      }
      header {
        padding: 28px 30px 24px;
        border-radius: 32px;
        border: 1px solid rgba(255, 255, 255, 0.68);
        background:
          linear-gradient(135deg, rgba(255, 255, 255, 0.92), rgba(246, 248, 252, 0.72));
        box-shadow: var(--shadow-soft);
        backdrop-filter: saturate(180%) blur(28px);
        flex: 0 0 auto;
      }
      .header-top {
        display: flex;
        justify-content: space-between;
        gap: 24px;
        align-items: flex-start;
      }
      .hero-copy {
        max-width: 760px;
      }
      .eyebrow {
        display: inline-flex;
        align-items: center;
        gap: 8px;
        padding: 8px 12px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.7);
        border: 1px solid rgba(15, 23, 42, 0.08);
        color: var(--muted);
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }
      header h1 {
        margin: 18px 0 10px;
        font-size: clamp(32px, 4vw, 48px);
        line-height: 0.98;
        letter-spacing: -0.05em;
      }
      header p {
        max-width: 680px;
        margin: 0;
        color: var(--muted);
        font-size: 15px;
        line-height: 1.7;
      }
      header strong {
        color: var(--text);
        font-weight: 600;
      }
      .workspace-summary {
        min-width: min(360px, 100%);
        max-width: 420px;
        padding: 18px 20px;
        border-radius: 24px;
        background: rgba(255, 255, 255, 0.64);
        border: 1px solid rgba(15, 23, 42, 0.08);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.8);
      }
      .summary-label {
        color: var(--muted);
        font-size: 12px;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
      }
      .workspace-summary code {
        display: block;
        margin-top: 12px;
        padding: 14px 16px;
        border-radius: 18px;
        background: rgba(15, 23, 42, 0.04);
        border: 1px solid rgba(15, 23, 42, 0.08);
        color: #0b5bd3;
        font-size: 13px;
        line-height: 1.6;
        word-break: break-all;
      }
      .summary-note {
        margin-top: 12px;
        color: var(--muted);
        font-size: 13px;
        line-height: 1.6;
      }
      .toolbar {
        display: flex;
        justify-content: space-between;
        gap: 14px 18px;
        flex-wrap: wrap;
        margin-top: 24px;
        align-items: center;
      }
      .toolbar-primary,
      .toolbar-meta {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: center;
      }
      .toolbar-tabs {
        display: inline-flex;
        gap: 8px;
        padding: 6px;
        border: 1px solid var(--line);
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.54);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.78);
      }
      .layout {
        display: grid;
        grid-template-columns:
          var(--left-pane-width)
          var(--resizer-width)
          minmax(var(--min-center-width), 1fr)
          var(--resizer-width)
          var(--right-pane-width);
        flex: 0 0 auto;
        height: var(--layout-height);
        min-height: 0;
        align-items: stretch;
        border-radius: 34px;
        border: 1px solid rgba(255, 255, 255, 0.7);
        background: rgba(255, 255, 255, 0.38);
        box-shadow: var(--shadow-soft);
        backdrop-filter: saturate(180%) blur(28px);
        overflow: hidden;
      }
      .layout.modules-mode {
        display: grid;
        grid-template-columns: minmax(0, 1fr);
        grid-template-rows: minmax(0, 1fr);
      }
      .layout.modules-mode #centerPane {
        grid-column: 1;
        grid-row: 1;
        width: 100%;
        max-width: none;
        height: 100%;
        min-height: 0;
        overflow: auto;
      }
      .pane {
        padding: 18px;
        height: 100%;
        overflow: auto;
        min-height: 0;
        overscroll-behavior: contain;
      }
      #rightPane {
        overflow-y: auto;
        overflow-x: hidden;
      }
      .center-pane-shell,
      .workbench-stack,
      .context-stack,
      .right-pane-shell {
        display: grid;
        gap: 16px;
        align-content: start;
      }
      .center-pane-shell {
        min-height: 0;
      }
      .sticky-rail {
        position: static;
        min-height: auto;
      }
      .sticky-rail > .context-stack,
      .sticky-rail > .right-pane-shell {
        max-height: none;
        overflow: visible;
        padding-right: 0;
      }
      .right-pane-shell {
        height: auto;
        min-height: 0;
        overflow: visible;
        padding-right: 0;
      }
      .context-stack > .card,
      .workbench-stack > .card,
      .right-pane-shell > .card {
        margin-bottom: 0;
      }
      #leftResizer,
      #rightResizer {
        align-self: stretch;
      }
      .pane::-webkit-scrollbar,
      .preview-render::-webkit-scrollbar,
      pre::-webkit-scrollbar,
      textarea::-webkit-scrollbar {
        width: 10px;
        height: 10px;
      }
      .pane::-webkit-scrollbar-thumb,
      .preview-render::-webkit-scrollbar-thumb,
      pre::-webkit-scrollbar-thumb,
      textarea::-webkit-scrollbar-thumb {
        background: rgba(148, 163, 184, 0.35);
        border-radius: 999px;
      }
      .resizer {
        position: relative;
        cursor: col-resize;
        user-select: none;
        touch-action: none;
        background: transparent;
      }
      .resizer::before {
        content: "";
        position: absolute;
        top: 20px;
        bottom: 20px;
        left: 50%;
        width: 2px;
        transform: translateX(-50%);
        background: linear-gradient(180deg, rgba(15, 23, 42, 0), rgba(15, 23, 42, 0.08), rgba(15, 23, 42, 0));
      }
      .resizer::after {
        content: "";
        position: absolute;
        inset: 18px 0;
        border-radius: 999px;
        background: rgba(10, 132, 255, 0);
        transition: background 0.18s ease;
      }
      .resizer:hover::after,
      .resizer.dragging::after {
        background: rgba(10, 132, 255, 0.12);
      }
      .card {
        background:
          linear-gradient(180deg, rgba(255, 255, 255, 0.92), rgba(247, 249, 252, 0.72));
        border: 1px solid rgba(15, 23, 42, 0.1);
        border-radius: 28px;
        padding: 22px;
        margin-bottom: 16px;
        box-shadow: var(--shadow-panel);
        backdrop-filter: saturate(180%) blur(24px);
      }
      .card h2,
      .card h3,
      .card h4 {
        margin-top: 0;
        color: var(--text);
      }
      .hero-card h2 {
        font-size: clamp(28px, 3vw, 38px);
        line-height: 1.05;
        letter-spacing: -0.04em;
        margin-bottom: 10px;
      }
      .muted { color: var(--muted); }
      .tree {
        font-size: 14px;
        line-height: 1.45;
      }
      .tree-group { margin: 6px 0; }
      .tree-children {
        display: grid;
        gap: 8px;
        padding-left: 16px;
        border-left: 1px solid rgba(148, 163, 184, 0.18);
        margin-left: 12px;
      }
      .tree .folder,
      .tree .node {
        -webkit-appearance: none;
        appearance: none;
        background: none;
        background-color: transparent;
        background-image: none;
        border: 0;
        box-shadow: none;
        transform: none;
        filter: none;
      }
      .tree .folder:hover,
      .tree .node:hover {
        background: rgba(15, 23, 42, 0.04);
        box-shadow: none;
        transform: none;
      }
      .tree .node {
        display: block;
        width: 100%;
        text-align: left;
        color: inherit;
        padding: 8px 10px;
        border-radius: 14px;
        cursor: pointer;
        margin: 0;
        transition: background 0.18s ease;
      }
      .tree .node:active {
        transform: none;
      }
      .tree .active {
        background: linear-gradient(135deg, rgba(10, 132, 255, 0.14), rgba(90, 200, 250, 0.08));
        box-shadow: inset 0 0 0 1px rgba(10, 132, 255, 0.12);
      }
      .node-row {
        display: flex;
        gap: 8px;
        align-items: center;
        min-width: 0;
      }
      .node-toggle {
        width: 18px;
        flex: 0 0 18px;
        text-align: center;
        color: var(--muted-2);
        font-size: 12px;
      }
      .node-spacer {
        width: 18px;
        flex: 0 0 18px;
      }
      .node-label {
        min-width: 0;
        flex: 1 1 auto;
        display: flex;
        align-items: center;
        gap: 8px;
      }
      .node-name {
        min-width: 0;
        flex: 1 1 auto;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        font-weight: 600;
      }
      .tree-group.collapsed > .tree-children {
        display: none;
      }
      .tree-case .node-name {
        font-weight: 500;
      }
      .badge {
        display: inline-flex;
        align-items: center;
        font-size: 11px;
        border: 1px solid rgba(15, 23, 42, 0.12);
        color: #0b5bd3;
        padding: 4px 9px;
        border-radius: 999px;
        background: rgba(255, 255, 255, 0.92);
      }
      .badge.green {
        color: var(--success);
      }
      .badge.yellow {
        color: var(--warn);
      }
      .section-title {
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: var(--muted);
        margin-bottom: 12px;
        font-weight: 700;
      }
      input,
      select,
      textarea,
      button {
        font: inherit;
      }
      input,
      select,
      textarea {
        width: 100%;
        background: rgba(255, 255, 255, 0.96);
        color: var(--text);
        border: 1px solid rgba(15, 23, 42, 0.16);
        border-radius: 16px;
        padding: 12px 14px;
        box-shadow:
          0 1px 2px rgba(15, 23, 42, 0.04),
          inset 0 1px 0 rgba(255, 255, 255, 0.96);
        transition: border-color 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
      }
      input:hover,
      select:hover,
      textarea:hover {
        border-color: rgba(15, 23, 42, 0.22);
      }
      input:focus,
      select:focus,
      textarea:focus {
        outline: none;
        border-color: rgba(10, 132, 255, 0.42);
        box-shadow:
          0 0 0 4px rgba(10, 132, 255, 0.12),
          0 1px 2px rgba(15, 23, 42, 0.04),
          inset 0 1px 0 rgba(255, 255, 255, 0.96),
          inset 0 0 0 1px rgba(10, 132, 255, 0.16);
        background: rgba(255, 255, 255, 1);
      }
      select[multiple] {
        min-height: 126px;
        padding: 8px;
        border-color: rgba(15, 23, 42, 0.18);
        background: rgba(255, 255, 255, 0.98);
      }
      select[multiple] option {
        padding: 10px 12px;
        border-radius: 10px;
      }
      select[multiple] option:checked {
        background: rgba(15, 23, 42, 0.22);
        color: var(--text);
      }
      textarea {
        min-height: 260px;
        resize: vertical;
      }
      button {
        background: linear-gradient(180deg, #101828, #0f172a);
        color: #f8fafc;
        border: 1px solid rgba(15, 23, 42, 0.1);
        border-radius: 999px;
        padding: 10px 16px;
        cursor: pointer;
        font-weight: 700;
        letter-spacing: -0.01em;
        box-shadow: 0 12px 24px rgba(15, 23, 42, 0.12);
        transition: transform 0.18s ease, box-shadow 0.18s ease, background 0.18s ease;
      }
      button:hover {
        transform: translateY(-1px);
        box-shadow: 0 14px 28px rgba(15, 23, 42, 0.16);
      }
      button:active {
        transform: translateY(0);
      }
      button.secondary {
        background: rgba(255, 255, 255, 0.72);
        color: var(--text);
        border: 1px solid rgba(15, 23, 42, 0.08);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.8);
      }
      button.warn {
        background: linear-gradient(180deg, #ffb84d, #ff9f0a);
        color: #3b2300;
      }
      button.danger {
        background: linear-gradient(180deg, #ff8f8f, #ff453a);
        color: #fff7f7;
      }
      .grid2 {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
      }
      .actions {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
      }
      .field-list {
        display: grid;
        gap: 12px;
      }
      .field-item {
        border: 1px solid rgba(15, 23, 42, 0.1);
        border-radius: 20px;
        padding: 16px;
        background: rgba(255, 255, 255, 0.74);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.78);
      }
      .field-item.compact {
        padding: 10px 12px;
      }
      .tabs {
        display: inline-flex;
        gap: 8px;
        padding: 6px;
        background: rgba(255, 255, 255, 0.62);
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 999px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.78);
      }
      .tab-button {
        background: transparent;
        color: var(--muted);
        border: 0;
        border-radius: 999px;
        padding: 9px 14px;
        font-weight: 600;
        box-shadow: none;
      }
      .tab-button.active {
        background: rgba(255, 255, 255, 0.96);
        color: var(--text);
        box-shadow: 0 8px 18px rgba(15, 23, 42, 0.08);
      }
      .tool-module-section {
        display: none;
      }
      .tool-module-section.active {
        display: block;
      }
      .preview-shell {
        display: grid;
        gap: 12px;
      }
      .preview-render {
        min-height: 120px;
        padding: 18px;
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 20px;
        background: rgba(255, 255, 255, 0.82);
        overflow: auto;
      }
      .preview-render h1,
      .preview-render h2,
      .preview-render h3,
      .preview-render h4,
      .preview-render h5,
      .preview-render h6 {
        margin-top: 0;
        margin-bottom: 12px;
      }
      .preview-render p,
      .preview-render li,
      .preview-render blockquote {
        line-height: 1.7;
      }
      .preview-render table {
        width: 100%;
        border-collapse: collapse;
        margin: 12px 0;
      }
      .preview-render th,
      .preview-render td {
        border: 1px solid rgba(15, 23, 42, 0.08);
        padding: 10px 12px;
        text-align: left;
      }
      .preview-render blockquote {
        margin: 12px 0;
        padding: 12px 16px;
        border-left: 3px solid var(--accent);
        background: rgba(10, 132, 255, 0.08);
        border-radius: 0 16px 16px 0;
      }
      .preview-render pre {
        margin-top: 12px;
      }
      .preview-render code {
        font-family: "SF Mono", SFMono-Regular, Menlo, Consolas, monospace;
      }
      .preview-render .mermaid-host {
        overflow: auto;
        padding: 14px;
        border: 1px solid rgba(10, 132, 255, 0.14);
        border-radius: 18px;
        background: rgba(255, 255, 255, 0.78);
      }
      .field-item code,
      .path {
        color: #0b5bd3;
        word-break: break-all;
      }
      .small {
        font-size: 13px;
      }
      .status {
        padding: 13px 16px;
        border-radius: 18px;
        border: 1px solid rgba(15, 23, 42, 0.08);
        background: rgba(255, 255, 255, 0.72);
        margin-bottom: 12px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.82);
      }
      .status.error {
        border-color: rgba(239, 68, 68, 0.18);
        color: #b42318;
        background: rgba(254, 242, 242, 0.88);
      }
      .status.success {
        border-color: rgba(34, 197, 94, 0.18);
        color: #166534;
        background: rgba(240, 253, 244, 0.88);
      }
      pre {
        white-space: pre-wrap;
        word-break: break-word;
        background: rgba(255, 255, 255, 0.84);
        border: 1px solid rgba(15, 23, 42, 0.08);
        border-radius: 20px;
        padding: 18px;
        margin: 0;
        color: var(--text);
      }
      .empty {
        color: var(--muted);
        font-style: italic;
      }
      .meta-line {
        margin: 6px 0;
        color: var(--muted);
      }
      @media (max-width: 1320px) {
        body {
          padding: 14px;
          gap: 14px;
        }
        header {
          padding: 24px;
        }
        .header-top {
          flex-direction: column;
        }
        .workspace-summary {
          max-width: none;
          width: 100%;
        }
      }
      @media (max-width: 1120px) {
        .grid2 {
          grid-template-columns: 1fr;
        }
        .layout {
          grid-template-columns:
            minmax(280px, var(--left-pane-width))
            var(--resizer-width)
            minmax(0, 1fr);
          grid-template-areas:
            "left leftResizer center"
            "right right right";
        }
        #leftPane { grid-area: left; }
        #leftResizer { grid-area: leftResizer; }
        #centerPane { grid-area: center; }
        #rightPane {
          grid-area: right;
          border-top: 1px solid rgba(15, 23, 42, 0.06);
        }
        #rightResizer {
          display: none;
        }
      }
      @media (max-width: 920px) {
        html,
        body {
          height: auto;
          overflow: auto;
        }
        body {
          min-height: 100%;
          height: auto;
          padding: 12px;
        }
        header {
          border-radius: 26px;
          padding: 22px 18px 18px;
        }
        .layout,
        .layout.modules-mode {
          display: flex;
          flex-direction: column;
          height: auto;
          overflow: visible;
        }
        .pane {
          overflow: visible;
          height: auto;
          min-height: auto;
          padding: 14px;
        }
        #rightPane {
          overflow: visible;
        }
        .center-pane-shell,
        .context-stack,
        .workbench-stack,
        .right-pane-shell,
        .sticky-rail {
          height: auto;
          max-height: none;
          overflow: visible;
          min-height: auto;
          padding-right: 0;
        }
        .resizer {
          display: none;
        }
        .card {
          border-radius: 24px;
          padding: 18px;
        }
        .toolbar,
        .toolbar-primary,
        .toolbar-meta {
          width: 100%;
        }
      }
    </style>
  </head>
  <body>
    <header>
      <div class="header-top">
        <div class="hero-copy">
          <span class="eyebrow">Workspace Console</span>
          <h1>WMS AI Agent 配置中心</h1>
          <p>按 <strong>工作区 → 项目 → 数据源 → 文件</strong> 的层级直接维护本地 workspace 文件，不做额外数据库，所有配置都直接落在可审计的本地目录里。</p>
        </div>
        <div class="workspace-summary">
          <div class="summary-label">当前工作区</div>
          <code id="workspaceRoot"></code>
          <div class="summary-note">界面只是 workspace 文件的精致壳层，不引入额外状态源，保留当前数据结构和回退路径。</div>
        </div>
      </div>
      <div class="toolbar">
        <div class="toolbar-primary">
          <button id="refreshBtn" class="secondary">刷新工作区树</button>
          <button id="newProjectBtn">新增项目</button>
          <button id="newDatasourceBtn" class="warn">新增数据源</button>
        </div>
        <div class="toolbar-meta">
          <div class="toolbar-tabs">
            <button id="menuWorkspaceBtn" class="tab-button active">工作区配置</button>
            <button id="menuToolModulesBtn" class="tab-button">工具模块</button>
          </div>
        </div>
      </div>
    </header>
    <main class="layout">
      <section class="pane" id="leftPane">
        <div class="card" id="treeCard">
          <div class="section-title">工作区树</div>
          <div id="tree" class="tree"></div>
        </div>
        <div class="card" id="toolModulesSidebarCard" style="display:none;">
          <div class="section-title">工具模块导航</div>
          <div id="toolModulesSidebarSummary" class="muted small" style="margin-bottom:12px;"></div>
          <div id="toolModulesSidebarList" class="field-list"></div>
        </div>
      </section>
      <div class="resizer" id="leftResizer" aria-label="调整左侧栏宽度" role="separator"></div>
      <section class="pane" id="centerPane">
        <div class="center-pane-shell">
          <div class="sticky-rail">
            <div class="context-stack">
              <div id="status"></div>
              <div class="card hero-card">
                <div class="section-title">当前节点</div>
                <h2 id="nodeTitle">请选择一个节点</h2>
                <div id="nodeMeta" class="small muted"></div>
                <p id="nodeSummary" class="muted"></p>
              </div>
              <div class="card" id="nodeActionsCard" style="display:none;">
                <div class="section-title">当前节点操作</div>
                <div class="actions">
                  <button id="renameProjectBtn" class="secondary" style="display:none;">重命名项目 ID</button>
                  <button id="deleteProjectBtn" class="danger" style="display:none;">删除当前项目</button>
                  <button id="setActiveProjectBtn" class="secondary" style="display:none;">设为默认项目</button>
                  <button id="renameDatasourceBtn" class="secondary" style="display:none;">重命名数据源 ID</button>
                  <button id="deleteDatasourceBtn" class="danger" style="display:none;">删除当前数据源</button>
                  <button id="testDatasourceBtn" class="warn" style="display:none;">测试当前数据源</button>
                </div>
              </div>
              <div class="card" id="howToFillCard">
                <div class="section-title">如何填写</div>
                <div id="howToFill" class="field-list"></div>
              </div>
            </div>
          </div>
          <div class="workbench-stack">
            <div class="card" id="toolModulesCard" style="display:none;">
              <div class="section-title">工具模块</div>
              <p class="muted small">这里只管理 MCP 工具模块和本地 tool store，不影响你已配置的数据源与凭证。</p>
              <div class="actions" style="margin-bottom:12px;">
                <button id="refreshToolModulesBtn" class="secondary">刷新模块视图</button>
                <button id="reloadToolModulesBtn" class="warn">重载 MCP 工具模块</button>
              </div>
              <div class="tabs" style="margin-bottom:12px;">
                <button id="toolModulesLoadedTab" class="tab-button active">已加载模块</button>
                <button id="toolModulesLocalStoreTab" class="tab-button">本地 Store</button>
                <button id="toolModulesRemoteSourcesTab" class="tab-button">远程源</button>
                <button id="toolModulesRemoteStoreTab" class="tab-button">远程包</button>
              </div>
              <div class="field-item tool-module-section active" id="toolModulesLoadedSection">
                <div style="font-weight:700; margin-bottom:8px;">已加载模块</div>
                <div id="loadedModulesSummary" class="muted small"></div>
                <div id="loadedModulesList" class="field-list" style="margin-top:12px;"></div>
              </div>
              <div class="field-item tool-module-section" id="toolModulesLocalStoreSection" style="margin-top:12px;">
                <div style="font-weight:700; margin-bottom:8px;">本地 Tool Store</div>
                <div class="small muted" style="margin-bottom:12px;">从仓库内置的本地包安装到当前 workspace 的 <code>tool-modules</code> 目录。</div>
                <div class="grid2">
                  <div>
                    <label class="small muted">可安装包</label>
                    <select id="toolStorePackageSelect"></select>
                  </div>
                  <div>
                    <label class="small muted">覆盖已安装</label>
                    <select id="toolStoreOverwriteSelect">
                      <option value="false">false</option>
                      <option value="true">true</option>
                    </select>
                  </div>
                </div>
                <div class="actions" style="margin-top:12px;">
                  <button id="installToolStorePackageBtn">安装本地包</button>
                </div>
                <div id="toolStorePackagesList" class="field-list" style="margin-top:12px;"></div>
              </div>
              <div class="field-item tool-module-section" id="toolModulesRemoteSourcesSection" style="margin-top:12px;">
                <div style="font-weight:700; margin-bottom:8px;">远程源</div>
                <div class="small muted" style="margin-bottom:12px;">远程源配置保存在 <code>~/.wms-ai-agent/tool-store</code>，不会改你当前 workspace 里的 datasource/secret 文件。</div>
                <div class="grid2">
                  <div>
                    <label class="small muted">远程源 ID</label>
                    <input id="remoteSourceIdInput" placeholder="例如 official-registry" />
                  </div>
                  <div>
                    <label class="small muted">远程源名称</label>
                    <input id="remoteSourceLabelInput" placeholder="例如 Official Registry Mirror" />
                  </div>
                </div>
                <div style="margin-top:12px;">
                  <label class="small muted">Catalog URL</label>
                  <input id="remoteSourceUrlInput" placeholder="例如 https://example.com/mcp-catalog.json" />
                </div>
                <div style="margin-top:12px;">
                  <label class="small muted">说明（可选）</label>
                  <input id="remoteSourceDescriptionInput" placeholder="说明这个远程源的用途" />
                </div>
                <div class="actions" style="margin-top:12px;">
                  <button id="saveRemoteSourceBtn">保存远程源</button>
                  <button id="removeRemoteSourceBtn" class="danger">删除远程源</button>
                </div>
                <div id="remoteSourcesList" class="field-list" style="margin-top:12px;"></div>
              </div>
              <div class="field-item tool-module-section" id="toolModulesRemoteStoreSection" style="margin-top:12px;">
                <div style="font-weight:700; margin-bottom:8px;">远程 Tool Store</div>
                <div class="small muted" style="margin-bottom:12px;">手动选择远程包并安装到当前 workspace 的 <code>tool-modules</code> 目录。</div>
                <div class="grid2">
                  <div>
                    <label class="small muted">远程源</label>
                    <select id="remoteSourceSelect"></select>
                  </div>
                  <div>
                    <label class="small muted">覆盖已安装</label>
                    <select id="remoteOverwriteSelect">
                      <option value="false">false</option>
                      <option value="true">true</option>
                    </select>
                  </div>
                </div>
                <div style="margin-top:12px;">
                  <label class="small muted">远程包</label>
                  <select id="remotePackageSelect"></select>
                </div>
                <div class="actions" style="margin-top:12px;">
                  <button id="refreshRemotePackagesBtn" class="secondary">刷新远程包</button>
                  <button id="installRemotePackageBtn" class="warn">安装远程包</button>
                </div>
                <div id="remotePackagesList" class="field-list" style="margin-top:12px;"></div>
              </div>
            </div>
            <div class="card" id="datasourceFormCard" style="display:none;">
              <div class="section-title">数据源配置</div>
              <div id="datasourceTemplateHint" class="muted small" style="margin-bottom:12px;"></div>
              <div class="grid2">
                <div>
                  <label class="small muted">名称</label>
                  <input id="dsLabelInput" placeholder="例如 SRE-READYDB(172.21.3.102)" />
                </div>
                <div>
                  <label class="small muted">类型</label>
                  <input id="dsTypeInput" readonly />
                </div>
              </div>
              <div class="grid2" style="margin-top:12px;">
                <div>
                  <label class="small muted">启用</label>
                  <select id="dsEnabledInput">
                    <option value="true">true</option>
                    <option value="false">false</option>
                  </select>
                </div>
                <div>
                  <label class="small muted">绑定项目</label>
                  <select id="dsProjectIdsInput" multiple size="4"></select>
                </div>
              </div>
              <div id="dsProjectIdsHint" class="small muted" style="margin-top:8px;"></div>
              <div id="sqlFormSection" style="display:none; margin-top:16px;">
                <div class="grid2">
                  <div>
                    <label class="small muted">主机</label>
                    <input id="dsSqlHostInput" placeholder="例如 db.example.internal" />
                  </div>
                  <div>
                    <label class="small muted">端口</label>
                    <input id="dsSqlPortInput" placeholder="例如 33061" />
                  </div>
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">用户名</label>
                    <input id="dsSqlUsernameInput" placeholder="例如 readonly_user" />
                  </div>
                  <div>
                    <label class="small muted">密码</label>
                    <input id="dsSqlSecretInput" placeholder="密码或临时 token" />
                  </div>
                </div>
                <div style="margin-top:12px;">
                  <label class="small muted">Database（可选）</label>
                  <input id="dsSqlDatabaseInput" placeholder="例如 inventory" />
                </div>
              </div>
              <div id="mongoFormSection" style="display:none; margin-top:16px;">
                <div>
                  <label class="small muted">URI</label>
                  <input id="dsMongoUriInput" placeholder="例如 mongodb://user:pass@host:17017/?authSource=admin" />
                </div>
              </div>
              <div id="kafkaFormSection" style="display:none; margin-top:16px;">
                <div>
                  <label class="small muted">Brokers（逗号分隔）</label>
                  <input id="dsKafkaBrokersInput" placeholder="例如 host1:9092,host2:9092" />
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">Client ID</label>
                    <input id="dsKafkaClientIdInput" placeholder="例如 wms-ai-agent" />
                  </div>
                  <div>
                    <label class="small muted">SSL</label>
                    <select id="dsKafkaSslInput">
                      <option value="true">true</option>
                      <option value="false">false</option>
                    </select>
                  </div>
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">用户名</label>
                    <input id="dsKafkaUsernameInput" placeholder="Kafka 用户名" />
                  </div>
                  <div>
                    <label class="small muted">密码 / Secret</label>
                    <input id="dsKafkaSecretInput" placeholder="Kafka 密码或 Secret" />
                  </div>
                </div>
              </div>
              <div id="logcenterFormSection" style="display:none; margin-top:16px;">
                <div>
                  <label class="small muted">Base URL / URI</label>
                  <input id="dsLogcenterUriInput" placeholder="例如 https://logs.example.com" />
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">认证模式</label>
                    <select id="dsLogcenterAuthModeInput">
                      <option value="basic">basic</option>
                      <option value="form">form</option>
                    </select>
                  </div>
                  <div>
                    <label class="small muted">登录路径</label>
                    <input id="dsLogcenterLoginPathInput" placeholder="/login" />
                  </div>
                </div>
                <div style="margin-top:12px;">
                  <label class="small muted">Data View</label>
                  <input id="dsLogcenterDataViewInput" placeholder="data view id 或 title，例如 logs-*" />
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">用户名</label>
                    <input id="dsLogcenterUsernameInput" placeholder="Logcenter 用户名" />
                  </div>
                  <div>
                    <label class="small muted">密码 / Secret</label>
                    <input id="dsLogcenterSecretInput" placeholder="Logcenter 密码或 Secret" />
                  </div>
                </div>
              </div>
              <div id="monitorFormSection" style="display:none; margin-top:16px;">
                <div>
                  <label class="small muted">Base URL / URI</label>
                  <input id="dsMonitorUriInput" placeholder="例如 https://grafana.example.com 或 https://skywalking.example.com" />
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">用户名</label>
                    <input id="dsMonitorUsernameInput" placeholder="Monitor / SkyWalking 用户名（可选）" />
                  </div>
                  <div>
                    <label class="small muted">密码 / Secret</label>
                    <input id="dsMonitorSecretInput" placeholder="Monitor / SkyWalking 密码或 Secret（可选）" />
                  </div>
                </div>
              </div>
              <div id="wmsAgentFormSection" style="display:none; margin-top:16px;">
                <div>
                  <label class="small muted">Base URL / URI</label>
                  <input id="dsWmsAgentUriInput" placeholder="例如 http://127.0.0.1:19610" />
                </div>
                <div class="grid2" style="margin-top:12px;">
                  <div>
                    <label class="small muted">账号 / Email</label>
                    <input id="dsWmsAgentUsernameInput" placeholder="远端 WMS Agent 登录邮箱或账号" />
                  </div>
                  <div>
                    <label class="small muted">密码</label>
                    <input id="dsWmsAgentSecretInput" placeholder="远端 WMS Agent 登录密码" />
                  </div>
                </div>
              </div>
              <div style="margin-top:16px;">
                <label class="small muted">角色说明</label>
                <input id="dsRoleInput" placeholder="例如 primary relational store for inventory reconciliation" />
              </div>
              <div style="margin-top:12px;">
                <label class="small muted">补充说明</label>
                <textarea id="dsUsageNotesInput" style="min-height:120px;" placeholder="说明这个数据源的用途、环境和注意事项"></textarea>
              </div>
              <div class="actions" style="margin-top:12px;">
                <button id="saveDatasourceConfigBtn">保存数据源配置</button>
              </div>
            </div>
            <div class="card" id="editorCard" style="display:none;">
              <div class="section-title">文件编辑</div>
              <textarea id="editor"></textarea>
              <div class="actions" style="margin-top:12px;">
                <button id="saveFileBtn">保存当前文件</button>
              </div>
            </div>
          </div>
        </div>
      </section>
      <div class="resizer" id="rightResizer" aria-label="调整右侧栏宽度" role="separator"></div>
      <aside class="pane" id="rightPane">
        <div class="sticky-rail">
          <div class="right-pane-shell">
            <div class="card" id="fieldGuideCard">
              <div class="section-title">字段说明</div>
              <div id="fieldGuide" class="field-list"></div>
            </div>
            <div class="card" id="previewCard">
              <div class="section-title">当前文件预览</div>
              <div class="preview-shell">
                <div class="tabs">
                  <button id="previewVisualTab" class="tab-button active">可视化预览</button>
                  <button id="previewSourceTab" class="tab-button">源码</button>
                </div>
                <div id="previewRender" class="preview-render"><div class="empty">选择文件后可在这里看到预览。</div></div>
                <pre id="previewSource" class="empty" style="display:none;">选择文件后可在这里看到源码。</pre>
              </div>
            </div>
          </div>          
        </div>
      </aside>
    </main>
    <script src="/assets/marked.js"></script>
    <script src="/assets/mermaid.js"></script>
    <script>
      const state = {
        tree: null,
        store: null,
        toolModules: null,
        menu: "workspace",
        toolModulesTab: "loaded",
        selectedRelPath: "",
        selectedKind: "workspace",
        currentFileContent: "",
        previewMode: "rendered",
        expandedTree: { "": true, "projects": true, "datasources": true },
      };

      function clamp(value, min, max) {
        return Math.max(min, Math.min(max, value));
      }

      function getCssPxVar(name, fallback) {
        const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
        const matched = /^([0-9.]+)px$/.exec(raw);
        return matched ? Number(matched[1]) : fallback;
      }

      function applyPaneWidths(leftWidth, rightWidth) {
        document.documentElement.style.setProperty("--left-pane-width", Math.round(leftWidth) + "px");
        document.documentElement.style.setProperty("--right-pane-width", Math.round(rightWidth) + "px");
        try {
          localStorage.setItem("wms-ai-agent:left-pane-width", String(Math.round(leftWidth)));
          localStorage.setItem("wms-ai-agent:right-pane-width", String(Math.round(rightWidth)));
        } catch (error) {
        }
      }

      function loadPaneWidths() {
        try {
          const leftRaw = localStorage.getItem("wms-ai-agent:left-pane-width");
          const rightRaw = localStorage.getItem("wms-ai-agent:right-pane-width");
          const leftWidth = leftRaw ? Number(leftRaw) : NaN;
          const rightWidth = rightRaw ? Number(rightRaw) : NaN;
          const nextLeft = Number.isFinite(leftWidth) ? leftWidth : getCssPxVar("--left-pane-width", 340);
          const nextRight = Number.isFinite(rightWidth) ? rightWidth : getCssPxVar("--right-pane-width", 380);
          applyPaneWidths(nextLeft, nextRight);
        } catch (error) {
        }
      }

      function syncLayoutHeight() {
        const layout = document.querySelector(".layout");
        const header = document.querySelector("header");
        if (!layout || !header) return;

        if (window.innerWidth <= 920) {
          document.documentElement.style.setProperty("--layout-height", "auto");
          return;
        }

        const bodyStyles = getComputedStyle(document.body);
        const paddingTop = parseFloat(bodyStyles.paddingTop || "0") || 0;
        const paddingBottom = parseFloat(bodyStyles.paddingBottom || "0") || 0;
        const gap = parseFloat(bodyStyles.rowGap || bodyStyles.gap || "0") || 0;
        const headerHeight = Math.ceil(header.getBoundingClientRect().height);
        const available = Math.max(320, window.innerHeight - paddingTop - paddingBottom - gap - headerHeight);
        document.documentElement.style.setProperty("--layout-height", available + "px");
      }

      function initPaneResizers() {
        const layout = document.querySelector(".layout");
        const leftResizer = document.getElementById("leftResizer");
        const rightResizer = document.getElementById("rightResizer");
        if (!layout || !leftResizer || !rightResizer) return;

        function startDrag(side, event) {
          event.preventDefault();
          const minLeft = getCssPxVar("--min-left-width", 260);
          const minCenter = getCssPxVar("--min-center-width", 460);
          const minRight = getCssPxVar("--min-right-width", 300);
          const divider = getCssPxVar("--resizer-width", 10);
          const activeResizer = side === "left" ? leftResizer : rightResizer;
          activeResizer.classList.add("dragging");
          document.body.style.cursor = "col-resize";

          function onMove(moveEvent) {
            const rect = layout.getBoundingClientRect();
            const currentLeft = getCssPxVar("--left-pane-width", 340);
            const currentRight = getCssPxVar("--right-pane-width", 380);

            if (side === "left") {
              const maxLeft = rect.width - currentRight - minCenter - divider * 2;
              const nextLeft = clamp(moveEvent.clientX - rect.left, minLeft, maxLeft);
              applyPaneWidths(nextLeft, currentRight);
            } else {
              const maxRight = rect.width - currentLeft - minCenter - divider * 2;
              const nextRight = clamp(rect.right - moveEvent.clientX, minRight, maxRight);
              applyPaneWidths(currentLeft, nextRight);
            }
          }

          function onUp() {
            activeResizer.classList.remove("dragging");
            document.body.style.cursor = "";
            window.removeEventListener("pointermove", onMove);
            window.removeEventListener("pointerup", onUp);
          }

          window.addEventListener("pointermove", onMove);
          window.addEventListener("pointerup", onUp, { once: true });
        }

        leftResizer.addEventListener("pointerdown", (event) => startDrag("left", event));
        rightResizer.addEventListener("pointerdown", (event) => startDrag("right", event));
      }

      function getSelectedDatasourceId() {
        if (state.selectedKind === "datasource") {
          return state.selectedRelPath.split("/").pop() || "";
        }
        return "";
      }

      function getSelectedDatasource() {
        const datasourceId = getSelectedDatasourceId();
        if (!datasourceId || !state.store?.datasources) {
          return null;
        }
        return state.store.datasources.find((item) => item.id === datasourceId) || null;
      }

      function setStatus(message, type = "") {
        const root = document.getElementById("status");
        root.innerHTML = message ? '<div class="status ' + type + '">' + message + "</div>" : "";
      }

      async function api(path, options = {}) {
        const response = await fetch(path, {
          headers: { "content-type": "application/json" },
          ...options,
        });
        const data = await response.json();
        if (!response.ok || !data.ok) {
          throw new Error(data.error || "请求失败");
        }
        return data;
      }

      function isCaseNode(node) {
        return typeof node.relPath === "string" && node.relPath.includes("/memory/cases/");
      }

      function getDisplayName(node) {
        const rawName = String(node.name || "workspace");
        if (isCaseNode(node) && node.kind === "file") {
          const withoutExt = rawName.replace(/\.md$/i, "");
          const stripped = withoutExt.replace(/^\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-\d{3}Z-/, "");
          return stripped.replace(/-/g, " ");
        }
        return rawName;
      }

      function isExpanded(node) {
        const rel = node.relPath || "";
        if (Object.prototype.hasOwnProperty.call(state.expandedTree, rel)) {
          return !!state.expandedTree[rel];
        }
        if (!rel || rel === "projects" || rel === "datasources") return true;
        if (rel.endsWith("/memory") || rel.endsWith("/memory/cases")) return false;
        if (rel.endsWith("/knowledge-base")) return false;
        return true;
      }

      function renderTreeNode(node, depth = 0) {
        const isFolder = node.children && node.children.length > 0;
        const displayName = getDisplayName(node);
        const title = escapeAttr(String(node.name || "workspace"));
        const caseClass = isCaseNode(node) ? " tree-case" : "";
        const expanded = isFolder ? isExpanded(node) : false;
        const toggle = isFolder
          ? '<span class="node-toggle" data-toggle="' + escapeAttr(node.relPath) + '">' + (expanded ? "▾" : "▸") + "</span>"
          : '<span class="node-spacer"></span>';
        const label =
          '<div class="node-row' + caseClass + '">' +
          toggle +
          '<span class="node-label">' +
          '<span class="node-name" title="' + title + '">' + escapeHtml(displayName) + '</span>' +
          '<span class="badge">' + escapeHtml(node.kind) + "</span>" +
          "</span></div>";
        if (isFolder) {
          return '<div class="tree-group' + (expanded ? "" : " collapsed") + '" data-group="' + escapeAttr(node.relPath) + '"><button class="node folder" data-rel="' + escapeAttr(node.relPath) + '" data-kind="' + escapeAttr(node.kind) + '">' + label + '</button><div class="tree-children">' + node.children.map((child) => renderTreeNode(child, depth + 1)).join("") + "</div></div>";
        }
        return '<button class="node" data-rel="' + escapeAttr(node.relPath) + '" data-kind="' + escapeAttr(node.kind) + '">' + label + "</button>";
      }

      function findNode(node, relPath) {
        if ((node.relPath || "") === (relPath || "")) {
          return node;
        }
        for (const child of node.children || []) {
          const found = findNode(child, relPath);
          if (found) return found;
        }
        return null;
      }

      function escapeHtml(value) {
        return String(value || "")
          .replaceAll("&", "&amp;")
          .replaceAll("<", "&lt;")
          .replaceAll(">", "&gt;")
          .replaceAll('"', "&quot;");
      }

      function escapeAttr(value) {
        return escapeHtml(value).replaceAll("'", "&#39;");
      }

      function renderHowToFill(guide) {
        const root = document.getElementById("howToFill");
        root.innerHTML = guide.howToFill.length
          ? guide.howToFill.map((item, index) => '<div class="field-item"><strong>' + (index + 1) + '.</strong> ' + escapeHtml(item) + "</div>").join("")
          : '<div class="empty">当前节点没有额外说明。</div>';
      }

      function renderNodeActions() {
        const card = document.getElementById("nodeActionsCard");
        const renameProjectBtn = document.getElementById("renameProjectBtn");
        const deleteProjectBtn = document.getElementById("deleteProjectBtn");
        const setActiveProjectBtn = document.getElementById("setActiveProjectBtn");
        const renameDatasourceBtn = document.getElementById("renameDatasourceBtn");
        const deleteDatasourceBtn = document.getElementById("deleteDatasourceBtn");
        const testDatasourceBtn = document.getElementById("testDatasourceBtn");

        const isProject = state.selectedKind === "project";
        const isDatasource = state.selectedKind === "datasource";

        card.style.display = isProject || isDatasource ? "block" : "none";
        renameProjectBtn.style.display = isProject ? "inline-flex" : "none";
        deleteProjectBtn.style.display = isProject ? "inline-flex" : "none";
        setActiveProjectBtn.style.display = isProject ? "inline-flex" : "none";
        renameDatasourceBtn.style.display = isDatasource ? "inline-flex" : "none";
        deleteDatasourceBtn.style.display = isDatasource ? "inline-flex" : "none";
        testDatasourceBtn.style.display = isDatasource ? "inline-flex" : "none";
      }

      function renderRemotePackageOptions(packages, selectedSourceId) {
        const filtered = packages.filter((item) => !selectedSourceId || item.sourceId === selectedSourceId);
        const packageSelect = document.getElementById("remotePackageSelect");
        packageSelect.innerHTML = filtered.length
          ? filtered.map((item) => '<option value="' + escapeAttr(item.id || "") + '">' + escapeHtml((item.label || item.id || "") + " (" + (item.version || "") + ")") + '</option>').join("")
          : '<option value="">当前远程源没有可安装包</option>';
      }

      function renderToolModulesTabs() {
        const tabMap = {
          loaded: {
            button: document.getElementById("toolModulesLoadedTab"),
            section: document.getElementById("toolModulesLoadedSection"),
          },
          localStore: {
            button: document.getElementById("toolModulesLocalStoreTab"),
            section: document.getElementById("toolModulesLocalStoreSection"),
          },
          remoteSources: {
            button: document.getElementById("toolModulesRemoteSourcesTab"),
            section: document.getElementById("toolModulesRemoteSourcesSection"),
          },
          remoteStore: {
            button: document.getElementById("toolModulesRemoteStoreTab"),
            section: document.getElementById("toolModulesRemoteStoreSection"),
          },
        };
        Object.entries(tabMap).forEach(([key, entry]) => {
          const active = state.toolModulesTab === key;
          entry.button.classList.toggle("active", active);
          entry.section.classList.toggle("active", active);
        });
      }

      function renderToolModulesCard() {
        const card = document.getElementById("toolModulesCard");
        const isWorkspace = state.menu === "modules";
        card.style.display = isWorkspace ? "block" : "none";
        if (!isWorkspace) return;
        renderToolModulesTabs();

        const payload = state.toolModules || {
          modules: [],
          packages: [],
          watch: null,
          failures: [],
          remoteSources: [],
          remotePackages: [],
        };
        const modules = Array.isArray(payload.modules) ? payload.modules : [];
        const packages = Array.isArray(payload.packages) ? payload.packages : [];
        const remoteSources = Array.isArray(payload.remoteSources) ? payload.remoteSources : [];
        const remotePackages = Array.isArray(payload.remotePackages) ? payload.remotePackages : [];
        const failures = Array.isArray(payload.failures) ? payload.failures : [];

        document.getElementById("loadedModulesSummary").textContent =
          "模块 " + modules.length + " 个；本地包 " + packages.length + " 个" +
          "；远程源 " + remoteSources.length + " 个" +
          (failures.length ? "；失败 " + failures.length + " 个" : "");

        document.getElementById("loadedModulesList").innerHTML = modules.length
          ? modules.map((item) => {
              const toolNames = Array.isArray(item.toolNames) ? item.toolNames.slice(0, 8).join(", ") : "";
              const actions = item.source === "external"
                ? '<div class="actions" style="margin-top:10px;">' +
                    '<button class="secondary" data-module-toggle="' + escapeAttr(item.id || "") + '" data-module-enabled="' + escapeAttr(String(Boolean(item.enabled))) + '">' + escapeHtml(item.enabled ? "禁用模块" : "启用模块") + '</button>' +
                    '<button class="danger" data-module-uninstall="' + escapeAttr(item.id || "") + '">卸载模块</button>' +
                  '</div>'
                : "";
              return '<div class="field-item">' +
                '<div><strong>' + escapeHtml(item.label || item.id || "module") + '</strong> <span class="badge">' + escapeHtml(item.source || "module") + '</span></div>' +
                '<div class="meta-line">ID：<code>' + escapeHtml(item.id || "") + '</code></div>' +
                '<div class="meta-line">版本：' + escapeHtml(item.version || "") + '，工具数：' + escapeHtml(String(item.toolCount || 0)) + '</div>' +
                '<div class="meta-line">状态：' + escapeHtml(item.enabled ? "启用" : "停用") + '</div>' +
                '<div class="meta-line">标签：' + escapeHtml(Array.isArray(item.tags) ? item.tags.join(", ") : "") + '</div>' +
                '<div class="meta-line">工具：' + escapeHtml(toolNames || "无") + '</div>' +
                actions +
                '</div>';
            }).join("")
          : '<div class="empty">当前没有已加载的模块。</div>';

        const packageSelect = document.getElementById("toolStorePackageSelect");
        const selectable = packages.filter((item) => !item.installed);
        packageSelect.innerHTML = selectable.length
          ? selectable.map((item) => '<option value="' + escapeAttr(item.id || "") + '">' + escapeHtml((item.label || item.id || "") + " (" + (item.id || "") + ")") + '</option>').join("")
          : '<option value="">当前没有可安装的新包</option>';

        document.getElementById("toolStorePackagesList").innerHTML = packages.length
          ? packages.map((item) =>
              '<div class="field-item">' +
              '<div><strong>' + escapeHtml(item.label || item.id || "package") + '</strong> ' +
              '<span class="badge ' + (item.installed ? "green" : "") + '">' + escapeHtml(item.installed ? "已安装" : "未安装") + '</span></div>' +
              '<div class="meta-line">ID：<code>' + escapeHtml(item.id || "") + '</code></div>' +
              '<div class="meta-line">版本：' + escapeHtml(item.version || "") + '</div>' +
              '<div class="meta-line">说明：' + escapeHtml(item.description || "") + '</div>' +
              '</div>'
            ).join("")
          : '<div class="empty">当前没有可用的本地 tool store 包。</div>';

        const remoteSourceSelect = document.getElementById("remoteSourceSelect");
        remoteSourceSelect.innerHTML = remoteSources.length
          ? remoteSources.map((item) => '<option value="' + escapeAttr(item.id || "") + '">' + escapeHtml((item.label || item.id || "") + " (" + (item.id || "") + ")") + '</option>').join("")
          : '<option value="">当前没有远程源</option>';
        if (remoteSources.length > 0 && !remoteSourceSelect.value) {
          remoteSourceSelect.value = remoteSources[0].id;
        }
        renderRemotePackageOptions(remotePackages, remoteSourceSelect.value);

        document.getElementById("remoteSourcesList").innerHTML = remoteSources.length
          ? remoteSources.map((item) =>
              '<button class="node field-item compact" data-remote-source="' + escapeAttr(item.id || "") + '">' +
              '<div><strong>' + escapeHtml(item.label || item.id || "source") + '</strong> ' +
              '<span class="badge ' + (item.enabled ? "green" : "") + '">' + escapeHtml(item.enabled ? "启用" : "停用") + '</span></div>' +
              '<div class="meta-line"><code>' + escapeHtml(item.id || "") + '</code></div>' +
              '<div class="meta-line">' + escapeHtml(item.url || "") + '</div>' +
              (item.description ? '<div class="meta-line">' + escapeHtml(item.description) + '</div>' : '') +
              '</button>'
            ).join("")
          : '<div class="empty">当前没有配置远程源。</div>';

        document.getElementById("remotePackagesList").innerHTML = remotePackages.length
          ? remotePackages
              .filter((item) => !remoteSourceSelect.value || item.sourceId === remoteSourceSelect.value)
              .map((item) =>
                '<div class="field-item">' +
                '<div><strong>' + escapeHtml(item.label || item.id || "package") + '</strong> ' +
                '<span class="badge">' + escapeHtml(item.sourceLabel || item.sourceId || "") + '</span> ' +
                '<span class="badge ' + (item.installed ? "green" : "") + '">' + escapeHtml(item.installed ? "已安装" : "未安装") + '</span></div>' +
                '<div class="meta-line"><code>' + escapeHtml(item.id || "") + '</code> · 版本 ' + escapeHtml(item.version || "") + '</div>' +
                '<div class="meta-line">' + escapeHtml(item.description || "") + '</div>' +
                '<div class="meta-line">manifest：' + escapeHtml(item.manifestUrl || "") + '</div>' +
                '</div>'
              ).join("")
          : '<div class="empty">当前没有远程包。</div>';

        document.querySelectorAll("[data-remote-source]").forEach((element) => {
          element.addEventListener("click", () => {
            const sourceId = element.getAttribute("data-remote-source") || "";
            const source = remoteSources.find((item) => item.id === sourceId);
            if (!source) return;
            document.getElementById("remoteSourceIdInput").value = source.id || "";
            document.getElementById("remoteSourceLabelInput").value = source.label || "";
            document.getElementById("remoteSourceUrlInput").value = source.url || "";
            document.getElementById("remoteSourceDescriptionInput").value = source.description || "";
            remoteSourceSelect.value = source.id || "";
            renderRemotePackageOptions(remotePackages, source.id || "");
          });
        });

        document.querySelectorAll("[data-module-toggle]").forEach((element) => {
          element.addEventListener("click", async () => {
            const moduleId = element.getAttribute("data-module-toggle") || "";
            const enabled = element.getAttribute("data-module-enabled") === "true";
            try {
              setStatus("正在更新模块状态...", "");
              await api("/api/tool-modules/set-enabled", {
                method: "POST",
                body: JSON.stringify({ moduleId, enabled: !enabled }),
              });
              await loadToolModules();
              setStatus("模块状态已更新。", "success");
            } catch (error) {
              setStatus((error && error.message) || String(error), "error");
            }
          });
        });

        document.querySelectorAll("[data-module-uninstall]").forEach((element) => {
          element.addEventListener("click", async () => {
            const moduleId = element.getAttribute("data-module-uninstall") || "";
            if (!window.confirm("确认卸载这个外部模块吗？")) return;
            try {
              setStatus("正在卸载模块...", "");
              await api("/api/tool-modules/uninstall", {
                method: "POST",
                body: JSON.stringify({ moduleId }),
              });
              await loadToolModules();
              setStatus("模块已卸载。", "success");
            } catch (error) {
              setStatus((error && error.message) || String(error), "error");
            }
          });
        });
      }

      function renderToolModulesSidebar() {
        const card = document.getElementById("toolModulesSidebarCard");
        const isModules = state.menu === "modules";
        card.style.display = isModules ? "block" : "none";
        if (!isModules) return;

        const payload = state.toolModules || { modules: [], packages: [], watch: null, failures: [] };
        const modules = Array.isArray(payload.modules) ? payload.modules : [];
        const watch = payload.watch || null;

        document.getElementById("toolModulesSidebarSummary").textContent =
          "当前已加载模块 " + modules.length + " 个" +
          (watch && watch.active ? "，自动监听已开启" : "，自动监听未开启");

        document.getElementById("toolModulesSidebarList").innerHTML = modules.length
          ? modules.map((item) =>
              '<div class="field-item">' +
              '<div><strong>' + escapeHtml(item.label || item.id || "module") + '</strong></div>' +
              '<div class="meta-line"><code>' + escapeHtml(item.id || "") + '</code></div>' +
              '<div class="meta-line">来源：' + escapeHtml(item.source || "") + '</div>' +
              '<div class="meta-line">工具数：' + escapeHtml(String(item.toolCount || 0)) + '</div>' +
              '</div>'
            ).join("")
          : '<div class="empty">当前没有已加载的模块。</div>';
      }

      function renderMenuState() {
        const workspaceBtn = document.getElementById("menuWorkspaceBtn");
        const modulesBtn = document.getElementById("menuToolModulesBtn");
        workspaceBtn.classList.toggle("active", state.menu === "workspace");
        modulesBtn.classList.toggle("active", state.menu === "modules");

        const layout = document.querySelector(".layout");
        const leftPane = document.getElementById("leftPane");
        const rightPane = document.getElementById("rightPane");
        const rightResizer = document.getElementById("rightResizer");
        const treeCard = document.getElementById("treeCard");
        const toolModulesSidebarCard = document.getElementById("toolModulesSidebarCard");
        const nodeActionsCard = document.getElementById("nodeActionsCard");
        const datasourceFormCard = document.getElementById("datasourceFormCard");
        const editorCard = document.getElementById("editorCard");
        const howToFillCard = document.getElementById("howToFillCard");
        const fieldGuideCard = document.getElementById("fieldGuideCard");
        const previewCard = document.getElementById("previewCard");

        if (state.menu === "modules") {
          layout.classList.add("modules-mode");
          leftPane.style.display = "none";
          document.getElementById("leftResizer").style.display = "none";
          rightPane.style.display = "none";
          rightResizer.style.display = "none";
          treeCard.style.display = "none";
          toolModulesSidebarCard.style.display = "none";
          nodeActionsCard.style.display = "none";
          datasourceFormCard.style.display = "none";
          editorCard.style.display = "none";
          howToFillCard.style.display = "none";
          fieldGuideCard.style.display = "block";
          previewCard.style.display = "none";
          document.getElementById("nodeTitle").textContent = "工具模块";
          document.getElementById("nodeMeta").innerHTML = '<div class="meta-line">这里管理 MCP 工具模块、本地 tool store 和模块重载。</div>';
          document.getElementById("nodeSummary").textContent = "工具模块与工作区文件配置分开管理，避免干扰日常项目和数据源编辑。";
          document.getElementById("fieldGuide").innerHTML =
            '<div class="field-item"><div><code>tool_module_catalog</code></div><div class="meta-line">查看当前已加载模块、来源、版本和包含的工具。</div></div>' +
            '<div class="field-item"><div><code>tool_store_catalog</code></div><div class="meta-line">查看仓库内置的本地 tool store 包。</div></div>' +
            '<div class="field-item"><div><code>tool_store_install_local</code></div><div class="meta-line">把本地包安装到当前 workspace 的 tool-modules 目录。</div></div>' +
            '<div class="field-item"><div><code>tool_store_remote_source_add</code></div><div class="meta-line">配置远程源，远程源配置不会进入 workspace。</div></div>' +
            '<div class="field-item"><div><code>tool_store_install_remote</code></div><div class="meta-line">把远程包安装到当前 workspace 的 tool-modules 目录。</div></div>';
          renderToolModulesCard();
          return;
        }

        layout.classList.remove("modules-mode");
        leftPane.style.display = "";
        document.getElementById("leftResizer").style.display = "";
        rightPane.style.display = "";
        rightResizer.style.display = "";
        treeCard.style.display = "";
        toolModulesSidebarCard.style.display = "none";
        howToFillCard.style.display = "";
        fieldGuideCard.style.display = "block";
        previewCard.style.display = "block";
        renderNodeActions();
        renderDatasourceForm();
        renderToolModulesCard();
        renderToolModulesSidebar();
        syncLayoutHeight();
      }

      function renderFieldGuide(guide) {
        const root = document.getElementById("fieldGuide");
        root.innerHTML = guide.fields.length
          ? guide.fields.map((field) => '<div class="field-item"><div><code>' + escapeHtml(field.key) + '</code> ' + (field.required ? '<span class="badge yellow">必填</span>' : '<span class="badge">可选</span>') + '</div><div class="meta-line">' + escapeHtml(field.description) + '</div>' + (field.example ? '<div class="meta-line">示例：<code>' + escapeHtml(field.example) + '</code></div>' : '') + "</div>").join("")
          : '<div class="empty">当前节点没有字段级说明。</div>';
      }

      function setPreviewMode(mode) {
        state.previewMode = mode === "source" ? "source" : "rendered";
        document.getElementById("previewVisualTab").classList.toggle("active", state.previewMode === "rendered");
        document.getElementById("previewSourceTab").classList.toggle("active", state.previewMode === "source");
        document.getElementById("previewRender").style.display = state.previewMode === "rendered" ? "block" : "none";
        document.getElementById("previewSource").style.display = state.previewMode === "source" ? "block" : "none";
      }

      function looksLikeMermaid(content) {
        const text = String(content || "").trim();
        return /^(flowchart|graph|sequenceDiagram|classDiagram|stateDiagram|erDiagram|journey|gantt|pie|mindmap|timeline|requirementDiagram|gitGraph|C4Context|C4Container|C4Component|C4Dynamic|C4Deployment)\b/m.test(text);
      }

      function isMarkdownFile(relPath) {
        return /\.md$/i.test(relPath || "");
      }

      function isJsonFile(relPath) {
        return /\.json$/i.test(relPath || "");
      }

      async function renderMermaidBlocks(root) {
        if (!window.mermaid) return;
        if (!window.__wmsMermaidInitialized) {
          window.mermaid.initialize({
            startOnLoad: false,
            securityLevel: "loose",
            theme: "dark",
          });
          window.__wmsMermaidInitialized = true;
        }
        const nodes = Array.from(root.querySelectorAll(".mermaid"));
        if (!nodes.length) return;
        nodes.forEach((node) => node.removeAttribute("data-processed"));
        await window.mermaid.run({ nodes });
      }

      function normalizeMermaidCodeBlocks(root) {
        const blocks = root.querySelectorAll("pre code.language-mermaid, pre code.language-flowchart");
        blocks.forEach((codeBlock) => {
          const pre = codeBlock.parentElement;
          if (!pre || !pre.parentElement) return;
          const host = document.createElement("div");
          host.className = "mermaid-host";
          const mermaidNode = document.createElement("div");
          mermaidNode.className = "mermaid";
          mermaidNode.textContent = codeBlock.textContent || "";
          host.appendChild(mermaidNode);
          pre.parentElement.replaceChild(host, pre);
        });
      }

      async function renderPreviewContent(content, relPath, isFile) {
        const previewRender = document.getElementById("previewRender");
        const previewSource = document.getElementById("previewSource");
        const raw = String(content || "");
        previewSource.textContent = raw || "当前没有源码内容。";

        if (!isFile) {
          previewRender.innerHTML = '<div class="empty">' + escapeHtml(raw || "当前节点没有可预览文件内容。") + "</div>";
          return;
        }

        if (isMarkdownFile(relPath)) {
          const markedApi = window.marked;
          const html = markedApi && typeof markedApi.parse === "function"
            ? markedApi.parse(raw, { gfm: true, breaks: true })
            : "<pre><code>" + escapeHtml(raw) + "</code></pre>";
          previewRender.innerHTML = html;
          normalizeMermaidCodeBlocks(previewRender);
          await renderMermaidBlocks(previewRender);
          return;
        }

        if (looksLikeMermaid(raw)) {
          previewRender.innerHTML = '<div class="mermaid-host"><div class="mermaid">' + escapeHtml(raw) + "</div></div>";
          await renderMermaidBlocks(previewRender);
          return;
        }

        if (isJsonFile(relPath)) {
          try {
            const pretty = JSON.stringify(JSON.parse(raw), null, 2);
            previewRender.innerHTML = "<pre><code>" + escapeHtml(pretty) + "</code></pre>";
            return;
          } catch (error) {
          }
        }

        previewRender.innerHTML = "<pre><code>" + escapeHtml(raw || "当前没有文件内容。") + "</code></pre>";
      }

      function renderDatasourceForm() {
        const datasource = getSelectedDatasource();
        const card = document.getElementById("datasourceFormCard");
        if (!datasource) {
          card.style.display = "none";
          return;
        }

        card.style.display = "block";
        const type = datasource.type || "";
        document.getElementById("dsLabelInput").value = datasource.label || "";
        document.getElementById("dsTypeInput").value = type;
        document.getElementById("dsEnabledInput").value = String(datasource.enabled !== false);
        const projectSelect = document.getElementById("dsProjectIdsInput");
        const projects = Array.isArray(state.store?.projects) ? state.store.projects : [];
        projectSelect.innerHTML = "";
        for (const project of projects) {
          const option = document.createElement("option");
          option.value = project.id;
          option.textContent = project.label && project.label !== project.id
            ? project.label + " (" + project.id + ")"
            : project.id;
          option.selected = Array.isArray(datasource.projectIds) && datasource.projectIds.includes(project.id);
          projectSelect.appendChild(option);
        }
        document.getElementById("dsProjectIdsHint").textContent = projects.length > 0
          ? "自动加载工作区项目；按住 Command/Ctrl 可多选。"
          : "当前没有可绑定的项目。";
        document.getElementById("dsRoleInput").value = datasource.role || "";
        document.getElementById("dsUsageNotesInput").value = datasource.usageNotes || "";

        const sqlSection = document.getElementById("sqlFormSection");
        const mongoSection = document.getElementById("mongoFormSection");
        const kafkaSection = document.getElementById("kafkaFormSection");
        const logcenterSection = document.getElementById("logcenterFormSection");
        const monitorSection = document.getElementById("monitorFormSection");
        const wmsAgentSection = document.getElementById("wmsAgentFormSection");
        sqlSection.style.display = (type === "mysql" || type === "postgres") ? "block" : "none";
        mongoSection.style.display = type === "mongo" ? "block" : "none";
        kafkaSection.style.display = type === "kafka" ? "block" : "none";
        logcenterSection.style.display = type === "logcenter" ? "block" : "none";
        monitorSection.style.display = (type === "monitor" || type === "skywalking") ? "block" : "none";
        wmsAgentSection.style.display = type === "wms_agent" ? "block" : "none";

        const hint =
          type === "mongo"
            ? "Mongo 按简化模板配置：名称 + URI。只要 URI 完整，通常不用再单独填用户名密码。"
            : type === "mysql" || type === "postgres"
              ? "SQL 按固定模板配置：名称、主机、端口、用户名、密码。Database 可选填。"
              : type === "kafka"
                ? "Kafka 继续使用 brokers/clientId/认证信息模板。"
                : type === "logcenter"
                  ? "Logcenter 按远程日志模板配置：URI、认证模式、登录路径、Data View、用户名和密码。"
                  : type === "monitor"
                    ? "Monitor 按 Grafana 只读查询模板配置：Base URL、用户名、密码。"
                    : type === "skywalking"
                      ? "SkyWalking 按 GraphQL 只读查询模板配置：Base URL、用户名、密码。"
                      : "WMS Agent 远端适配按登录态模板配置：Base URL、账号/邮箱、密码。";
        document.getElementById("datasourceTemplateHint").textContent = hint;

        document.getElementById("dsSqlHostInput").value = datasource.connection?.host || "";
        document.getElementById("dsSqlPortInput").value =
          datasource.connection?.port === undefined || datasource.connection?.port === null
            ? ""
            : String(datasource.connection.port);
        document.getElementById("dsSqlUsernameInput").value = datasource.auth?.username || "";
        document.getElementById("dsSqlSecretInput").value = datasource.auth?.secret || "";
        document.getElementById("dsSqlDatabaseInput").value = datasource.connection?.database || "";

        document.getElementById("dsMongoUriInput").value = datasource.connection?.uri || "";

        document.getElementById("dsKafkaBrokersInput").value = (datasource.connection?.brokers || []).join(",");
        document.getElementById("dsKafkaClientIdInput").value = datasource.connection?.clientId || "";
        document.getElementById("dsKafkaSslInput").value = String(!!datasource.connection?.ssl);
        document.getElementById("dsKafkaUsernameInput").value = datasource.auth?.username || "";
        document.getElementById("dsKafkaSecretInput").value = datasource.auth?.secret || "";

        document.getElementById("dsLogcenterUriInput").value = datasource.connection?.uri || "";
        document.getElementById("dsLogcenterAuthModeInput").value = datasource.connection?.authMode || "basic";
        document.getElementById("dsLogcenterLoginPathInput").value = datasource.connection?.loginPath || "/login";
        document.getElementById("dsLogcenterDataViewInput").value = datasource.connection?.dataView || "";
        document.getElementById("dsLogcenterUsernameInput").value = datasource.auth?.username || "";
        document.getElementById("dsLogcenterSecretInput").value = datasource.auth?.secret || "";

        document.getElementById("dsMonitorUriInput").value = datasource.connection?.uri || "";
        document.getElementById("dsMonitorUsernameInput").value = datasource.auth?.username || "";
        document.getElementById("dsMonitorSecretInput").value = datasource.auth?.secret || "";

        document.getElementById("dsWmsAgentUriInput").value = datasource.connection?.uri || "";
        document.getElementById("dsWmsAgentUsernameInput").value = datasource.auth?.username || "";
        document.getElementById("dsWmsAgentSecretInput").value = datasource.auth?.secret || "";
      }

      function getSelectedProjectIds() {
        const select = document.getElementById("dsProjectIdsInput");
        return Array.from(select.selectedOptions || [])
          .map((option) => option.value.trim())
          .filter(Boolean);
      }

      async function loadState(selectRelPath = state.selectedRelPath || "") {
        const data = await api("/api/state");
        state.tree = data.tree;
        state.store = data.store;
        document.getElementById("workspaceRoot").textContent = data.workspaceRoot;
        document.getElementById("tree").innerHTML = renderTreeNode(data.tree);
        document.querySelectorAll("[data-rel]").forEach((element) => {
          element.addEventListener("click", async (event) => {
            if (event.target && event.target.closest("[data-toggle]")) {
              return;
            }
            const relPath = element.getAttribute("data-rel") || "";
            const kind = element.getAttribute("data-kind") || "workspace";
            await selectNode(relPath, kind);
          });
        });
        document.querySelectorAll("[data-toggle]").forEach((element) => {
          element.addEventListener("click", async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const relPath = element.getAttribute("data-toggle") || "";
            state.expandedTree[relPath] = !isExpanded({ relPath });
            document.getElementById("tree").innerHTML = renderTreeNode(state.tree);
            await loadState(state.selectedRelPath);
          });
        });
        if (selectRelPath || selectRelPath === "") {
          const selectedNode = findNode(state.tree, selectRelPath) || state.tree;
          await selectNode(selectedNode.relPath || "", selectedNode.kind || "workspace");
        }
      }

      async function loadToolModules() {
        const data = await api("/api/tool-modules");
        state.toolModules = data;
        renderToolModulesCard();
      }

      async function saveRemoteSource() {
        const sourceId = document.getElementById("remoteSourceIdInput").value.trim();
        const label = document.getElementById("remoteSourceLabelInput").value.trim();
        const url = document.getElementById("remoteSourceUrlInput").value.trim();
        const description = document.getElementById("remoteSourceDescriptionInput").value.trim();
        if (!sourceId || !url) {
          throw new Error("远程源 ID 和 Catalog URL 都必填");
        }
        await api("/api/tool-modules/remote-sources", {
          method: "POST",
          body: JSON.stringify({ sourceId, label, url, description, overwrite: true }),
        });
        await loadToolModules();
        setStatus("远程源已保存。", "success");
      }

      async function removeRemoteSourceByForm() {
        const sourceId = document.getElementById("remoteSourceIdInput").value.trim();
        if (!sourceId) {
          throw new Error("请先选择或填写要删除的远程源 ID");
        }
        if (!window.confirm("确认删除这个远程源吗？这不会卸载已安装模块。")) return;
        await api("/api/tool-modules/remote-sources/remove", {
          method: "POST",
          body: JSON.stringify({ sourceId }),
        });
        document.getElementById("remoteSourceIdInput").value = "";
        document.getElementById("remoteSourceLabelInput").value = "";
        document.getElementById("remoteSourceUrlInput").value = "";
        document.getElementById("remoteSourceDescriptionInput").value = "";
        await loadToolModules();
        setStatus("远程源已删除。", "success");
      }

      async function installRemotePackageFromSelection() {
        const sourceId = document.getElementById("remoteSourceSelect").value;
        const packageId = document.getElementById("remotePackageSelect").value;
        if (!sourceId || !packageId) {
          throw new Error("请先选择远程源和远程包");
        }
        const overwrite = document.getElementById("remoteOverwriteSelect").value === "true";
        setStatus("正在安装远程包...", "");
        const data = await api("/api/tool-modules/install-remote", {
          method: "POST",
          body: JSON.stringify({ sourceId, packageId, overwrite }),
        });
        await loadToolModules();
        setStatus("已安装远程包：" + escapeHtml(data.installed.packageId) + "。", "success");
      }

      async function selectNode(relPath, kind) {
        state.selectedRelPath = relPath || "";
        state.selectedKind = kind || "workspace";
        document.querySelectorAll("[data-rel]").forEach((el) => {
          const match = (el.getAttribute("data-rel") || "") === state.selectedRelPath;
          el.classList.toggle("active", match);
        });
        const node = findNode(state.tree, state.selectedRelPath) || state.tree;
        const guide = getGuideForClient(node);
        document.getElementById("nodeTitle").textContent = guide.title;
        document.getElementById("nodeMeta").innerHTML =
          '<div class="meta-line">路径：<span class="path">' +
          escapeHtml(node.absPath) +
          '</span></div><div class="meta-line">类型：' +
          escapeHtml(node.kind) +
          '</div>';
        document.getElementById("nodeSummary").textContent = guide.summary;
        renderNodeActions();
        renderHowToFill(guide);
        renderFieldGuide(guide);
        renderDatasourceForm();
        renderToolModulesCard();
        if (node.kind === "file") {
          const data = await api("/api/file?relPath=" + encodeURIComponent(node.relPath));
          state.currentFileContent = data.content || "";
          document.getElementById("editor").value = state.currentFileContent;
          await renderPreviewContent(data.content || "", node.relPath, true);
          document.getElementById("editorCard").style.display = "block";
        } else {
          state.currentFileContent = "";
          await renderPreviewContent(node.description || "当前节点没有文件内容。", node.relPath || "", false);
          document.getElementById("editorCard").style.display = "none";
        }
      }

      async function saveDatasourceConfig() {
        const datasource = getSelectedDatasource();
        if (!datasource) {
          throw new Error("请先选中一个数据源");
        }
        const type = datasource.type;
        const payload = {
          datasourceId: datasource.id,
          type,
          label: document.getElementById("dsLabelInput").value.trim(),
          enabled: document.getElementById("dsEnabledInput").value === "true",
          role: document.getElementById("dsRoleInput").value.trim(),
          usageNotes: document.getElementById("dsUsageNotesInput").value,
        };

        if (type === "mongo") {
          payload.uri = document.getElementById("dsMongoUriInput").value.trim();
        } else if (type === "mysql" || type === "postgres") {
          payload.host = document.getElementById("dsSqlHostInput").value.trim();
          payload.port = document.getElementById("dsSqlPortInput").value.trim();
          payload.username = document.getElementById("dsSqlUsernameInput").value.trim();
          payload.secret = document.getElementById("dsSqlSecretInput").value;
          payload.database = document.getElementById("dsSqlDatabaseInput").value.trim();
        } else if (type === "kafka") {
          payload.brokers = document.getElementById("dsKafkaBrokersInput").value.trim();
          payload.clientId = document.getElementById("dsKafkaClientIdInput").value.trim();
          payload.ssl = document.getElementById("dsKafkaSslInput").value === "true";
          payload.username = document.getElementById("dsKafkaUsernameInput").value.trim();
          payload.secret = document.getElementById("dsKafkaSecretInput").value;
        } else if (type === "logcenter") {
          payload.uri = document.getElementById("dsLogcenterUriInput").value.trim();
          payload.authMode = document.getElementById("dsLogcenterAuthModeInput").value;
          payload.loginPath = document.getElementById("dsLogcenterLoginPathInput").value.trim();
          payload.dataView = document.getElementById("dsLogcenterDataViewInput").value.trim();
          payload.username = document.getElementById("dsLogcenterUsernameInput").value.trim();
          payload.secret = document.getElementById("dsLogcenterSecretInput").value;
        } else if (type === "monitor" || type === "skywalking") {
          payload.uri = document.getElementById("dsMonitorUriInput").value.trim();
          payload.username = document.getElementById("dsMonitorUsernameInput").value.trim();
          payload.secret = document.getElementById("dsMonitorSecretInput").value;
        } else if (type === "wms_agent") {
          payload.uri = document.getElementById("dsWmsAgentUriInput").value.trim();
          payload.username = document.getElementById("dsWmsAgentUsernameInput").value.trim();
          payload.secret = document.getElementById("dsWmsAgentSecretInput").value;
        }

        await api("/api/datasource/config", {
          method: "POST",
          body: JSON.stringify(payload),
        });

        const desiredProjectIds = getSelectedProjectIds();
        const currentProjectIds = Array.isArray(datasource.projectIds) ? datasource.projectIds : [];
        const projectIdsToBind = desiredProjectIds.filter((projectId) => !currentProjectIds.includes(projectId));
        const projectIdsToUnbind = currentProjectIds.filter((projectId) => !desiredProjectIds.includes(projectId));

        for (const projectId of projectIdsToBind) {
          await api("/api/project/bind-datasource", {
            method: "POST",
            body: JSON.stringify({ projectId, datasourceId: datasource.id }),
          });
        }

        for (const projectId of projectIdsToUnbind) {
          await api("/api/project/unbind-datasource", {
            method: "POST",
            body: JSON.stringify({ projectId, datasourceId: datasource.id }),
          });
        }

        setStatus("数据源配置已保存。", "success");
        await loadState(state.selectedRelPath);
      }

      function getGuideForClient(node) {
        return node.guide || {
          title: node.name || "workspace",
          summary: node.description || "",
          howToFill: [],
          fields: [],
          nodeActions: [],
        };
      }

      async function saveCurrentFile() {
        if (state.selectedKind !== "file") return;
        const content = document.getElementById("editor").value;
        await api("/api/file", {
          method: "POST",
          body: JSON.stringify({ relPath: state.selectedRelPath, content }),
        });
        setStatus("文件已保存。", "success");
        await renderPreviewContent(content, state.selectedRelPath, true);
      }

      async function createProject() {
        const id = window.prompt("输入新的项目 ID，例如 inventory-sync");
        if (!id) throw new Error("请填写项目 ID 或名称");
        const label = window.prompt("输入项目展示名（可选）", id) || "";
        await api("/api/project/create", {
          method: "POST",
          body: JSON.stringify({ id, label }),
        });
        setStatus("项目骨架已创建。", "success");
        await loadState("projects/" + slugify(id));
      }

      async function createDatasource() {
        const id = window.prompt("输入新的数据源 ID，例如 inventory-postgres");
        if (!id) throw new Error("请填写数据源 ID 或名称");
        const label = window.prompt("输入数据源展示名（可选）", id) || "";
        const type = (window.prompt("输入数据源类型：mongo / mysql / postgres / kafka / logcenter / monitor / skywalking / wms_agent", "mongo") || "").trim().toLowerCase();
        if (!["mongo", "mysql", "postgres", "kafka", "logcenter", "monitor", "skywalking", "wms_agent"].includes(type)) {
          throw new Error("数据源类型必须是 mongo、mysql、postgres、kafka、logcenter、monitor、skywalking、wms_agent 之一");
        }
        await api("/api/datasource/create", {
          method: "POST",
          body: JSON.stringify({ id, label, type }),
        });
        setStatus("数据源骨架已创建。", "success");
        await loadState("datasources/" + slugify(id));
      }

      async function bindSelectedDatasourceToCurrentProject() {
        const projectId = state.selectedKind === "project"
          ? state.selectedRelPath.split("/").pop()
          : (window.prompt("输入要绑定的项目 ID", state.store?.activeProjectId || "") || "").trim();
        const datasourceId =
          state.selectedKind === "datasource"
            ? state.selectedRelPath.split("/").pop()
            : state.selectedKind === "file" && state.selectedRelPath.startsWith("datasources/")
              ? state.selectedRelPath.split("/")[1]
              : (window.prompt("输入要绑定的数据源 ID", "") || "").trim();
        if (!projectId || !datasourceId) throw new Error("请先选择项目和数据源");
        await api("/api/project/bind-datasource", {
          method: "POST",
          body: JSON.stringify({ projectId, datasourceId }),
        });
        setStatus("数据源已绑定到项目。", "success");
        await loadState("projects/" + projectId);
      }

      async function unbindDatasourceFromCurrentProject() {
        const projectId =
          state.selectedKind === "project"
            ? state.selectedRelPath.split("/").pop()
            : state.selectedRelPath.startsWith("projects/") && state.selectedRelPath.endsWith("/datasources.txt")
              ? state.selectedRelPath.split("/")[1]
              : (window.prompt("输入要解绑的项目 ID", state.store?.activeProjectId || "") || "").trim();
        const datasourceId = (window.prompt("输入要解绑的数据源 ID", "") || "").trim();
        if (!projectId || !datasourceId) throw new Error("请先选择项目和数据源");
        if (!window.confirm("确认把这个数据源从当前项目解绑吗？")) return;
        await api("/api/project/unbind-datasource", {
          method: "POST",
          body: JSON.stringify({ projectId, datasourceId }),
        });
        setStatus("数据源已从项目解绑。", "success");
        await loadState("projects/" + projectId);
      }

      async function setSelectedProjectAsActive() {
        const projectId = state.selectedKind === "project"
          ? state.selectedRelPath.split("/").pop()
          : (window.prompt("输入要设为默认的项目 ID", state.store?.activeProjectId || "") || "").trim();
        if (!projectId) throw new Error("请先选择项目");
        await api("/api/workspace/active-project", {
          method: "POST",
          body: JSON.stringify({ projectId }),
        });
        setStatus("默认项目已更新。", "success");
        await loadState(state.selectedRelPath);
      }

      async function deleteCurrentProject() {
        const projectId = state.selectedKind === "project" ? state.selectedRelPath.split("/").pop() : "";
        if (!projectId) throw new Error("请先选中一个项目");
        if (!window.confirm("确认删除当前项目目录及其文件吗？项目里的数据源绑定会一起清理。")) return;
        await api("/api/project/delete", {
          method: "POST",
          body: JSON.stringify({ projectId }),
        });
        setStatus("项目已删除。", "success");
        await loadState("");
      }

      async function renameCurrentProject() {
        const projectId = state.selectedKind === "project" ? state.selectedRelPath.split("/").pop() : "";
        if (!projectId) throw new Error("请先选中一个项目");
        const nextProjectId = window.prompt("输入新的项目 ID（会同时修改目录名）", projectId);
        if (!nextProjectId || nextProjectId.trim() === projectId) return;
        const data = await api("/api/project/rename", {
          method: "POST",
          body: JSON.stringify({ oldProjectId: projectId, newProjectId: nextProjectId }),
        });
        setStatus("项目已重命名。", "success");
        await loadState("projects/" + data.projectId);
      }

      async function deleteCurrentDatasource() {
        const datasourceId =
          state.selectedKind === "datasource"
            ? state.selectedRelPath.split("/").pop()
            : state.selectedKind === "file" && state.selectedRelPath.startsWith("datasources/")
              ? state.selectedRelPath.split("/")[1]
              : "";
        if (!datasourceId) throw new Error("请先选中一个数据源");
        if (!window.confirm("确认删除当前数据源目录及其文件吗？所有项目里的绑定也会一起清理。")) return;
        await api("/api/datasource/delete", {
          method: "POST",
          body: JSON.stringify({ datasourceId }),
        });
        setStatus("数据源已删除。", "success");
        await loadState("");
      }

      async function renameCurrentDatasource() {
        const datasourceId =
          state.selectedKind === "datasource"
            ? state.selectedRelPath.split("/").pop()
            : state.selectedKind === "file" && state.selectedRelPath.startsWith("datasources/")
              ? state.selectedRelPath.split("/")[1]
              : "";
        if (!datasourceId) throw new Error("请先选中一个数据源");
        const nextDatasourceId = window.prompt("输入新的数据源 ID（会同时修改目录名和项目绑定）", datasourceId);
        if (!nextDatasourceId || nextDatasourceId.trim() === datasourceId) return;
        const data = await api("/api/datasource/rename", {
          method: "POST",
          body: JSON.stringify({ oldDatasourceId: datasourceId, newDatasourceId: nextDatasourceId }),
        });
        setStatus("数据源已重命名。", "success");
        await loadState("datasources/" + data.datasourceId);
      }

      async function runDatasourceTestFromSelection() {
        let datasourceId = "";
        if (state.selectedKind === "datasource") {
          datasourceId = state.selectedRelPath.split("/").pop();
        } else if (state.selectedKind === "file" && state.selectedRelPath.startsWith("datasources/")) {
          datasourceId = state.selectedRelPath.split("/")[1];
        } else {
          datasourceId = (window.prompt("输入要测试的数据源 ID", "") || "").trim();
        }
        if (!datasourceId) throw new Error("请先选择数据源");
        setStatus("正在测试数据源连接...", "");
        const data = await api("/api/datasource/test", {
          method: "POST",
          body: JSON.stringify({ datasourceId }),
        });
        setStatus("测试完成：" + escapeHtml(data.result.message) + "（" + data.result.durationMs + "ms）", data.result.ok ? "success" : "error");
      }

      function slugify(value) {
        return String(value || "").trim().toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "") || "item";
      }

      function bindStartupEvents() {
        document.getElementById("refreshBtn").onclick = async () => {
          try { setStatus(""); await loadState(state.selectedRelPath); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("previewVisualTab").onclick = () => setPreviewMode("rendered");
        document.getElementById("previewSourceTab").onclick = () => setPreviewMode("source");
        document.getElementById("newProjectBtn").onclick = async () => {
          try { await createProject(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("newDatasourceBtn").onclick = async () => {
          try { await createDatasource(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("menuWorkspaceBtn").onclick = async () => {
          try {
            state.menu = "workspace";
            renderMenuState();
            await loadState(state.selectedRelPath || "");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("menuToolModulesBtn").onclick = async () => {
          try {
            state.menu = "modules";
            state.toolModulesTab = "loaded";
            await loadToolModules();
            renderMenuState();
            setStatus("");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("renameProjectBtn").onclick = async () => {
          try { await renameCurrentProject(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("deleteProjectBtn").onclick = async () => {
          try { await deleteCurrentProject(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("setActiveProjectBtn").onclick = async () => {
          try { await setSelectedProjectAsActive(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("renameDatasourceBtn").onclick = async () => {
          try { await renameCurrentDatasource(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("deleteDatasourceBtn").onclick = async () => {
          try { await deleteCurrentDatasource(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("testDatasourceBtn").onclick = async () => {
          try { await runDatasourceTestFromSelection(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("saveFileBtn").onclick = async () => {
          try { await saveCurrentFile(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("saveDatasourceConfigBtn").onclick = async () => {
          try { await saveDatasourceConfig(); } catch (error) { setStatus((error && error.message) || String(error), "error"); }
        };
        document.getElementById("refreshToolModulesBtn").onclick = async () => {
          try {
            setStatus("正在刷新工具模块视图...", "");
            await loadToolModules();
            setStatus("工具模块视图已刷新。", "success");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("reloadToolModulesBtn").onclick = async () => {
          try {
            setStatus("正在重载 MCP 工具模块...", "");
            const data = await api("/api/tool-modules/reload", { method: "POST", body: JSON.stringify({}) });
            await loadToolModules();
            setStatus("重载完成：模块 " + data.summary.moduleCount + " 个，失败 " + data.summary.failureCount + " 个。", "success");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("installToolStorePackageBtn").onclick = async () => {
          try {
            const packageId = document.getElementById("toolStorePackageSelect").value;
            if (!packageId) throw new Error("当前没有可安装的本地包");
            const overwrite = document.getElementById("toolStoreOverwriteSelect").value === "true";
            setStatus("正在安装本地 tool store 包...", "");
            const data = await api("/api/tool-modules/install-local", {
              method: "POST",
              body: JSON.stringify({ packageId, overwrite }),
            });
            await loadToolModules();
            setStatus("已安装本地包：" + escapeHtml(data.installed.packageId) + "。", "success");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("saveRemoteSourceBtn").onclick = async () => {
          try {
            await saveRemoteSource();
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("removeRemoteSourceBtn").onclick = async () => {
          try {
            await removeRemoteSourceByForm();
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("refreshRemotePackagesBtn").onclick = async () => {
          try {
            setStatus("正在刷新远程包...", "");
            await loadToolModules();
            setStatus("远程包列表已刷新。", "success");
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("installRemotePackageBtn").onclick = async () => {
          try {
            await installRemotePackageFromSelection();
          } catch (error) {
            setStatus((error && error.message) || String(error), "error");
          }
        };
        document.getElementById("remoteSourceSelect").onchange = () => {
          const payload = state.toolModules || { remotePackages: [] };
          const remotePackages = Array.isArray(payload.remotePackages) ? payload.remotePackages : [];
          renderRemotePackageOptions(remotePackages, document.getElementById("remoteSourceSelect").value);
        };
        document.getElementById("toolModulesLoadedTab").onclick = () => {
          state.toolModulesTab = "loaded";
          renderToolModulesTabs();
        };
        document.getElementById("toolModulesLocalStoreTab").onclick = () => {
          state.toolModulesTab = "localStore";
          renderToolModulesTabs();
        };
        document.getElementById("toolModulesRemoteSourcesTab").onclick = () => {
          state.toolModulesTab = "remoteSources";
          renderToolModulesTabs();
        };
        document.getElementById("toolModulesRemoteStoreTab").onclick = () => {
          state.toolModulesTab = "remoteStore";
          renderToolModulesTabs();
        };
      }

      async function init() {
        loadPaneWidths();
        syncLayoutHeight();
        initPaneResizers();
        bindStartupEvents();
        setPreviewMode("rendered");
        renderMenuState();
        setStatus("正在加载工作区...", "");
        await loadState("");
        syncLayoutHeight();
        setStatus("");
      }

      window.addEventListener("resize", () => {
        syncLayoutHeight();
      });

      window.addEventListener("error", (event) => {
        setStatus("前端脚本错误：" + ((event.error && event.error.message) || event.message || "unknown error"), "error");
      });
      window.addEventListener("unhandledrejection", (event) => {
        const reason = event.reason;
        setStatus("前端异步错误：" + ((reason && reason.message) || String(reason)), "error");
      });

      init().catch((error) => setStatus(((error && error.message) || String(error)), "error"));
    </script>
  </body>
</html>`;
}

async function handleRequest(
  storePath: string,
  request: http.IncomingMessage,
  response: http.ServerResponse,
): Promise<void> {
  const url = new URL(request.url || "/", "http://127.0.0.1");
  const method = request.method || "GET";
  const demoStorePackageDir = path.join(PROJECT_ROOT, "tool-store", "packages", "project-snapshot");

  if (method === "GET" && url.pathname === "/") {
    sendHtml(response, htmlPage());
    return;
  }

  if (method === "GET" && url.pathname === "/assets/marked.js") {
    const filePath = path.join(PROJECT_ROOT, "node_modules", "marked", "lib", "marked.umd.js");
    const body = await fs.readFile(filePath, "utf8");
    sendText(response, 200, "application/javascript; charset=utf-8", body);
    return;
  }

  if (method === "GET" && url.pathname === "/assets/mermaid.js") {
    const filePath = path.join(PROJECT_ROOT, "node_modules", "mermaid", "dist", "mermaid.min.js");
    const body = await fs.readFile(filePath, "utf8");
    sendText(response, 200, "application/javascript; charset=utf-8", body);
    return;
  }

  if (method === "GET" && url.pathname === "/api/tool-store/remote-demo/catalog.json") {
    sendJson(response, 200, {
      ok: true,
      packages: [
        {
          id: "project.snapshot",
          label: "Project Snapshot",
          version: "1.0.0",
          description: "Generate a concise snapshot for one or more projects, including repo/log/datasource counts and labels.",
          tags: ["catalog", "project", "summary"],
          manifestUrl: "http://127.0.0.1:3789/api/tool-store/remote-demo/manifest.json",
        },
      ],
    });
    return;
  }

  if (method === "GET" && url.pathname === "/api/tool-store/remote-demo/manifest.json") {
    const manifestPath = path.join(demoStorePackageDir, "manifest.json");
    const raw = JSON.parse(await fs.readFile(manifestPath, "utf8")) as Record<string, unknown>;
    const payload = {
      ...raw,
      entry: "./module.js",
    };
    sendText(response, 200, "application/json; charset=utf-8", JSON.stringify(payload, null, 2));
    return;
  }

  if (method === "GET" && url.pathname === "/api/tool-store/remote-demo/module.js") {
    const modulePath = path.join(demoStorePackageDir, "module.js");
    const body = await fs.readFile(modulePath, "utf8");
    sendText(response, 200, "application/javascript; charset=utf-8", body);
    return;
  }

  if (method === "GET" && url.pathname === "/api/state") {
    await ensureWorkspaceRoot(storePath);
    const tree = buildWorkspaceTree(storePath);
    const store = readStoreSync(storePath);
    attachGuides(tree);
    sendJson(response, 200, { ok: true, workspaceRoot: storePath, tree, store });
    return;
  }

  if (method === "GET" && url.pathname === "/api/file") {
    const relPath = url.searchParams.get("relPath") || "";
    const file = await readWorkspaceFile(storePath, relPath);
    sendJson(response, 200, { ok: true, ...file });
    return;
  }

  if (method === "POST" && url.pathname === "/api/file") {
    const body = (await readBody(request)) as { relPath?: string; content?: string };
    if (!body.relPath) {
      throw new Error("relPath is required");
    }
    await writeWorkspaceFile(storePath, body.relPath, String(body.content ?? ""));
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "GET" && url.pathname === "/api/tool-modules") {
    const catalog = await runToolLocally(storePath, "tool_module_catalog", { includeDisabled: true });
    const storeCatalog = await runToolLocally(storePath, "tool_store_catalog", {});
    const remoteSourceCatalog = await runToolLocally(storePath, "tool_store_remote_source_catalog", {});
    const remoteCatalog = await runToolLocally(storePath, "tool_store_remote_catalog", {});
    sendJson(response, 200, {
      ok: true,
      modules: Array.isArray((catalog as Record<string, unknown>).modules) ? (catalog as Record<string, unknown>).modules : [],
      failures: Array.isArray((catalog as Record<string, unknown>).failures) ? (catalog as Record<string, unknown>).failures : [],
      watch: (catalog as Record<string, unknown>).watch ?? null,
      packages: Array.isArray((storeCatalog as Record<string, unknown>).packages) ? (storeCatalog as Record<string, unknown>).packages : [],
      remoteSources: Array.isArray((remoteSourceCatalog as Record<string, unknown>).sources)
        ? (remoteSourceCatalog as Record<string, unknown>).sources
        : [],
      remotePackages: Array.isArray((remoteCatalog as Record<string, unknown>).packages)
        ? (remoteCatalog as Record<string, unknown>).packages
        : [],
    });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/reload") {
    const summary = await runToolLocally(storePath, "tool_module_reload", {});
    sendJson(response, 200, { ok: true, summary });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/set-enabled") {
    const body = (await readBody(request)) as { moduleId?: string; enabled?: boolean };
    if (!body.moduleId?.trim() || typeof body.enabled !== "boolean") {
      throw new Error("moduleId and enabled are required");
    }
    const result = await runToolLocally(storePath, "tool_module_set_enabled", {
      moduleId: body.moduleId.trim(),
      enabled: Boolean(body.enabled),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/uninstall") {
    const body = (await readBody(request)) as { moduleId?: string };
    if (!body.moduleId?.trim()) {
      throw new Error("moduleId is required");
    }
    const result = await runToolLocally(storePath, "tool_module_uninstall", {
      moduleId: body.moduleId.trim(),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/install-local") {
    const body = (await readBody(request)) as { packageId?: string; overwrite?: boolean };
    if (!body.packageId?.trim()) {
      throw new Error("packageId is required");
    }
    const result = await runToolLocally(storePath, "tool_store_install_local", {
      packageId: body.packageId,
      overwrite: Boolean(body.overwrite),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/remote-sources") {
    const body = (await readBody(request)) as {
      sourceId?: string;
      label?: string;
      url?: string;
      description?: string;
      overwrite?: boolean;
    };
    if (!body.sourceId?.trim() || !body.url?.trim()) {
      throw new Error("sourceId and url are required");
    }
    const result = await runToolLocally(storePath, "tool_store_remote_source_add", {
      sourceId: body.sourceId.trim(),
      label: body.label ?? "",
      url: body.url.trim(),
      description: body.description ?? "",
      overwrite: Boolean(body.overwrite),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/remote-sources/remove") {
    const body = (await readBody(request)) as { sourceId?: string };
    if (!body.sourceId?.trim()) {
      throw new Error("sourceId is required");
    }
    const result = await runToolLocally(storePath, "tool_store_remote_source_remove", {
      sourceId: body.sourceId.trim(),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/tool-modules/install-remote") {
    const body = (await readBody(request)) as {
      sourceId?: string;
      packageId?: string;
      overwrite?: boolean;
    };
    if (!body.sourceId?.trim() || !body.packageId?.trim()) {
      throw new Error("sourceId and packageId are required");
    }
    const result = await runToolLocally(storePath, "tool_store_install_remote", {
      sourceId: body.sourceId.trim(),
      packageId: body.packageId.trim(),
      overwrite: Boolean(body.overwrite),
    });
    sendJson(response, 200, { ok: true, ...(result as Record<string, unknown>) });
    return;
  }

  if (method === "POST" && url.pathname === "/api/project/create") {
    const body = (await readBody(request)) as { id?: string; label?: string };
    if (!body.id?.trim()) {
      throw new Error("project id is required");
    }
    const projectId = await createProjectSkeleton(storePath, body.id, body.label);
    sendJson(response, 200, { ok: true, projectId });
    return;
  }

  if (method === "POST" && url.pathname === "/api/datasource/create") {
    const body = (await readBody(request)) as { id?: string; label?: string; type?: string };
    if (!body.id?.trim()) {
      throw new Error("datasource id is required");
    }
    if (!body.type?.trim()) {
      throw new Error("datasource type is required");
    }
    const datasourceId = await createDatasourceSkeleton(storePath, body.id, body.label, body.type);
    sendJson(response, 200, { ok: true, datasourceId });
    return;
  }

  if (method === "POST" && url.pathname === "/api/project/bind-datasource") {
    const body = (await readBody(request)) as { projectId?: string; datasourceId?: string };
    if (!body.projectId?.trim() || !body.datasourceId?.trim()) {
      throw new Error("projectId and datasourceId are required");
    }
    await bindDatasourceToProject(storePath, body.projectId, body.datasourceId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/project/unbind-datasource") {
    const body = (await readBody(request)) as { projectId?: string; datasourceId?: string };
    if (!body.projectId?.trim() || !body.datasourceId?.trim()) {
      throw new Error("projectId and datasourceId are required");
    }
    await unbindDatasourceFromProject(storePath, body.projectId, body.datasourceId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/project/rename") {
    const body = (await readBody(request)) as { oldProjectId?: string; newProjectId?: string };
    if (!body.oldProjectId?.trim() || !body.newProjectId?.trim()) {
      throw new Error("oldProjectId and newProjectId are required");
    }
    const projectId = await renameProject(storePath, body.oldProjectId, body.newProjectId);
    sendJson(response, 200, { ok: true, projectId });
    return;
  }

  if (method === "POST" && url.pathname === "/api/datasource/rename") {
    const body = (await readBody(request)) as { oldDatasourceId?: string; newDatasourceId?: string };
    if (!body.oldDatasourceId?.trim() || !body.newDatasourceId?.trim()) {
      throw new Error("oldDatasourceId and newDatasourceId are required");
    }
    const datasourceId = await renameDatasource(storePath, body.oldDatasourceId, body.newDatasourceId);
    sendJson(response, 200, { ok: true, datasourceId });
    return;
  }

  if (method === "POST" && url.pathname === "/api/workspace/active-project") {
    const body = (await readBody(request)) as { projectId?: string };
    if (!body.projectId?.trim()) {
      throw new Error("projectId is required");
    }
    await setActiveProject(storePath, body.projectId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/project/delete") {
    const body = (await readBody(request)) as { projectId?: string };
    if (!body.projectId?.trim()) {
      throw new Error("projectId is required");
    }
    await deleteProject(storePath, body.projectId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/datasource/delete") {
    const body = (await readBody(request)) as { datasourceId?: string };
    if (!body.datasourceId?.trim()) {
      throw new Error("datasourceId is required");
    }
    await deleteDatasource(storePath, body.datasourceId);
    sendJson(response, 200, { ok: true });
    return;
  }

  if (method === "POST" && url.pathname === "/api/datasource/test") {
    const body = (await readBody(request)) as { datasourceId?: string };
    if (!body.datasourceId?.trim()) {
      throw new Error("datasourceId is required");
    }
    const store = readStoreSync(storePath);
    const datasource = store.datasources.find((item) => item.id === body.datasourceId);
    if (!datasource) {
      throw new Error(`Datasource not found: ${body.datasourceId}`);
    }
    const result = await testDatasource(datasource);
    sendJson(response, 200, { ok: true, result });
    return;
  }

  if (method === "POST" && url.pathname === "/api/datasource/config") {
    const body = await readBody(request);
    if (!body || typeof body !== "object" || Array.isArray(body)) {
      throw new Error("Invalid datasource config payload");
    }
    const input = body as Record<string, unknown>;
    if (!String(input.datasourceId ?? "").trim()) {
      throw new Error("datasourceId is required");
    }
    await writeDatasourceConfig(storePath, {
      datasourceId: String(input.datasourceId ?? ""),
      type: String(input.type ?? ""),
      label: input.label === undefined ? undefined : String(input.label ?? ""),
      enabled: input.enabled === undefined ? undefined : Boolean(input.enabled),
      description: input.description === undefined ? undefined : String(input.description ?? ""),
      role: input.role === undefined ? undefined : String(input.role ?? ""),
      usageNotes: input.usageNotes === undefined ? undefined : String(input.usageNotes ?? ""),
      host: input.host === undefined ? undefined : String(input.host ?? ""),
      port: input.port === undefined ? undefined : String(input.port ?? ""),
      database: input.database === undefined ? undefined : String(input.database ?? ""),
      uri: input.uri === undefined ? undefined : String(input.uri ?? ""),
      authSource: input.authSource === undefined ? undefined : String(input.authSource ?? ""),
      brokers: input.brokers === undefined ? undefined : String(input.brokers ?? ""),
      clientId: input.clientId === undefined ? undefined : String(input.clientId ?? ""),
      ssl: input.ssl === undefined ? undefined : Boolean(input.ssl),
      saslMechanism: input.saslMechanism === undefined ? undefined : String(input.saslMechanism ?? ""),
      mongoMode: input.mongoMode === undefined ? undefined : String(input.mongoMode ?? ""),
      authMode: input.authMode === undefined ? undefined : String(input.authMode ?? ""),
      loginPath: input.loginPath === undefined ? undefined : String(input.loginPath ?? ""),
      dataView: input.dataView === undefined ? undefined : String(input.dataView ?? ""),
      optionsJson: input.optionsJson === undefined ? undefined : String(input.optionsJson ?? ""),
      username: input.username === undefined ? undefined : String(input.username ?? ""),
      secret: input.secret === undefined ? undefined : String(input.secret ?? ""),
      expiresAt: input.expiresAt === undefined ? undefined : String(input.expiresAt ?? ""),
    });
    sendJson(response, 200, { ok: true });
    return;
  }

  sendJson(response, 404, { ok: false, error: "Not found" });
}

type TreeNodeWithGuide = WorkspaceNode & { guide?: ReturnType<typeof getGuideForNode> };

function attachGuides(node: TreeNodeWithGuide): void {
  const relPath = node.relPath || "";
  const kind = node.kind || "workspace";
  node.guide = getGuideForNode(relPath, kind);
  const children = Array.isArray(node.children) ? node.children : [];
  for (const child of children) {
    attachGuides(child as TreeNodeWithGuide);
  }
}

async function main() {
  const storePath = resolveStorePath(process.env.WMS_AI_AGENT_STORE_PATH);
  const port = Number(process.env.WMS_AI_AGENT_CONFIG_PORT || 3789);
  await ensureWorkspaceRoot(storePath);

  const server = http.createServer(async (request, response) => {
    try {
      await handleRequest(storePath, request, response);
    } catch (error) {
      sendJson(response, 500, {
        ok: false,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  });

  server.listen(port, "127.0.0.1", () => {
    console.error(
      `wms-ai-agent config ui running at http://127.0.0.1:${port} (workspace=${safeJsonStringify(
        path.resolve(storePath),
      )})`,
    );
  });
}

main().catch((error) => {
  console.error("Fatal:", error);
  process.exit(1);
});
