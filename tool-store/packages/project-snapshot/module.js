function parseProjectIdsJson(value) {
  if (!value || typeof value !== "string" || !value.trim()) {
    return [];
  }
  const parsed = JSON.parse(value);
  if (!Array.isArray(parsed)) {
    throw new Error("projectIdsJson must be a JSON array");
  }
  return parsed.map((item) => String(item ?? "").trim()).filter(Boolean);
}

function summarizeProject(project, store) {
  const datasourceIds = Array.isArray(project.datasourceIds) ? project.datasourceIds : [];
  const datasources = store.datasources.filter((item) => datasourceIds.includes(item.id));
  return {
    id: project.id,
    label: project.label,
    description: project.description,
    enabled: project.enabled,
    repoRoots: project.repoRoots,
    logRoots: project.logRoots,
    datasourceIds,
    datasourceCount: datasources.length,
    datasourceLabels: datasources.map((item) => item.label),
    playbookLength: (project.playbook ?? "").length,
    checklistLength: (project.checklist ?? "").length,
  };
}

export const manifest = {
  id: "project.snapshot",
  description: "Generate a concise snapshot for one or more projects.",
};

export function buildToolDefinitions(getStore) {
  return [
    {
      name: "project_snapshot_report",
      category: "project",
      description:
        "Return a compact snapshot for one or more projects, including repo roots, log roots, and bound datasources.",
      schema: {},
      handler: async (_context, args) => {
        const store = getStore();
        const requested = parseProjectIdsJson(args?.projectIdsJson);
        const projects = requested.length > 0
          ? store.projects.filter((project) => requested.includes(project.id))
          : store.projects.filter((project) => project.enabled);
        return {
          workspaceActiveProjectId: store.activeProjectId ?? null,
          projectCount: projects.length,
          projects: projects.map((project) => summarizeProject(project, store)),
        };
      },
    },
  ];
}
