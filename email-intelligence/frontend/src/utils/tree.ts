import type { DashboardNode } from "../api/types";

export function findNode(root: DashboardNode, id: string): DashboardNode | null {
  if (root.id === id) return root;
  for (const child of root.children) {
    const found = findNode(child, id);
    if (found) return found;
  }
  return null;
}

export function pathToNode(root: DashboardNode, id: string): DashboardNode[] | null {
  if (root.id === id) return [root];
  for (const child of root.children) {
    const sub = pathToNode(child, id);
    if (sub) return [root, ...sub];
  }
  return null;
}
