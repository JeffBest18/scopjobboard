/**
 * Job board filter state <-> URL query string. Shared by the server pages
 * (initial render) and the client board (history updates), so links like
 * `/?company=rogo&department=Engineering` are shareable.
 */

export type BoardView = "company" | "all";

export interface BoardState {
  q: string;
  departments: string[];
  companies: string[];
  /** "company": grouped by company (default). "all": flat list, newest first. */
  view: BoardView;
  page: number;
}

export function boardStateFromParams(sp: URLSearchParams): BoardState {
  return {
    q: sp.get("q")?.trim() ?? "",
    departments: sp.getAll("department"),
    companies: sp.getAll("company"),
    view: sp.get("view") === "all" ? "all" : "company",
    page: Math.max(1, Number(sp.get("page")) || 1),
  };
}

/** For server components, whose searchParams arrive as a plain record. */
export function boardStateFromRecord(record: Record<string, string | string[] | undefined>): BoardState {
  const sp = new URLSearchParams();
  for (const [key, value] of Object.entries(record)) {
    for (const v of ([] as string[]).concat(value ?? [])) sp.append(key, v);
  }
  return boardStateFromParams(sp);
}

export function boardStateToQuery(s: BoardState): string {
  const sp = new URLSearchParams();
  if (s.q) sp.set("q", s.q);
  s.companies.forEach((c) => sp.append("company", c));
  s.departments.forEach((d) => sp.append("department", d));
  if (s.view === "all") sp.set("view", "all");
  if (s.page > 1) sp.set("page", String(s.page));
  const qs = sp.toString();
  return qs ? `?${qs}` : "";
}
