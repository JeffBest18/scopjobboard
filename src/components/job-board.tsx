"use client";

import { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import Image from "next/image";
import Link from "next/link";
import { DEPARTMENT_TAGS, JOBS_PER_PAGE } from "@/lib/constants";
import type { BoardCompany, BoardJob } from "@/lib/types";
import {
  boardStateFromParams,
  boardStateToQuery,
  type BoardState,
  type BoardView,
  type PostedWindow,
} from "@/lib/board-params";
import { Pagination } from "@/components/pagination";
import { formatSalary } from "@/lib/ingestion/clean-salary";

/** Roles shown per company in the grouped overview before "View all". */
const GROUP_PREVIEW = 3;

const DAY_MS = 86_400_000;

/** Server render time, so relative dates and "past week" match between server and browser. */
const NowContext = createContext(0);

/** Days since a role was listed (first_seen_at = posting date when known, else first seen). */
function daysListed(job: BoardJob, nowMs: number): number {
  return Math.max(0, Math.floor((nowMs - new Date(job.first_seen_at).getTime()) / DAY_MS));
}

interface JobBoardProps {
  jobs: BoardJob[];
  companies: BoardCompany[];
  initialState: BoardState;
  basePath: string;
  /** ISO timestamp of the server render. */
  now: string;
}

interface CompanyInfo {
  slug: string;
  name: string;
  homepage: string | null;
}

/**
 * The job board: company directory, department chips, sticky search, and a
 * results list that is grouped by company by default so no single company's
 * volume buries the rest. All filtering is client-side; state lives in the URL.
 */
export function JobBoard({ jobs, companies, initialState, basePath, now }: JobBoardProps) {
  const nowMs = useMemo(() => new Date(now).getTime(), [now]);
  const [state, setState] = useState<BoardState>(initialState);
  const resultsRef = useRef<HTMLDivElement>(null);

  // Browser back/forward restores the filters for that history entry.
  useEffect(() => {
    const onPop = () => setState(boardStateFromParams(new URLSearchParams(window.location.search)));
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  /**
   * Apply a filter change. Company navigation pushes a history entry (so Back
   * returns to the overview); typing and chip toggles replace the current one.
   */
  function update(patch: Partial<BoardState>, opts: { push?: boolean; scroll?: boolean } = {}) {
    const next: BoardState = { ...state, page: 1, ...patch };
    setState(next);
    const url = `${basePath}${boardStateToQuery(next)}`;
    if (opts.push) window.history.pushState(null, "", url);
    else window.history.replaceState(null, "", url);
    if (opts.scroll) {
      requestAnimationFrame(() => resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
    }
  }

  // ----- Derived data ------------------------------------------------------

  /** Every company with at least one live role, alphabetical. */
  const hiring: CompanyInfo[] = useMemo(() => {
    const meta = new Map(companies.map((c) => [c.slug, c]));
    const seen = new Map<string, CompanyInfo>();
    for (const j of jobs) {
      if (seen.has(j.company_slug)) continue;
      const m = meta.get(j.company_slug);
      seen.set(j.company_slug, {
        slug: j.company_slug,
        name: m?.name ?? j.company_name,
        homepage: m?.homepage_url ?? null,
      });
    }
    return [...seen.values()].sort((a, b) => a.name.localeCompare(b.name));
  }, [jobs, companies]);
  const companyBySlug = useMemo(() => new Map(hiring.map((c) => [c.slug, c])), [hiring]);

  const q = state.q.trim().toLowerCase();
  const deptSet = useMemo(() => new Set(state.departments), [state.departments]);
  const companySet = useMemo(() => new Set(state.companies), [state.companies]);

  const searched = useMemo(() => (q ? jobs.filter((j) => matchesSearch(j, q)) : jobs), [jobs, q]);
  const inWindow = useMemo(
    () => (state.posted ? searched.filter((j) => daysListed(j, nowMs) <= state.posted!) : searched),
    [searched, state.posted, nowMs]
  );

  // Faceted counts: each facet counts what you'd get if you picked it, given the other filters.
  const companyCounts = useMemo(
    () => countBy(inWindow.filter((j) => !deptSet.size || deptSet.has(j.department_tag)), (j) => j.company_slug),
    [inWindow, deptSet]
  );
  const postedCounts = useMemo(() => {
    const pool = searched.filter(
      (j) => (!companySet.size || companySet.has(j.company_slug)) && (!deptSet.size || deptSet.has(j.department_tag))
    );
    return {
      7: pool.filter((j) => daysListed(j, nowMs) <= 7).length,
      30: pool.filter((j) => daysListed(j, nowMs) <= 30).length,
    };
  }, [searched, companySet, deptSet, nowMs]);
  /** Per company, roles listed in the past week (ignores filters, so the tiles stay stable). */
  const newThisWeek = useMemo(
    () => countBy(jobs.filter((j) => daysListed(j, nowMs) <= 7), (j) => j.company_slug),
    [jobs, nowMs]
  );
  const inCompanies = useMemo(
    () => inWindow.filter((j) => !companySet.size || companySet.has(j.company_slug)),
    [inWindow, companySet]
  );
  const deptCounts = useMemo(() => countBy(inCompanies, (j) => j.department_tag), [inCompanies]);
  const results = useMemo(
    () => inCompanies.filter((j) => !deptSet.size || deptSet.has(j.department_tag)),
    [inCompanies, deptSet]
  );

  const selected = state.companies.map((s) => companyBySlug.get(s)).filter((c): c is CompanyInfo => !!c);
  const mode: "detail" | "grouped" | "flat" = selected.length ? "detail" : state.view === "newest" ? "flat" : "grouped";
  const hasFilters = !!(q || state.departments.length || state.companies.length || state.posted);
  const clearAll = () => update({ q: "", departments: [], companies: [], posted: null }, { push: true });

  // ----- Actions ------------------------------------------------------------

  const openCompany = (slug: string) => update({ companies: [slug] }, { push: true, scroll: true });
  const backToAll = () => update({ companies: [] }, { push: true, scroll: true });
  const toggleDept = (tag: string) =>
    update({
      departments: deptSet.has(tag) ? state.departments.filter((d) => d !== tag) : [...state.departments, tag],
    });
  const setView = (view: BoardView) => update({ view, companies: [] });
  const setPosted = (posted: PostedWindow) => update({ posted });
  const goToPage = (page: number) => {
    update({ page });
    requestAnimationFrame(() => resultsRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }));
  };

  // ----- Render -------------------------------------------------------------

  const visibleDepts = DEPARTMENT_TAGS.filter((t) => deptCounts.get(t) || deptSet.has(t));

  return (
    <NowContext.Provider value={nowMs}>
    <div>
      {/* Company directory */}
      <section aria-labelledby="directory-heading" className="mb-10">
        <div className="mb-4 flex items-baseline justify-between gap-4">
          <h3 id="directory-heading" className="text-sm font-medium text-black">
            Companies hiring
          </h3>
          <p className="text-xs text-gray-400">
            {hiring.length} companies &middot; {jobs.length} roles
          </p>
        </div>
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6">
          {hiring.map((c) => {
            const count = companyCounts.get(c.slug) ?? 0;
            const fresh = newThisWeek.get(c.slug) ?? 0;
            const isSelected = companySet.has(c.slug);
            return (
              <button
                key={c.slug}
                type="button"
                onClick={() => (isSelected ? backToAll() : openCompany(c.slug))}
                disabled={count === 0 && !isSelected}
                aria-pressed={isSelected}
                className={`flex items-center gap-2.5 rounded-lg border px-3 py-2.5 text-left transition-colors duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 disabled:cursor-default disabled:opacity-35 ${
                  isSelected
                    ? "border-black bg-gray-50"
                    : "border-gray-200 hover:border-gray-400 hover:bg-gray-50"
                }`}
              >
                <CompanyLogo slug={c.slug} name={c.name} size={28} />
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium text-black">{c.name}</span>
                  <span className="block truncate text-xs text-gray-400">
                    {count} {count === 1 ? "role" : "roles"}
                    {fresh > 0 && <span className="text-emerald-600"> &middot; {fresh} new</span>}
                  </span>
                </span>
              </button>
            );
          })}
        </div>
      </section>

      {/* Scroll target for company navigation and paging (the sticky bar itself can't be one). */}
      <div ref={resultsRef} aria-hidden="true" />

      {/* Sticky controls */}
      <div
        className="sticky top-0 z-20 -mx-2 border-b border-gray-100 bg-white/95 px-2 py-3 backdrop-blur supports-[backdrop-filter]:bg-white/80"
      >
        <div className="flex flex-wrap items-center gap-3">
          <SearchInput value={state.q} onChange={(value) => update({ q: value })} />
          {mode === "detail" ? (
            <button
              type="button"
              onClick={backToAll}
              className="flex items-center gap-1.5 rounded-full border border-gray-300 py-1.5 pl-3 pr-2.5 text-sm text-gray-600 transition-colors duration-150 hover:border-black hover:text-black"
            >
              <ArrowLeft />
              All companies
            </button>
          ) : null}
          {mode === "detail" && selected.length === 1 && (
            // Keeps "where am I" visible while scrolling a long company list.
            <span className="flex min-w-0 items-center gap-2 text-sm font-medium text-black">
              <CompanyLogo slug={selected[0].slug} name={selected[0].name} size={24} />
              <span className="truncate">{selected[0].name}</span>
            </span>
          )}
          {mode !== "detail" && (
            <ViewToggle view={state.view} onChange={setView} />
          )}
        </div>
      </div>

      {/* Posted-date chips */}
      <div className="mt-4 flex flex-wrap items-center gap-2" role="group" aria-label="Filter by posting date">
        <span className="mr-1 w-20 text-xs text-gray-400">Posted</span>
        <Chip active={!state.posted} onClick={() => setPosted(null)}>
          Any time
        </Chip>
        {([7, 30] as const).map((days) => (
          <Chip key={days} active={state.posted === days} onClick={() => setPosted(state.posted === days ? null : days)}>
            {days === 7 ? "Past week" : "Past month"}
            <span className={state.posted === days ? "text-gray-300" : "text-gray-400"}>{postedCounts[days]}</span>
          </Chip>
        ))}
      </div>

      {/* Department chips */}
      <div className="mt-2 flex flex-wrap items-center gap-2" role="group" aria-label="Filter by department">
        <span className="mr-1 w-20 text-xs text-gray-400">Department</span>
        <Chip active={!state.departments.length} onClick={() => update({ departments: [] })}>
          All departments
        </Chip>
        {visibleDepts.map((tag) => (
          <Chip key={tag} active={deptSet.has(tag)} onClick={() => toggleDept(tag)}>
            {tag}
            <span className={deptSet.has(tag) ? "text-gray-300" : "text-gray-400"}>{deptCounts.get(tag) ?? 0}</span>
          </Chip>
        ))}
      </div>

      {/* Result summary */}
      <div className="mt-6 flex flex-wrap items-baseline justify-between gap-2">
        <p className="text-sm text-gray-500" aria-live="polite">
          {summarize(results.length, selected, state.departments, state.q.trim(), mode === "grouped" ? countDistinct(results) : null, state.posted)}
        </p>
        {hasFilters && (
          <button
            type="button"
            onClick={clearAll}
            className="text-xs text-gray-400 underline-offset-2 transition-colors hover:text-black hover:underline"
          >
            Clear filters
          </button>
        )}
      </div>

      {/* Results */}
      <div className="mt-4">
        {results.length === 0 ? (
          <EmptyState onClear={clearAll} />
        ) : mode === "grouped" ? (
          <GroupedResults results={results} companyBySlug={companyBySlug} onOpenCompany={openCompany} />
        ) : (
          <PagedResults
            results={mode === "flat" ? sortNewest(results) : results}
            page={state.page}
            onPageChange={goToPage}
            detailCompanies={mode === "detail" ? selected : null}
          />
        )}
      </div>
    </div>
    </NowContext.Provider>
  );
}

// ---------------------------------------------------------------- Results

function GroupedResults({
  results,
  companyBySlug,
  onOpenCompany,
}: {
  results: BoardJob[];
  companyBySlug: Map<string, CompanyInfo>;
  onOpenCompany: (slug: string) => void;
}) {
  const groups = groupByCompany(results);
  return (
    // Two columns on wide screens: halves the page length, and rows don't need the full width.
    <div className="grid grid-cols-1 gap-x-10 gap-y-10 lg:grid-cols-2">
      {groups.map(([slug, roles]) => {
        const company = companyBySlug.get(slug) ?? { slug, name: roles[0].company_name, homepage: null };
        const hidden = roles.length - GROUP_PREVIEW;
        return (
          <section key={slug} aria-label={company.name}>
            <CompanyHeader company={company} count={roles.length} onOpen={() => onOpenCompany(slug)} />
            <RoleList jobs={roles.slice(0, GROUP_PREVIEW)} />
            {hidden > 0 && (
              <button
                type="button"
                onClick={() => onOpenCompany(slug)}
                className="mt-2 flex items-center gap-1.5 px-1 text-sm font-medium text-gray-500 transition-colors duration-150 hover:text-black"
              >
                View all {roles.length} roles at {company.name}
                <ArrowRight />
              </button>
            )}
          </section>
        );
      })}
    </div>
  );
}

function PagedResults({
  results,
  page,
  onPageChange,
  detailCompanies,
}: {
  results: BoardJob[];
  page: number;
  onPageChange: (page: number) => void;
  /** When viewing specific companies, show their headers and hide per-row company names. */
  detailCompanies: CompanyInfo[] | null;
}) {
  const totalPages = Math.max(1, Math.ceil(results.length / JOBS_PER_PAGE));
  const current = Math.min(page, totalPages);
  const slice = results.slice((current - 1) * JOBS_PER_PAGE, current * JOBS_PER_PAGE);

  if (detailCompanies) {
    const groups = groupByCompany(slice);
    const totals = countBy(results, (j) => j.company_slug);
    const bySlug = new Map(detailCompanies.map((c) => [c.slug, c]));
    return (
      <>
        <div className="space-y-10">
          {groups.map(([slug, roles]) => {
            const company = bySlug.get(slug) ?? { slug, name: roles[0].company_name, homepage: null };
            return (
              <section key={slug} aria-label={company.name}>
                <CompanyHeader company={company} count={totals.get(slug) ?? roles.length} large />
                <RoleList jobs={roles} />
              </section>
            );
          })}
        </div>
        <Pagination currentPage={current} totalPages={totalPages} onPageChange={onPageChange} />
      </>
    );
  }

  return (
    <>
      <RoleList jobs={slice} showCompany />
      <Pagination currentPage={current} totalPages={totalPages} onPageChange={onPageChange} />
    </>
  );
}

function CompanyHeader({
  company,
  count,
  onOpen,
  large,
}: {
  company: CompanyInfo;
  count: number;
  onOpen?: () => void;
  large?: boolean;
}) {
  const name = (
    <span className={`${large ? "text-2xl font-light tracking-tight" : "text-lg font-medium"} text-black`}>
      {company.name}
    </span>
  );
  return (
    <div className="mb-3 flex items-center gap-3">
      <CompanyLogo slug={company.slug} name={company.name} size={large ? 48 : 36} />
      <div className="min-w-0 flex-1">
        <h3 className="leading-tight">
          {onOpen ? (
            <button type="button" onClick={onOpen} className="text-left hover:underline hover:underline-offset-4">
              {name}
            </button>
          ) : (
            name
          )}
        </h3>
        <p className="mt-0.5 text-xs text-gray-400">
          {count} open {count === 1 ? "role" : "roles"}
          {company.homepage && (
            <>
              {" "}&middot;{" "}
              <a
                href={company.homepage}
                target="_blank"
                rel="noopener noreferrer"
                className="transition-colors hover:text-black hover:underline"
              >
                {hostname(company.homepage)}
              </a>
            </>
          )}
        </p>
      </div>
    </div>
  );
}

function RoleList({ jobs, showCompany }: { jobs: BoardJob[]; showCompany?: boolean }) {
  return (
    <ul className="divide-y divide-gray-100 overflow-hidden rounded-lg border border-gray-100">
      {jobs.map((job) => (
        <li key={job.id}>
          <RoleRow job={job} showCompany={showCompany} />
        </li>
      ))}
    </ul>
  );
}

function RoleRow({ job, showCompany }: { job: BoardJob; showCompany?: boolean }) {
  const nowMs = useContext(NowContext);
  const days = daysListed(job, nowMs);
  return (
    <Link
      href={`/jobs/${job.job_key}`}
      target="_blank"
      rel="noopener noreferrer"
      className="group flex items-center gap-4 px-4 py-3.5 transition-colors duration-150 hover:bg-gray-50 focus:outline-none focus-visible:bg-gray-50"
    >
      {showCompany && <CompanyLogo slug={job.company_slug} name={job.company_name} size={36} />}
      <div className="min-w-0 flex-1">
        <p className="line-clamp-2 text-sm font-medium text-black">{job.title}</p>
        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-gray-500">
          {showCompany && <span className="font-medium text-gray-700">{job.company_name}</span>}
          <span>{job.department_tag}</span>
          {job.location && (
            <span className="flex min-w-0 items-center gap-1 text-gray-400">
              <PinIcon />
              <span className="truncate">{job.location}</span>
            </span>
          )}
          {job.salary_raw && (
            <span className="flex items-center gap-1 text-gray-400">
              <CurrencyIcon />
              {formatSalary(job.salary_raw)}
            </span>
          )}
          <span className={days <= 7 ? "font-medium text-emerald-600" : "text-gray-400"} title={new Date(job.first_seen_at).toDateString()}>
            {postedLabel(days, job.first_seen_at, nowMs)}
          </span>
        </div>
      </div>
      <span className="shrink-0 text-gray-300 transition-colors duration-150 group-hover:text-black">
        <ArrowRight />
      </span>
    </Link>
  );
}

function EmptyState({ onClear }: { onClear: () => void }) {
  return (
    <div className="rounded-lg border border-dashed border-gray-200 py-16 text-center">
      <p className="text-sm text-gray-500">No roles match these filters.</p>
      <button
        type="button"
        onClick={onClear}
        className="mt-3 text-sm font-medium text-black underline-offset-4 hover:underline"
      >
        Clear filters
      </button>
    </div>
  );
}

// --------------------------------------------------------------- Controls

function SearchInput({ value, onChange }: { value: string; onChange: (value: string) => void }) {
  return (
    <div className="relative min-w-0 flex-1 basis-64 sm:max-w-md">
      <svg
        width="18"
        height="18"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
        strokeLinejoin="round"
        className="pointer-events-none absolute left-3.5 top-1/2 -translate-y-1/2 text-gray-400"
        aria-hidden="true"
      >
        <circle cx="11" cy="11" r="8" />
        <line x1="21" y1="21" x2="16.65" y2="16.65" />
      </svg>
      <input
        type="search"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder="Search roles, companies, locations…"
        aria-label="Search jobs"
        className="w-full rounded-lg border border-gray-300 bg-white py-2.5 pl-10 pr-4 text-sm text-gray-700 placeholder:text-gray-400 transition-colors duration-150 focus:border-gray-400 focus:outline-none focus:ring-1 focus:ring-gray-400"
      />
    </div>
  );
}

function ViewToggle({ view, onChange }: { view: BoardView; onChange: (view: BoardView) => void }) {
  const options: { value: BoardView; label: string }[] = [
    { value: "company", label: "By company" },
    { value: "newest", label: "Newest first" },
  ];
  return (
    <div role="radiogroup" aria-label="Layout" className="flex rounded-lg border border-gray-300 p-0.5">
      {options.map((o) => (
        <button
          key={o.value}
          type="button"
          role="radio"
          aria-checked={view === o.value}
          onClick={() => onChange(o.value)}
          className={`rounded-md px-3 py-1.5 text-sm transition-colors duration-150 ${
            view === o.value ? "bg-black text-white" : "text-gray-500 hover:text-black"
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  );
}

function Chip({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-pressed={active}
      className={`flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors duration-150 focus:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 ${
        active ? "border-black bg-black text-white" : "border-gray-200 text-gray-600 hover:border-gray-400"
      }`}
    >
      {children}
    </button>
  );
}

function CompanyLogo({ slug, name, size }: { slug: string; name: string; size: number }) {
  return (
    <span
      className="flex shrink-0 items-center justify-center overflow-hidden rounded-md border border-gray-100 bg-white"
      style={{ width: size, height: size }}
    >
      <Image src={`/logos/${slug}.png`} alt={name} width={size} height={size} className="h-full w-full object-contain" />
    </span>
  );
}

// ------------------------------------------------------------------ Icons

function ArrowRight() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M5 12h14" />
      <path d="m12 5 7 7-7 7" />
    </svg>
  );
}

function ArrowLeft() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M19 12H5" />
      <path d="m12 19-7-7 7-7" />
    </svg>
  );
}

function PinIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0" aria-hidden="true">
      <path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z" />
      <circle cx="12" cy="10" r="3" />
    </svg>
  );
}

function CurrencyIcon() {
  return (
    <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0" aria-hidden="true">
      <line x1="12" y1="1" x2="12" y2="23" />
      <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
    </svg>
  );
}

// ---------------------------------------------------------------- Helpers

function matchesSearch(job: BoardJob, q: string): boolean {
  const haystack = `${job.title} ${job.company_name} ${job.location ?? ""} ${job.department_tag}`.toLowerCase();
  // Every word must appear somewhere, so "engineer new york" works.
  return q.split(/\s+/).every((word) => haystack.includes(word));
}

function countBy<T>(items: T[], key: (item: T) => string): Map<string, number> {
  const counts = new Map<string, number>();
  for (const item of items) counts.set(key(item), (counts.get(key(item)) ?? 0) + 1);
  return counts;
}

function countDistinct(jobs: BoardJob[]): number {
  return new Set(jobs.map((j) => j.company_slug)).size;
}

/** Preserves the server order (company name, then newest first within each company). */
function groupByCompany(jobs: BoardJob[]): [string, BoardJob[]][] {
  const groups = new Map<string, BoardJob[]>();
  for (const j of jobs) {
    const list = groups.get(j.company_slug);
    if (list) list.push(j);
    else groups.set(j.company_slug, [j]);
  }
  return [...groups.entries()];
}

function sortNewest(jobs: BoardJob[]): BoardJob[] {
  return [...jobs].sort(
    (a, b) => b.first_seen_at.localeCompare(a.first_seen_at) || a.company_name.localeCompare(b.company_name)
  );
}

function summarize(
  count: number,
  companies: CompanyInfo[],
  departments: string[],
  q: string,
  companyCount: number | null,
  posted: PostedWindow
): string {
  const dept = departments.length === 1 ? `${departments[0]} ` : "";
  const roles = `${count} ${dept}${count === 1 ? "role" : "roles"}`;
  const where =
    companies.length === 1
      ? ` at ${companies[0].name}`
      : companies.length > 1
        ? ` at ${companies.length} companies`
        : companyCount !== null
          ? ` across ${companyCount} ${companyCount === 1 ? "company" : "companies"}`
          : "";
  const search = q ? ` matching “${q}”` : "";
  const when = posted === 7 ? " posted in the past week" : posted === 30 ? " posted in the past month" : "";
  const order = companyCount === null && companies.length === 0 ? ", newest first" : "";
  return `${roles}${when}${where}${search}${order}`;
}

function hostname(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, "");
  } catch {
    return url;
  }
}

/** "Posted today", "Posted 3 days ago", "Posted 2 weeks ago", "Posted Mar 4". */
function postedLabel(days: number, iso: string, nowMs: number): string {
  if (days === 0) return "Posted today";
  if (days === 1) return "Posted yesterday";
  if (days < 14) return `Posted ${days} days ago`;
  if (days < 60) return `Posted ${Math.floor(days / 7)} weeks ago`;
  const d = new Date(iso);
  const sameYear = d.getUTCFullYear() === new Date(nowMs).getUTCFullYear();
  return `Posted ${d.toLocaleDateString("en-US", { month: "short", day: "numeric", year: sameYear ? undefined : "numeric", timeZone: "UTC" })}`;
}
