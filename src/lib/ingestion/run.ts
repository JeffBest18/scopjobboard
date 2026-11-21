import { createAdminClient } from "../supabase/admin";
import type { DepartmentTag } from "../constants";
import { canonicalizeUrl, generateJobKey, generateInlineJobKey } from "./canonicalize";
import { classifyDepartments } from "./classify-department";
import { cleanSalary } from "./clean-salary";
import { publishSnapshot, failSnapshot, type ProcessedJob } from "./publish";
import { collectJobs, type SourceJob } from "./sources";
import { resolveListingDate } from "../listing-date";
import { staleReason } from "./staleness";

export interface IngestionResult {
  success: boolean;
  jobCount?: number;
  /** Postings the source still lists but that were dropped as stale. */
  staleCount?: number;
  method?: string;
  error?: string;
}

/**
 * If a scrape (not an ATS API) suddenly returns far fewer jobs than the last
 * published snapshot, it's more likely the page changed than that the company
 * closed most of its roles. Keep the old listings and flag it instead.
 */
const SUSPICIOUS_DROP_MIN_PREVIOUS = 5;
const SUSPICIOUS_DROP_RATIO = 0.3;

export async function runIngestion(companyId: string): Promise<IngestionResult> {
  const supabase = createAdminClient();

  const { data: company, error: companyError } = await supabase
    .from("companies")
    .select("*")
    .eq("id", companyId)
    .single();

  if (companyError || !company) return { success: false, error: "Company not found" };
  if (!company.enabled) return { success: false, error: "Company is disabled" };
  if (!company.careers_url) return { success: false, error: "Company has no careers URL" };

  let method = "unknown";
  try {
    const source = await collectJobs(company.careers_url);
    method = source.method;

    if (source.jobs.length === 0 && !source.authoritative) {
      const error = `[${method}] Found 0 jobs on ${company.careers_url}; keeping previous listings`;
      await failSnapshot(companyId, error);
      return { success: false, method, error };
    }

    if (!source.authoritative) {
      const { data: prev } = await supabase
        .from("snapshots")
        .select("job_count")
        .eq("company_id", companyId)
        .eq("status", "published")
        .order("created_at", { ascending: false })
        .limit(1)
        .maybeSingle();
      if (
        prev &&
        prev.job_count >= SUSPICIOUS_DROP_MIN_PREVIOUS &&
        source.jobs.length < prev.job_count * SUSPICIOUS_DROP_RATIO
      ) {
        const error = `[${method}] Found ${source.jobs.length} jobs vs ${prev.job_count} last time; keeping previous listings`;
        await failSnapshot(companyId, error);
        return { success: false, method, error };
      }
    }

    // Key and de-duplicate what the source returned.
    const canonicalCareersUrl = canonicalizeUrl(company.careers_url);
    const byKey = new Map<string, { job_key: string; url: string; source: SourceJob }>();
    for (const job of source.jobs) {
      if (!job.title) continue;
      let url: string;
      try {
        url = canonicalizeUrl(job.url);
      } catch {
        url = canonicalCareersUrl;
      }
      const inline = url === canonicalCareersUrl;
      const jobKey = inline
        ? generateInlineJobKey(companyId, canonicalCareersUrl, job.title)
        : generateJobKey(companyId, url);
      if (!byKey.has(jobKey)) byKey.set(jobKey, { job_key: jobKey, url, source: job });
    }
    const keyed = [...byKey.values()];

    // What we already know about these jobs: department tags and when we first saw them.
    const knownTags = new Map<string, DepartmentTag>();
    const firstSeen = new Map<string, string>();
    if (keyed.length > 0) {
      const { data: existing } = await supabase
        .from("jobs")
        .select("job_key, department_tag, first_seen_at")
        .eq("company_id", companyId)
        .in("job_key", keyed.map((j) => j.job_key));
      for (const row of existing ?? []) {
        // "Other" is often a past fallback (e.g. the API was down), so re-classify those.
        if (row.department_tag !== "Other") knownTags.set(row.job_key, row.department_tag as DepartmentTag);
        const prev = firstSeen.get(row.job_key);
        if (!prev || row.first_seen_at < prev) firstSeen.set(row.job_key, row.first_seen_at);
      }
    }
    const { count: priorSnapshots } = await supabase
      .from("snapshots")
      .select("id", { count: "exact", head: true })
      .eq("company_id", companyId)
      .eq("status", "published");

    // Listing date for each job, then drop stale ones (very old, past school
    // year, general applications). Checked after the guards above, which are
    // about whether the source worked at all.
    const now = new Date();
    const stale: string[] = [];
    const jobs: Omit<ProcessedJob, "department_tag">[] = [];
    for (const { job_key, url, source: job } of keyed) {
      const listedAt = resolveListingDate({
        postedAt: job.posted_at,
        existingFirstSeen: firstSeen.get(job_key),
        companyHasHistory: (priorSnapshots ?? 0) > 0,
        now,
      });
      const reason = staleReason(job, now, listedAt);
      if (reason) {
        stale.push(`${job.title} (${reason})`);
        continue;
      }
      jobs.push({
        job_key,
        title: job.title,
        department_raw: job.department_raw,
        location: job.location,
        salary_raw: cleanSalary(job.salary_raw),
        description: job.description ?? "",
        job_url: url,
        first_seen_at: listedAt,
      });
    }
    if (stale.length) {
      console.log(`[ingest] ${company.name}: dropped ${stale.length} stale: ${stale.join("; ").slice(0, 1500)}`);
    }

    const unknown = jobs.filter((j) => !knownTags.has(j.job_key));
    const newTags = await classifyDepartments(unknown);
    unknown.forEach((j, i) => knownTags.set(j.job_key, newTags[i]));

    const processed: ProcessedJob[] = jobs.map((j) => ({
      ...j,
      department_tag: knownTags.get(j.job_key) ?? "Other",
    }));

    await publishSnapshot(companyId, processed);
    return { success: true, jobCount: processed.length, staleCount: stale.length, method };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    await failSnapshot(companyId, `[${method}] ${message}`);
    return { success: false, method, error: message };
  }
}
