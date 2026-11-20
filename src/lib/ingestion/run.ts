import { createAdminClient } from "../supabase/admin";
import type { DepartmentTag } from "../constants";
import { canonicalizeUrl, generateJobKey, generateInlineJobKey } from "./canonicalize";
import { classifyDepartments } from "./classify-department";
import { cleanSalary } from "./clean-salary";
import { publishSnapshot, failSnapshot, type ProcessedJob } from "./publish";
import { collectJobs } from "./sources";
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

    // Drop very old postings, past-school-year postings, and general applications.
    // (Checked after the guards above, which are about whether the source worked.)
    const stale: string[] = [];
    const freshJobs = source.jobs.filter((job) => {
      const reason = staleReason(job);
      if (reason) stale.push(`${job.title} (${reason})`);
      return !reason;
    });
    if (stale.length) {
      console.log(`[ingest] ${company.name}: dropped ${stale.length} stale: ${stale.join("; ").slice(0, 1500)}`);
    }

    const canonicalCareersUrl = canonicalizeUrl(company.careers_url);
    const byKey = new Map<string, Omit<ProcessedJob, "department_tag">>();
    for (const job of freshJobs) {
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
      if (byKey.has(jobKey)) continue;
      byKey.set(jobKey, {
        job_key: jobKey,
        title: job.title,
        department_raw: job.department_raw,
        location: job.location,
        salary_raw: cleanSalary(job.salary_raw),
        description: job.description ?? "",
        job_url: url,
      });
    }
    const jobs = [...byKey.values()];

    // Reuse department tags for jobs we've already seen (stable + saves LLM calls).
    const knownTags = new Map<string, DepartmentTag>();
    if (jobs.length > 0) {
      const { data: existing } = await supabase
        .from("jobs")
        .select("job_key, department_tag")
        .eq("company_id", companyId)
        .in("job_key", jobs.map((j) => j.job_key));
      // "Other" is often a past fallback (e.g. the API was down), so re-classify those.
      for (const row of existing ?? []) {
        if (row.department_tag !== "Other") knownTags.set(row.job_key, row.department_tag as DepartmentTag);
      }
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
