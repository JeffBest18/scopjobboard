import { createAdminClient } from "../supabase/admin";
import type { DepartmentTag } from "../constants";

export interface ProcessedJob {
  job_key: string;
  title: string;
  department_tag: DepartmentTag;
  department_raw: string | null;
  location: string | null;
  salary_raw: string | null;
  description: string;
  job_url: string;
  /** When the company posted it, if the source says (ISO 8601). */
  posted_at?: string | null;
}

export async function publishSnapshot(
  companyId: string,
  jobs: ProcessedJob[]
): Promise<void> {
  const supabase = createAdminClient();

  // 1. Create pending snapshot
  const { data: snapshot, error: snapError } = await supabase
    .from("snapshots")
    .insert({ company_id: companyId, status: "pending", job_count: 0 })
    .select("id")
    .single();

  if (snapError || !snapshot) {
    throw new Error(`Failed to create snapshot: ${snapError?.message}`);
  }

  try {
    // 2. Look up existing first_seen_at values for these job_keys
    const jobKeys = jobs.map((j) => j.job_key);
    const { data: existingJobs } =
      jobKeys.length > 0
        ? await supabase
            .from("jobs")
            .select("job_key, first_seen_at")
            .eq("company_id", companyId)
            .in("job_key", jobKeys)
        : { data: [] };

    const firstSeenMap = new Map<string, string>();
    if (existingJobs) {
      for (const ej of existingJobs) {
        // Keep the earliest first_seen_at per job_key
        const existing = firstSeenMap.get(ej.job_key);
        if (!existing || ej.first_seen_at < existing) {
          firstSeenMap.set(ej.job_key, ej.first_seen_at);
        }
      }
    }

    // 3. Insert job rows.
    // first_seen_at is the role's listing date: the earliest of when the company
    // says it was posted and when this board first saw it. New roles with no
    // posting date get "now" (the daily refresh sees them within a day).
    const now = new Date().toISOString();
    const listedAt = (j: ProcessedJob): string => {
      const candidates = [firstSeenMap.get(j.job_key), validPastDate(j.posted_at)].filter(
        (d): d is string => !!d
      );
      return candidates.length ? candidates.sort()[0] : now;
    };
    const jobRows = jobs.map((j) => ({
      job_key: j.job_key,
      snapshot_id: snapshot.id,
      company_id: companyId,
      title: j.title,
      department_tag: j.department_tag,
      department_raw: j.department_raw,
      location: j.location,
      salary_raw: j.salary_raw,
      description: j.description,
      job_url: j.job_url,
      first_seen_at: listedAt(j),
    }));

    // An empty snapshot is valid: it means the company has no open roles right now.
    if (jobRows.length > 0) {
      const { error: jobsError } = await supabase.from("jobs").insert(jobRows);
      if (jobsError) {
        throw new Error(`Failed to insert jobs: ${jobsError.message}`);
      }
    }

    // 4. Publish the snapshot
    const { error: publishError } = await supabase
      .from("snapshots")
      .update({ status: "published", job_count: jobs.length })
      .eq("id", snapshot.id);

    if (publishError) {
      throw new Error(`Failed to publish snapshot: ${publishError.message}`);
    }
  } catch (err) {
    // Mark snapshot as failed if anything went wrong
    await supabase
      .from("snapshots")
      .update({
        status: "failed",
        error_message: err instanceof Error ? err.message : "Unknown error",
      })
      .eq("id", snapshot.id);
    throw err;
  }
}

export async function failSnapshot(
  companyId: string,
  errorMessage: string
): Promise<void> {
  const supabase = createAdminClient();

  await supabase.from("snapshots").insert({
    company_id: companyId,
    status: "failed",
    job_count: 0,
    error_message: errorMessage,
  });
}

/** Normalises to ISO, rejecting unparseable dates and dates in the future. */
function validPastDate(value: string | null | undefined): string | null {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime()) || d.getTime() > Date.now() + 86_400_000) return null;
  return d.toISOString();
}
