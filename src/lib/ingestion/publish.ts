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
  /** Listing date (see lib/listing-date.ts); stored as first_seen_at. */
  first_seen_at: string;
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
    // 2. Insert job rows (listing dates were resolved by the caller).
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
      first_seen_at: j.first_seen_at,
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

