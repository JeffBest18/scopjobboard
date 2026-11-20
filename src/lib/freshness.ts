import type { SupabaseClient } from "@supabase/supabase-js";

/**
 * Backstop for broken refreshes: if a company's jobs are auto-ingested and
 * haven't been confirmed by a successful run in this many days, stop showing
 * them rather than listing roles that may have closed. Companies without a
 * careers URL (jobs managed by hand in /admin) are exempt.
 */
export const MAX_UNCONFIRMED_DAYS = 30;

export interface FreshnessCheck {
  isCurrent: (job: { company_id: string; created_at: string }) => boolean;
}

export async function getFreshnessCheck(supabase: SupabaseClient): Promise<FreshnessCheck> {
  const { data } = await supabase.from("companies").select("id").not("careers_url", "is", null);
  const autoIngested = new Set((data ?? []).map((c) => c.id as string));
  const cutoff = new Date(Date.now() - MAX_UNCONFIRMED_DAYS * 86_400_000).toISOString();
  return {
    // A job row's created_at is when the snapshot that confirmed it was published.
    isCurrent: (job) => !autoIngested.has(job.company_id) || job.created_at >= cutoff,
  };
}
