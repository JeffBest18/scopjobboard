import { createClient } from "@/lib/supabase/server";
import type { BoardCompany, BoardJob } from "@/lib/types";
import { getFreshnessCheck } from "@/lib/freshness";

/**
 * Everything the board needs, in two queries. Descriptions are left out, so
 * even a few hundred roles is a small payload, and all filtering happens
 * instantly in the browser.
 */
export async function getBoardData(): Promise<{ jobs: BoardJob[]; companies: BoardCompany[] }> {
  const supabase = await createClient();
  const [jobsRes, companiesRes, freshness] = await Promise.all([
    supabase
      .from("live_jobs")
      .select("id, job_key, title, company_name, company_slug, department_tag, location, salary_raw, first_seen_at, company_id, created_at")
      .order("company_name", { ascending: true })
      .order("first_seen_at", { ascending: false })
      .order("title", { ascending: true })
      .range(0, 4999),
    supabase.from("companies").select("slug, name, homepage_url").eq("enabled", true),
    getFreshnessCheck(supabase),
  ]);
  type Row = BoardJob & { company_id: string; created_at: string };
  const jobs = ((jobsRes.data as Row[]) ?? [])
    .filter((j) => freshness.isCurrent(j))
    // Strip the bookkeeping fields before they're sent to the browser.
    .map(
      (j): BoardJob => ({
        id: j.id,
        job_key: j.job_key,
        title: j.title,
        company_name: j.company_name,
        company_slug: j.company_slug,
        department_tag: j.department_tag,
        location: j.location,
        salary_raw: j.salary_raw,
        first_seen_at: j.first_seen_at,
      })
    );
  return {
    jobs,
    companies: (companiesRes.data as BoardCompany[]) ?? [],
  };
}
