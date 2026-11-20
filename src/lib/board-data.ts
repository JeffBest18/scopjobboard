import { createClient } from "@/lib/supabase/server";
import type { BoardCompany, BoardJob } from "@/lib/types";

/**
 * Everything the board needs, in two queries. Descriptions are left out, so
 * even a few hundred roles is a small payload, and all filtering happens
 * instantly in the browser.
 */
export async function getBoardData(): Promise<{ jobs: BoardJob[]; companies: BoardCompany[] }> {
  const supabase = await createClient();
  const [jobsRes, companiesRes] = await Promise.all([
    supabase
      .from("live_jobs")
      .select("id, job_key, title, company_name, company_slug, department_tag, location, salary_raw, first_seen_at")
      .order("company_name", { ascending: true })
      .order("first_seen_at", { ascending: false })
      .order("title", { ascending: true })
      .range(0, 4999),
    supabase.from("companies").select("slug, name, homepage_url").eq("enabled", true),
  ]);
  return {
    jobs: (jobsRes.data as BoardJob[]) ?? [],
    companies: (companiesRes.data as BoardCompany[]) ?? [],
  };
}
