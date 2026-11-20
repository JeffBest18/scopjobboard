import type { SupabaseClient } from "@supabase/supabase-js";

/**
 * Careers-page corrections found on 2026-11-19 by checking every portfolio
 * company's site. Each fix only applies while the company still has the exact
 * `from` value, so anything later edited in /admin is never overwritten.
 * Also recorded in supabase/migrations/006_careers_url_fixes.sql.
 */
const FIXES: { slug: string; from: string | null; to: string | null }[] = [
  // Had no careers URL; these companies post openings here:
  { slug: "artiphishell", from: null, to: "https://www.artiphishell.com/careers/" },
  { slug: "spacture", from: null, to: "https://www.spacture.ai/careers" },
  { slug: "heytutor", from: null, to: "https://heytutor.applytojob.com/apply" },
  // Set on 2026-11-19 and reverted: Indeed requires a login to view
  // HealthArc's jobs, and trylifeguard.com/careers is not a careers page.
  { slug: "healtharc", from: "https://www.indeed.com/cmp/Healtharc/jobs", to: null },
  { slug: "lifeguard", from: "https://www.trylifeguard.com/careers", to: null },
  // The listing on smartbarrel.io is injected by a script from this site:
  {
    slug: "smartbarrel",
    from: "https://smartbarrel.io/careers/#jobslisting",
    to: "https://careers.smartbarrel.io/jobs",
  },
];

export async function applyCompanyFixes(supabase: SupabaseClient): Promise<string[]> {
  const applied: string[] = [];
  for (const fix of FIXES) {
    let q = supabase.from("companies").update({ careers_url: fix.to }).eq("slug", fix.slug);
    q = fix.from === null ? q.is("careers_url", null) : q.eq("careers_url", fix.from);
    const { data, error } = await q.select("slug");
    if (error) console.warn(`[ingest] careers fix for ${fix.slug} failed: ${error.message}`);
    else if (data?.length) applied.push(fix.slug);
  }
  return applied;
}
