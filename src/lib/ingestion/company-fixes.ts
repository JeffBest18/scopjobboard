import type { SupabaseClient } from "@supabase/supabase-js";

/**
 * Careers-page corrections found on 2026-11-19 by checking every portfolio
 * company's site. Each fix only applies while the company still has the exact
 * `from` value, so anything later edited in /admin is never overwritten.
 * Also recorded in supabase/migrations/006_careers_url_fixes.sql.
 */
const FIXES: { slug: string; from: string | null; to: string }[] = [
  // Had no careers URL; these companies post openings here:
  { slug: "artiphishell", from: null, to: "https://www.artiphishell.com/careers/" },
  { slug: "spacture", from: null, to: "https://www.spacture.ai/careers" },
  { slug: "heytutor", from: null, to: "https://heytutor.applytojob.com/apply" },
  { slug: "healtharc", from: null, to: "https://www.indeed.com/cmp/Healtharc/jobs" },
  { slug: "lifeguard", from: null, to: "https://www.trylifeguard.com/careers" },
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
