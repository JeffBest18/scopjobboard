import { NextRequest, NextResponse } from "next/server";
import { createAdminClient } from "@/lib/supabase/admin";
import { runIngestion, type IngestionResult } from "@/lib/ingestion/run";
import { mapLimit } from "@/lib/ingestion/http";
import { applyCompanyFixes } from "@/lib/ingestion/company-fixes";

export const maxDuration = 300;

const CONCURRENCY = 5;
/** Stop starting new companies after this long, so in-flight ones can finish within maxDuration. */
const START_BUDGET_MS = 200_000;

/**
 * Daily cron (see vercel.json). Ingests every enabled company in this one
 * function, oldest-refreshed first. Previously this fanned out HTTP requests
 * to /api/ingest/[companyId] and awaited each batch, which hit the 300s limit
 * after the first batch or two, so later companies were never refreshed.
 *
 * Optional: ?company=<slug> to ingest a single company.
 */
export async function GET(request: NextRequest) {
  if (request.headers.get("authorization") !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const supabase = createAdminClient();
  const onlySlug = request.nextUrl.searchParams.get("company");
  const fixesApplied = await applyCompanyFixes(supabase);
  if (fixesApplied.length) console.log(`[ingest] careers URL fixes applied: ${fixesApplied.join(", ")}`);

  let query = supabase
    .from("companies")
    .select("id, name, slug")
    .eq("enabled", true)
    .not("careers_url", "is", null);
  if (onlySlug) query = query.eq("slug", onlySlug);
  const { data: companies, error } = await query;

  if (error || !companies) {
    return NextResponse.json({ error: "Failed to load companies" }, { status: 500 });
  }

  // Oldest attempt first, so if we ever run out of time the stalest get refreshed.
  const { data: snaps } = await supabase
    .from("snapshots")
    .select("company_id, created_at")
    .order("created_at", { ascending: false })
    .limit(2000);
  const lastAttempt = new Map<string, string>();
  for (const s of snaps ?? []) if (!lastAttempt.has(s.company_id)) lastAttempt.set(s.company_id, s.created_at);
  companies.sort((a, b) => (lastAttempt.get(a.id) ?? "").localeCompare(lastAttempt.get(b.id) ?? ""));

  const started = Date.now();
  const results = await mapLimit(companies, CONCURRENCY, async (company) => {
    if (Date.now() - started > START_BUDGET_MS) {
      return { company: company.name, success: false, skipped: true, error: "Out of time; will run next cycle" };
    }
    const t0 = Date.now();
    let result: IngestionResult;
    try {
      result = await runIngestion(company.id);
    } catch (err) {
      result = { success: false, error: err instanceof Error ? err.message : String(err) };
    }
    const summary = { company: company.name, ...result, seconds: Math.round((Date.now() - t0) / 1000) };
    console.log(`[ingest] ${JSON.stringify(summary)}`);
    return summary;
  });

  const ok = results.filter((r) => r.success).length;
  console.log(`[ingest] done: ${ok}/${results.length} ok in ${Math.round((Date.now() - started) / 1000)}s`);
  return NextResponse.json({
    fixesApplied,
    ok,
    failed: results.length - ok,
    seconds: Math.round((Date.now() - started) / 1000),
    results,
  });
}
