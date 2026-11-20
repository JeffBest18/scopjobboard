import { NextRequest, NextResponse } from "next/server";
import { getFreshnessCheck } from "@/lib/freshness";
import { createAdminClient } from "@/lib/supabase/admin";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ job_key: string }> }
) {
  const { job_key } = await params;
  const supabase = createAdminClient();

  // Look up job from live_jobs
  const { data: job } = await supabase
    .from("live_jobs")
    .select("job_url, company_id, created_at")
    .eq("job_key", job_key)
    .limit(1)
    .single();

  if (!job || !(await getFreshnessCheck(supabase)).isCurrent(job)) {
    return NextResponse.json({ error: "Job not found" }, { status: 404 });
  }

  // Record click
  await supabase.from("clicks").insert({
    job_key,
    company_id: job.company_id,
  });

  // 302 redirect to the stored job URL
  return NextResponse.redirect(job.job_url, 302);
}
