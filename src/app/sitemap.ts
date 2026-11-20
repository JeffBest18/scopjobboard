import type { MetadataRoute } from "next";

// Regenerate hourly; without this the sitemap was frozen at build time.
export const revalidate = 3600;
import { createAdminClient } from "@/lib/supabase/admin";
import { getFreshnessCheck } from "@/lib/freshness";

export default async function sitemap(): Promise<MetadataRoute.Sitemap> {
  const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? "http://localhost:3000";
  const supabase = createAdminClient();

  const [{ data: jobs }, freshness] = await Promise.all([
    supabase.from("live_jobs").select("job_key, company_id, created_at"),
    getFreshnessCheck(supabase),
  ]);

  const jobEntries: MetadataRoute.Sitemap = (jobs ?? []).filter(freshness.isCurrent).map((job) => ({
    url: `${siteUrl}/jobs/${job.job_key}`,
    changeFrequency: "weekly",
    lastModified: new Date(),
  }));

  return [
    {
      url: `${siteUrl}/jobs`,
      changeFrequency: "weekly",
      lastModified: new Date(),
    },
    ...jobEntries,
  ];
}
