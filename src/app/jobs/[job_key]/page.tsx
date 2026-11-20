import { notFound } from "next/navigation";
import Image from "next/image";
import Link from "next/link";
import type { Metadata } from "next";
import { createClient } from "@/lib/supabase/server";
import type { LiveJob } from "@/lib/types";
import { formatSalary } from "@/lib/ingestion/clean-salary";
import { getFreshnessCheck } from "@/lib/freshness";
import { MarkdownRenderer } from "@/components/markdown-renderer";

interface Props {
  params: Promise<{ job_key: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { job_key } = await params;
  const supabase = await createClient();

  const { data: job } = await supabase
    .from("live_jobs")
    .select("*")
    .eq("job_key", job_key)
    .limit(1)
    .single();

  if (!job || !(await getFreshnessCheck(supabase)).isCurrent(job)) {
    return { title: "Job Not Found" };
  }

  const siteUrl = process.env.NEXT_PUBLIC_SITE_URL ?? "";
  const description = (job.description || `${job.title} at ${job.company_name}`)
    .replace(/[#*_`\[\]]/g, "")
    .slice(0, 155)
    .trim();

  return {
    title: `${job.title} at ${job.company_name}`,
    description,
    alternates: {
      canonical: `${siteUrl}/jobs/${job_key}`,
    },
    openGraph: {
      title: `${job.title} at ${job.company_name}`,
      description,
      url: `${siteUrl}/jobs/${job_key}`,
      type: "website",
    },
    twitter: {
      card: "summary",
      title: `${job.title} at ${job.company_name}`,
      description,
    },
  };
}

export default async function JobDetailPage({ params }: Props) {
  const { job_key } = await params;
  const supabase = await createClient();

  const { data } = await supabase
    .from("live_jobs")
    .select("*")
    .eq("job_key", job_key)
    .limit(1)
    .single();

  if (!data || !(await getFreshnessCheck(supabase)).isCurrent(data)) {
    notFound();
  }

  const job = data as LiveJob;

  // Fetch company homepage URL
  const { data: company } = await supabase
    .from("companies")
    .select("homepage_url")
    .eq("id", job.company_id)
    .single();

  const homepageUrl = company?.homepage_url ?? null;

  return (
    <div className="mx-auto max-w-4xl px-6 py-8 sm:px-10 sm:py-10">
      <Link
        href="/#jobs"
        className="mb-8 inline-block text-sm text-gray-400 transition-colors duration-150 hover:text-black"
      >
        &larr; Back to all jobs
      </Link>

      {/* Top card */}
      <div className="rounded-lg border border-gray-100 px-4 py-4 sm:px-6 sm:py-5">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between sm:gap-6">
          <div className="flex items-start gap-3 sm:gap-4">
            <Image
              src={`/logos/${job.company_slug}.png`}
              alt={job.company_name}
              width={56}
              height={56}
              className="rounded shrink-0"
            />
            <div className="min-w-0">
              <h1 className="text-xl font-semibold tracking-tight text-black sm:text-2xl">
                {job.title}
              </h1>
              <div className="mt-1 flex items-center gap-1.5">
                {homepageUrl ? (
                  <a
                    href={homepageUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-sm text-gray-500 transition-colors duration-150 hover:text-black sm:text-base"
                  >
                    {job.company_name}
                    <svg
                      width="14"
                      height="14"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      className="ml-1 inline-block -translate-y-px"
                    >
                      <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                      <polyline points="15 3 21 3 21 9" />
                      <line x1="10" y1="14" x2="21" y2="3" />
                    </svg>
                  </a>
                ) : (
                  <span className="text-sm text-gray-500 sm:text-base">
                    {job.company_name}
                  </span>
                )}
              </div>
            </div>
          </div>
          <a
            href={`/r/${job.job_key}`}
            target="_blank"
            rel="noopener noreferrer"
            className="inline-flex shrink-0 items-center gap-2 self-start rounded bg-black px-5 py-2.5 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90"
          >
            View on {job.company_name}
            <svg
              width="16"
              height="16"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
              <polyline points="15 3 21 3 21 9" />
              <line x1="10" y1="14" x2="21" y2="3" />
            </svg>
          </a>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-3">
          <span className="rounded-full border border-gray-200 px-3 py-1 text-sm font-medium text-gray-500">
            {job.department_tag}
          </span>
          {job.location && (
            <span className="flex items-center gap-1.5 text-sm text-gray-400">
              <svg
                width="15"
                height="15"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="shrink-0"
              >
                <path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z" />
                <circle cx="12" cy="10" r="3" />
              </svg>
              {job.location}
            </span>
          )}
          {job.salary_raw && (
            <span className="flex items-center gap-1.5 text-sm text-gray-400">
              <svg
                width="15"
                height="15"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                strokeLinecap="round"
                strokeLinejoin="round"
                className="shrink-0"
              >
                <line x1="12" y1="1" x2="12" y2="23" />
                <path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6" />
              </svg>
              {formatSalary(job.salary_raw)}
            </span>
          )}
        </div>
      </div>

      {/* Description */}
      <div className="mt-8 text-base">
        {job.description ? (
          <MarkdownRenderer content={job.description} />
        ) : (
          <p className="text-gray-500">
            Full details are on {job.company_name}&apos;s careers site.
          </p>
        )}
      </div>

      {/* Bottom CTA */}
      <div className="mt-10">
        <a
          href={`/r/${job.job_key}`}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-flex items-center gap-2 rounded bg-black px-5 py-2.5 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-90"
        >
          View on {job.company_name}
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <polyline points="15 3 21 3 21 9" />
            <line x1="10" y1="14" x2="21" y2="3" />
          </svg>
        </a>
      </div>
    </div>
  );
}
