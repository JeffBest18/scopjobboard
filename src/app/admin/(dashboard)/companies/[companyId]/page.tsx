import Link from "next/link";
import { notFound } from "next/navigation";
import { createAdminClient } from "@/lib/supabase/admin";
import { CompanyJobs } from "./company-jobs";
import { VisibilityControl } from "../../company-controls";
import type { Job, Snapshot } from "@/lib/types";

export default async function CompanyJobsPage({
  params,
}: {
  params: Promise<{ companyId: string }>;
}) {
  const { companyId } = await params;
  const admin = createAdminClient();

  const { data: company } = await admin
    .from("companies")
    .select("id, name, careers_url, enabled")
    .eq("id", companyId)
    .single();

  if (!company) notFound();

  // Get latest published snapshot
  const { data: snapshot } = await admin
    .from("snapshots")
    .select("id")
    .eq("company_id", companyId)
    .eq("status", "published")
    .order("created_at", { ascending: false })
    .limit(1)
    .single();

  let jobs: Job[] = [];
  if (snapshot) {
    const { data } = await admin
      .from("jobs")
      .select("*")
      .eq("snapshot_id", snapshot.id)
      .order("title");

    jobs = (data as Job[]) ?? [];
  }

  // Get all snapshots for history log (most recent first, limit 20)
  const { data: snapshots } = await admin
    .from("snapshots")
    .select("id, status, job_count, error_message, created_at")
    .eq("company_id", companyId)
    .order("created_at", { ascending: false })
    .limit(20);

  const snapshotHistory = (snapshots as Snapshot[]) ?? [];

  return (
    <div>
      <Link
        href="/admin"
        className="mb-4 inline-block text-xs text-gray-400 transition-colors duration-150 hover:text-black"
      >
        &larr; Back to companies
      </Link>
      <h1 className="mb-1 text-2xl font-semibold tracking-tight text-black">
        {company.name}
      </h1>
      <div className="mb-8 flex items-center gap-4">
        <p className="text-sm text-gray-500">
          {jobs.length} {jobs.length === 1 ? "job" : "jobs"} in published snapshot
        </p>
        {company.careers_url && (
          <a
            href={company.careers_url}
            target="_blank"
            rel="noopener noreferrer"
            className="text-xs text-gray-400 transition-colors duration-150 hover:text-black"
          >
            Careers page &rarr;
          </a>
        )}
      </div>

      <div className="mb-8">
        <VisibilityControl companyId={company.id} name={company.name} enabled={company.enabled} />
      </div>

      <CompanyJobs companyId={companyId} jobs={jobs} />

      {/* Snapshot History */}
      <div className="mt-12">
        <h2 className="mb-4 text-lg font-semibold tracking-tight text-black">
          Snapshot History
        </h2>
        {snapshotHistory.length === 0 ? (
          <p className="text-sm text-gray-400">No snapshots yet.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="pb-2 text-xs font-medium text-gray-500">Time</th>
                  <th className="pb-2 text-xs font-medium text-gray-500">Status</th>
                  <th className="pb-2 text-xs font-medium text-gray-500">Jobs</th>
                  <th className="pb-2 text-xs font-medium text-gray-500">Details</th>
                </tr>
              </thead>
              <tbody>
                {snapshotHistory.map((s) => {
                  const statusColor =
                    s.status === "published"
                      ? "text-emerald-600"
                      : s.status === "failed"
                        ? "text-red-600"
                        : "text-gray-400";
                  return (
                    <tr key={s.id} className="border-b border-gray-100">
                      <td className="px-1 py-3 text-xs text-gray-500">
                        {new Date(s.created_at).toLocaleDateString("en-US", {
                          month: "short",
                          day: "numeric",
                          year: "numeric",
                          hour: "2-digit",
                          minute: "2-digit",
                        })}
                      </td>
                      <td className="px-1 py-3">
                        <span className={`text-xs font-medium ${statusColor}`}>
                          {s.status}
                        </span>
                      </td>
                      <td className="px-1 py-3 text-xs text-gray-500">
                        {s.job_count}
                      </td>
                      <td className="px-1 py-3 text-xs text-gray-500">
                        {s.status === "failed" && s.error_message ? (
                          <span className="text-red-600">{s.error_message}</span>
                        ) : s.status === "published" ? (
                          <span className="text-gray-400">Success</span>
                        ) : (
                          <span className="text-gray-400">—</span>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
