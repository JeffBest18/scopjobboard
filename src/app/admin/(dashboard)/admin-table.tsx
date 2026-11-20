"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { Switch, updateCompany } from "./company-controls";

interface CompanyRow {
  id: string;
  name: string;
  slug: string;
  careers_url: string | null;
  enabled: boolean;
  latest_snapshot_status: string | null;
  latest_snapshot_at: string | null;
  latest_error: string | null;
  published_job_count: number;
}

type Feedback = { kind: "ok" | "error"; text: string } | null;

export function AdminTable({ rows }: { rows: CompanyRow[] }) {
  const shown = rows.filter((r) => r.enabled).length;
  return (
    <div>
      <p className="mb-4 text-xs text-gray-500">
        {shown} of {rows.length} companies shown on the job board. Hidden companies don&apos;t appear on the
        board and are skipped by the daily refresh.
      </p>
      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] text-left">
          <thead>
            <tr className="border-b border-gray-100">
              <th className="pb-2 text-xs font-medium text-gray-500">Company</th>
              <th className="pb-2 text-xs font-medium text-gray-500">On job board</th>
              <th className="pb-2 text-xs font-medium text-gray-500">Jobs</th>
              <th className="pb-2 text-xs font-medium text-gray-500">Last refresh</th>
              <th className="pb-2 text-xs font-medium text-gray-500">Actions</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <CompanyRowItem key={row.id} row={row} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CompanyRowItem({ row }: { row: CompanyRow }) {
  const router = useRouter();
  const [editingUrl, setEditingUrl] = useState(false);
  const [urlValue, setUrlValue] = useState(row.careers_url ?? "");
  const [loading, setLoading] = useState<"rerun" | "toggle" | "url" | null>(null);
  const [feedback, setFeedback] = useState<Feedback>(null);

  async function rerun() {
    setLoading("rerun");
    setFeedback(null);
    try {
      const res = await fetch("/api/admin/rerun", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ companyId: row.id }),
      });
      const body = await res.json().catch(() => ({}));
      if (!res.ok) {
        setFeedback({
          kind: "error",
          text: res.status === 401 ? "Your session expired. Sign in again." : body.error ?? `Re-run failed (HTTP ${res.status})`,
        });
      } else if (body.success) {
        const stale = body.staleCount ? `, ${body.staleCount} stale removed` : "";
        setFeedback({ kind: "ok", text: `Updated: ${body.jobCount} ${body.jobCount === 1 ? "job" : "jobs"}${stale}` });
      } else {
        setFeedback({ kind: "error", text: body.error ?? "Re-run failed" });
      }
    } catch {
      setFeedback({ kind: "error", text: "Network error during re-run." });
    }
    setLoading(null);
    router.refresh();
  }

  async function toggleEnabled() {
    setLoading("toggle");
    setFeedback(null);
    const error = await updateCompany(row.id, { enabled: !row.enabled });
    setFeedback(
      error
        ? { kind: "error", text: error }
        : { kind: "ok", text: row.enabled ? "Hidden from the job board" : "Shown on the job board" }
    );
    setLoading(null);
    router.refresh();
  }

  async function saveUrl() {
    setLoading("url");
    setFeedback(null);
    const error = await updateCompany(row.id, { careers_url: urlValue.trim() });
    setFeedback(error ? { kind: "error", text: error } : { kind: "ok", text: "Careers URL saved. Re-run to refresh jobs." });
    setLoading(null);
    if (!error) setEditingUrl(false);
    router.refresh();
  }

  const statusColor =
    row.latest_snapshot_status === "published"
      ? "bg-emerald-500"
      : row.latest_snapshot_status === "failed"
        ? "bg-red-500"
        : "bg-gray-300";

  return (
    <>
      <tr className={`border-b border-gray-100 align-top ${row.enabled ? "" : "bg-gray-50/60"}`}>
        <td className="py-3 pr-4">
          <Link
            href={`/admin/companies/${row.id}`}
            className={`text-sm font-medium hover:underline ${row.enabled ? "text-black" : "text-gray-400"}`}
          >
            {row.name}
          </Link>
          {editingUrl ? (
            <div className="mt-1 flex items-center gap-2">
              <input
                value={urlValue}
                onChange={(e) => setUrlValue(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && saveUrl()}
                placeholder="https://example.com/careers"
                autoFocus
                className="w-72 rounded border border-gray-300 px-2 py-0.5 text-xs text-gray-600 focus:outline-none focus:ring-1 focus:ring-gray-400"
              />
              <button
                onClick={saveUrl}
                disabled={loading === "url"}
                className="text-xs font-medium text-black hover:text-gray-600 disabled:opacity-50"
              >
                {loading === "url" ? "Saving…" : "Save"}
              </button>
              <button
                onClick={() => {
                  setEditingUrl(false);
                  setUrlValue(row.careers_url ?? "");
                }}
                className="text-xs text-gray-400 hover:text-black"
              >
                Cancel
              </button>
            </div>
          ) : (
            <button
              onClick={() => setEditingUrl(true)}
              className="mt-0.5 block max-w-[18rem] truncate text-left text-xs text-gray-400 hover:text-black"
              title={row.careers_url ? `${row.careers_url} (click to edit)` : "Add a careers URL"}
            >
              {row.careers_url ?? "No careers URL (jobs managed by hand)"}
            </button>
          )}
        </td>
        <td className="py-3 pr-4">
          <Switch on={row.enabled} busy={loading === "toggle"} onToggle={toggleEnabled} label={row.name} />
        </td>
        <td className="py-3 pr-4 text-sm text-black">{row.enabled ? row.published_job_count : "—"}</td>
        <td className="py-3 pr-4">
          <span className="flex items-center gap-2 text-xs text-gray-500">
            <span className={`h-2 w-2 shrink-0 rounded-full ${statusColor}`} aria-hidden="true" />
            {row.latest_snapshot_at
              ? new Date(row.latest_snapshot_at).toLocaleDateString("en-US", {
                  month: "short",
                  day: "numeric",
                  hour: "2-digit",
                  minute: "2-digit",
                })
              : "Never"}
            <span className="sr-only">{row.latest_snapshot_status}</span>
          </span>
        </td>
        <td className="py-3">
          <button
            onClick={rerun}
            disabled={loading === "rerun" || !row.careers_url || !row.enabled}
            title={
              !row.careers_url
                ? "No careers URL configured"
                : !row.enabled
                  ? "Show the company first"
                  : "Refresh this company's jobs now"
            }
            className="rounded border border-gray-300 bg-white px-3 py-1 text-xs font-medium text-gray-600 transition-colors duration-150 hover:bg-gray-50 disabled:opacity-40"
          >
            {loading === "rerun" ? "Running…" : "Re-run"}
          </button>
        </td>
      </tr>
      {(feedback || (row.latest_snapshot_status === "failed" && row.latest_error && row.enabled)) && (
        <tr className="border-b border-gray-100">
          <td colSpan={5} className="pb-2">
            {feedback ? (
              <p role="status" className={`text-xs ${feedback.kind === "ok" ? "text-emerald-700" : "text-red-600"}`}>
                {feedback.text}
              </p>
            ) : (
              <p className="text-xs text-red-600">Last refresh failed: {row.latest_error}</p>
            )}
          </td>
        </tr>
      )}
    </>
  );
}
