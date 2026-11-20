"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";

/** PATCH /api/admin/company. Returns an error message, or null on success. */
export async function updateCompany(
  companyId: string,
  changes: { enabled?: boolean; careers_url?: string }
): Promise<string | null> {
  try {
    const res = await fetch("/api/admin/company", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ companyId, ...changes }),
    });
    if (res.ok) return null;
    if (res.status === 401) return "Your session expired. Sign in again.";
    const body = await res.json().catch(() => ({}));
    return body.error ?? `Request failed (HTTP ${res.status})`;
  } catch {
    return "Network error. Check your connection and try again.";
  }
}

/** An on/off switch with a visible text label, so its state is obvious. */
export function Switch({
  on,
  busy,
  onToggle,
  label,
}: {
  on: boolean;
  busy: boolean;
  onToggle: () => void;
  label: string;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={on}
      aria-label={`Show ${label} on the job board`}
      onClick={onToggle}
      disabled={busy}
      className="flex items-center gap-2 disabled:opacity-60"
    >
      <span
        className={`relative inline-flex h-5 w-9 shrink-0 items-center rounded-full transition-colors duration-150 ${
          on ? "bg-emerald-500" : "bg-gray-300"
        }`}
      >
        <span
          className={`inline-block h-4 w-4 rounded-full bg-white shadow transition-transform duration-150 ${
            on ? "translate-x-[18px]" : "translate-x-0.5"
          }`}
        />
      </span>
      <span className={`whitespace-nowrap text-xs font-medium ${on ? "text-emerald-700" : "text-gray-500"}`}>
        {busy ? "Saving…" : on ? "Shown" : "Hidden"}
      </span>
    </button>
  );
}

/** Show/hide switch with its own feedback, for the company detail page. */
export function VisibilityControl({ companyId, name, enabled }: { companyId: string; name: string; enabled: boolean }) {
  const router = useRouter();
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<{ ok: boolean; text: string } | null>(null);

  async function toggle() {
    setBusy(true);
    setMessage(null);
    const error = await updateCompany(companyId, { enabled: !enabled });
    setMessage(error ? { ok: false, text: error } : { ok: true, text: enabled ? "Hidden from the job board" : "Shown on the job board" });
    setBusy(false);
    router.refresh();
  }

  return (
    <div className="flex flex-wrap items-center gap-3 rounded-lg border border-gray-200 px-4 py-3">
      <span className="text-sm text-gray-600">On job board</span>
      <Switch on={enabled} busy={busy} onToggle={toggle} label={name} />
      {message && (
        <span role="status" className={`text-xs ${message.ok ? "text-emerald-700" : "text-red-600"}`}>
          {message.text}
        </span>
      )}
    </div>
  );
}
