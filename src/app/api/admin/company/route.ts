import { NextRequest, NextResponse } from "next/server";
import { createClient } from "@/lib/supabase/server";
import { createAdminClient } from "@/lib/supabase/admin";

export async function PATCH(request: NextRequest) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();

  if (!user) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const { companyId, careers_url, enabled } = await request.json();
  if (!companyId || typeof companyId !== "string") {
    return NextResponse.json({ error: "companyId required" }, { status: 400 });
  }
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  if (!uuidRegex.test(companyId)) {
    return NextResponse.json({ error: "Invalid companyId" }, { status: 400 });
  }

  if (enabled !== undefined && typeof enabled !== "boolean") {
    return NextResponse.json({ error: "enabled must be true or false" }, { status: 400 });
  }
  if (careers_url !== undefined && careers_url !== null && careers_url !== "") {
    let ok = false;
    try {
      ok = ["http:", "https:"].includes(new URL(String(careers_url)).protocol);
    } catch {}
    if (!ok) {
      return NextResponse.json({ error: "Careers URL must start with http:// or https://" }, { status: 400 });
    }
  }

  const admin = createAdminClient();
  const updates: Record<string, unknown> = {};
  if (careers_url !== undefined) updates.careers_url = careers_url || null;
  if (enabled !== undefined) updates.enabled = enabled;
  if (Object.keys(updates).length === 0) {
    return NextResponse.json({ error: "Nothing to update" }, { status: 400 });
  }

  const { data, error } = await admin
    .from("companies")
    .update(updates)
    .eq("id", companyId)
    .select()
    .single();

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }

  return NextResponse.json(data);
}
