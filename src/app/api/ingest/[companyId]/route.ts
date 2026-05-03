import { NextRequest, NextResponse } from "next/server";
import { runIngestion } from "@/lib/ingestion/run";

export const maxDuration = 300;

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ companyId: string }> }
) {
  const { companyId } = await params;

  const authHeader = request.headers.get("authorization");
  if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  const result = await runIngestion(companyId);
  return NextResponse.json(result, { status: result.success ? 200 : 500 });
}
