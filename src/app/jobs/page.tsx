import { getBoardData } from "@/lib/board-data";
import { boardStateFromRecord } from "@/lib/board-params";
import { JobBoard } from "@/components/job-board";

export const metadata = {
  title: "Portfolio Job Directory",
  description:
    "Browse open roles across our portfolio companies and find your next opportunity.",
};

export default async function JobsPage({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const initialState = boardStateFromRecord(await searchParams);
  const { jobs, companies } = await getBoardData();

  return (
    <div className="mx-auto max-w-6xl px-4 py-6 sm:px-6 sm:py-8">
      <h1 className="mb-6 text-4xl font-light tracking-tight text-black">
        Open Positions
      </h1>
      <JobBoard jobs={jobs} companies={companies} initialState={initialState} now={new Date().toISOString()} basePath="/jobs" />
    </div>
  );
}
