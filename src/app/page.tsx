import { getBoardData } from "@/lib/board-data";
import { boardStateFromRecord } from "@/lib/board-params";
import { JobBoard } from "@/components/job-board";
import { DotField } from "@/components/dot-field";
import { HomeTabs } from "@/components/home-tabs";

export default async function Home({
  searchParams,
}: {
  searchParams: Promise<Record<string, string | string[] | undefined>>;
}) {
  const initialState = boardStateFromRecord(await searchParams);
  const { jobs, companies } = await getBoardData();
  const hiringCount = new Set(jobs.map((j) => j.company_slug)).size;

  return (
    <>
      {/* Hero section */}
      <section className="relative flex min-h-[calc(100vh-5rem)] items-center overflow-hidden -mt-8">
        <div className="grid w-full grid-cols-1 md:grid-cols-2">
          {/* Text — left half, padded to align with header logo */}
          <div className="flex flex-col justify-center px-10 py-16 md:py-0 md:pl-36 md:pr-12">
            <h1 className="text-3xl font-light tracking-tight text-black sm:text-5xl lg:text-6xl xl:text-[5.5rem]">
              Opportunities<br />
              at ScOp Portfolio<br />
              Companies
            </h1>
            <p className="mt-4 text-lg text-gray-500 sm:mt-6 sm:text-xl lg:text-2xl">
              Find roles at leading software and AI companies.
            </p>
            <div className="mt-10 flex items-center gap-6">
              <p className="text-base tracking-wide text-gray-400">
                {hiringCount} Companies Hiring &middot; {jobs.length} Open Roles
              </p>
              <a
                href="#jobs"
                className=""
                aria-label="Scroll to job listings"
              >
                <svg
                  width="24"
                  height="24"
                  viewBox="0 0 24 24"
                  fill="none"
                  className="text-gray-400 transition-colors duration-150 hover:text-black"
                >
                  <path
                    d="M12 5v14m0 0l-6-6m6 6l6-6"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </a>
            </div>
          </div>
          {/* Dot field — right half, trimmed from bottom and right */}
          <div className="hidden h-[min(42rem,74vh)] md:block">
            <DotField />
          </div>
        </div>
      </section>

      {/* Job listings */}
      <section id="jobs" className="px-6 py-10 sm:px-10 md:px-36">
        <HomeTabs
          jobsContent={
            <>
              <h2 className="mb-6 text-4xl font-light tracking-tight text-black">
                Open Positions
              </h2>
              <JobBoard jobs={jobs} companies={companies} initialState={initialState} now={new Date().toISOString()} basePath="/" />
            </>
          }
        />
      </section>
    </>
  );
}
