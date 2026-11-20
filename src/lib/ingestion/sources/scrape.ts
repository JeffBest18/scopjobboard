/**
 * Fallback for careers pages that aren't backed by a known ATS: read the page,
 * have Claude list the openings, then read each posting for its description.
 *
 * Guards against hallucinated jobs: a job is kept only if its title appears in
 * the page text or its link appears in the page HTML.
 */
import Hyperbrowser from "@hyperbrowser/sdk";
import { fetchText, findSalaryInText, htmlToMarkdown, mapLimit } from "../http";
import { extractJson } from "../llm";
import type { SourceJob, SourceResult } from "./types";

const MAX_PAGE_CHARS = 200_000;
const MAX_DETAIL_PAGES = 40;

/** Pages whose readable text is shorter than this are assumed to need JS rendering. */
export const THIN_PAGE_CHARS = 600;

export async function renderWithBrowser(url: string): Promise<string | null> {
  if (!process.env.HYPERBROWSER_API_KEY) return null;
  try {
    const client = new Hyperbrowser({ apiKey: process.env.HYPERBROWSER_API_KEY });
    const result = await client.scrape.startAndWait({
      url,
      scrapeOptions: { formats: ["html"], onlyMainContent: false, waitFor: 2500 },
    });
    return result.status === "completed" ? (result.data?.html ?? null) : null;
  } catch (err) {
    console.warn(`[ingest] browser render failed for ${url}:`, err);
    return null;
  }
}

export function readableLength(html: string): number {
  return htmlToMarkdown(html).replace(/\[([^\]]*)\]\([^)]*\)/g, "$1").length;
}

const LISTING_SYSTEM = `You read a company's careers page (converted to Markdown) and list its currently open job postings.

Rules:
- Only include roles that are explicitly listed on the page as open positions. Never invent roles.
- Do not include generic links such as "View all jobs", "General application", or "Talent community" unless the page lists nothing else.
- "url" is the link to that specific posting exactly as it appears in the Markdown (relative links are fine). Use "" if the role has no link of its own.
- "summary" is any description text shown for the role on this page, in Markdown; "" if none.
- Use "" for any field the page doesn't state. Do not guess locations, departments, or salaries.
- Set "no_open_roles_stated" to true only if the page explicitly says there are currently no open positions.`;

const LISTING_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["no_open_roles_stated", "jobs"],
  properties: {
    no_open_roles_stated: { type: "boolean" },
    jobs: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["title", "url", "location", "department", "salary", "summary"],
        properties: {
          title: { type: "string" },
          url: { type: "string" },
          location: { type: "string" },
          department: { type: "string" },
          salary: { type: "string" },
          summary: { type: "string" },
        },
      },
    },
  },
};

interface ListingOutput {
  no_open_roles_stated: boolean;
  jobs: { title: string; url: string; location: string; department: string; salary: string; summary: string }[];
}

const DETAIL_SYSTEM = `You read a single job posting page (converted to Markdown) and return its details.

- "description": the full job description in Markdown — overview, responsibilities, requirements, benefits, about the company. Keep headings and bullet points. Remove site navigation, footers, cookie notices, application forms, and the short metadata lines (title, location, department, salary, employment type) at the top.
- "location", "department", "salary": only if explicitly shown; otherwise "".
- If the page is not a job posting or has no description, return "" for description.`;

const DETAIL_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["description", "location", "department", "salary"],
  properties: {
    description: { type: "string" },
    location: { type: "string" },
    department: { type: "string" },
    salary: { type: "string" },
  },
};

const norm = (s: string) => s.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
const orNull = (s: string | undefined) => (s && s.trim() ? s.trim() : null);

export async function scrapeCareersPage(careersUrl: string, html: string): Promise<SourceResult> {
  let markdown = htmlToMarkdown(html);
  if (markdown.length > MAX_PAGE_CHARS) {
    console.warn(`[ingest] ${careersUrl}: page is ${markdown.length} chars, using the first ${MAX_PAGE_CHARS}`);
    markdown = markdown.slice(0, MAX_PAGE_CHARS);
  }

  const listing = await extractJson<ListingOutput>({
    system: LISTING_SYSTEM,
    user: `Careers page URL: ${careersUrl}\n\n<page>\n${markdown}\n</page>`,
    schema: LISTING_SCHEMA,
  });

  const pageText = norm(markdown);
  const careersPage = stripHash(careersUrl);
  const seen = new Set<string>();
  const candidates = listing.jobs
    .map((j) => {
      let url = careersPage;
      if (j.url) {
        try {
          url = stripHash(new URL(j.url, careersUrl).toString());
        } catch {
          // Unparseable link: treat as an inline listing.
        }
      }
      return { ...j, title: j.title.trim(), url };
    })
    .filter((j) => {
      if (!j.title) return false;
      const grounded = pageText.includes(norm(j.title)) || (j.url !== careersPage && html.includes(new URL(j.url).pathname));
      if (!grounded) console.warn(`[ingest] ${careersUrl}: dropping ungrounded job "${j.title}"`);
      const key = `${j.url}#${norm(j.title)}`;
      if (!grounded || seen.has(key)) return false;
      seen.add(key);
      return true;
    });

  let detailBudget = MAX_DETAIL_PAGES;
  const jobs: SourceJob[] = await mapLimit(candidates, 4, async (c) => {
    let description = c.summary;
    let location = orNull(c.location);
    let department = orNull(c.department);
    let salary = orNull(c.salary);

    if (c.url !== careersPage && detailBudget-- > 0) {
      try {
        const detailMd = htmlToMarkdown(await fetchText(c.url)).slice(0, MAX_PAGE_CHARS);
        if (detailMd.length > 200) {
          const d = await extractJson<{ description: string; location: string; department: string; salary: string }>({
            system: DETAIL_SYSTEM,
            user: `Job title: ${c.title}\nURL: ${c.url}\n\n<page>\n${detailMd}\n</page>`,
            schema: DETAIL_SCHEMA,
          });
          if (d.description.length > description.length) description = d.description;
          location ??= orNull(d.location);
          department ??= orNull(d.department);
          salary ??= orNull(d.salary);
        }
      } catch (err) {
        console.warn(`[ingest] detail fetch failed for ${c.url}:`, err instanceof Error ? err.message : err);
      }
    }

    return {
      title: c.title,
      url: c.url,
      location,
      department_raw: department,
      salary_raw: salary ?? findSalaryInText(description),
      description,
    };
  });

  return {
    method: "scrape",
    jobs,
    authoritative: jobs.length === 0 && listing.no_open_roles_stated,
  };
}

function stripHash(url: string): string {
  const u = new URL(url);
  u.hash = "";
  return u.toString();
}
