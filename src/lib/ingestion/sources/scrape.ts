/**
 * Fallback for careers pages that aren't backed by a known ATS: read the page,
 * have Claude list the openings, then read each posting for its description.
 *
 * Guards against hallucinated jobs: a job is kept only if its title appears in
 * the page text or its link appears in the page HTML.
 */
import Hyperbrowser from "@hyperbrowser/sdk";
import { extractJobPostingLd, fetchText, findSalaryInText, htmlToMarkdown, mapLimit } from "../http";
import { extractJson } from "../llm";
import type { SourceJob, SourceResult } from "./types";

const MAX_PAGE_CHARS = 200_000;
const MAX_DETAIL_PAGES = 40;

/** Pages whose readable text is shorter than this are assumed to need JS rendering. */
export const THIN_PAGE_CHARS = 600;

/**
 * Renders a page in a real browser, for sites that block plain requests (403,
 * old TLS) or only show content after JavaScript runs. Uses Firecrawl when
 * FIRECRAWL_API_KEY is set, otherwise Hyperbrowser; returns null if neither works.
 */
export async function renderWithBrowser(url: string): Promise<string | null> {
  if (process.env.FIRECRAWL_API_KEY) {
    try {
      const res = await fetch("https://api.firecrawl.dev/v2/scrape", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${process.env.FIRECRAWL_API_KEY}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          url,
          formats: ["rawHtml"],
          onlyMainContent: false,
          waitFor: 2500,
          proxy: "auto", // retries with stealth proxies only if the basic fetch is blocked
          timeout: 60_000,
        }),
        signal: AbortSignal.timeout(90_000),
      });
      const body = (await res.json()) as { success?: boolean; error?: string; data?: { rawHtml?: string } };
      if (res.ok && body.success && body.data?.rawHtml) return body.data.rawHtml;
      console.warn(`[ingest] firecrawl render failed for ${url}: HTTP ${res.status} ${body.error ?? ""}`);
    } catch (err) {
      console.warn(`[ingest] firecrawl render failed for ${url}:`, err instanceof Error ? err.message : err);
    }
  }

  if (process.env.HYPERBROWSER_API_KEY) {
    try {
      const client = new Hyperbrowser({ apiKey: process.env.HYPERBROWSER_API_KEY });
      const result = await client.scrape.startAndWait({
        url,
        scrapeOptions: { formats: ["html"], onlyMainContent: false, waitFor: 2500 },
      });
      if (result.status === "completed" && result.data?.html) return result.data.html;
      console.warn(`[ingest] hyperbrowser render failed for ${url}: ${result.status} ${result.error ?? ""}`);
    } catch (err) {
      console.warn(`[ingest] hyperbrowser render failed for ${url}:`, err instanceof Error ? err.message : err);
    }
  }
  return null;
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
- "posted" is the date the role was posted, only if the page shows one (e.g. "Posted Nov 3" or "3 days ago"), as YYYY-MM-DD using today's date given below; otherwise "".
- Use "" for any field the page doesn't state. Do not guess locations, departments, salaries, or dates.
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
        required: ["title", "url", "location", "department", "salary", "summary", "posted"],
        properties: {
          title: { type: "string" },
          url: { type: "string" },
          location: { type: "string" },
          department: { type: "string" },
          salary: { type: "string" },
          summary: { type: "string" },
          posted: { type: "string" },
        },
      },
    },
  },
};

interface ListingOutput {
  no_open_roles_stated: boolean;
  jobs: { title: string; url: string; location: string; department: string; salary: string; summary: string; posted: string }[];
}

const DETAIL_SYSTEM = `You read a single job posting page (converted to Markdown) and return its details.

- "description": the full job description in Markdown — overview, responsibilities, requirements, benefits, about the company. Keep headings and bullet points. Remove site navigation, footers, cookie notices, application forms, and the short metadata lines (title, location, department, salary, employment type) at the top.
- "location", "department", "salary": only if explicitly shown; otherwise "".
- "posted": the date the role was posted, only if shown (e.g. "Posted Nov 3", "3 days ago"), as YYYY-MM-DD using today's date given below; otherwise "".
- If the page is not a job posting or has no description, return "" for description.`;

const DETAIL_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["description", "location", "department", "salary", "posted"],
  properties: {
    description: { type: "string" },
    location: { type: "string" },
    department: { type: "string" },
    salary: { type: "string" },
    posted: { type: "string" },
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
    user: `Today is ${today()}.\nCareers page URL: ${careersUrl}\n\n<page>\n${markdown}\n</page>`,
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

  // Several roles sharing one link (e.g. every "Apply" button -> /contact) means the
  // link isn't a posting. Treat those roles as listed inline on the careers page.
  const urlCounts = new Map<string, number>();
  for (const c of candidates) urlCounts.set(c.url, (urlCounts.get(c.url) ?? 0) + 1);
  for (const c of candidates) if ((urlCounts.get(c.url) ?? 0) > 1) c.url = careersPage;

  let detailBudget = MAX_DETAIL_PAGES;
  const jobs: SourceJob[] = await mapLimit(candidates, 4, async (c) => {
    let description = c.summary;
    let location = orNull(c.location);
    let department = orNull(c.department);
    let salary = orNull(c.salary);
    let postedAt: string | null = parsePosted(c.posted);

    if (c.url !== careersPage && detailBudget-- > 0) {
      try {
        const detailHtml = await fetchText(c.url);
        const ld = extractJobPostingLd(detailHtml);
        const detailMd = ld ? "" : htmlToMarkdown(detailHtml).slice(0, MAX_PAGE_CHARS);
        if (ld) {
          if (ld.description.length > description.length) description = ld.description;
          location ??= ld.location;
          department ??= ld.department;
          salary ??= ld.salary;
          postedAt ??= ld.posted_at;
        } else if (detailMd.length > 200) {
          const d = await extractJson<{ description: string; location: string; department: string; salary: string; posted: string }>({
            system: DETAIL_SYSTEM,
            user: `Today is ${today()}.\nJob title: ${c.title}\nURL: ${c.url}\n\n<page>\n${detailMd}\n</page>`,
            schema: DETAIL_SCHEMA,
          });
          if (d.description.length > description.length) description = d.description;
          location ??= orNull(d.location);
          department ??= orNull(d.department);
          salary ??= orNull(d.salary);
          postedAt ??= parsePosted(d.posted);
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
      posted_at: postedAt,
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

function today(): string {
  return new Date().toISOString().slice(0, 10);
}

/** "2026-11-03" -> ISO; anything unparseable or in the future -> null. */
function parsePosted(value: string | undefined): string | null {
  if (!value || !/^\d{4}-\d{2}-\d{2}/.test(value)) return null;
  const d = new Date(`${value.slice(0, 10)}T12:00:00Z`);
  if (Number.isNaN(d.getTime()) || d.getTime() > Date.now() + 86_400_000) return null;
  return d.toISOString();
}
