/**
 * Adapters for applicant-tracking systems with public job-board APIs.
 *
 * Most portfolio companies host their openings on one of these. Reading the
 * ATS's JSON feed is fast (one or two requests), free, complete, and exact,
 * which is why it is always tried before scraping.
 */
import {
  decodeEntities,
  extractJobPostingLd,
  fetchJson,
  fetchText,
  findSalaryInText,
  htmlToMarkdown,
  mapLimit,
} from "../http";
import type { SourceJob } from "./types";

export type AtsProvider =
  | "greenhouse"
  | "ashby"
  | "lever"
  | "rippling"
  | "bamboohr"
  | "teamtailor"
  | "workable"
  | "recruitee"
  | "smartrecruiters"
  | "jazzhr";

export interface AtsRef {
  provider: AtsProvider;
  /** Board token / account slug. For Teamtailor, the careers-site origin. */
  token: string;
}

const IGNORED_TOKENS = new Set(["embed", "api", "v1", "jobs", "www", "js", "app", "static", "assets"]);

const PATTERNS: { provider: AtsProvider; re: RegExp }[] = [
  { provider: "ashby", re: /api\.ashbyhq\.com\/posting-api\/job-board\/([A-Za-z0-9._%-]+)/i },
  { provider: "ashby", re: /jobs\.ashbyhq\.com\/([A-Za-z0-9._%-]+)/i },
  { provider: "greenhouse", re: /boards-api\.greenhouse\.io\/v1\/boards\/([A-Za-z0-9_-]+)/i },
  { provider: "greenhouse", re: /greenhouse\.io\/embed\/job_board(?:\/js)?\?for=([A-Za-z0-9_-]+)/i },
  { provider: "greenhouse", re: /(?:job-boards|boards)(?:\.eu)?\.greenhouse\.io\/([A-Za-z0-9_-]+)/i },
  { provider: "lever", re: /api\.lever\.co\/v0\/postings\/([A-Za-z0-9._-]+)/i },
  { provider: "lever", re: /jobs\.lever\.co\/([A-Za-z0-9._-]+)/i },
  { provider: "rippling", re: /ats\.rippling\.com\/(?:[a-z]{2}(?:-[A-Z]{2})?\/)?([A-Za-z0-9_-]+)/i },
  { provider: "bamboohr", re: /\/\/([a-z0-9-]+)\.bamboohr\.com\/(?:careers|jobs)/i },
  { provider: "workable", re: /apply\.workable\.com\/([A-Za-z0-9_-]+)/i },
  { provider: "recruitee", re: /\/\/([a-z0-9-]+)\.recruitee\.com/i },
  { provider: "smartrecruiters", re: /(?:jobs|careers)\.smartrecruiters\.com\/([A-Za-z0-9_-]+)/i },
  { provider: "jazzhr", re: /\/\/([a-z0-9-]+)\.applytojob\.com/i },
];

/**
 * Finds an ATS board reference in a URL or an HTML document. When scanning
 * HTML, the provider with the most mentions wins, so a stray footer link
 * doesn't beat the actual embedded board.
 */
export function detectAts(text: string, pageUrl?: string): AtsRef | null {
  const counts = new Map<string, { ref: AtsRef; n: number }>();
  for (const { provider, re } of PATTERNS) {
    const global = new RegExp(re.source, "gi");
    for (const m of text.matchAll(global)) {
      const token = decodeURIComponent(m[1]).replace(/[\\"']+$/, "");
      if (!token || IGNORED_TOKENS.has(token.toLowerCase())) continue;
      const key = `${provider}:${token.toLowerCase()}`;
      const entry = counts.get(key) ?? { ref: { provider, token }, n: 0 };
      entry.n++;
      counts.set(key, entry);
    }
  }
  if (counts.size > 0) {
    return [...counts.values()].sort((a, b) => b.n - a.n)[0].ref;
  }
  // Teamtailor careers sites live on the company's own domain.
  if (pageUrl && /teamtailor-cdn\.com|teamtailor\.com\/assets/i.test(text)) {
    return { provider: "teamtailor", token: new URL(pageUrl).origin };
  }
  return null;
}

export async function fetchAtsJobs(ref: AtsRef): Promise<SourceJob[]> {
  switch (ref.provider) {
    case "greenhouse":
      return greenhouse(ref.token);
    case "ashby":
      return ashby(ref.token);
    case "lever":
      return lever(ref.token);
    case "rippling":
      return rippling(ref.token);
    case "bamboohr":
      return bamboohr(ref.token);
    case "teamtailor":
      return teamtailor(ref.token);
    case "workable":
      return workable(ref.token);
    case "recruitee":
      return recruitee(ref.token);
    case "smartrecruiters":
      return smartrecruiters(ref.token);
    case "jazzhr":
      return jazzhr(ref.token);
  }
}

function iso(value: string | number | null | undefined): string | null {
  if (value === null || value === undefined || value === "") return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function job(partial: Omit<SourceJob, "salary_raw"> & { salary_raw?: string | null }): SourceJob {
  return {
    ...partial,
    title: partial.title.trim(),
    salary_raw: partial.salary_raw ?? findSalaryInText(partial.description),
  };
}

// ---------------------------------------------------------------- Greenhouse

async function greenhouse(token: string): Promise<SourceJob[]> {
  const data = await fetchJson<{
    jobs: {
      title: string;
      absolute_url: string;
      location?: { name?: string };
      departments?: { name: string }[];
      content?: string;
      first_published?: string;
    }[];
  }>(`https://boards-api.greenhouse.io/v1/boards/${encodeURIComponent(token)}/jobs?content=true`);

  return data.jobs.map((j) =>
    job({
      title: j.title,
      url: j.absolute_url,
      location: j.location?.name || null,
      department_raw: j.departments?.[0]?.name || null,
      description: htmlToMarkdown(j.content),
      posted_at: iso(j.first_published),
    })
  );
}

// --------------------------------------------------------------------- Ashby

async function ashby(token: string): Promise<SourceJob[]> {
  const data = await fetchJson<{
    jobs: {
      title: string;
      jobUrl: string;
      location?: string;
      secondaryLocations?: { location: string }[];
      department?: string;
      team?: string;
      isListed?: boolean;
      isRemote?: boolean;
      descriptionHtml?: string;
      publishedAt?: string;
      shouldDisplayCompensationOnJobPostings?: boolean;
      compensation?: { compensationTierSummary?: string; scrapeableCompensationSalarySummary?: string };
    }[];
  }>(`https://api.ashbyhq.com/posting-api/job-board/${encodeURIComponent(token)}?includeCompensation=true`);

  return data.jobs
    .filter((j) => j.isListed !== false)
    .map((j) => {
      const description = htmlToMarkdown(j.descriptionHtml);
      const locations = [j.location, ...(j.secondaryLocations ?? []).map((l) => l.location)].filter(Boolean);
      const comp = j.shouldDisplayCompensationOnJobPostings
        ? j.compensation?.scrapeableCompensationSalarySummary || j.compensation?.compensationTierSummary
        : null;
      return job({
        title: j.title,
        url: j.jobUrl,
        location: locations.length ? locations.join(" · ") : j.isRemote ? "Remote" : null,
        department_raw: j.department || j.team || null,
        salary_raw: comp || findSalaryInText(description),
        description,
        posted_at: iso(j.publishedAt),
      });
    });
}

// --------------------------------------------------------------------- Lever

async function lever(token: string): Promise<SourceJob[]> {
  const host = "https://api.lever.co";
  const data = await fetchJson<
    {
      text: string;
      hostedUrl: string;
      categories?: { location?: string; department?: string; team?: string; allLocations?: string[] };
      description?: string;
      lists?: { text: string; content: string }[];
      additional?: string;
      salaryRange?: { min?: number; max?: number; currency?: string };
      createdAt?: number;
    }[]
  >(`${host}/v0/postings/${encodeURIComponent(token)}?mode=json`);

  return data.map((j) => {
    const html = [
      j.description ?? "",
      ...(j.lists ?? []).map((l) => `<h3>${l.text}</h3><ul>${l.content}</ul>`),
      j.additional ?? "",
    ].join("\n");
    const description = htmlToMarkdown(html);
    const range = j.salaryRange;
    const salary =
      range?.min && range?.max
        ? `$${range.min.toLocaleString("en-US")} - $${range.max.toLocaleString("en-US")}`
        : null;
    return job({
      title: j.text,
      url: j.hostedUrl,
      location: j.categories?.allLocations?.join(" · ") || j.categories?.location || null,
      department_raw: j.categories?.department || j.categories?.team || null,
      salary_raw: salary ?? findSalaryInText(description),
      description,
      posted_at: iso(j.createdAt),
    });
  });
}

// ------------------------------------------------------------------ Rippling

async function rippling(token: string): Promise<SourceJob[]> {
  const base = `https://api.rippling.com/platform/api/ats/v1/board/${encodeURIComponent(token)}/jobs`;
  const list = await fetchJson<
    { uuid: string; name: string; url: string; department?: { label?: string }; workLocation?: { label?: string } }[]
  >(base);

  // The list endpoint returns one row per (job, location); fold them together.
  const byId = new Map<string, { row: (typeof list)[number]; locations: string[] }>();
  for (const row of list) {
    const entry = byId.get(row.uuid) ?? { row, locations: [] };
    if (row.workLocation?.label) entry.locations.push(row.workLocation.label);
    byId.set(row.uuid, entry);
  }

  return mapLimit([...byId.values()], 5, async ({ row, locations }) => {
    let description = "";
    let salary: string | null = null;
    let postedAt: string | null = null;
    try {
      const detail = await fetchJson<{
        createdOn?: string;
        description?: { company?: string; role?: string };
        payRangeDetails?: { rangeStart?: number; rangeEnd?: number; currency?: string }[];
      }>(`${base}/${row.uuid}`);
      // Role first: the company blurb is identical on every posting.
      description = htmlToMarkdown([detail.description?.role, detail.description?.company].filter(Boolean).join("\n"));
      postedAt = iso(detail.createdOn);
      const pay = detail.payRangeDetails?.[0];
      if (pay?.rangeStart && pay?.rangeEnd) {
        salary = `$${pay.rangeStart.toLocaleString("en-US")} - $${pay.rangeEnd.toLocaleString("en-US")}`;
      }
    } catch {
      // Keep the listing even if the detail call fails.
    }
    return job({
      title: row.name,
      url: row.url,
      location: [...new Set(locations)].join(" · ") || null,
      department_raw: row.department?.label || null,
      salary_raw: salary ?? findSalaryInText(description),
      description,
      posted_at: postedAt,
    });
  });
}

// ------------------------------------------------------------------ BambooHR

async function bamboohr(token: string): Promise<SourceJob[]> {
  const base = `https://${token}.bamboohr.com/careers`;
  const list = await fetchJson<{
    result: {
      id: string;
      jobOpeningName: string;
      departmentLabel?: string;
      location?: { city?: string; state?: string };
      isRemote?: boolean | null;
      locationType?: string;
    }[];
  }>(`${base}/list`);

  return mapLimit(list.result, 5, async (row) => {
    let description = "";
    let salary: string | null = null;
    let postedAt: string | null = null;
    try {
      const detail = await fetchJson<{
        result: { jobOpening: { description?: string; compensation?: string; datePosted?: string } };
      }>(`${base}/${row.id}/detail`);
      description = htmlToMarkdown(detail.result.jobOpening.description);
      salary = detail.result.jobOpening.compensation || null;
      postedAt = iso(detail.result.jobOpening.datePosted);
    } catch {
      // Keep the listing even if the detail call fails.
    }
    const place = [row.location?.city, row.location?.state].filter(Boolean).join(", ");
    const remote = row.isRemote || row.locationType === "1";
    return job({
      title: row.jobOpeningName,
      url: `${base}/${row.id}`,
      location: place ? (remote ? `${place} (Remote)` : place) : remote ? "Remote" : null,
      department_raw: row.departmentLabel || null,
      salary_raw: salary ?? findSalaryInText(description),
      description,
      posted_at: postedAt,
    });
  });
}

// ---------------------------------------------------------------- Teamtailor

async function teamtailor(origin: string): Promise<SourceJob[]> {
  const xml = await fetchText(`${origin}/jobs.rss`);
  const items = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].map((m) => m[1]);
  const tag = (block: string, name: string) => {
    const m = block.match(new RegExp(`<${name}[^>]*>([\\s\\S]*?)</${name}>`));
    if (!m) return null;
    return m[1].replace(/^<!\[CDATA\[|\]\]>$/g, "").trim() || null;
  };
  return items.map((it) => {
    const rawDesc = tag(it, "description") ?? "";
    const description = htmlToMarkdown(rawDesc.includes("<") ? rawDesc : decodeEntities(rawDesc));
    const city = tag(it, "tt:city");
    const country = tag(it, "tt:country");
    return job({
      title: decodeEntities(tag(it, "title") ?? ""),
      url: tag(it, "link") ?? origin,
      location: [city, country].filter(Boolean).join(", ") || (tag(it, "remoteStatus") === "fully" ? "Remote" : null),
      department_raw: tag(it, "tt:department"),
      description,
      posted_at: iso(tag(it, "pubDate")),
    });
  });
}

// ------------------------------------------------------------------ Workable

async function workable(token: string): Promise<SourceJob[]> {
  const base = `https://apply.workable.com/api`;
  const list = await fetchJson<{
    results: {
      shortcode: string;
      title: string;
      department?: string[];
      location?: { city?: string; region?: string; country?: string };
      remote?: boolean;
      published?: string;
    }[];
  }>(`${base}/v3/accounts/${encodeURIComponent(token)}/jobs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: "", location: [], department: [], worktype: [], remote: [] }),
  });

  return mapLimit(list.results, 5, async (row) => {
    let description = "";
    try {
      const d = await fetchJson<{ description?: string; requirements?: string; benefits?: string }>(
        `${base}/v2/accounts/${encodeURIComponent(token)}/jobs/${row.shortcode}`
      );
      description = htmlToMarkdown(
        [d.description, d.requirements && `<h3>Requirements</h3>${d.requirements}`, d.benefits && `<h3>Benefits</h3>${d.benefits}`]
          .filter(Boolean)
          .join("\n")
      );
    } catch {
      // Keep the listing even if the detail call fails.
    }
    const place = [row.location?.city, row.location?.region, row.location?.country].filter(Boolean).join(", ");
    return job({
      title: row.title,
      url: `https://apply.workable.com/${token}/j/${row.shortcode}/`,
      location: place || (row.remote ? "Remote" : null),
      department_raw: row.department?.[0] || null,
      description,
      posted_at: iso(row.published),
    });
  });
}

// ----------------------------------------------------------------- Recruitee

async function recruitee(token: string): Promise<SourceJob[]> {
  const data = await fetchJson<{
    offers: {
      title: string;
      careers_url: string;
      location?: string;
      department?: string;
      description?: string;
      requirements?: string;
      remote?: boolean;
      published_at?: string;
    }[];
  }>(`https://${token}.recruitee.com/api/offers/`);

  return data.offers.map((o) =>
    job({
      title: o.title,
      url: o.careers_url,
      location: o.location || (o.remote ? "Remote" : null),
      department_raw: o.department || null,
      description: htmlToMarkdown([o.description, o.requirements].filter(Boolean).join("\n")),
      posted_at: iso(o.published_at),
    })
  );
}

// ----------------------------------------------------------- SmartRecruiters

async function smartrecruiters(token: string): Promise<SourceJob[]> {
  const base = `https://api.smartrecruiters.com/v1/companies/${encodeURIComponent(token)}/postings`;
  const list = await fetchJson<{
    content: {
      id: string;
      name: string;
      location?: { city?: string; region?: string; country?: string; remote?: boolean };
      department?: { label?: string };
      releasedDate?: string;
    }[];
  }>(`${base}?limit=100`);

  return mapLimit(list.content, 5, async (row) => {
    let description = "";
    try {
      const d = await fetchJson<{ jobAd?: { sections?: Record<string, { title?: string; text?: string }> } }>(
        `${base}/${row.id}`
      );
      const sections = d.jobAd?.sections ?? {};
      description = htmlToMarkdown(
        ["jobDescription", "qualifications", "additionalInformation", "companyDescription"]
          .map((k) => sections[k])
          .filter((s) => s?.text)
          .map((s) => `<h3>${s!.title ?? ""}</h3>${s!.text}`)
          .join("\n")
      );
    } catch {
      // Keep the listing even if the detail call fails.
    }
    const place = [row.location?.city, row.location?.region].filter(Boolean).join(", ");
    return job({
      title: row.name,
      url: `https://jobs.smartrecruiters.com/${token}/${row.id}`,
      location: place || (row.location?.remote ? "Remote" : null),
      department_raw: row.department?.label || null,
      description,
      posted_at: iso(row.releasedDate),
    });
  });
}

// -------------------------------------------------------------------- JazzHR

/**
 * Recruiters often post the same role once per sourcing channel
 * ("... - Facebook", "... Handshake", "... Referral Link"). Strip those
 * suffixes so each real role shows up once.
 */
const CHANNEL_SUFFIX =
  /\s*[-–:]?\s*(facebook|hand ?shake|zip ?recruiter|referral(?: link)?|(?:community )?outreach|college central|college of marin|career ?link|csu long beach|indeed|linkedin|craigslist)\s*$/i;

async function jazzhr(token: string): Promise<SourceJob[]> {
  const base = `https://${token}.applytojob.com/apply`;
  const html = await fetchText(base);
  const rowRe =
    /<a href="(https:\/\/[a-z0-9-]+\.applytojob\.com\/apply\/[A-Za-z0-9]+\/[^"]*)">\s*([^<]+?)\s*<\/a>\s*<\/h3>\s*<ul[^>]*>([\s\S]*?)<\/ul>/g;

  const unique = new Map<string, { url: string; title: string; location: string | null; department: string | null }>();
  for (const m of html.matchAll(rowRe)) {
    const title = decodeEntities(m[2]).replace(CHANNEL_SUFFIX, "").trim();
    const attrs = m[3];
    const location = attrs.match(/fa-map-marker[^>]*><\/i>\s*([^<]+)/)?.[1]?.trim() || null;
    const department = attrs.match(/fa-sitemap[^>]*><\/i>\s*([^<]+)/)?.[1]?.trim() || null;
    const key = `${title.toLowerCase()}|${(location ?? "").toLowerCase()}`;
    if (!unique.has(key)) unique.set(key, { url: m[1], title, location, department });
  }

  return mapLimit([...unique.values()], 6, async (row) => {
    let description = "";
    let salary: string | null = null;
    let postedAt: string | null = null;
    try {
      const page = await fetchText(row.url);
      const ld = extractJobPostingLd(page);
      if (ld) {
        description = ld.description;
        salary = ld.salary;
        postedAt = ld.posted_at;
      } else {
        // Some postings omit JSON-LD; the description is in #job-description.
        const body = page.match(/id="job-description"[^>]*>([\s\S]*?)<div class="resumator-mobile-apply-wrapper/);
        if (body) description = htmlToMarkdown(body[1]);
      }
    } catch {
      // Keep the listing even if the detail call fails.
    }
    return job({
      title: row.title,
      url: row.url,
      location: row.location,
      department_raw: row.department,
      salary_raw: salary ?? findSalaryInText(description),
      description,
      posted_at: postedAt,
    });
  });
}
