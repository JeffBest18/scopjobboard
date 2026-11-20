import { fetchText } from "../http";
import { detectAts, fetchAtsJobs } from "./ats";
import { readableLength, renderWithBrowser, scrapeCareersPage, THIN_PAGE_CHARS } from "./scrape";
import type { SourceResult } from "./types";

export type { SourceJob, SourceResult } from "./types";

/**
 * Collects the open jobs for one careers URL, using the most reliable source available:
 *   1. The careers URL is itself an ATS board          -> ATS API
 *   2. The careers page embeds or links to an ATS board -> ATS API
 *   3. The page only renders with JavaScript            -> render, then 2 or 4
 *   4. Anything else                                    -> Claude reads the page
 */
export async function collectJobs(careersUrl: string): Promise<SourceResult> {
  const direct = detectAts(careersUrl, careersUrl);
  if (direct) {
    return { method: `${direct.provider}:${direct.token}`, jobs: await fetchAtsJobs(direct), authoritative: true };
  }

  let html: string | null = null;
  let fetchError: unknown = null;
  try {
    html = await fetchText(careersUrl);
  } catch (err) {
    fetchError = err;
  }

  if (html) {
    const embedded = detectAts(html, careersUrl);
    if (embedded) {
      return { method: `${embedded.provider}:${embedded.token}`, jobs: await fetchAtsJobs(embedded), authoritative: true };
    }
  }

  if (!html || readableLength(html) < THIN_PAGE_CHARS) {
    const rendered = await renderWithBrowser(careersUrl);
    if (rendered) {
      const embedded = detectAts(rendered, careersUrl);
      if (embedded) {
        return { method: `${embedded.provider}:${embedded.token}`, jobs: await fetchAtsJobs(embedded), authoritative: true };
      }
      html = rendered;
    }
  }

  if (!html) {
    throw new Error(
      `Could not load careers page: ${fetchError instanceof Error ? fetchError.message : "empty response"}`
    );
  }

  return scrapeCareersPage(careersUrl, html);
}
