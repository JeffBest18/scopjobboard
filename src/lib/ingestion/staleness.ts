import type { SourceJob } from "./sources";
import { isUnknownListingDate } from "../listing-date";

/**
 * Postings older than this are dropped even if the company's job system still
 * lists them. Override with MAX_POSTING_AGE_DAYS.
 */
export const MAX_POSTING_AGE_DAYS = Number(process.env.MAX_POSTING_AGE_DAYS) || 180;

/** "We're always hiring"-style catch-alls rather than real roles. */
const GENERAL_APPLICATION =
  /general application|open application|spontaneous application|talent (?:pool|community|network)|role doesn'?t exist yet|don'?t see (?:a|the|your) role|future opportunities/i;

/**
 * Returns why a posting is stale, or null if it's current. Three rules:
 *  1. It's a general/open application, not a role.
 *  2. It was listed more than MAX_POSTING_AGE_DAYS ago: the listing date the
 *     board shows (earliest of the source's posting date and when we first saw it).
 *  3. Its title names a school year that has already ended (e.g. "Tutor '25/'26").
 */
export function staleReason(job: SourceJob, now: Date = new Date(), listedAt?: string | null): string | null {
  if (GENERAL_APPLICATION.test(job.title)) return "general application";

  const since = listedAt && !isUnknownListingDate(listedAt) ? listedAt : job.posted_at;
  if (since) {
    const days = Math.floor((now.getTime() - new Date(since).getTime()) / 86_400_000);
    if (days > MAX_POSTING_AGE_DAYS) return `listed ${days} days ago`;
  }

  const schoolYear = endedSchoolYear(job.title, now);
  if (schoolYear) return `for the ${schoolYear} school year`;

  return null;
}

/** "Tutor '25/'26", "Tutor 25'/26'", "Tutor '26-'27" -> "2025–26" if that year has ended (after June). */
function endedSchoolYear(title: string, now: Date): string | null {
  const re = /(?:^|[\s(])'?(\d{2})'?\s*[/-]\s*'?(\d{2})'?(?=$|[\s),:])/g;
  for (const m of title.matchAll(re)) {
    const start = Number(m[1]);
    const end = Number(m[2]);
    if (end !== start + 1 || start < 20 || start > 60) continue;
    const yearEnds = new Date(Date.UTC(2000 + end, 6, 1)); // July 1 after the school year
    if (now >= yearEnds) return `20${m[1]}–${m[2]}`;
  }
  return null;
}
