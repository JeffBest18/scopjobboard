/**
 * A role's listing date, stored in jobs.first_seen_at.
 *
 * It's the earliest of (a) when the company says it was posted and (b) when
 * this board first saw it. When neither tells us anything real, the role gets
 * UNKNOWN_LISTING_DATE: the board shows no date, leaves it out of "past
 * week/month", and sorts it last in "Newest first".
 */

export const UNKNOWN_LISTING_DATE = "1970-01-01T00:00:00.000Z";

export function isUnknownListingDate(iso: string | null | undefined): boolean {
  return !iso || iso < "2000-01-01";
}

/**
 * The board started tracking many companies' roles on 2026-11-19/20 (new
 * ingestion pipeline). An undated role first seen in that window was not
 * necessarily new then, so it is treated as "date unknown" rather than "new".
 */
const BACKFILL_FROM = "2026-11-19T00:00:00.000Z";
const BACKFILL_UNTIL = "2026-11-21T00:00:00.000Z";

export function resolveListingDate(opts: {
  /** Posting date from the source, if any. */
  postedAt: string | null | undefined;
  /** Earliest first_seen_at already stored for this job, if any. */
  existingFirstSeen: string | null | undefined;
  /** Whether the company had a published snapshot before this run. */
  companyHasHistory: boolean;
  now?: Date;
}): string {
  const now = opts.now ?? new Date();
  const posted = validPastDate(opts.postedAt, now);
  const seenBefore = !!opts.existingFirstSeen; // includes roles stored as "unknown"
  const existing = isUnknownListingDate(opts.existingFirstSeen) ? null : opts.existingFirstSeen!;
  const existingIsBackfill = !!existing && existing >= BACKFILL_FROM && existing < BACKFILL_UNTIL;
  // A first-seen date we can trust as "the role was up by then".
  const trustedSeen = existing && !existingIsBackfill ? existing : null;

  if (posted) {
    // The source's date, unless we have proof the role was up earlier.
    return trustedSeen && trustedSeen < posted ? trustedSeen : posted;
  }
  if (trustedSeen) return trustedSeen;
  // Seen before but with no usable date (stored as unknown, or a backfill date).
  if (seenBefore) return UNKNOWN_LISTING_DATE;
  // A brand-new undated role at a company we were already tracking daily is
  // genuinely new (within a day). On a company's first load we can't tell.
  return opts.companyHasHistory ? now.toISOString() : UNKNOWN_LISTING_DATE;
}

function validPastDate(value: string | null | undefined, now: Date): string | null {
  if (!value) return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime()) || d.getTime() > now.getTime() + 86_400_000) return null;
  return d.toISOString();
}
