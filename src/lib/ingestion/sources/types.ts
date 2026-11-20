export interface SourceJob {
  title: string;
  /** Link to the individual posting. Equals the careers page URL for inline-only listings. */
  url: string;
  location: string | null;
  department_raw: string | null;
  salary_raw: string | null;
  /** Markdown. May be empty when the source exposes no description. */
  description: string;
  /** When the posting went up (ISO 8601), if the source says. Used to drop very old postings. */
  posted_at?: string | null;
}

export interface SourceResult {
  /** e.g. "ashby:rogo", "greenhouse:flip", "scrape" */
  method: string;
  jobs: SourceJob[];
  /**
   * True when the result came from a structured source we trust (an ATS API, or a
   * page that explicitly says there are no openings). An authoritative empty result
   * means "this company has no open roles" and clears its listings; a
   * non-authoritative empty result is treated as a failed run.
   */
  authoritative: boolean;
}
