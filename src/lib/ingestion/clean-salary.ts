/**
 * Cleans raw salary text to just the numeric range.
 * Examples:
 *   "Compensation Range: $120K - $150K" → "$120K - $150K"
 *   "$160,000 - $200,000 per year" → "$160,000 - $200,000"
 *   "$120,000 - $180,000 OTE" → "$120,000 - $180,000"
 *   "$95,000" → "$95,000"
 *   "Competitive salary" → null
 */
export function cleanSalary(raw: string | null): string | null {
  if (!raw) return null;

  // Match salary ranges: $XXX,XXX - $XXX,XXX or $XXXK - $XXXK (with optional decimals)
  const rangeMatch = raw.match(
    /\$[\d,]+(?:\.\d+)?[Kk]?\s*[-–—to]+\s*\$[\d,]+(?:\.\d+)?[Kk]?/
  );
  // Keep the unit for hourly pay, otherwise "$24 - $28" reads like a typo.
  const hourly = /\/\s*h(ou)?r|per hour|hourly/i.test(raw) ? "/hr" : "";

  if (rangeMatch) return formatSalary(rangeMatch[0] + hourly);

  // Match single salary: $XXX,XXX or $XXXK
  const singleMatch = raw.match(/\$[\d,]+(?:\.\d+)?[Kk]?/);
  if (singleMatch) return formatSalary(singleMatch[0] + hourly);

  // No recognizable salary pattern
  return null;
}

/**
 * Normalises how a salary reads: "$50,000-$55,000.00" -> "$50,000 – $55,000",
 * "$95k to $100k" -> "$95K – $100K". Idempotent, so it is also safe to apply
 * at display time to values stored before this existed.
 */
export function formatSalary(value: string): string {
  return value
    .trim()
    .replace(/\.00\b/g, "")
    .replace(/(\d)\s*k\b/g, "$1K")
    .replace(/\s*(?:-|–|—|\bto\b)\s*(?=\$?\d)/g, " – ")
    // "$180 – $220K" means $180K: carry the K back when the first number is bare.
    .replace(/^\$(\d{2,3}) – (\$?\d{2,3}K)/, "$$$1K – $2");
}
