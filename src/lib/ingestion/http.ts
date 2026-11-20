import TurndownService from "turndown";

// A real browser UA: several portfolio sites (WordPress/Cloudflare) serve
// empty or blocked pages to obviously-bot user agents.
const UA =
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0 Safari/537.36";

export class HttpError extends Error {
  constructor(
    public status: number,
    url: string
  ) {
    super(`HTTP ${status} for ${url}`);
  }
}

export async function fetchText(
  url: string,
  init: RequestInit & { timeoutMs?: number } = {}
): Promise<string> {
  const { timeoutMs = 20_000, headers, ...rest } = init;
  const res = await fetch(url, {
    ...rest,
    redirect: "follow",
    headers: { "User-Agent": UA, Accept: "text/html,application/json,*/*", ...headers },
    signal: AbortSignal.timeout(timeoutMs),
    cache: "no-store",
  });
  if (!res.ok) throw new HttpError(res.status, url);
  return res.text();
}

export async function fetchJson<T>(
  url: string,
  init: RequestInit & { timeoutMs?: number } = {}
): Promise<T> {
  const text = await fetchText(url, {
    ...init,
    headers: { Accept: "application/json", ...init.headers },
  });
  return JSON.parse(text) as T;
}

const turndown = new TurndownService({
  headingStyle: "atx",
  bulletListMarker: "-",
  codeBlockStyle: "fenced",
});
turndown.remove(["script", "style", "noscript", "iframe", "head", "form", "button"]);
turndown.remove((node) => node.nodeName.toLowerCase() === "svg");

/** Converts HTML (possibly entity-encoded, as Greenhouse returns it) to Markdown. */
export function htmlToMarkdown(html: string | null | undefined): string {
  if (!html) return "";
  let source = html;
  // Greenhouse's `content` field is HTML-escaped HTML.
  if (/&lt;\/?[a-z]/i.test(source) && !/<\/?[a-z]/i.test(source)) {
    source = decodeEntities(source);
  }
  return turndown
    .turndown(source)
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export function decodeEntities(s: string): string {
  return s
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;|&#x27;|&apos;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&#(\d+);/g, (_, d) => String.fromCharCode(Number(d)))
    .replace(/&amp;/g, "&");
}

/** Run `fn` over `items` with at most `limit` in flight. Preserves order. */
export async function mapLimit<T, R>(
  items: T[],
  limit: number,
  fn: (item: T, index: number) => Promise<R>
): Promise<R[]> {
  const results = new Array<R>(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

/** Pulls the first dollar salary range out of free text, e.g. "$120,000 - $150,000". */
export function findSalaryInText(text: string): string | null {
  const m = text.match(
    /\$\s?\d[\d,.]*\s?[kK]?\s*(?:-|–|—|to)\s*\$?\s?\d[\d,.]*\s?[kK]?/
  );
  return m ? m[0].replace(/\s+/g, " ") : null;
}
