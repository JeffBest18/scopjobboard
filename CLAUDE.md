# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Portfolio Job Directory — a public job board that aggregates open roles across ~30 portfolio companies and directs candidates to original postings. Discovery and traffic generation only; no ATS, no candidate accounts.

## Tech Stack

- **Framework**: Next.js (App Router, TypeScript, Tailwind CSS)
- **Hosting**: Vercel
- **Database**: Supabase (PostgreSQL) via `@supabase/supabase-js` and `@supabase/ssr`
- **Auth**: Supabase Auth (admin-only, email/password)
- **Job sources**: public ATS APIs first; Hyperbrowser (`@hyperbrowser/sdk`) only to render JS/blocked pages
- **LLM (scraping fallback + department tagging)**: Anthropic API — `claude-haiku-5-5` by default, override with `INGEST_MODEL` (`@anthropic-ai/sdk`)
- **Markdown rendering**: `react-markdown`

## Common Commands

```bash
npm run dev          # Start dev server
npm run build        # Production build
npm run lint         # ESLint
```

## Architecture

### Supabase Clients (3 variants)
- `src/lib/supabase/browser.ts` — Client Components (browser), uses `createBrowserClient`
- `src/lib/supabase/server.ts` — Server Components/Actions/Route Handlers, uses `createServerClient` with cookie handling
- `src/lib/supabase/admin.ts` — Service role client for ingestion/admin (bypasses RLS), uses `createClient` directly

### Database
- **Tables**: `companies`, `snapshots`, `jobs`, `clicks`
- **Key view**: `live_jobs` — joins jobs from the latest published snapshot per enabled company; this is the only view public pages query
- **Migrations**: `supabase/migrations/` — run manually in Supabase SQL Editor
- **Job identity**: `job_key = sha256(company_id + canonical_job_url)`

### Ingestion Pipeline (`src/lib/ingestion/`)
Runs **daily** via Vercel Cron (`vercel.json`, 13:00 UTC). `/api/ingest` ingests every enabled company with a careers URL **in-process** (concurrency 5, stalest first, stops starting new companies after 200s). It used to fan out HTTP calls to itself and timed out after the first couple of batches; don't reintroduce that.

Per company (`run.ts` → `sources/index.ts#collectJobs`), most reliable source first:
1. **ATS API** (`sources/ats.ts`): careers URL *is* an ATS board, or the careers page HTML embeds/links one. Supported: Ashby, Greenhouse, Lever, Rippling, BambooHR, Teamtailor, Workable, Recruitee, SmartRecruiters, JazzHR (applytojob; duplicate per-channel postings are collapsed). Posting pages with schema.org JobPosting JSON-LD are read directly, without the LLM. One or two JSON requests, complete and exact. Most portfolio companies land here.
2. **Browser render** (Hyperbrowser, only if `HYPERBROWSER_API_KEY` is set): when plain fetch fails (403) or the page is JS-rendered; then re-check for an ATS.
3. **Claude scrape** (`sources/scrape.ts`): Claude (Haiku, structured outputs) lists jobs from the page, then reads each posting page for its description. Jobs whose title/link doesn't appear on the page are dropped (hallucination guard).

Then: batched department classification (`classify-department.ts`, tags reused for known job_keys) → `publishSnapshot`.

Snapshot rules:
- ATS result (or a page explicitly saying "no open roles") is **authoritative**: 0 jobs publishes an empty snapshot, clearing the company's listings.
- Scrape result with 0 jobs, or <30% of the previous count (when previous ≥5), is saved as `failed` and the previous snapshot stays live.
- Failed snapshot `error_message` is prefixed with the method, e.g. `[scrape] ...`, `[ashby:rogo] ...`; shown in /admin.
- Companies with no `careers_url` are skipped (their jobs are managed by hand in /admin).
- `company-fixes.ts` applies careers-URL corrections at the start of each run, only where the old value is unchanged.
- Department tags: keyword guess first (`guessDepartment`), overwritten by Claude when the API works.

Manual run: `curl -H "Authorization: Bearer $CRON_SECRET" "https://jobs.scopvc.com/api/ingest?company=<slug>"` (omit `company` for all). Admin "Re-run" button calls `runIngestion` directly.

### Route Structure
- `/jobs` — SSR directory listing with server-side pagination (25/page) and filters (department, company via URL params)
- `/jobs/[job_key]` — SSR job detail page with SEO metadata
- `/r/[job_key]` — Click redirect: logs to `clicks` table, 302 to stored job URL (never accepts URL param)
- `/admin` — Auth-gated dashboard (Supabase Auth `getUser()`, not `getSession()`)
- `/api/admin/*` — Admin actions (rerun, company update), auth-gated via `getUser()`
- `/api/ingest` — Cron dispatcher, auth via `CRON_SECRET` bearer token

### Key Constants (`src/lib/constants.ts`)
- `DEPARTMENT_TAGS` — 12-value taxonomy for department classification
- `JOBS_PER_PAGE = 25`

## Environment Variables

See `.env.example`. Required:
- `NEXT_PUBLIC_SUPABASE_URL`, `NEXT_PUBLIC_SUPABASE_ANON_KEY`, `SUPABASE_SERVICE_ROLE_KEY`
- `CRON_SECRET` (authenticates ingestion endpoints)
- `ANTHROPIC_API_KEY` (prepaid; if the balance hits zero, scraped companies fail and new jobs get tagged "Other")
- `HYPERBROWSER_API_KEY` (optional; only for pages that block plain fetch or need JS)
- `NEXT_PUBLIC_SITE_URL`

## Implementation Phases

The project follows 6 sequential phases defined in `IMPLEMENTATION_PHASES.md`. Phases 2 and 3 can run in parallel after Phase 1. Steps marked `[USER]` require manual action; steps marked `[CODE]` are implementation tasks.

## Important Patterns

- **Auth**: Admin routes use `supabase.auth.getUser()` (server-validated), never `getSession()` (JWT-only)
- **Ingestion auth**: Bearer token via `CRON_SECRET`, checked in `Authorization` header
- **Filters**: URL search params (`?department=X&company=Y&page=N`), not client-side state — preserves SSR
- **Logos**: Static files at `/public/logos/{company_slug}.png`, updated via code changes
- **URL canonicalization**: Strip tracking params (utm_*, fbclid, gclid, mc_cid, mc_eid), remove trailing slashes, lowercase hostname; preserve all other query params
- **Department tagging**: one batched Claude call per 60 jobs; previously-seen job_keys keep their tag; failures fall back to "Other"
