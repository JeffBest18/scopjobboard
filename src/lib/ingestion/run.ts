import { createAdminClient } from "../supabase/admin";
import { canonicalizeUrl, generateJobKey, generateInlineJobKey } from "./canonicalize";
import { extractListings } from "./extract-listings";
import { extractJobDetails } from "./extract-details";
import { extractInlineJobs } from "./extract-inline-jobs";
import { classifyDepartment } from "./classify-department";
import { isValidJobDetail, isValidInlineJob, isValidRun } from "./validate";
import { cleanSalary } from "./clean-salary";
import { cleanDescription } from "./clean-description";
import { publishSnapshot, failSnapshot, type ProcessedJob } from "./publish";

export async function runIngestion(companyId: string): Promise<{ success: boolean; jobCount?: number; mode?: string; error?: string }> {
  const supabase = createAdminClient();

  const { data: company, error: companyError } = await supabase
    .from("companies")
    .select("*")
    .eq("id", companyId)
    .single();

  if (companyError || !company) return { success: false, error: "Company not found" };
  if (!company.enabled) return { success: false, error: "Company is disabled" };
  if (!company.careers_url) return { success: false, error: "Company has no careers URL" };

  try {
    const rawUrls = await extractListings(company.careers_url);

    if (rawUrls.length === 0) {
      return runInlineFallback(companyId, company.careers_url);
    }

    const canonicalUrls = rawUrls.map((url) => canonicalizeUrl(url));

    const results: { url: string; detail: Awaited<ReturnType<typeof extractJobDetails>> }[] = [];
    for (const url of canonicalUrls) {
      const detail = await extractJobDetails(url);
      results.push({ url, detail });
    }

    const validResults = results.filter(
      (r): r is { url: string; detail: NonNullable<typeof r.detail> } =>
        r.detail !== null && isValidJobDetail(r.detail)
    );

    if (!isValidRun(canonicalUrls, validResults.map((r) => r.detail))) {
      const rate = validResults.length / canonicalUrls.length;
      await failSnapshot(
        companyId,
        `Validation failed: ${validResults.length}/${canonicalUrls.length} valid (${(rate * 100).toFixed(0)}%, need 70%)`
      );
      return { success: false, error: "Run validation failed" };
    }

    const processedJobs: ProcessedJob[] = [];
    for (const { url, detail } of validResults) {
      const [departmentTag, description] = await Promise.all([
        classifyDepartment(detail.title, detail.department_raw, detail.description),
        cleanDescription(detail.description),
      ]);

      processedJobs.push({
        job_key: generateJobKey(companyId, url),
        title: detail.title,
        department_tag: departmentTag,
        department_raw: detail.department_raw,
        location: detail.location,
        salary_raw: cleanSalary(detail.salary_raw),
        description,
        job_url: url,
      });
    }

    await publishSnapshot(companyId, processedJobs);
    return { success: true, jobCount: processedJobs.length };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    await failSnapshot(companyId, message);
    return { success: false, error: message };
  }
}

async function runInlineFallback(companyId: string, careersUrl: string) {
  try {
    const inlineJobs = await extractInlineJobs(careersUrl);
    const validJobs = inlineJobs.filter(isValidInlineJob);

    if (validJobs.length === 0) {
      await failSnapshot(companyId, "No job URLs found and inline fallback returned 0 jobs");
      return { success: false, error: "No jobs found (tried URL extraction and inline fallback)" };
    }

    const canonicalCareersUrl = canonicalizeUrl(careersUrl);
    const processedJobs: ProcessedJob[] = [];

    for (const job of validJobs) {
      const [departmentTag, description] = await Promise.all([
        classifyDepartment(job.title, job.department_raw, job.description),
        job.description ? cleanDescription(job.description) : Promise.resolve(""),
      ]);

      processedJobs.push({
        job_key: generateInlineJobKey(companyId, canonicalCareersUrl, job.title),
        title: job.title,
        department_tag: departmentTag,
        department_raw: job.department_raw,
        location: job.location,
        salary_raw: cleanSalary(job.salary_raw),
        description,
        job_url: canonicalCareersUrl,
      });
    }

    await publishSnapshot(companyId, processedJobs);
    return { success: true, jobCount: processedJobs.length, mode: "inline_fallback" };
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    await failSnapshot(companyId, `Inline fallback failed: ${message}`);
    return { success: false, error: message };
  }
}
