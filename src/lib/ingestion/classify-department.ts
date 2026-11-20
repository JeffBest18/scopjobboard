import { DEPARTMENT_TAGS, type DepartmentTag } from "../constants";
import { extractJson } from "./llm";

const BATCH_SIZE = 60;

const SYSTEM_PROMPT = `You classify job postings into departments. For each job, pick exactly one tag from this list:

${DEPARTMENT_TAGS.join(", ")}

Base the choice on the job title first, then the company's own department label if given. Use "Other" only when nothing fits.`;

const SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["results"],
  properties: {
    results: {
      type: "array",
      items: {
        type: "object",
        additionalProperties: false,
        required: ["id", "tag"],
        properties: {
          id: { type: "integer" },
          tag: { type: "string", enum: [...DEPARTMENT_TAGS] },
        },
      },
    },
  },
};

/**
 * Classifies many jobs with one Claude call per batch of 60 (previously one call
 * per job). Any job the model skips, or any batch that errors, falls back to "Other".
 */
export async function classifyDepartments(
  jobs: { title: string; department_raw: string | null }[]
): Promise<DepartmentTag[]> {
  // Keyword guess first, so jobs are still sensibly tagged if the API is unavailable.
  const tags: DepartmentTag[] = jobs.map((j) => guessDepartment(j.title, j.department_raw));

  for (let start = 0; start < jobs.length; start += BATCH_SIZE) {
    const batch = jobs.slice(start, start + BATCH_SIZE);
    const lines = batch
      .map((j, i) => `${i}. ${j.title}${j.department_raw ? ` [company label: ${j.department_raw}]` : ""}`)
      .join("\n");
    try {
      const out = await extractJson<{ results: { id: number; tag: DepartmentTag }[] }>({
        system: SYSTEM_PROMPT,
        user: `Classify each job. Return one result per id.\n\n${lines}`,
        schema: SCHEMA,
        maxTokens: 8000,
      });
      for (const r of out.results) {
        if (r.id >= 0 && r.id < batch.length && DEPARTMENT_TAGS.includes(r.tag)) {
          tags[start + r.id] = r.tag;
        }
      }
    } catch (err) {
      console.warn("[ingest] department classification failed:", err instanceof Error ? err.message : err);
    }
  }
  return tags;
}

const KEYWORDS: [DepartmentTag, RegExp][] = [
  ["Customer Success", /customer success|client success|account manag|support|onboarding|implementation|solutions? (engineer|consultant)|forward deployed/i],
  ["Data", /\bdata\b|analytics|analyst|machine learning|\bml\b|\bai (research|scientist)|research scientist|scientist/i],
  ["Design", /design|ux|ui\b|creative|motion/i],
  ["Product", /product manag|product owner|\bpm\b|product lead|head of product/i],
  ["Engineering", /engineer|developer|devops|sre\b|architect|software|technical lead|cto|firmware|hardware|qa\b|security/i],
  ["Sales", /sales|account executive|\bae\b|\bsdr\b|\bbdr\b|business development|gtm|go-to-market|partnerships|revenue/i],
  ["Marketing", /marketing|growth|content|brand|communications|seo|social media|community|demand gen/i],
  ["Finance", /financ|accountant|accounting|controller|\bcfo\b|fp&a|treasury|payroll/i],
  ["Legal", /legal|counsel|attorney|lawyer|compliance|paralegal/i],
  ["People / HR", /recruit|talent|people|\bhr\b|human resources/i],
  ["Operations", /operations|\bops\b|chief of staff|project manag|program manag|logistics|office manager|strategy|tutor|paraprofessional|program staff/i],
];

/** Cheap keyword fallback: company label first, then title. */
export function guessDepartment(title: string, departmentRaw: string | null): DepartmentTag {
  for (const text of [departmentRaw, title]) {
    if (!text) continue;
    for (const [tag, re] of KEYWORDS) if (re.test(text)) return tag;
  }
  return "Other";
}
