import Anthropic from "@anthropic-ai/sdk";

const client = new Anthropic();

/** Cheap, fast model for extraction and classification. Override with INGEST_MODEL. */
export const INGEST_MODEL = process.env.INGEST_MODEL || "claude-haiku-5-5";

/**
 * One Claude call constrained to a JSON schema (structured outputs). Throws on
 * refusal, truncation, or an unparseable response so callers can fall back.
 */
export async function extractJson<T>(opts: {
  system: string;
  user: string;
  schema: Record<string, unknown>;
  maxTokens?: number;
}): Promise<T> {
  const response = await client.messages.create({
    model: INGEST_MODEL,
    max_tokens: opts.maxTokens ?? 16000,
    system: opts.system,
    messages: [{ role: "user", content: opts.user }],
    output_config: {
      effort: "low",
      format: { type: "json_schema", schema: opts.schema },
    },
  });

  if (response.stop_reason === "refusal") throw new Error("Model declined the request");
  if (response.stop_reason === "max_tokens") throw new Error("Model output was truncated (max_tokens)");

  const text = response.content
    .filter((b): b is Anthropic.TextBlock => b.type === "text")
    .map((b) => b.text)
    .join("");
  return JSON.parse(text) as T;
}
