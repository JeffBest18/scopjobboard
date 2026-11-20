"use client";

import { useState, useRef } from "react";
import Link from "next/link";
import { DEPARTMENT_TAGS } from "@/lib/constants";

const TOTAL_STEPS = 4;

export default function TalentJoinPage() {
  const [step, setStep] = useState(1);
  const [submitting, setSubmitting] = useState(false);
  const [submitted, setSubmitted] = useState(false);
  const [error, setError] = useState("");

  // Form state
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [linkedinUrl, setLinkedinUrl] = useState("");
  const [location, setLocation] = useState("");
  const [departments, setDepartments] = useState<string[]>([]);
  const [resumeFile, setResumeFile] = useState<File | null>(null);
  const [honeypot, setHoneypot] = useState("");
  const fileInputRef = useRef<HTMLInputElement>(null);

  function canAdvance(): boolean {
    if (step === 1) return fullName.trim() !== "" && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
    if (step === 3) return departments.length > 0;
    return true;
  }

  function next() {
    if (canAdvance() && step < TOTAL_STEPS) setStep(step + 1);
  }

  function back() {
    if (step > 1) setStep(step - 1);
  }

  function toggleDepartment(tag: string) {
    setDepartments((prev) =>
      prev.includes(tag) ? prev.filter((d) => d !== tag) : [...prev, tag]
    );
  }

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0] ?? null;
    if (file) {
      if (file.type !== "application/pdf") {
        setError("Please upload a PDF file.");
        return;
      }
      if (file.size > 5 * 1024 * 1024) {
        setError("File must be under 5 MB.");
        return;
      }
      setError("");
    }
    setResumeFile(file);
  }

  async function handleSubmit() {
    if (!canAdvance()) return;

    setSubmitting(true);
    setError("");

    const formData = new FormData();
    formData.set("full_name", fullName.trim());
    formData.set("email", email.trim());
    if (linkedinUrl.trim()) formData.set("linkedin_url", linkedinUrl.trim());
    if (location.trim()) formData.set("location", location.trim());
    formData.set("departments", JSON.stringify(departments));
    if (resumeFile) formData.set("resume", resumeFile);
    if (honeypot) formData.set("company_url", honeypot);

    try {
      const res = await fetch("/api/talent/join", {
        method: "POST",
        body: formData,
      });
      const data = await res.json();
      if (!res.ok) {
        setError(data.error || "Something went wrong.");
        setSubmitting(false);
        return;
      }
      setSubmitted(true);
    } catch {
      setError("Network error. Please try again.");
    } finally {
      setSubmitting(false);
    }
  }

  if (submitted) {
    return (
      <div className="mx-auto flex min-h-[60vh] max-w-lg flex-col items-center justify-center px-6 py-16 text-center">
        <div className="mb-6 flex h-16 w-16 items-center justify-center rounded-full bg-gray-100">
          <svg width="32" height="32" viewBox="0 0 24 24" fill="none" className="text-black">
            <path d="M5 13l4 4L19 7" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </div>
        <h1 className="text-2xl font-light tracking-tight text-black">
          You&apos;re in the network
        </h1>
        <p className="mt-3 text-gray-500">
          Thanks for joining! We&apos;ll reach out when roles match your interests.
        </p>
        <Link
          href="/"
          className="mt-8 text-sm text-gray-400 transition-colors duration-150 hover:text-black"
        >
          &larr; Back to job board
        </Link>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-lg px-6 py-16">
      <h1 className="text-2xl font-light tracking-tight text-black">
        Join the Talent Network
      </h1>
      <p className="mt-2 text-sm text-gray-500">
        Step {step} of {TOTAL_STEPS}
      </p>

      {/* Progress bar */}
      <div className="mt-4 flex gap-1.5">
        {Array.from({ length: TOTAL_STEPS }, (_, i) => (
          <div
            key={i}
            className={`h-1 flex-1 rounded-full transition-colors duration-200 ${
              i < step ? "bg-black" : "bg-gray-200"
            }`}
          />
        ))}
      </div>

      <form onSubmit={(e) => e.preventDefault()} className="mt-10">
        {/* Honeypot — hidden from real users, filled by bots */}
        <div aria-hidden="true" className="absolute -left-[9999px]">
          <input
            type="text"
            name="company_url"
            tabIndex={-1}
            autoComplete="off"
            value={honeypot}
            onChange={(e) => setHoneypot(e.target.value)}
          />
        </div>

        {/* Step 1: Name & Email */}
        {step === 1 && (
          <div className="space-y-5">
            <div>
              <label htmlFor="fullName" className="block text-sm font-medium text-gray-700">
                Full Name <span className="text-gray-400">*</span>
              </label>
              <input
                id="fullName"
                type="text"
                value={fullName}
                onChange={(e) => setFullName(e.target.value)}
                placeholder="Jane Smith"
                className="mt-1.5 w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm text-gray-600 placeholder:text-gray-400 focus:outline-none focus:ring-1 focus:ring-gray-400"
                autoFocus
              />
            </div>
            <div>
              <label htmlFor="email" className="block text-sm font-medium text-gray-700">
                Email <span className="text-gray-400">*</span>
              </label>
              <input
                id="email"
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="jane@example.com"
                className="mt-1.5 w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm text-gray-600 placeholder:text-gray-400 focus:outline-none focus:ring-1 focus:ring-gray-400"
              />
            </div>
          </div>
        )}

        {/* Step 2: LinkedIn & Location */}
        {step === 2 && (
          <div className="space-y-5">
            <div>
              <label htmlFor="linkedin" className="block text-sm font-medium text-gray-700">
                LinkedIn URL <span className="text-xs text-gray-400">(optional)</span>
              </label>
              <input
                id="linkedin"
                type="url"
                value={linkedinUrl}
                onChange={(e) => setLinkedinUrl(e.target.value)}
                placeholder="https://linkedin.com/in/janesmith"
                className="mt-1.5 w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm text-gray-600 placeholder:text-gray-400 focus:outline-none focus:ring-1 focus:ring-gray-400"
                autoFocus
              />
            </div>
            <div>
              <label htmlFor="location" className="block text-sm font-medium text-gray-700">
                Location <span className="text-xs text-gray-400">(optional)</span>
              </label>
              <input
                id="location"
                type="text"
                value={location}
                onChange={(e) => setLocation(e.target.value)}
                placeholder="San Francisco, CA"
                className="mt-1.5 w-full rounded-lg border border-gray-300 px-4 py-2.5 text-sm text-gray-600 placeholder:text-gray-400 focus:outline-none focus:ring-1 focus:ring-gray-400"
              />
            </div>
          </div>
        )}

        {/* Step 3: Department Interests */}
        {step === 3 && (
          <div>
            <p className="text-sm font-medium text-gray-700">
              What kind of roles interest you? <span className="text-gray-400">*</span>
            </p>
            <p className="mt-1 text-xs text-gray-400">Select at least one.</p>
            <div className="mt-4 flex flex-wrap gap-2">
              {DEPARTMENT_TAGS.map((tag) => {
                const selected = departments.includes(tag);
                return (
                  <button
                    key={tag}
                    type="button"
                    onClick={() => toggleDepartment(tag)}
                    className={`rounded-full border px-4 py-2 text-sm transition-colors duration-150 ${
                      selected
                        ? "border-black bg-black text-white"
                        : "border-gray-300 text-gray-600 hover:bg-gray-50"
                    }`}
                  >
                    {tag}
                  </button>
                );
              })}
            </div>
          </div>
        )}

        {/* Step 4: Resume Upload */}
        {step === 4 && (
          <div>
            <p className="text-sm font-medium text-gray-700">
              Upload your resume <span className="text-xs text-gray-400">(optional)</span>
            </p>
            <p className="mt-1 text-xs text-gray-400">PDF only, max 5 MB.</p>
            <div
              onClick={() => fileInputRef.current?.click()}
              onDragOver={(e) => e.preventDefault()}
              onDrop={(e) => {
                e.preventDefault();
                const file = e.dataTransfer.files[0];
                if (file) {
                  if (file.type !== "application/pdf") {
                    setError("Please upload a PDF file.");
                    return;
                  }
                  if (file.size > 5 * 1024 * 1024) {
                    setError("File must be under 5 MB.");
                    return;
                  }
                  setError("");
                  setResumeFile(file);
                }
              }}
              className="mt-4 flex cursor-pointer flex-col items-center justify-center rounded-lg border-2 border-dashed border-gray-300 px-6 py-10 transition-colors duration-150 hover:border-gray-400"
            >
              {resumeFile ? (
                <div className="flex items-center gap-2 text-sm text-gray-600">
                  <svg width="16" height="16" viewBox="0 0 24 24" fill="none" className="text-black">
                    <path d="M14 2H6a2 2 0 00-2 2v16a2 2 0 002 2h12a2 2 0 002-2V8z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    <path d="M14 2v6h6M16 13H8M16 17H8M10 9H8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                  {resumeFile.name}
                  <button
                    type="button"
                    onClick={(e) => {
                      e.stopPropagation();
                      setResumeFile(null);
                      if (fileInputRef.current) fileInputRef.current.value = "";
                    }}
                    className="ml-1 text-gray-400 hover:text-black"
                  >
                    &times;
                  </button>
                </div>
              ) : (
                <>
                  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" className="text-gray-400">
                    <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4M17 8l-5-5-5 5M12 3v12" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                  <p className="mt-2 text-sm text-gray-500">
                    Drop a PDF here or click to browse
                  </p>
                </>
              )}
              <input
                ref={fileInputRef}
                type="file"
                accept="application/pdf"
                onChange={handleFileChange}
                className="hidden"
              />
            </div>
          </div>
        )}

        {/* Error message */}
        {error && (
          <p className="mt-4 text-sm text-red-600">{error}</p>
        )}

        {/* Navigation buttons */}
        <div className="mt-10 flex items-center justify-between">
          {step > 1 ? (
            <button
              type="button"
              onClick={back}
              className="text-sm text-gray-400 transition-colors duration-150 hover:text-black"
            >
              &larr; Back
            </button>
          ) : (
            <span />
          )}

          {step < TOTAL_STEPS ? (
            <button
              type="button"
              onClick={next}
              disabled={!canAdvance()}
              className="rounded-lg bg-black px-6 py-2.5 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-80 disabled:opacity-40"
            >
              Next
            </button>
          ) : (
            <button
              type="button"
              onClick={handleSubmit}
              disabled={submitting}
              className="rounded-lg bg-black px-6 py-2.5 text-sm font-medium text-white transition-opacity duration-150 hover:opacity-80 disabled:opacity-40"
            >
              {submitting ? "Submitting..." : "Submit"}
            </button>
          )}
        </div>
      </form>
    </div>
  );
}
