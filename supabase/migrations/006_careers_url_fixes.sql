-- 006_careers_url_fixes.sql
-- Careers pages found on 2026-11-19. The app applies these automatically on the
-- next ingestion run (src/lib/ingestion/company-fixes.ts); this file is the record.
-- Each update only touches a row that still has the old value.

UPDATE companies SET careers_url = 'https://www.artiphishell.com/careers/'      WHERE slug = 'artiphishell' AND careers_url IS NULL;
UPDATE companies SET careers_url = 'https://www.spacture.ai/careers'            WHERE slug = 'spacture'     AND careers_url IS NULL;
UPDATE companies SET careers_url = 'https://heytutor.applytojob.com/apply'      WHERE slug = 'heytutor'     AND careers_url IS NULL;
UPDATE companies SET careers_url = 'https://www.indeed.com/cmp/Healtharc/jobs'  WHERE slug = 'healtharc'    AND careers_url IS NULL;
UPDATE companies SET careers_url = 'https://www.trylifeguard.com/careers'       WHERE slug = 'lifeguard'    AND careers_url IS NULL;
UPDATE companies SET careers_url = 'https://careers.smartbarrel.io/jobs'        WHERE slug = 'smartbarrel'  AND careers_url = 'https://smartbarrel.io/careers/#jobslisting';
