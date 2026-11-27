# Startup Jobs Intelligence v2 (Clean-Slate Gold Pipeline)

A zero-legacy (`no all.json`), high-precision (`90–99%`), continuously growing startup & jobs intelligence pipeline backed by **Supabase Postgres** and **GitHub Actions**.

## Architecture

- **Pure Direct Startup Sources (`src/sources.py`)**
  - `yc-oss` (`isHiring`, tags, stage, locations) + `ycombinator.com/companies/{slug}` (`founders` + `socials`)
  - `500.co/api/startups` (`companyLinkedIn`, business model, stage, country)
  - `a16z Speedrun` (`SR001–SR007` + embedded `faces[]` founders)
  - `Alchemist Accelerator` (paginated JSON:API)
  - `HN Who Is Hiring` (last 6 monthly threads)
  - `SEC Form D` (`primary_doc.xml` + `<relatedPersonInfo>` Executive Officers)
  - `Common Crawl` (auto-discovers latest `CC-MAIN-*` index via `collinfo.json`)
- **Strict 1-to-1 ATS Verification (`src/validators.py`)**
  - Database-enforced `domain` PK + `UNIQUE (ats_provider, ats_slug)` in `ats_boards`
  - Blocks `.edu` / `.gov` / `.mil` domains, checks backlinks & fuzzy brand ownership (`>= 55` score)
- **Full Job Descriptions + ML Enrichment (`src/ats_adapters.py`, `src/classifiers.py`)**
  - 12 ATS adapters (`ashby`, `greenhouse`, `lever` US+EU, `rippling`, `workable`, `bamboohr`, `smartrecruiters`, `breezy`, `pinpoint`, `recruitee`, `personio`, `teamtailor`)
  - Stores **both** `short_summary` AND full `description_text` + `content_hash` (skips unchanged jobs on daily sync)
  - Leak-free `CalibratedClassifierCV(LinearSVC)` (`industry_primary`, `customer_type`), `LogisticRegression` (`role_family`), case-sensitive `FlashText` (`skills[]`), strict `workplace_type`, `experience_level`, `salary_min`/`salary_max`, `offers_equity`
  - Logs `job_events` (`opened`, `reopened`, `closed`, `salary_changed`, `updated`) and aggregates `open_jobs_count`, `hiring_velocity_7d`, `tech_stack_inferred`, `median_salary_usd`, `remote_ratio` on `companies`
- **5-Layer `$0` Founders & Socials (`src/enrich_founders_socials.py`)**
  - `yc_page` -> `speedrun_faces` -> `500_direct` -> `sec_form_d` -> `wikidata_sparql` -> website `JSON-LD` + `/about`/`/team` + `r.jina.ai` fallback

## Setup

1. **Apply Clean-Slate Schema in Supabase (`supabase_v2_schema.sql`)**
   - In Supabase SQL Editor (or after dropping legacy v1 tables if wiping), execute `supabase_v2_schema.sql`.
2. **GitHub Secrets** (same names as Phase 1 — IPv4 HTTPS PostgREST):
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
3. **Run Initial Load**
   - GitHub Actions -> **01 Initial Gold Seed (Clean Slate v2)** -> Run workflow (`all_in_one`).
4. **Automated Schedules**
   - `02 Daily Job Sync & Velocity Delta`: runs daily at `06:00 UTC`.
   - `03 Weekly Discovery & Founder Enrichment`: runs Sundays at `04:00 UTC`.
