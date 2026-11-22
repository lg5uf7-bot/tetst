# Phase 1: Initial Load Repo (GitHub Actions → Supabase)

This clean repo loads your initial dataset into Supabase in 3 manual steps. Once done, you can replace it with the clean daily update-only workflow.

## 1. Add GitHub Secrets
In your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**
- `SUPABASE_URL` → Supabase **Project Settings → Data API → Project URL**
- `SUPABASE_SERVICE_ROLE_KEY` → Supabase **Project Settings → API Keys → service_role** secret

## 2. Run the 3 Steps in Order
Go to **Actions → Initial Load — Companies, ATS Boards & Jobs → Run workflow**:

1. **`sync_companies_only`** — Loads all **24,077** companies from `all.json` (~10 sec)
2. **`apply_verified_ats`** — Activates the **1,960** verified ATS boards (~2 sec)
3. **`sync_jobs_only`** — Fetches all **33,136+** live jobs across 1,532 ATS APIs (~50 sec)
