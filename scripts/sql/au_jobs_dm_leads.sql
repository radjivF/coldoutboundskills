-- AU LinkedIn jobs → DM leads (Instantly campaign mirror)
-- Run in Supabase SQL editor once, then pipeline/backfill can upsert.

create table if not exists public.au_jobs_dm_leads (
  id uuid primary key default gen_random_uuid(),
  email text not null,
  first_name text,
  last_name text,
  company_name text,
  website text,
  linkedin_url text,
  job_title text,              -- role the company is hiring for
  position text,               -- decision-maker title
  instantly_campaign_id text,
  instantly_lead_id text,
  source text not null default 'au_jobs_dm_pipeline',
  run_date date,
  raw jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint au_jobs_dm_leads_email_key unique (email)
);

create index if not exists au_jobs_dm_leads_run_date_idx
  on public.au_jobs_dm_leads (run_date desc);

create index if not exists au_jobs_dm_leads_company_idx
  on public.au_jobs_dm_leads (company_name);

alter table public.au_jobs_dm_leads enable row level security;

-- Service role bypasses RLS; no anon/authenticated policies (private table).

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists au_jobs_dm_leads_set_updated_at on public.au_jobs_dm_leads;
create trigger au_jobs_dm_leads_set_updated_at
  before update on public.au_jobs_dm_leads
  for each row execute function public.set_updated_at();
