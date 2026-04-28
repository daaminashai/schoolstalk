// Optional Postgres sink. When `DATABASE_URL` is set, every school upserts
// its teachers into `public.extracted_teachers` immediately after the per-
// school CSV is written. This means a crashed mid-batch run still has its
// teachers in Postgres for downstream phases (teacher-verifier) to consume.
//
// Schema expected (per the canonical pipeline):
//   public.extracted_teachers (
//     hs_id        int        not null,
//     first_name   text       not null,
//     last_name    text       not null,
//     title        text,
//     source_url   text,
//     email        text,
//     scraped_at   timestamptz default now(),
//     primary key (hs_id, first_name, last_name)
//   )
//
// If the schema doesn't have `email` (older deployments), columns are added
// best-effort on first connect. Failure to write does NOT fail the scrape —
// the CSV on disk is still the durable record.

import type { Teacher } from "./types";
import { SQL } from "bun";

let sqlPromise: Promise<any> | null = null;
let warned = false;

export function databaseConfigured(): boolean {
  return !!process.env.DATABASE_URL?.trim();
}

async function getSql(): Promise<any | null> {
  const url = process.env.DATABASE_URL;
  if (!url) return null;
  if (sqlPromise) return sqlPromise;

  sqlPromise = (async () => {
    // Bun.sql is the project's blessed Postgres client (CLAUDE.md).
    const sql = new SQL(url);
    // Best-effort schema bootstrap — safe to run repeatedly.
    try {
      await sql`
        create table if not exists public.extracted_teachers (
          hs_id      int  not null,
          first_name text not null,
          last_name  text not null,
          title      text,
          source_url text,
          email      text,
          scraped_at timestamptz not null default now(),
          primary key (hs_id, first_name, last_name)
        )
      `;
    } catch (err) {
      if (!warned) {
        console.warn(
          `[db] could not ensure extracted_teachers table: ${err instanceof Error ? err.message : String(err)}`,
        );
        warned = true;
      }
    }
    return sql;
  })();

  return sqlPromise;
}

/**
 * Upsert one school's teachers into `public.extracted_teachers`. Idempotent on
 * (hs_id, first_name, last_name) — re-running the same school updates rather
 * than duplicates. Silent no-op when DATABASE_URL is unset.
 */
export async function upsertTeachers(
  hsId: number | null,
  sourceUrl: string,
  teachers: Teacher[],
): Promise<{ ok: true; written: number } | { ok: false; written: 0; error: string } | null> {
  if (hsId == null || teachers.length === 0) return null;

  const sql = await getSql();
  if (!sql) return null;

  const rows = teachers.map((t) => ({
    hs_id: hsId,
    first_name: t.firstName,
    last_name: t.lastName,
    title: t.role || null,
    source_url: sourceUrl,
    email: t.email,
  }));

  try {
    // Bun.sql supports tagged-template multi-row insert. ON CONFLICT keeps the
    // newer values so re-scrapes overwrite stale data cleanly.
    await sql`
      insert into public.extracted_teachers
        (hs_id, first_name, last_name, title, source_url, email)
      values ${sql(rows, "hs_id", "first_name", "last_name", "title", "source_url", "email")}
      on conflict (hs_id, first_name, last_name) do update set
        title      = excluded.title,
        source_url = excluded.source_url,
        email      = excluded.email,
        scraped_at = now()
    `;
    return { ok: true, written: rows.length };
  } catch (err) {
    // DB failure must not kill the scrape — the CSV is the durable record.
    const error = err instanceof Error ? err.message : String(err);
    console.warn(`[db] upsert failed for hs_id=${hsId}: ${error}`);
    return { ok: false, written: 0, error };
  }
}
