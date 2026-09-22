---
title: Optional local history budget
description: Keep recent local history while preserving a complete central archive and verified recovery copies.
---

# Optional local history budget

AgentsView keeps its complete SQLite archive by default. A managed deployment
with an authoritative PostgreSQL archive and verified backups can explicitly
run `agentsview cache trim --max-bytes 5000000000` while its local daemon is
stopped. The command acquires the normal archive and vector writer locks.

The budget counts regular files below the configured data directory, including
SQLite, vectors, logs and migration backups, and the configured archive/vector databases when moved onto another volume.
Other symlink targets, the recovery `backups/` directory and a managed
native-vault job's transient `.agentsview-native-vault-metadata/` snapshot
staging are excluded.
Eligible sessions are removed oldest first by normalized last-activity time,
with session ID breaking ties. Pinned, incomplete, unpublished or unverified
sessions stay local even if this prevents meeting the budget. If auxiliary
files alone exceed the budget, transcript eviction does not run.

Only local message bodies, tool payloads, FTS and derived local vectors are
evicted. Small session identity and source-fingerprint records remain, together
with durable local eviction receipts. Incremental sync, parser upgrades and
full resync respect these receipts. A genuinely changed source is parsed in
full before local content becomes publishable again. Eviction is never a
central deletion or an empty replacement. PostgreSQL transcript/vector rows
and source files are preserved.

Evicted sessions are absent from local lists/search; direct session, message
and tool-call reads return HTTP 410 and direct users to the central archive.
The central viewer continues to serve the full history. This command does not
change the retention of source files or backups.

## Backup integration

1. As the backup administrator, run
   `agentsview cache refresh-coverage --max-duration 5m` before opening a dump
   snapshot. It installs PostgreSQL statement triggers and saves complete
   normalized content checks in `cache_coverage_state_v1`. Grant device accounts
   SELECT on this table, never INSERT, UPDATE, DELETE or TRUNCATE. Source writes
   invalidate checks in their own transaction; device accounts cannot forge
   them. This opt-in command requires ownership of the source tables.
2. Export a PostgreSQL repeatable-read snapshot and keep its transaction open.
   Capture `agentsview cache coverage --snapshot SNAPSHOT` and `pg_dump` using
   that same snapshot. The coverage contains identifiers and normalized content
   fingerprints, not transcript text. A protected `AGENTSVIEW_CACHE_PG_URL`
   environment variable can select the backup connection.
3. Encrypt and upload the dump and its coverage manifest, then verify readback.
4. Only after verification, add a nonempty `backup_id` identifying the artifact
   and publish the coverage JSON as `sync_metadata.cache_backup_coverage_v1`.
   Reserve writes to this key for the backup authority; device ingest accounts
   must only read it. Alternatively an operator can supply the verified
   coverage through `--verified-backup FILE`.

Refresh processes only unchecked or changed sessions, oldest first, and commits
bounded batches. An interruption loses at most the current batch; the next run
reuses completed checks. A timeout is a successful partial refresh, reported in
its JSON result. The snapshot captures only checks still valid in that snapshot,
so a partial refresh can support cleanup without certifying unchecked history.
Initial archive coverage can span runs; recurring work scales with changed
content. Capture and trim read saved checks without transferring central
transcript bodies. Missing or disabled invalidation triggers prevent use of the
store; administrator repair clears its checks before rebuilding them.

Trim requires coverage captured within 72 hours and compares each candidate
against both that backup and current PostgreSQL content. Ordinary daily backup
failure therefore protects local history. The backup integration must retain
the referenced verified artifact beyond this validity period. Connection,
proof or comparison failures never authorize eviction. An under-budget run
does not open databases or contact PostgreSQL.

Compaction returns free SQLite/vector pages to disk. Allow temporary free disk
space for SQLite VACUUM; a failed compaction can leave a valid but oversized
cache and reports failure. Stop using an older executable after enabling this
policy: it does not understand eviction receipts. Recovery uses a compatible
release or restoration of a complete archive.
