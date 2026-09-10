# Central message embeddings

`agentsview pg vectors build --target <existing-target> --machine <source-machine>`
encodes a finite pass of sessions already published to PostgreSQL. The machine
argument selects the original source; it does not change session ownership,
project, or reader permissions. Use the existing source's database identity and
row-level policies. The runtime must not use a migration or administrative role.

The command requires an existing generation matching the configured embedding
recipe and its chunk table. It never creates a generation, changes grants, runs
source discovery, or starts a scheduler. Generation IDs are resolved by
fingerprint rather than copied from a local SQLite ordinal. Matching completed
publications are reused without model calls. The existing model client and run
reducer/chunker preserve the local producer's recipe.

Before enabling this command, the schema owner must apply this additive change
through the normal migration process:

```sql
ALTER TABLE vector_push_state ADD COLUMN IF NOT EXISTS source_revision TEXT;
```

Existing table-level privileges cover the new column; column-only grants need
an explicit grant on this field. No policy changes are needed when the worker
uses each original source's existing ingest identity. The command fails before
inference if the column or generation is absent. The normal privileged schema migration also installs this column. Deploy the
schema before the producer binary; existing restricted ingest roles only probe
an already migrated substrate.

A source checkpoint combines the relational transcript revision, publication
timestamp, and grouping/ownership metadata. Only pending source metadata is
selected; unchanged transcripts are not loaded. Source content is read from one
repeatable-read snapshot, and each session's complete vector set plus checkpoint
is installed atomically after locking and rechecking the source row. A source
that changes during inference remains pending for the next pass. The normal
relational writer must continue updating transcript_revision/updated_at in the
same transaction as message changes.

An exclusive PostgreSQL advisory lock prevents duplicate central passes in one
schema and releases on worker connection loss. Publication uses that exact
connection, so a disconnected worker cannot publish using a replacement pool
connection. A normal source push is serialized by the session row lock. The
command does not transfer the source publisher's identity to the encoder host.

Defaults are25 examined sources,256 encoded chunks,16MiB maximum source text,
and a five-minute whole-pass deadline. `--max-sources`, `--max-chunks`,
`--max-source-bytes`, and `--timeout` expose these finite bounds. Requests use
the existing configured batch size, sequentially. A source larger than a whole
pass limit returns an actionable bound error; increasing the explicit bound
admits it without discarding prior checkpoints. Interrupted unfinished sources
retry; committed current sources do not re-encode. JSON output reports examined,
published, reused, deferred, encoded chunks, requests and budget completion.

Run finite passes from the existing server-owned source sync cadence; source
laptops need only publish relational updates when online. Scheduling, runtime
credential custody, initial checkpoint transfer, freshness/error reporting and
consumer search verification remain deployment responsibilities. This command
publishes only its selected existing generation; it does not switch a reader's
configured generation or claim whole-corpus activation. Keep the existing reader
recipe until the intended scope is complete and verified.
