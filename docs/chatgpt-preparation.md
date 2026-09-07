---
title: Prepare an admitted ChatGPT export
description: Select hash-bound ChatGPT conversations before database import while retaining raw mappings and referenced assets
---

`agentsview import prepare-chatgpt EXPORT_DIR --manifest MANIFEST.json --output NEW_DIR`

This command does not load application configuration or open a database. The
manifest records an externally verified admission decision; the command does not
infer project membership from conversation titles or certify that evidence.

```json
{
  "version": 1,
  "membership_evidence": "membership.json#verified-conversations",
  "conversation_ids": ["example-conversation"],
  "shards": [
    {"name": "conversations-000.json", "sha256": "<exact SHA-256>"}
  ]
}
```

Declare every conversation shard in the export directory. Missing, undeclared,
changed or malformed shards, duplicate/conflicting identities and missing admitted
IDs fail before output publication. Every unlisted conversation is excluded.
Only explicitly referenced assets from admitted conversations are copied;
missing or ambiguous referenced assets fail preparation. Source files remain
unchanged. The output directory must not already exist; it is published by rename
with mode 0700, containing mode 0600 files and a hashed preparation receipt.

The output contains the original selected JSON objects, including all message
IDs, parent/child links and sibling branches. The existing importer still displays
current-node ancestry and preserves previously archived branches additively;
preparation does not make every raw branch searchable.

After separately confirming the intended archive and its access policy, use the
existing `agentsview import --type chatgpt NEW_DIR` command. Rerunning that import
resumes through per-session atomic writes and additive message matching. A
preparation receipt proves the selected source artifact, not database import,
project inventory completeness, search coverage or ongoing project membership.
Do not point the unfiltered importer at the original mixed export.
