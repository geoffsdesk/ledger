-- Spanner schema for ledger (GoogleSQL dialect).
-- Apply with: gcloud spanner databases ddl update <db> --instance <inst> --ddl-file=schema.sql
-- or via the --init flag against the emulator.

CREATE TABLE kine (
  id              INT64       NOT NULL,
  name            STRING(MAX) NOT NULL,
  created         BOOL        NOT NULL,
  deleted         BOOL        NOT NULL,
  create_revision INT64       NOT NULL,
  prev_revision   INT64       NOT NULL,
  lease           INT64       NOT NULL,
  value           BYTES(MAX)  NOT NULL,
  old_value       BYTES(MAX)  NOT NULL,
) PRIMARY KEY (id);

-- Single-row counters: 'revision' (the global revision allocator) and
-- 'compact_revision' (the compaction watermark).
CREATE TABLE kine_meta (
  k STRING(MAX) NOT NULL,
  v INT64        NOT NULL,
) PRIMARY KEY (k);

-- Latest-version-per-key lookups and prefix range scans.
CREATE INDEX kine_name_id ON kine(name, id DESC);

-- Lease reaping: live keys attached to an expiring lease.
CREATE INDEX kine_lease ON kine(lease, name);
