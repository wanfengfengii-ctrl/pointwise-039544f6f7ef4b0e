# Offline transaction capture

Python 3.11 standard library and SQLite; no network or external database.
Run from this directory:

```sh
python3.11 -B -m unittest discover -s tests -t . -v
python3.11 -B -m capture.intake.collector demo
```

The demo uses a temporary archive, pauses after a child rollback, closes,
reopens and completes two transactions. Before commit the archive is empty;
after recovery it contains accounts 1=10 and 2=30 with two receipts. It then
runs a second archive through the late-object scenario of OBJECTS.md: a commit
is accepted while its referenced object is still incomplete, waits durably
across a reopen, and publishes once the last part arrives. Tests create only
their own temporary directories. No installation is needed.

## Public API

`capture.intake.collector.open(db_path, source, *, hooks=None)` returns a
context-managed Collector. The archive is created if absent. A database binds
one source session, event format version 1 and fixed schemas. Another session,
schema or unsupported archive version is rejected without changing existing
business state. This is a single worker, synchronous feed API; sources and
Collector instances are not shared across threads.

The source implements `capture.source.session.SourcePort`:

- `describe()` returns `SourceSession(session_id, event_version, tables)`.
- `poll(after_position)` returns `PollResult("segment", segment)`,
  `PollResult("idle")` or `PollResult("end")`.
- `acknowledge(position)` idempotently confirms a position. Success can take
  effect even when its response raises an exception.
- `close()` releases source resources.

`ReplaySource` is the finite local implementation. It supports bounded batches,
specified idle poll numbers, one lost acknowledgement response at specified
positions, and optional disposal of acknowledged source events.
`load_replay_events(path)` loads a finite JSON event list. A fresh ReplaySource
can represent a reopened session; production source ownership is outside V1.

Collector methods:

- `feed({"events": [...]})` or `feed(Segment(...))` accepts one nonempty segment.
  `FeedResult.accepted_position` is the durable source prefix;
  `completed_top_ids` contains newly published top IDs in completion order.
  Exact commit replay returns no new completions.
- `poll_once()` drives a source result and returns FeedResult or None.
  `last_poll_state` distinguishes segment, idle and end. Idle/end never commit.
- `checkpoint()` returns an immutable IntakeCheckpoint with
  `accepted_position`, `confirmed_position`, `pending_position` (None when
  confirmed), and sorted `unfinished_top_ids`. Reading has no side effects.
- `close()` is idempotent, closes source and database, and preserves unfinished
  transactions. Feed/checkpoint/poll after close raise CollectorClosedError.

`collector.archive.read(table, keys=None)` reads published full rows.
An independent `capture.archive.query.Archive(db_path)` reader is also a context
manager. Keys are a list of key tuples/lists, including for one-column keys.
All results use declared primary-key tuple order, with numeric integer ordering;
duplicate requested keys do not duplicate rows. Empty keys selects no rows.
Each read owns one SQLite snapshot and excludes unfinished transactions.

Example schema:

```python
from capture.source.session import SourceSession
from capture.source.replay import ReplaySource
from capture.intake.collector import open
tables = {
    "accounts": {
        "columns": {
            "id": {"type": "int", "nullable": False},
            "value": {"type": "int", "nullable": False}},
        "primary_key": ["id"]}}
source = ReplaySource(SourceSession("local", 1, tables), [])
with open(":memory:", source) as collector:
    assert collector.archive.read("accounts") == []
```

Columns have exact types int, str, float or bool and an explicit nullable bool.
Full rows contain exactly the declared columns. Primary keys consist of
nonnull int/str columns; bool never counts as int. Float requires a finite
JSON float. Primary key order is significant; JSON object member order is not.

## Events and transaction semantics

Each event has exactly session_id, source_position, event_id, top_id,
branch_id, kind and payload; branch events additionally have parent_id.
Identifiers are nonempty strings. Positions are positive consecutive integers.
All controls use empty payloads. JSON values must be finite and canonicalizable.
The finite sample is in samples/events.json.

| kind | Meaning |
| --- | --- |
| begin | Establish a new top and its root branch |
| branch | Establish a new branch under an existing active parent in that top |
| row | Apply a full-image insert, update or delete to the indicated active branch |
| seal | Stop further writes and child creation on a child branch; do not publish |
| rollback | Revoke a child and all descendants, including sealed descendants |
| commit | Complete the top, addressed to its root, and publish surviving modifications |
| abort | Complete the top without changing published rows or creating a receipt |

Branches are scoped by top_id. A branch cannot be reparented or reused.
A sealed branch remains under its ancestors. Rollback cannot address the root;
use abort. A new rollback on an already aborted branch is rejected. New events
after top completion are rejected; exact replay of any accepted event is safe.
Top commit may include still-active children. No source concurrency arbitration
is performed: each candidate starts from the current archive at its publication
point and interprets its surviving row events in source_position order.

Row payloads:

```json
{"table":"accounts","op":"insert","row":{"id":1,"value":10}}
{"table":"accounts","op":"update","key":[1],"row":{"id":2,"value":20}}
{"table":"accounts","op":"delete","key":[2]}
```

Update requires an existing old key and carries a complete new row. A key
change removes the old key and writes the new key as one operation; an occupied
target is an error. Insert requires absence and delete requires existence.
The collector validates each incoming row format and each transaction's
current surviving interpretation on row/rollback events; commit revalidates
against the publication-time archive. There is no sparse-image completion.
Parent A=10, child A=20, rollback child, commit produces A=10. Child deletes
and child key moves are similarly undone by replaying only surviving changes.

## Late-arriving large fields

OBJECTS.md extends the same envelope with root-branch object events and
non-primary-key string references. The event format version stays 1; all V1
interfaces, error types and tests are unchanged.

New event kinds, always addressed to the root branch of an already begun top
(non-root is TransactionRelationError; before begin, after abort or after
publication is TransactionStateError):

```json
{"kind":"object_part","payload":{"object_id":"X","index":0,"data":"5Lit"}}
{"kind":"object_seal","payload":{"object_id":"X","parts":2,"size":6,"sha256":"..."}}
```

An object identity is (session, top, object_id); objects never cross tops.
`data` is canonical standard Base64 (no whitespace, URL alphabet, bad padding
or nonzero padding bits; decoding and re-encoding must reproduce it). Parts
may arrive out of order, before or after the seal, and may be empty; indexes
concatenate from zero. A repeated part is accepted only with identical bytes,
a repeated seal only with an identical (parts, size, sha256) declaration.
Once a seal exists, part indexes must stay below `parts`, and a seal that
arrives after parts checks their indexes. When every declared part is present
the assembled bytes are checked immediately against `size` and `sha256` and
must be strict UTF-8; a single character may be split across parts. All of
these violations are EnvelopeError and reject the whole segment, even for
objects that are unreferenced, withdrawn or overwritten.

In insert/update rows, a non-primary-key str column (nullable or not) may
hold exactly `{"$object":"X"}` instead of a string or an allowed null. Other
dictionaries, empty object ids, non-str columns and primary-key references
are RowValidationError. A reference denotes the final validated string of
that object inside the same top transaction. Reads never return references,
placeholders or partial content.

### Waiting and publication states

Commit freezes the top's rows and lineage. Afterwards only root-branch object
events and exact replays are accepted for that top until it publishes; abort
still produces no receipt and admits only replays. A commit whose final
written rows reference incomplete objects is durably accepted into a waiting
state instead of publishing: the `pending` table holds the commit order and
`pending_refs` the object ids actually needed. Top states are therefore
`active` → `committed` (waiting, persisted) → published, or `active` →
`aborted`. `checkpoint().unfinished_top_ids` lists active plus waiting tops
in sorted order; accepted/confirmed/pending_position keep their V1 meaning.
Idle and end poll results never publish, complete or cancel waiting.

Only references in the transaction's final written rows are waited for;
references that were withdrawn, overwritten or deleted do not block, and the
wait set is never the union of historical references. Publication happens in
commit source_position order: after every newly accepted event the collector
publishes the longest ready prefix of waiting commits. A ready successor
never overtakes an incomplete predecessor, and a predecessor is not skipped
because a successor overwrites its rows. One late part can release several
old commits in a single feed. `FeedResult.completed_top_ids` lists the tops
published by that feed in order; replays list nothing. Each publication
interprets surviving operations against the real predecessor archive base and
keeps the original commit event identity for its receipt.

Row/rollback previews and commit revalidation interpret against a logical
base: the physical archive plus the surviving operations of every waiting
predecessor in commit order. Key occupancy is content independent, so
unresolved references in that overlay are harmless; a successor can update or
move a key that a waiting predecessor logically inserted. The overlay is
never visible through `Archive.read` and does not imply the predecessor has a
receipt. Uncommitted tops never enter the base.

Rows, receipts, object acceptance, wait state and accepted_position remain
one feed write unit: if a segment first releases old publications and then
meets an illegal event or a hook failure, the whole segment rolls back.

### Reclamation and evidence lifetimes

Object content is intake staging, not archive result. Once the source has
confirmed the part positions and no unpublished transaction can still need an
object (its top is published or aborted, or committed and not referencing
it), the next successful maintenance scrubs every persistent content copy:
the object store content, part bytes, and the canonical event bytes of
object_part identities in the permanent ledger. Raw segment payloads still
follow the V1 rule (confirmed and unreferenced by unfinished tops, so a
segment needed by a waiting top is retained whole). Published rows in the
archive are results and are kept. Permanent metadata survives: event
identity/position/kind, SHA-256 digests of canonical events, part digests and
seal declarations serve as dedup and recovery evidence, so exact replay after
reclamation is still idempotent while the same identity with changed content
is still EventConflictError; reclaimed objects are never rebuilt and no new
receipt appears. Content of objects that an active top might still reference,
or that a waiting top's final rows do reference, is always retained.

Recovery revalidates the accepted identity prefix (including scrubbed
object_part entries by digest), object accepted facts and seals against the
object tables, the unpublished journal and lineage, the pending commit order
and wait sets, and the receipt prefix before allowing further confirmation.
Inconsistent state is rejected with RecoveryConsistencyError; waiting and
visible rows are restored exactly.

`samples/objects.py` provides the public fixtures: `selfcheck` validates the
materials only, `verify` runs the withdrawn and ordered scenarios event by
event with independent reads, reopens and replays, and `demo` prints the
phases. `tests/test_objects.py` adds an independent small-domain reference
engine and the business regression for these rules.

A segment must be internally consecutive. It can be wholly repeated, entirely
new, or an already accepted prefix followed by a new suffix starting exactly
at accepted_position+1. Same ID/position/content is a duplicate. Any content
change, reused ID at another position, reused position, hole or wrong session
rejects the entire segment. Canonical content includes every envelope and
payload value. Accepted identity facts remain after raw payload reclamation.

## Persistence, errors and recovery

One feed write unit contains event identities, ordered per-top journal,
lineage, raw-payload references, all publications in that segment, unique
receipts, top completion and accepted checkpoint. No component commits inside
that unit. Even a later error after an earlier top completed in the segment
rolls the entire segment back. Rows and receipts become visible together.
ReceiptStore provides immutable session/top/completion identity and publication
sequence records. Source positions and publication sequences are separate.

The public error classes are EnvelopeError (source.envelope),
SegmentPositionError (source.segments), EventConflictError
(transactions.identity), TransactionRelationError (transactions.lineage),
TransactionStateError (transactions.completion), RowValidationError and
ArchiveParameterError (rows.schema), SessionConflictError (source.session),
StorageVersionError (archive.database), RecoveryConsistencyError
(intake.recovery), and CollectorClosedError (intake.collector).
CanonicalEncodingError rejects unstable JSON. Row and envelope errors reject
a complete feed; invalid read parameters never write. Storage exceptions
inside the feed write unit propagate unchanged after rollback.

Acknowledgements are sent only after durable feed commit. A failed or lost
confirmation does not turn accepted work into unaccepted work:
feed still returns its durable result, pending_position remains, and the
original exception is available at
`collector.acknowledgements.last_error`. Confirmation-local storage errors
also remain pending. They are retried by subsequent feed/poll calls.
Post-acceptance reclamation failures retain their original exception at
`collector.last_maintenance_error` and are retried. These maintenance outcomes
cannot undo an already durable feed or an external acknowledgement.

Raw segment payloads are collected only when confirmed and unreferenced by
every unfinished top. A shared payload remains while any long transaction
needs it. Completed journals release references, while permanent identity
facts, terminal lineage and receipts remain. Recovery validates the complete
accepted identity prefix against top/branch states, journal offsets, live
references, retained payloads, receipts and checkpoints before allowing further
source confirmation. Inconsistent state is rejected.

`PublicationHooks(callback)` is an explicit optional fault/observation port.
It receives after_rows, before_receipt and before_commit while the feed write
unit is open. Raising a real exception rolls the unit back. Default hooks do
nothing. Hooks must not reenter feed or close the collector.

## Code and regressions

Source modules validate sessions, canonical envelopes, segments and replay.
Transaction modules own identity, lineage, ordered journals, completion,
object acceptance and the durable pending queue. Row modules own schemas,
typed keys, operation interpretation, isolated workspaces and immutable net
candidates. Archive modules own SQLite units, publication, receipts and
snapshot queries. Intake owns the feed boundary, recovery cross-checks,
checkpoints, acknowledgement ordering, payload retention and the ordered
ready-prefix publisher. All 27 production modules participate in this path.

Tests/test_normal.py covers full-image CRUD, interleaving, key moves and
conflicts, whole-segment validation and typed/compound-key reads.
Tests/test_transactions.py covers parent restoration, sealed descendants,
siblings, delete/move restoration, abort, invalid lineage, terminal rejection
and an independent ordered reference across deterministic combined cases.
Tests/test_recovery.py covers canonical replay/conflict, overlap/gaps,
all publication fault hooks, independent reader visibility, per-event reopen,
lost acknowledgements, shared payload references, session/version guards,
close/resume and idle/end. Tests/test_contract.py additionally checks malformed
values, source/schema errors and corruption rejection before confirmation.
Tests/test_objects.py covers the OBJECTS.md rules: overwritten, deleted and
withdrawn references never wait, key occupancy against the logical base,
object declarations and conflicts, zero-part objects, cross-part UTF-8,
whole-segment rollback of triggered publications, interleaved commits released
in commit order, reopen at every acceptance boundary, lost acknowledgement
replies, reclamation gating and replay after reclamation, plus a generated
interleaving checked against an independent small-domain reference engine.

V1 has no network protocol adapters, schema evolution, snapshot backfill,
multiworker takeover, distributed transactions or historical row queries.
