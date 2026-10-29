# Decisions

Running log of choices made on this project and what we measured or observed as a
result. Add an entry whenever a non-obvious call is made on the code, data, or
process, so we can trace back *why* when something breaks later. Newest entries on
top.

Template:

```
## YYYY-MM-DD: <short title>

**Decision:** what we chose to do.

**Why:** the reasoning / constraint that drove it.

**Alternatives considered:** what else we looked at, and why not.

**Result:** (fill in later) what actually happened once this shipped.
```

---

## 2026-10-29: Contracts ride the S3 -> OneLake sync; bronze gates on schema conformance

**Decision:** `fetch_and_stage.py` reads `licence.id` out of `contracts/*.yaml`
(`load_contract`) instead of a hardcoded dict, and copies `contracts/*.yaml` into
`data/stage/_contracts/` (`stage_contracts`) on every run, so they sync to S3 and
through the OneLake shortcut alongside the evidence, landing at
`Files/he-raw/_contracts/*.yaml`. `nb_01_bronze_ingest` loads those two files and
validates every raw JSON payload (`brz_docket`, `brz_audio_meta`, `brz_oyez_case`,
`brz_oyez_transcript`) against its contract's JSON Schema, appending a PASS/FAIL
per file to `dq_results` and raising on an `error`-severity FAIL.

**Why:** The `required_provenance_fields` and licence values in the YAML contracts
were being hand-copied into `fetch_and_stage.py` (a literal `LICENCE` dict) with no
way to catch drift if the contract changed, and the contract's JSON Schema
(`schema.docket`, `schema.audio_meta`, `schema.case`, `schema.transcript`) was
validated only in a test skipped outside local dev (`tests/test_contract_schema.py`)
and never checked against what actually lands in bronze.

**Alternatives considered:** Validating in `fetch_and_stage.py` before upload
instead of in bronze; rejected for now since catching a schema break earlier is
a future improvement, not a measured gap today. Duplicating the YAML as a Fabric
notebook built-in resource (`fabric/nb_01_bronze_ingest.Notebook/builtin/`);
rejected because it would need a second, hand-kept copy of the same contract,
the exact drift risk this decision removes. `jsonschema` is not confirmed present
in the default Fabric Spark runtime, so the notebook cell installs it with
`%pip install jsonschema -q`; unverified until run in Fabric.

**Result:** _pending — run `fetch_and_stage.py`, confirm `Files/he-raw/_contracts/*.yaml`
appears via the shortcut, pull the notebook update into the Fabric workspace from
`fabric-sync`, and run it to confirm the `%pip install` and the new `dq_results` rows._

---

## 2026-10-24: `duration_match` must check measured MP3 duration, not CourtListener's `duration` field (issue [#19](https://github.com/spykeshivam/hearing-evidence-fabric/issues/19))

**Decision:** The silver `duration_match` check blocks on the measured MP3 duration, not CourtListener's reported `duration`; the CourtListener value becomes a warn-level monitored check instead.

**Why:** 21-1333 and 21-1496 failed `duration_match` by ~22 min. Measured duration (mutagen, sha256-verified files) agrees with the Oyez transcript's last stop to within 2s for all four hearings; CourtListener's `duration` field is wrong for these two, not the audio.

**Result:** _pending — implement the measured-duration check in the silver notebook._

---

## 2026-10-23: Silver promotes only sha256-verified hearings, rebuilds idempotently

**Decision:** Silver promotes only hearings whose MP3 `sha256` check passed in the
latest bronze verification run. Silver tables use fully qualified names
(`lh_silver.dbo.*`) and rebuild idempotently by overwrite.

**Why:** `sha256` is the check that matches all four served MP3s (see the
`sha1`/`sha256` decision below), so it's the correct gate for whether a
hearing's audio artefact is trustworthy enough to promote. Fully qualified
table names and overwrite rebuilds make the Silver notebook safe to rerun
without first deleting output or depending on which lakehouse is the current
default context.

**Alternatives considered:** Gating promotion on `sha1` as well as `sha256`;
rejected since `sha1` doesn't match any of the four served MP3s. Appending to
Silver tables on each run instead of overwriting; rejected because overwrite
from source gives a deterministic, idempotent rebuild.

**Result:** _pending._

---

## 2026-10-23: Bronze verification checks carry a severity, error or warn

**Decision:** Bronze verification checks carry a severity, `error` or `warn`.
A verification run fails only on `error`-level FAILs. The `sha1` check is
`warn`. Rows written before severity existed carry `NULL` for it; a backfill
fills them in by `check_id`.

**Why:** CourtListener's served `sha1` doesn't match any of the four served
MP3s, while `sha256` matches all four — the `sha1` on the docket describes a
different artefact (hypothesis: the original court file; unverified). A
mismatch on a field describing a different artefact isn't a sign the served
MP3 is wrong, so it shouldn't fail the run.

**Alternatives considered:** Dropping the `sha1` check entirely; rejected
because keeping it at `warn` keeps the discrepancy visible instead of hiding
it.

**Result:** _pending — the hypothesis that CourtListener's `sha1` describes
the original court file is unverified._

---

## 2026-10-23: Fabric Git integration tenant settings enable cross-region GitHub sync

**Decision:** The Fabric tenant has "sync with Git repositories", "sync with
GitHub repositories", and "export items to Git repositories in other
geographical locations" enabled. Only notebook code and other item
definitions commit to Git; no data moves this way.

**Why:** GitHub hosts repositories outside the capacity region (UK West), so
committing workspace items moves their definitions out of region. In a
regulated firm this is a data-residency decision that belongs to the tenant
admin.

**Alternatives considered:** Leaving cross-geography export off and keeping
Git sync limited to repositories inside the capacity's region; not available
as a separate tenant setting from the general Git/GitHub sync toggles.

**Result:** _pending._

---

## 2026-10-22: Contract `required` lists name only what downstream code depends on

**Decision:** Dropped five fields from `required` in `courtlistener_audio.yaml`
(`docket.case_name_short`; `audio_meta.case_name_short`, `stt_transcript`,
`filepath_ia`, `stt_source`, `ia_upload_failure_count`) and five in
`oyez_transcripts.yaml` (`case.decided_by`, `decisions`, `opinion_announcement`;
`speaker.roles`, `thumbnail`). They stay modelled under `properties` with their
existing types; `stt_source` and `ia_upload_failure_count` already allowed
`null` there and still do.

**Why:** These fields are legitimately empty or null for some of the four
staged hearings — e.g. `case_name_short` is `''` for 23-980, `stt_transcript`
is `''` for the two 2022-term hearings, and `decided_by` / `decisions` /
`opinion_announcement` are `null` for the two cases dismissed as improvidently
granted. A field that can legitimately be missing must not be `required`, or a
hearing with a different gap than the four we've seen fails validation for a
reason that doesn't matter.

**Alternatives considered:** Add `"null"` to the type of every dropped field;
rejected for `case_name_short`, `stt_transcript`, and `filepath_ia` since the
actual gap in the data is an empty string, not JSON `null` — adding `null`
there would accept a value the source never produces.

**Result:** The project's scope is fixed at these four hearings, so the
`required` lists are final: no further hearings will be staged that could
surface a gap in a field still marked `required`.

---

## 2026-10-20: Verify S3 uploads against the bucket, not the sync exit code

**Decision:** `fetch_and_stage.py` writes its manifest *after* the S3 sync, not
before. It checks every file against S3 itself via `aws s3api head-object`
(`src/hearing_evidence/s3_verify.py`), records a per-file `upload_status`
(`uploaded`, `size_mismatch`, or `missing`) in `objects.csv`, and only then pushes
that manifest up. The script exits non-zero if anything isn't confirmed uploaded.

**Why:** A real run hit `AWS_IO_SOCKET_TIMEOUT` on two of four MP3s. The sync
still exited non-zero, but because the manifest had already been written and
synced *before* that failure, the batch sitting in S3 looked identical to a
successful one — there was no way to tell from the bucket alone which files were
actually there. Moving towards Fabric, bronze needs to be able to tell a corrupt
or missing object from a legitimate one without re-deriving it from local state.

**Alternatives considered:** Trust `aws s3 sync`'s own exit code and skip
per-file verification; rejected because sync's exit code is run-level, not
per-object, so it can't say *which* files failed. Comparing content hashes
(sha256) against S3's ETag instead of size; rejected for now since ETag isn't a
plain MD5 once a file is multipart-uploaded (true for our larger MP3s), so a
clean hash comparison needs more than a one-line check — size mismatch is a
reasonable confirmation that a complete file arrived in the bucket.

**Result:** _pending — rerun the upload and confirm the two previously-failed
MP3s now show `uploaded`._

---

## 2026-10-20: Pre-push git hook mirroring CI

**Decision:** Added `.githooks/pre-push` (ruff check, ruff format --check, pytest)
and pointed git at it with `git config core.hooksPath .githooks`. It's checked into
the repo so it's visible and reusable, but `core.hooksPath` itself is a local git
config, not something a clone picks up automatically — anyone (including a fresh
clone of this machine) has to run that `git config` line once.

**Why:** A PR was pushed and only failed CI after the fact (ruff lint errors that
had been sitting in the first commit). Pushing doesn't run anything by itself;
this hook catches the same checks CI runs, locally, before the push leaves the
machine.

**Alternatives considered:** A local-only `.git/hooks/pre-push` (not versioned,
invisible to anyone else, lost on re-clone) and a hook-management tool like
pre-commit or husky (unjustified weight for a solo repo with three checks).

**Result:** (fill in later)

---

## 2026-10-20: Branch + PR workflow, with CI gating merges to main

**Decision:** Stop committing straight to `main`. All work happens on a branch, goes
through a pull request, and merges only once CI (`.github/workflows/ci.yml`: ruff
lint, ruff format check, pytest) is green. Branch protection is enabled on `main` to
enforce this (no direct pushes, required status checks).

**Why:** This is evidence-handling code — the commit history and PR history need to
hold up as a record of what changed, why, and that it passed checks, not just a
log of pushes.

**Alternatives considered:** Keep pushing to `main` directly and relying on
discipline alone; rejected because it leaves no enforced record and it's easy to
skip CI by accident.

**Result:** _pending — first PR under this workflow not yet merged._

## 2026-10-20: Secrets via `.env`, with `.env.example` committed

**Decision:** Real credentials (`CL_TOKEN`, `HE_BUCKET`) live in a local `.env` file
(already in `.gitignore`). A `.env.example` with only variable names — no values —
is committed so anyone cloning the repo knows what to set.

**Why:** GitHub's secret scanning is on by default for public repos, and this repo
is public. Listing variable names only in the committed example avoids ever risking
a real token in git history.

**Alternatives considered:** Document the required env vars only in the README;
rejected since a `.env.example` is the convention tooling (and contributors)
expect, and it's self-documenting next to the actual `.env`.

**Result:** _pending._

## 2026-10-19: Defer the audio-lookup fallback

**Decision:** `fetch_and_stage.py` resolves hearing audio only from the
CourtListener docket's own `audio_files` field. The earlier fallback to a
hardcoded `audio_id` per hearing was removed for now (left as a `TODO`).

**Why:** Wanted to see how often the primary lookup path (docket -> audio_files)
actually succeeds before building a fallback for the cases where it doesn't —
building the fallback first risked masking a lookup bug.

**Alternatives considered:** Keep the hardcoded `audio_id` fallback immediately;
rejected since it was one data point (the NVIDIA hearing) and not yet validated
against the other dockets in `HEARINGS`.

**Result:** _pending — need to run against all four hearings in `HEARINGS` and see
which ones fall through._
