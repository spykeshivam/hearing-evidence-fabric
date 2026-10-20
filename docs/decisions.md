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
