# CLAUDE.md: Hearing Evidence (Fabric legal-audio POC)

## Who and how to work with me
- I'm Shiv, a research engineer (Windows, VS Code, PowerShell). Goal: a portfolio project for AI/data engineering interviews. LEARNING is the main aim.
- I want to understand each piece before it is built. Explain concepts briefly in plain words, one small step at a time (90 min max per task), then check my understanding.
- Be direct. Push back on bad ideas. Mark anything preview, unverified or paid.
- Hands-on UI for the Fabric part. AWS setup and data fetching can be scripted.
- Purpose test: add a tool or model only if the data flow needs it, and only if a measured result justifies it.

## Project
A legal-tech data engineering POC in Microsoft Fabric linking court hearing audio (unstructured) to case records (structured).
Repo: github.com/spykeshivam/hearing-evidence-fabric (public). Layout: contracts/, scripts/, src/hearing_evidence/, tests/, fabric/ (Git-synced), docs/.

## Data (all public)
Four Supreme Court hearings: NVIDIA v. Ohman J:or Fonder (23-970, term 2024), Facebook v. Amalgamated Bank (23-980, term 2024), Gonzalez v. Google (21-1333, term 2022), Twitter v. Taamneh (21-1496, term 2022).
- CourtListener API v4 gives docket, panel and audio (audio_files link from the docket; MP3 at https://storage.courtlistener.com/ + local_path_mp3). Free limits: 5/min, 50/hour, 125/day. Pace calls, never loop probes, ask me before any CourtListener call.
- Oyez (undocumented API): api.oyez.org/cases/{term}/{docket} -> oral_argument_audio[].href -> transcript.sections[].turns[] with speaker.name, start, stop, text_blocks. It is the ground truth. Oyez audio is access-denied, so CourtListener's MP3 is the audio. Checked for NVIDIA: last Oyez stop 5230.49 s vs CourtListener duration 5233 s.
- Licences: Oyez is CC BY-NC 4.0 (attribute, non-commercial, no endorsement implied). CourtListener terms: no explicit data licence; respect rate limits, one account, never share the token. Transcript text is Oyez's own published transcript; label speaker resolution and citation extractions as our own AI outputs. Keep Oyez files out of the repo.

## Architecture and decisions
- fetch_and_stage.py -> S3 (eu-north-1, private) -> OneLake shortcut (read-only, Files/he-raw in lh_bronze) -> medallion across three lakehouses (lh_bronze, lh_silver, lh_gold) in workspace ws-hearing-evidence-dev.
- Bronze (nb_01_bronze_ingest): raw plus lineage columns, append-only, incremental by sha256 manifest; verifies every MP3's hash against the manifest.
- Silver (nb_02_silver): promotes only verified hearings; builds speaker turns, hearings (with audio duration measured from the MP3) and case participants with their side, from Oyez reference turns and CourtListener metadata.
- Gold (nb_03_gold): builds only from a passing silver run. Star schema at turn grain (fact_utterance, dim_case, dim_hearing, dim_speaker) plus gold_evidence_index for search. dim_case is built from slv_hearing and assumes one hearing per case (check docs/decisions.md, 2026-11-11 for more details).
- Each layer writes its checks to a dq_results table and stops on a blocking failure.
- Governance and quality: YAML data contracts per source, sha256 and provenance columns, restricted-case row-level security in the semantic model, per-layer data-quality checks that fail on zero rows.
- Fabric items: Spark notebooks, SQL endpoint, Direct Lake semantic model (sm_hearing_evidence), report (rpt_hearing_evidence), Git integration. Skipped on purpose: Warehouse, Purview, Fabric IQ ontology (preview), Dataflows, MLflow.
- Deliverables: a report tile for speaking time by case, side and speaker.
- Out of scope / future work: tracked as GitHub issues in the repo, not listed here.

## Money and cloud (strict)
- Own-money budget under 20 GBP. A paid F2 capacity (hearingevidencef2, UK West) runs on Azure free credit. Assign the workspace to it AT CREATION.
- The capacity bills hourly while running. Never leave it running: ask me to pause it after every session. Never click "Upgrade" on the subscription. No overage protection.
- AWS free plan: eu-north-1 only. IAM users: he-raw-fabric-user (read-only, policy he-raw-fabric-read), he-uploader (CLI profile he-uploader).
- Never print, log or commit secrets. Tokens come from env vars (CL_TOKEN, HE_BUCKET). Do not run anything that creates cloud resources or costs money without asking.

## Conventions
- Windows and PowerShell. Use uv (uv sync, uv run ruff check ., uv run ruff format ., uv run pytest). Use Invoke-WebRequest with -UseBasicParsing.
- Logic goes in src/hearing_evidence/ (tested); scripts only call it. Heavy libraries stay in optional extras. Commit small, branch and PR, keep CI green.
- Every function gets a docstring: short for a simple one, longer for a complex one, stating what it does (see "Writing docs" for what goes in it).
- Keep a running log in docs/decisions.md.

## Current state
Done: uv, ruff, pytest and CI in place; AWS bucket, uploader and CourtListener token tested; scripts/fetch_and_stage.py fetches the four hearings, writes a sha256 manifest and uploads to S3; workspace ws-hearing-evidence-dev on the paused F2 capacity, with three lakehouses and an S3 shortcut at Files/he-raw; nb_01_bronze_ingest, nb_02_silver and nb_03_gold built and running end to end; sm_hearing_evidence (Direct Lake) and rpt_hearing_evidence built over gold.
Note: the project folder still sits inside OneDrive; move it to C:\dev.

## Working agreement

### 1. Writing docs
- Describe only what exists today: the current infrastructure, code, data and decisions.
- Do not describe what was removed, replaced or abandoned. No "previously", "no longer", "used to", "we switched from".
- Write in positive statements. Avoid contrast constructions such as "not X but Y", "this isn't X, it's Y" and "rather than X". State Y.
- Keep docs short, and update the affected doc in the same commit as the change.
- docs/decisions.md is the one place that records choices and their reasons. No need to mention history of the system anywhere else.
- Same rule for docstrings and code comments: state what the file/function does and its workflow, not why it was created or what decision shaped it. If current behaviour comes from a logged decision, cite it as "check docs/decisions.md, <date> for more details" instead of retelling it.

### 2. Chat replies
- Answer exactly what was asked. A simple question gets the simple answer, with no extras, no recap and no unrequested next steps.
- Add context only when asked, or when a risk (cost, data loss, secrets) needs a warning.

### 3. Explaining things
When I ask you to explain a topic:
1. Split it into small sub-topics and list them with one line each.
2. Ask which ones I find difficult or don't understand. Stop and wait.
3. Explain only the ones I pick, with a concrete example from this project.
4. Connect the pieces into the bigger picture, in a few sentences.
5. Ask one question to check my understanding.

### 4. Workflow (SDLC)
- Work on a branch and worktree per task, open a pull request, merge only when CI is green. Reference the issue in commits.
- Never delete fabric-sync (local or remote), even after its PR merges; after a merge, keep it and fast-forward it to main.
- Whenever a task has become big enough to push it, remind me than I need to push. We don't want to pile up lots of code in a single PR.
- Write or update tests with every change to code in src/. Run `uv run ruff check .`, `uv run ruff format .` and `uv run pytest` before committing.
- Small commits with clear messages. Never commit secrets, audio, Oyez files or data/.
- Keep logic in src/hearing_evidence/ and scripts thin.
- Plan first for anything touching more than two files: state the plan in a few lines and wait for my approval.

### 5. Safety
- Ask before any command that creates, resizes or deletes cloud resources, calls the CourtListener API, or costs money.
- After any Fabric work session, remind me to pause the capacity.