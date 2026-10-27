# CLAUDE.md: Hearing Evidence (Fabric legal-audio POC)

## Who and how to work with me
- I'm Shiv, a research engineer (Windows, VS Code, PowerShell). Goal: a portfolio project for AI/data engineering interviews. LEARNING is the main aim.
- I want to understand each piece before it is built. Explain concepts briefly in plain words, one small step at a time (90 min max per task), then check my understanding.
- Be direct. Push back on bad ideas. Mark anything preview, unverified or paid.
- Hands-on UI for the Fabric part. AWS setup and data fetching can be scripted.
- Purpose test: add a tool or model only if the data flow needs it, and only if a measured result justifies it.

## Project
A legal-tech data engineering POC in Microsoft Fabric linking court hearing audio (unstructured) to case records (structured).
Repo: github.com/spykeshivam/hearing-evidence-fabric (public). Layout: contracts/, ontology/, scripts/, src/hearing_evidence/, tests/, fabric/ (Git-synced), eval/labels/, terraform/, docs/.

## Data (all public)
Four Supreme Court hearings: NVIDIA v. Ohman J:or Fonder (23-970, term 2024), Facebook v. Amalgamated Bank (23-980, term 2024), Gonzalez v. Google (21-1333, term 2022), Twitter v. Taamneh (21-1496, term 2022).
- CourtListener API v4 gives docket, panel and audio (audio_files link from the docket; MP3 at https://storage.courtlistener.com/ + local_path_mp3). Free limits: 5/min, 50/hour, 125/day. Pace calls, never loop probes, ask me before any CourtListener call.
- Oyez (undocumented API): api.oyez.org/cases/{term}/{docket} -> oral_argument_audio[].href -> transcript.sections[].turns[] with speaker.name, start, stop, text_blocks. It is the ground truth. Oyez audio is access-denied, so CourtListener's MP3 is the audio. Checked for NVIDIA: last Oyez stop 5230.49 s vs CourtListener duration 5233 s.
- Licences: Oyez is CC BY-NC 4.0 (attribute, non-commercial, no endorsement implied). CourtListener terms: no explicit data licence; respect rate limits, one account, never share the token. Transcript text is Oyez's own published transcript; label speaker resolution and citation extractions as our own AI outputs. Keep Oyez files out of the repo.

## Architecture and decisions
- S3 (eu-north-1, private) -> OneLake shortcut (read-only, Files/raw in lh_bronze) -> medallion across three lakehouses (lh_bronze, lh_silver, lh_gold) in workspace ws-hearing-evidence-dev.
- Bronze: raw plus lineage columns, append-only, incremental by sha256 manifest. Silver: typed, cleaned, resolved. Gold: star schema (dim_date, court, case, party, speaker, hearing, audio_file, citation; fact_utterance, fact_citation_mention; bridges) plus an evidence index.
- Audio: librosa quality gate with quarantine.
- Transcripts: Oyez reference turns are the source for fact_utterance and the speaking-time tile; citation extraction runs on Oyez transcript text.
- NLP: rapidfuzz name resolution against panel and advocates; eyecite vs an LLM extractor (pydantic validation) scored on 40 hand-labelled utterances.
- Governance and quality: YAML data contracts per source, sha256 and provenance columns, restricted-case row-level security in the semantic model, per-layer data-quality checks that fail on zero rows.
- Fabric items: Spark notebooks, one Data Factory pipeline with gates, SQL endpoint, Direct Lake semantic model, Git integration. Skipped on purpose: Warehouse, Purview, Fabric IQ ontology (preview), Dataflows, MLflow.
- Deliverables: report tiles (speaking-time split, citation F1), pipeline health page, restricted-case demo, evidence search.
- Terraform at the end: workspace, role assignment, three lakehouses; destroy and rebuild test.

## Money and cloud (strict)
- Own-money budget under 20 GBP. A paid F2 capacity (hearingevidencef2, UK West) runs on Azure free credit. Assign the workspace to it AT CREATION.
- The capacity bills hourly while running. Never leave it running: ask me to pause it after every session. Never click "Upgrade" on the subscription. No overage protection.
- AWS free plan: eu-north-1 only. IAM users: he-raw-fabric-user (read-only, policy he-raw-fabric-read), he-uploader (CLI profile he-uploader).
- Never print, log or commit secrets. Tokens come from env vars (CL_TOKEN, HE_BUCKET). Do not run anything that creates cloud resources or costs money without asking.

## Conventions
- Windows and PowerShell. Use uv (uv sync, uv run ruff check ., uv run ruff format ., uv run pytest). Use Invoke-WebRequest with -UseBasicParsing.
- Logic goes in src/hearing_evidence/ (tested); scripts only call it. Heavy libraries (librosa, NLP) stay in optional extras. Commit small, branch and PR, keep CI green.
- Keep a running log in docs/decisions.md.

## Current state
Done: repo and skeleton pushed; uv, ruff, pytest and CI added; AWS bucket and uploader tested; CourtListener and Hugging Face tokens work; Azure subscription and paused F2 capacity exist; Oyez and CourtListener probes worked; scripts/fetch_and_stage.py written (fetches the four hearings, writes a sha256 manifest, uploads to S3).
Next: finish the dry run (python scripts/fetch_and_stage.py --no-upload), then upload; write the contract YAMLs and their field lists; then Fabric day 1 (workspace on the capacity, three lakehouses, S3 shortcut, bronze notebooks, librosa quality gate).
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