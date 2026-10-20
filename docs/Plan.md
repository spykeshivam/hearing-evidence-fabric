# Hearing Evidence: Fabric Legal-Audio POC Plan

Oct 13, 2026 · @Shiv

## 1. Pitch and success criteria

Hearing Evidence is a Fabric lakehouse that turns public court-hearing audio into searchable, measured, access-controlled evidence linked to case records. Target: 3 build days plus 1 buffer day, under £20.

**One-line flow:** S3 raw files, read in place through a OneLake shortcut, become bronze Delta tables, then silver (clean transcripts, speaker turns, resolved entities), then a gold star schema and a per-case evidence index, served through a Power BI semantic model.

**Done means all of these are true:**

- The 4 Supreme Court hearings flow S3 to gold to Power BI from one pipeline trigger, with no manual steps. The appeals-court hearing joins later as an unscored addition.
- ASR word error rate (WER) and diarization error are measured on the 4 Supreme Court hearings against Oyez, split by model and by audio quality bucket.
- Citation extraction is compared: eyecite vs an LLM extractor, on 40 hand-labelled utterances (precision, recall, F1).
- 3 planted bad audio files land in quarantine, and an empty batch fails the pipeline instead of passing quietly.
- A restricted-matter row-level security demo works in the semantic model.
- `terraform destroy` then rebuild passes a smoke test with identical gold row counts.

**Non-goals:** real-time streaming, production scale, any claim of legal or evidentiary accuracy, a custom web app, fine-tuning models.

### Deliverables you will show

Six things a reader can look at, each answering one question. Numbers in any mock-up are invented; real values come from the run.

| # | Deliverable | Question it answers | Built on |
| --- | --- | --- | --- |
| 1 | Report tile: word error rate by hearing, small model vs large model | Is the bigger model worth it? | `fact_asr_eval` |
| 2 | Report tile: share of speaking time for bench, petitioner and respondent | How is hearing time split between the bench and each side? | `fact_utterance` |
| 3 | Report tile: word error rate against silence share, one dot per hearing | Does bad audio explain the errors? | `fact_asr_eval`, `dim_audio_file` |
| 4 | Report tile: citation extraction precision, recall and F1, rule-based vs LLM | Does a language model beat a simple rule at finding citations? | `gold_eval_extraction` |
| 5 | Pipeline health page: last run, rows per layer, failed checks, quarantine rate | Did the load work, and did it quietly load nothing? | `dq_results`, `pipeline_run_log` |
| 6 | Restricted-case demo: the same report viewed as two roles | Are restricted cases really hidden from the general role? | `dim_case.is_restricted`, role `GeneralCounsel` |

One more output, outside the report: the evidence-search query (section 14), which returns a case's utterances with an audio link and start second.

**Flags used in this doc:** **\[PREVIEW\]** feature is in preview, **\[VERIFY\]** I have not confirmed this and you should check it on the day, **\[PAID\]** needs paid Fabric capacity.

## 2. Constraints and the decisions they force

Four constraints shape every choice below; each row says what it changed.

| Constraint | Your number | Design consequence |
| --- | --- | --- |
| Time | 3 days + 1 buffer | UI-first build, one workspace, default Spark pool (no custom Environment), thresholds calibrated from data rather than tuned |
| Money | under £20 | Paid F2 capacity (hearingevidencef2, UK West) on Azure free credit, paused when idle; free Colab GPU for ASR; no Azure OpenAI, Key Vault or Purview |
| Learning | Fabric from zero | Each Fabric item is used once with a stated purpose; glossary at the end |
| Focus | easily deviates | Every task is 90 minutes or less; the 45-minute stuck rule in section 17 |

**Your earlier answers, turned into decisions:**

- Weak ground truth is acceptable for non-Supreme-Court hearings, so they are reported as "agreement with a machine reference", never as accuracy. The README says plainly that this is a blueprint.
- AWS S3 stays in the design as the cross-cloud source; you will create the account and bucket yourself.
- Data science carries more weight than data engineering (about 55/45), so evaluation and plots get dedicated time on day 2.
- If things go badly, you will take an extra day. The buffer day is already in the plan.

## 3. Architecture and data flow

&#91;embedded content: architecture · sources to S3, Fabric layers, consumers\]

Audio is never copied: Fabric reads S3 through one shortcut, Colab does the GPU work and writes back to S3, and every layer is governed and tested.

## 4. Sources, licences and the data contract

Four sources feed the pipeline. Licence terms below are my understanding only; you check each one on Day 0 and write the result into its contract before downloading anything in bulk.

| Source | What you take | Role | Licence / terms | Action on Day 0 |
| --- | --- | --- | --- | --- |
| CourtListener audio API | Oral-argument MP3s, audio metadata (duration, hash, panel), docket link | Unstructured input | **\[VERIFY\]** Court audio is generally public; Free Law Project sets API terms and rate limits | Read the API terms; record licence text and URL in the contract |
| CourtListener dockets | Court, docket number, case name, argued date, judges | Structured input | **\[VERIFY\]** same as above | Inspect one real response and fix field names in the contract |
| Oyez (Supreme Court) | Speaker-labelled, timestamped transcripts; advocates and sides | Ground truth for ASR and speakers | **CC BY-NC 4.0, quoted from the oyez.org footer on 16 Oct 2026: attribution required, no commercial use. No official API; an undocumented one exists at api.oyez.org** | Read terms. If scraping or reuse is barred, fall back to CourtListener transcripts only and say so |
| Your own ASR output | Whisper plus pyannote JSON | Derived input | You own it | Store in S3 with model, version and input audio hash |

**Contract format:** one YAML file per source in `contracts/`, validated by a JSON Schema in CI. Fields:

- `owner`, `source_url`, `licence` (name, URL, date checked), `terms_notes`
- `refresh` (cadence) and `freshness_sla_hours`
- `schema` (columns, types, nullability, allowed values)
- `quality_expectations` (row-count floor, uniqueness keys, ranges)
- `classification` (public record; no private or victim audio), `retention`
- `provenance_fields` required on every object: `source_url`, `retrieved_at`, `sha256`, `licence_id`

**Why contracts are not shelf-ware:** the bronze notebook loads the YAML and checks incoming columns against it. A schema mismatch fails the load and writes to `dq_results`.

**Oyez licence, in short (CC BY-NC 4.0, checked 16 Oct 2026; not legal advice):**

- Credit Oyez, link the licence, and say what you changed (aligned, normalised, scored).
- Non-commercial only: no selling, no paid client work with this data. A public portfolio project with no revenue is generally treated as non-commercial.
- No share-alike condition. An older 2003 source said BY-NC-SA; ignore it.
- Do not use the Oyez name or logo as if they endorse the project.
- Keep Oyez files out of the public repo and the S3 bucket private. Short snippets in screenshots, with attribution, are fine.
- README line: "Speaker-labelled transcripts from Oyez (oyez.org), CC BY-NC 4.0, normalised and used as the evaluation reference."

## 5. S3 bucket, OneLake shortcut and raw ingestion

Raw files stay in S3 and Fabric reads them in place through a shortcut: no copy, no second set of files to keep in sync. A OneLake **shortcut** is a pointer that makes an external folder look like a folder inside a lakehouse.

**Bucket layout** (region eu-north-1, public access blocked):

```
s3://he-raw-<suffix>/
  courtlistener/
    audio/<audio_id>.mp3                     unstructured
    audio_meta/<audio_id>.json               structured
    dockets/<docket_id>.json                 structured
  oyez/
    cases/<term>_<docket>.json               structured (advocates, sides)
    transcripts/<term>_<docket>.json         structured (turns, timestamps)
  asr/run=<asr_run_id>/<hearing_id>.json     derived, written from Colab
  _manifests/batch=<batch_id>/objects.csv    sha256, source_url, retrieved_at, licence_id, bytes
```

**How each kind of raw file reaches bronze:**

| Raw data | Type | Read how | Bronze result |
| --- | --- | --- | --- |
| Audio MP3 | Unstructured | Never bulk-loaded. A notebook lists files under the shortcut, then reads individual files by path only when needed (quality gate, checksum) | `brz_audio_manifest`: one row per file with path, bytes, sha256, discovered\_at |
| Audio and docket JSON | Structured | Spark `multiLine` JSON read from the shortcut path | `brz_docket_json`, `brz_audio_meta_json`: payload kept raw, plus `_source_file`, `_batch_id`, `_ingested_at` |
| Oyez JSON | Structured | Same as above | `brz_oyez_case_json`, `brz_oyez_turn_json` |
| Manifest CSV | Structured | Spark CSV read | `brz_object_manifest`: chain-of-custody record per object |
| ASR JSON | Derived | Spark JSON read | `brz_asr_segment_raw`, `brz_asr_run` |

**Rules for bronze:** append-only, no cleaning, no joins. Ingestion is incremental: anti-join the manifest on `sha256` so a rerun only picks up new objects. Recompute the audio hash and compare it with the manifest; a mismatch goes to quarantine.

**Create the shortcut (Day 1, about 30 minutes):**

- [ ] AWS IAM: a read-only user for Fabric with `s3:ListBucket` and `s3:GetObject` on this bucket only. Keep a separate upload profile for your fetch script.
- [ ] In `lh_bronze`, Files, New shortcut, Amazon S3. Enter the bucket URL and create a connection with the access key. **\[VERIFY\]** exact URL format on screen.
- [ ] Point it at the bucket root and name it `raw`.
- [ ] Turn on shortcut caching in workspace settings so repeated reads do not repeat S3 egress. **\[VERIFY\]** availability for S3 shortcuts.
- [ ] Pass check: a notebook runs `notebookutils.fs.ls` on the shortcut and sees all 4 prefixes.

**Gotchas:**

- S3 shortcuts are read-only. Colab therefore writes ASR output to S3 with boto3, and Fabric sees it through the same shortcut.
- The shortcut sits under Files, not Tables, because MP3 and JSON are not Delta.
- Keys never go into the repo. The connection is created once by hand; its ID is passed to scripts as a variable.

## 6. Medallion layers

Three lakehouses in one workspace, one per layer: `lh_bronze`, `lh_silver`, `lh_gold`. A **lakehouse** is OneLake storage plus Delta tables plus an auto-created SQL endpoint. Separate lakehouses make the layer boundary visible, let you grant different access per layer, and avoid depending on lakehouse schemas **\[VERIFY\]** status.

| Layer | Holds | Allowed | Not allowed |
| --- | --- | --- | --- |
| Bronze | Raw as received, plus lineage columns (`_source_file`, `_batch_id`, `_ingested_at`, `sha256`) | Append, light typing for the manifest | Cleaning, joins, deduplication |
| Silver | Cleaned, typed, deduplicated, resolved | Joins, MERGE upserts, NLP, quality gate decisions | Business aggregates |
| Gold | Star schema, evidence index, eval facts | Surrogate keys, conformed dimensions, MERGE | Raw text fixes (fix upstream) |

**Silver tables:**

- `slv_hearing`: one row per hearing, joins audio, docket and Oyez case by docket number and term
- `slv_audio_quality`: librosa metrics and gate status per audio file
- `slv_audio_quarantine`: failed files with reason and timestamp; nothing downstream reads them
- `slv_asr_segment`: typed ASR segments (start, end, text, confidence, speaker cluster)
- `slv_ref_turn`: Oyez turns (speaker, role, start, end, text) for scoring only
- `slv_utterance`: merged speaker turns with resolved speaker, confidence and method
- `slv_entity_mention`: spoken names with candidates and the chosen resolution
- `slv_citation_mention`: eyecite and LLM mentions, with method and validity flag
- `slv_case_party`: parties parsed from docket and Oyez advocates, with source and confidence

**Gold tables** are defined in section 7. Operational tables (`dq_results`, `pipeline_run_log`) live in `lh_gold` so the health report reads one lakehouse.

**Engineering rules that make rebuilds safe:**

- Surrogate keys are deterministic hashes of natural keys, so a rebuild produces identical keys.
- Silver and gold write by Delta `MERGE` on those keys; reruns never duplicate.
- Every table keeps `_batch_id` and `_run_id`, so any row traces back to one pipeline run.
- ASR output is frozen in S3. Everything after it is deterministic, which is what makes the destroy-and-rebuild test meaningful.

## 7. Star schema: grain, keys, conformed dimensions

The model has 3 facts, 9 dimensions and 2 bridges. Changes from your candidate list: `dim_judge` folds into `dim_speaker` (a judge is a speaker with a role), and I added `dim_hearing`, `dim_asr_run` and `dim_citation`, each with a reason below.

| Table | Grain (one row per) | Key | Main attributes and foreign keys |
| --- | --- | --- | --- |
| dim\_date | calendar day | `date_key` (yyyymmdd) | year, month, quarter, term year |
| dim\_court | court | `court_key` | CourtListener court id, name, level (supreme, circuit) |
| dim\_case | docket (court + docket number) | `case_key` | docket number, case name, argued date, `is_restricted`, FK `court_key` |
| dim\_party | normalised party name | `party_key` | name, type (person, government, organisation), source |
| dim\_speaker | person or role seen speaking | `speaker_key` | canonical name, role (justice, advocate, unknown), CourtListener person id if known |
| dim\_hearing | oral-argument session | `hearing_key` | FKs to case, court, date, audio file; `is_scored` (has Oyez truth). Exists because a hearing can span several audio files |
| dim\_audio\_file | audio object version (sha256) | `audio_file_key` | S3 path, sha256, codec, sample rate, duration, quality metrics, gate status, source URL, retrieved\_at, licence |
| dim\_asr\_run | model plus parameters run | `asr_run_key` | engine, model, version, compute type, diarization model, run time, code commit. Exists so runs can be compared |
| dim\_citation | normalised authority | `citation_key` | normalised cite, type (case, statute), reporter, volume, page, resolved flag |
| bridge\_case\_party | party on a side of a case | (`case_key`, `party_key`) | side (petitioner, respondent, appellant), source, confidence |
| bridge\_hearing\_judge | judge on a panel | (`hearing_key`, `speaker_key`) | seat order if known |
| fact\_utterance | speaker turn in a hearing, from the chosen production ASR run | `utterance_key` | FKs: hearing, case, court, date, speaker, represented party (or unknown), `asr_run_key`. Measures: start\_s, end\_s, duration\_s, word\_count. Also resolution method and confidence |
| fact\_citation\_mention | citation mention in an utterance, per extraction method | `citation_mention_key` | FKs: hearing, case, date, speaker, citation; degenerate `utterance_key`. Method (eyecite or llm), char span, confidence, schema-valid flag |
| fact\_asr\_eval | hearing per ASR run | (`hearing_key`, `asr_run_key`) | WER, substitutions, deletions, insertions, errors (their sum), reference words, approximate DER, speaker-count error, quality bucket |

**Conformed dimensions** are the ones shared by more than one fact, so the same filter works across all of them:

| Fact | date | court | case | hearing | speaker | citation | asr\_run |
| --- | --- | --- | --- | --- | --- | --- | --- |
| fact\_utterance | yes | yes | yes | yes | yes |  | yes |
| fact\_citation\_mention | yes | yes | yes | yes | yes | yes |  |
| fact\_asr\_eval | yes | yes | yes | yes |  |  | yes |

**Serving table:** `gold_evidence_index` is deliberately denormalised, one row per evidence item (an utterance or a citation mention) per case. It carries the audio path, start and end seconds, snippet text, speaker, citations, confidence, `is_restricted` and an `evidence_hash`. The hash is sha256 of audio checksum, time span, text and ASR run, so a changed snippet is detectable.

**Modelling decisions to defend in an interview:**

- Slowly changing dimensions: type 1 only. A judge's role changes over years, not over 3 days. Type 2 is a documented extension.
- Every dimension has an unknown member with key -1, so unresolved speakers never drop rows.
- Citation mentions are stored per extraction method so eyecite and the LLM can be compared in one query.
- Facts never join to facts. `utterance_key` on the citation fact is a degenerate dimension.
- Party time is the advocate's speaking time attributed to the party they represent; judges carry party -1.

## 8. Fabric workloads: what to use and what to skip

Each row passes or fails the purpose test: does it do a job in the data flow that nothing cheaper does?

| Workload | Verdict | Purpose in this flow |
| --- | --- | --- |
| OneLake shortcut (S3) | Keep | Reads raw audio and JSON from S3 in place; the cross-cloud story |
| Lakehouse (x3) | Keep | Delta storage per medallion layer, plus the free SQL endpoint |
| Notebooks (Spark) | Keep | All ingestion, audio QC, NLP, silver and gold transforms. Audio QC is a plain Python loop on the driver; 4 files do not need distribution |
| Data Factory pipeline | Keep | Orchestrates notebooks in order and enforces gates: Lookup on `dq_results`, If condition, Fail activity. This is where "success with zero rows" gets caught |
| SQL analytics endpoint | Keep | Read-only T-SQL over Delta: data-quality queries, the evidence-search demo, the smoke test |
| Power BI semantic model (Direct Lake) | Keep | Measures, relationships and row-level security over gold |
| Power BI report | Keep | 3 pages: eval, hearing analytics, pipeline health |
| Git integration (GitHub) | Keep | Version control of notebooks, pipelines, model. Sync a `fabric/` folder only |
| Deployment pipeline | Keep, first to cut | One dev to test promotion; shows release process |
| Warehouse | Skip | Duplicates the lakehouse for this data volume, costs capacity, adds a second copy of gold. Interview answer: use it if you need multi-table T-SQL transactions or stored procedures |
| Dataflow Gen2 and Copy job | Skip | No low-code transforms needed, and notebooks already ingest |
| Eventstream, Eventhouse (real-time) | Skip | The data is batch |
| Experiments (MLflow) | Skip | `fact_asr_eval` plus plots already compare runs; a second tracker is duplication |
| Purview | Skip | Needs extra licences and setup. Use Fabric's built-in lineage view, workspace domain and endorsement instead (free) **\[VERIFY\]** admin rights |
| Fabric IQ ontology | Skip **\[PREVIEW\]** | Document the ontology as Turtle plus a mapping table in the repo, and test it (section 11). Revisit only on the buffer day |
| Copilot and AI functions | Skip **\[PAID\]** | Burns capacity and hides model choice. The LLM call is explicit in a notebook so you can evaluate it |
| Environment (custom libs) | Skip | Slow to publish. Install your package with `%pip` in the notebook instead |

**Capacity note:** the paid F2 capacity (hearingevidencef2, UK West) runs on Azure free credit and powers the whole project. Terraform attaches the workspace to it by name at creation. Pause it after every session. Cost is in section 18.

## 9. Audio processing: quality gate, transcription, evaluation

Three stages: a librosa quality gate in Fabric, transcription and diarization on a free Colab GPU, then evaluation back in Fabric against Oyez.

### 9.1 Quality gate (librosa, notebook `nb_02_audio_quality`)

Metrics are computed per file on the driver. Thresholds below are starting points; calibrate them on the first 5 real files, then freeze them in `contracts/audio_quality.yaml`.

| Metric | How | Starting rule | Result if breached |
| --- | --- | --- | --- |
| Decodes | `librosa.load` in a try block | must succeed | QUARANTINE |
| Duration | length / sample rate | at least 120 s | QUARANTINE |
| Sample rate | file header | at least 16 kHz | WARN |
| Silence ratio | share of time outside `librosa.effects.split` at 30 dB | below 0.50 | QUARANTINE above 0.70, WARN above 0.50 |
| Clipping ratio | share of samples with absolute value at or above 0.999 | below 0.1% | WARN, QUARANTINE above 1% |
| Loudness | RMS in dBFS | above -45 dB | WARN |
| Checksum | recomputed sha256 vs manifest | must match | QUARANTINE |

**Quarantine path:** a failing file gets a row in `slv_audio_quarantine` with reason and time. Downstream joins only read `gate_status = PASS` or `WARN`. The pipeline continues and reports the quarantine count. Reprocessing is a status change, not a data fix.

**Proof it works:** create 3 synthetic bad files from a good one (truncated, 70% silence inserted, clipped by gain). They must be the only quarantined files. The same metric functions are unit-tested in CI on generated sine waves, so the tests need no audio.

### 9.2 Transcription and diarization (Colab, notebook `colab/transcribe.ipynb`)

- Whisper has no useful GPU option inside a small Fabric capacity, so ASR runs outside Fabric and lands in S3. This is a real-world pattern: GPU work outside the lakehouse, with provenance back in.
- Engine: `faster-whisper`, word timestamps on, run twice on the same files: `small.en` and `large-v3`. Diarization: `pyannote` 3.1 **\[VERIFY\]** gated model; accept its terms on Hugging Face and use a token.
- Each output JSON carries `asr_run_id`, model, parameters, code commit and the **sha256 of its input audio**. Fabric rejects any ASR file whose audio hash is not in the manifest.
- Process the first 15 minutes of each hearing. That keeps Colab sessions short and Oyez alignment trivial (timestamps start at 0).

### 9.3 Evaluation (notebook `nb_05_eval`, output `fact_asr_eval`)

- **Reference:** Oyez turns for the 4 Supreme Court hearings. The appeals-court hearing, if added, is unscored and appears only in quality and speaking-time plots.
- **WER:** `jiwer` after one fixed normalisation (lowercase, strip punctuation, expand numbers). Report substitutions, deletions and insertions separately.
- **Speaker error:** map each diarization cluster to the Oyez speaker with the most overlap, then compute approximate DER with `pyannote.metrics` and a 0.5 s collar. Also record speaker-count error. Oyez timestamps are coarse turn boundaries, so label this "approximate DER".
- **Slices:** WER by model, by audio quality bucket, and by speaker role (justice vs advocate). Also a legal-term error rate: how often party and case names are misrecognised. That number motivates the entity-resolution step.

### 9.4 Decision rule for model work

Denoising, source separation, fine-tuning or a TCN or VAE enter the project only if the data demands them:

- If WER in the worst quality quartile is at least 5 absolute points above the best quartile, **and** WER correlates with measured noise rather than with accents or vocabulary, try one denoise step and re-measure on the same files.
- If legal-term error rate is above 20% and you have corrected transcripts to train on, consider a prompt or vocabulary hint before any fine-tuning.
- Otherwise write "measured, not needed" in `docs/decisions.md`. That is a legitimate and credible result.

## 10. NLP: speakers, names and citations

Every NLP step writes a `method` and a `confidence` column, so each result can be audited and scored.

### 10.1 Resolving speakers

Diarization gives anonymous clusters (`SPEAKER_00`). The pipeline resolves them to named people or roles in tiers:

1. **Oyez overlap** (scored hearings only): the cluster with the most time overlap with an Oyez speaker takes that name. Method `oyez_overlap`.
2. **Address patterns** (all hearings): phrases such as "Mr. Chief Justice", "Justice X", "thank you, counsel" identify who is being addressed or who is speaking. Method `address_rule`.
3. **Roster match:** names from the panel in `bridge_hearing_judge`. Method `roster`.
4. Anything left is role-only (`justice` or `advocate` by speaking pattern) or `unknown`, key -1.

### 10.2 Resolving spoken names to case parties and judges

- Normalise: strip honorifics, lowercase, remove punctuation.
- Candidate set per hearing is small (panel, advocates, parties), so matching is closed-set. `rapidfuzz` token-set ratio at or above 90 auto-accepts; 75 to 90 goes to an LLM adjudication step that may only pick from the candidate list or answer "none".
- Parties come from the docket case name ("X v. Y", low confidence) and from Oyez advocates (sides, higher confidence). CourtListener may not carry party lists for argument-only dockets **\[VERIFY\]**, which is why the source and confidence are stored.

### 10.3 Extracting citations

- **Baseline:** `eyecite` (open source, built for written citations). Spoken citations are often just case names ("Chevron", "Marbury"), so I expect low recall here. That is a finding, not a failure.
- **LLM extractor:** one prompt per utterance returning JSON that a `pydantic` model validates: `text`, `type` (case, statute, regulation, other), `normalised`, `char_start`, `char_end`, `confidence`. Rules: the span must be a substring of the utterance (grounding check), allow 2 retries, count schema failures as a metric. The utterances are public, so sending them to an external API is acceptable; keep the key out of the repo.
- **Optional guard:** check extracted case cites against CourtListener's citation lookup to catch invented cases **\[VERIFY\]**. First on the cut list.

### 10.4 Hand-labelled evaluation sample

- 40 utterances: 30 likely to contain citations (match on "v.", "U.S.C.", "Act", "versus") and 10 without. About 40 minutes, done in four 10-minute blocks.
- Labels go in `eval/labels/citations.jsonl`: span, type, normalised form.
- Plus 40 name mentions for section 10.2, labelled the same way.
- Score at mention level with span overlap: precision, recall, F1 per method. The LLM is only adopted if it beats eyecite on F1 by a margin that survives the small sample; report it with the sample size next to the number.

## 11. Ontology and semantic model

The ontology is the shared vocabulary; the semantic model turns it into measures people can use. Both are in the repo, and CI keeps them honest.

### 11.1 Ontology

&#91;embedded content: ontology · 10 concepts, 12 relationships\]

Arrows read subject to object: an Utterance is part of a Hearing, a Citation refers to an Authority. Eight concepts come from your list; AudioFile and Authority are added because provenance and citation analysis need them.

| Concept | Meaning | Table | Key |
| --- | --- | --- | --- |
| Court | A court that hears cases | `dim_court` | `court_key` |
| Case | One docket in one court | `dim_case` | `case_key` |
| Party | A named side participant | `dim_party` plus `bridge_case_party` | `party_key` |
| Judge | A bench member on a panel | `dim_speaker` (role justice) plus `bridge_hearing_judge` | `speaker_key` |
| Advocate | A lawyer arguing for a party | `dim_speaker` (role advocate) | `speaker_key` |
| Hearing | One oral-argument session | `dim_hearing` | `hearing_key` |
| AudioFile | A recording with checksum and quality | `dim_audio_file` | `audio_file_key` |
| Utterance | One speaker turn with a time span | `fact_utterance` | `utterance_key` |
| Citation | A mention of a cite inside an utterance | `fact_citation_mention` | `citation_mention_key` |
| Authority | A normalised case or statute | `dim_citation` | `citation_key` |

**Enforcement:** `ontology/ontology.ttl` declares the classes and properties. `concept_mapping.csv` lists the table and key behind each concept. A CI test fails if a concept has no table or key, so the ontology cannot drift from the model. Fabric IQ ontology **\[PREVIEW\]** is skipped; the Turtle file is portable if you adopt it later.

### 11.2 Semantic model

- **Name:** `sm_hearing_evidence`, Direct Lake over `lh_gold` **\[VERIFY\]**. If Direct Lake misbehaves, switch to import mode; nothing else changes.
- **Relationships:** each dimension one-to-many into its facts, single direction. Hide surrogate keys and technical columns.
- **Hierarchies:** Court, Case, Hearing; and Year, Month, Day.
- **Descriptions:** add one to every table and measure. It is free documentation.

| Measure | Definition (DAX sketch) | Why it exists |
| --- | --- | --- |
| Speaking Time (s) | `SUM(fact_utterance[duration_s])` | Base measure for everything below |
| Speaking Time Share | `DIVIDE([Speaking Time (s)], CALCULATE([Speaking Time (s)], REMOVEFILTERS(dim_speaker)))` | Share of hearing time by speaker, role or party |
| Advocate Time by Party | `CALCULATE([Speaking Time (s)], dim_speaker[role] = "advocate")` | Your "speaking time per party" measure, sliced by `dim_party` |
| Words per Minute | `DIVIDE(SUM(fact_utterance[word_count]), [Speaking Time (s)] / 60)` | Pace; also a plausibility check on ASR |
| Utterances | `COUNTROWS(fact_utterance)` | Volume |
| Citation Mentions | `COUNTROWS(fact_citation_mention)` | Volume, sliced by method |
| Distinct Authorities Cited | `DISTINCTCOUNT(fact_citation_mention[citation_key])` | Breadth of authority |
| WER (weighted) | `DIVIDE(SUM(fact_asr_eval[errors]), SUM(fact_asr_eval[ref_words]))` | Total errors over total reference words. Averaging per-hearing WER would overweight short hearings |
| Approx DER | weighted by reference duration | Diarization quality |
| Quarantine Rate | `DIVIDE(COUNTROWS(FILTER(dim_audio_file, dim_audio_file[gate_status] = "QUARANTINE")), COUNTROWS(dim_audio_file))` | Gate health. `dim_audio_file` keeps quarantined files; they simply never join to facts |
| Hours Since Last Load | `DATEDIFF(MAX(pipeline_run_log[finished_at]), NOW(), HOUR)` | Freshness on the health page |

Citation precision, recall and F1 come from a small summary table, `gold_eval_extraction` (method, task, precision, recall, F1, labelled count), outside the star. It is evaluation output, not a business fact.

## 12. Governance

Governance here means four things you can show working: contracts, provenance, lineage and access control. Contracts are in section 4.

### 12.1 Provenance and chain of custody

Every object carries `source_url`, `retrieved_at`, `sha256` and `licence_id` from the manifest into bronze, silver and gold. Three mechanisms keep the chain intact:

- Recomputed audio hash must equal the manifest hash, or the file is quarantined.
- ASR files must name the hash of the audio they were made from.
- `evidence_hash` on each evidence-index row ties a snippet to its audio, time span, text and ASR run. State in the README that this is a demonstration of the idea, not a forensically sound chain of custody.

### 12.2 Lineage

Use Fabric's workspace lineage view for item-to-item flow (shortcut to notebooks to lakehouses to semantic model to report). It does not show column-level lineage, so the row-level trail is the `_batch_id`, `_run_id` and `sha256` columns. A free 10-minute addition: assign the workspace to a domain and mark the semantic model Promoted **\[VERIFY\]** admin rights.

### 12.3 Permission-aware design: restricted matter

- Flag 2 of the hearings as restricted in a small seed table (`ref_matter_access`); gold copies it to `dim_case.is_restricted`. This is a simulation: all the audio is public.
- In the semantic model create two roles. `GeneralCounsel` filters `dim_case[is_restricted] = FALSE()`. `RestrictedTeam` has no filter.
- Demo with Power BI's "View as role". Show that totals, evidence search and speaking-time measures all shrink for `GeneralCounsel`.
- **Gotcha to state in the README:** row-level security does not apply to people with write or build access in the workspace (Admin, Member, Contributor). It only protects viewers. Real deployments put consumers in a separate app or a Viewer role.
- OneLake security and SQL-endpoint row-level policies are alternatives **\[PREVIEW\]** **\[VERIFY\]**. Skip them; one mechanism done properly is better than three half-done.

### 12.4 Data mesh: honest verdict

Mesh is overkill for one person. Its value is organisational: domain teams own data, platform teams offer self-serve tools, governance is federated. You have one team of one.

What is worth keeping is the **data product** idea. Frame "Hearing Evidence" as a product with:

- an owner (you), a contract and a freshness SLA
- an input port (the S3 shortcut) and output ports (gold tables via the SQL endpoint, the semantic model)
- documented quality metrics and a known-limitations page

Say in interviews: "I used data-product thinking without the mesh organisation, because mesh solves a coordination problem I did not have."

### 12.5 Responsible use

Only public hearings and synthetic planted files. No private, victim or sealed material. The README states that outputs are research artefacts and not admissible evidence.

## 13. Data quality and observability

Checks are plain PySpark assertions in `src/hearing_evidence/dq.py`. Each run appends one row per check to `dq_results` (check id, layer, table, run id, status, observed, expected, time). The pipeline reads that table and decides to continue or fail. Great Expectations and Soda are skipped: custom checks do the job at this size.

**The quiet failure:** a pipeline can finish green while loading nothing. Three defences, all required:

- A row-count floor from the contract, and a volume check against the previous run (alert if below 50%).
- A reconciliation check per layer: rows in equals rows out plus rows quarantined.
- A heartbeat row written every run, so "no new files when files were expected" is visible as a WARN rather than as silence.

| Layer | Check | Type | On failure |
| --- | --- | --- | --- |
| Source | Manifest present, every listed object exists in S3 | Completeness | Fail |
| Bronze | Rows greater than 0 per table; matches manifest count | Zero-row, volume | Fail |
| Bronze | Columns match the contract schema | Schema | Fail |
| Bronze | Audio sha256 equals manifest sha256 | Integrity | Quarantine file |
| Bronze | `max(_ingested_at)` within the freshness SLA | Freshness | Warn |
| Silver | Gate status in PASS, WARN or QUARANTINE only; quarantine rate below 30% | Domain, rate | Warn, then fail above threshold |
| Silver | Segment times monotonic; end after start; within audio duration | Validity | Drop row, log |
| Silver | Words per minute between 80 and 220 per hearing | Plausibility | Warn |
| Silver | Speaker clusters per hearing between 2 and 12 | Plausibility | Warn |
| Silver | Citation mention span is a substring of its utterance | Grounding | Reject mention |
| Silver | Share of utterances with unknown speaker below 25% | Resolution | Warn |
| Gold | Primary keys unique; no nulls in foreign keys (use -1 member) | Uniqueness, referential | Fail |
| Gold | Every fact FK exists in its dimension | Referential | Fail |
| Gold | Row counts reconcile to silver | Reconciliation | Fail |
| Gold | Restricted cases carry `is_restricted = true` in the index | Security | Fail |

**Observability:**

- Fabric's Monitoring hub already stores run history. Use it as is.
- A 3rd report page, "Pipeline health", reads `dq_results` and `pipeline_run_log`: rows per layer per run, failed checks, quarantine rate, hours since last load.
- Email on pipeline failure via an Outlook activity **\[VERIFY\]**. If it is fiddly, skip it; the health page is enough.
- **Prove it:** commit a test that injects an empty batch and show the pipeline going red. Screenshot it for the README.

## 14. Analytics and demo plots

Plots are the part that earns engagement, so each one answers a stated question. README images come from a Python notebook (`nb_08_plots`, matplotlib or seaborn, saved to `docs/img/`). The interactive versions live in Power BI. Numbers are whatever the data says; do not pre-decide the story.

**Example outputs.** The four tiles below show the shape of the report. Every number is invented, so none is a result or a target; the real values come from the run.

&#91;embedded content: example report tiles · invented numbers, shape only\]

They map to deliverables 1 to 4 in section 1: error rate by hearing and model, speaking-time split, audio quality against error, and citation extraction by method.

| # | Plot | Question it answers | Source |
| --- | --- | --- | --- |
| 1 | WER by hearing, small.en vs large-v3 (grouped bars) | Is the bigger model worth it? | `fact_asr_eval` |
| 2 | WER vs silence ratio or loudness (scatter, one point per hearing) | Does audio quality explain errors? This decides whether denoising is tried | `fact_asr_eval`, `dim_audio_file` |
| 3 | WER by speaker role (justice vs advocate) | Who is hard to transcribe, and why? | `fact_asr_eval`, `fact_utterance` |
| 4 | Approximate DER by hearing | How good is diarization on courtroom audio? | `fact_asr_eval` |
| 5 | Speaking-time share per side per hearing (stacked bars) | How is time split between bench, petitioner and respondent? | `fact_utterance` |
| 6 | Speaker timeline for one hearing (swimlane, one lane per speaker) | What does a hearing look like? The memorable README hero image | `fact_utterance` |
| 7 | Citation extraction precision, recall, F1 by method | Does the LLM beat eyecite on spoken legal text? | hand labels, `fact_citation_mention` |
| 8 | Most-cited authorities across hearings | What do these courts lean on? | `fact_citation_mention`, `dim_citation` |
| 9 | Quarantine and data-quality summary | Does the gate catch what it should? | `slv_audio_quarantine`, `dq_results` |

**Evidence search demo (SQL endpoint):** given a case name, return evidence items ordered by time, each with speaker, snippet, audio path and start second. A reader can jump to the exact moment in the audio. Show the same query as `GeneralCounsel` and `RestrictedTeam` to tie in the security story.

**Power BI report pages:**

1. **Evaluation:** plots 1-4 and 7 with a slicer for model and court.
2. **Hearing analytics:** plots 5, 6 and 8 with slicers for court, case and date.
3. **Pipeline health:** latest run, rows per layer, failed checks, quarantine rate, hours since last load.

Do not add a plot that does not answer a question in this table.

## 15. CI/CD, tests and Terraform

Two systems carry the build: **Fabric Git integration** for the items you build in the UI, and **Terraform** for the infrastructure around them. Use each for what it does well.

### 15.1 Git integration (from Day 1)

- Connect the workspace to a GitHub repo, branch `main`, and sync only the `fabric/` folder **\[VERIFY\]** the "Git folder" setting.
- Notebooks, pipelines, semantic model and report are the items to track. Lakehouse definitions are supported **\[VERIFY\]** whether shortcuts are included. Data is never in Git.
- Commit after every finished checklist item. Small commits make the stuck rule cheaper.
- Keep logic out of notebooks. Put it in `src/hearing_evidence/` as plain Python and install it in each notebook with `%pip install` pinned to a commit. This is what makes CI tests possible. Public repo needed, or upload a wheel to the lakehouse Files area as fallback.

### 15.2 CI on every push (GitHub Actions)

| Step | What it proves |
| --- | --- |
| `ruff` lint and format check | Code hygiene |
| `pytest` on synthetic signals and fixture JSON | Audio metrics, ID hashing, name normalisation, citation parsing, DQ functions work |
| Contract validation | Every YAML in `contracts/` matches the contract JSON Schema |
| Ontology test | Every ontology concept maps to a real table and key in `concept_mapping.csv` |
| `terraform fmt -check` and `terraform validate` | IaC is syntactically valid |
| `nbstripout` check on `colab/` | No outputs or secrets leaked in notebooks |

CI cannot run Fabric notebooks. The Fabric-side proof is the smoke test in 15.4.

### 15.3 Deployment pipeline (Fabric)

One promotion from `dev` to `test` workspace on the same capacity. Use it to show the release path and parameterise the S3 connection or lakehouse IDs per stage **\[VERIFY\]** deployment rules. Cut this first if time is short.

### 15.4 Terraform (Day 3, half-day box)

**Scope:** workspace, role assignment and the 3 lakehouses, attached to the F2 capacity (hearingevidencef2) by name at creation. Optional stretch: connect the workspace to Git so items hydrate from the repo.

- Providers: the Microsoft `fabric` provider only; no `azurerm`, because no Azure resource is created. A `capacity_name` variable selects the capacity, so anyone can point the code at their own trial or paid capacity. **\[VERIFY\]** provider version, resource names, and that a capacity can be looked up by name.
- Authentication: `az login` as yourself may be enough, so no service principal is needed. If the provider insists on one, enable the tenant setting that lets service principals call Fabric APIs **\[VERIFY\]**.
- The S3 connection is created by hand once, because it holds a secret. A script `scripts/create_shortcut.py` then creates the shortcut through the Fabric REST API using the connection ID. Check whether the provider can already do this **\[VERIFY\]**.
- State: local state file is fine for one person. Say so in the README and name remote state as the team upgrade.

**The rebuild test** (the whole point of the day):

1. `terraform destroy`
2. `terraform apply`
3. Git sync items into the new workspace; run `create_shortcut.py`
4. Trigger the pipeline
5. Run `scripts/smoke_test.sql` on the gold SQL endpoint: row counts per table must equal the pre-destroy counts, and `dq_results` must show no failures

Pass means counts match exactly. ASR output is frozen in S3, so the rebuild is deterministic. Resume the paused F2 capacity before this test and pause it again after, and put a screenshot or short video of the successful rebuild in the README.

**Time box:** 4 hours. If the Fabric provider lacks a resource you need, script it with REST and move on; log the gap in `docs/decisions.md`. A documented gap is better than an overrun.

## 16. Repo structure

One repo, public, with a README that a recruiter can read in 2 minutes. Pure logic lives in `src/` so CI can test it; Fabric items live in `fabric/` so Git sync can own them.

```
hearing-evidence-fabric/
  README.md                 pitch, architecture image, plots, how to run, limits
  Makefile                  make test | fetch | rebuild | destroy
  .github/workflows/ci.yml  ruff, pytest, contracts, ontology, terraform fmt+validate
  contracts/                one YAML per source + audio_quality.yaml + contract.schema.json
  ontology/                 ontology.ttl, concept_mapping.csv
  scripts/                  fetch_and_stage.py (local), create_shortcut.py, smoke_test.sql
  src/hearing_evidence/     audio_qc.py, ids.py, parse_courtlistener.py, parse_oyez.py,
                            resolve.py, citations.py, dq.py
  tests/                    pytest: synthetic signals, fixture JSON
  colab/                    transcribe.ipynb (faster-whisper + pyannote)
  fabric/                   Git-synced: notebooks, pipeline, semantic model, report
  eval/labels/              citations.jsonl, name_mentions.jsonl
  terraform/                providers.tf, main.tf, variables.tf, outputs.tf
  docs/                     decisions.md, limitations.md, img/, demo.gif
```

**Notebook list** (names fixed so the pipeline and README agree): `nb_01_bronze_ingest`, `nb_02_audio_quality`, `nb_03_bronze_asr`, `nb_04_silver_transcripts`, `nb_05_eval`, `nb_06_nlp`, `nb_07_gold_build`, `nb_08_plots`.

**README must contain:** the pitch, the architecture diagram, 3-4 of the plots, a "what I measured" results table, a "known limits" list (weak ground truth outside Supreme Court, small sample, RLS gotcha), the rebuild command, the cost breakdown, and a sources and licences block: Oyez attribution (CC BY-NC 4.0, normalised and used as the evaluation reference), credit to Free Law Project / CourtListener, and a line stating that all transcripts, speaker labels and extractions produced by this project are its own AI-generated outputs, not produced, endorsed or verified by CourtListener or Oyez.

**`docs/decisions.md`** is a short running log: each skill kept or skipped, each preview feature avoided, and each measured result that settled a question. It is the best interview artefact in the repo.

## 17. Three-day plan with checklists and pass/fail checks

Day 2 is the heavy one, so its semantic model and report sit in the buffer day by default. Each day has one gate; a day that fails its gate does not quietly roll leftovers forward, they go to the buffer list.

&#91;embedded content: roadmap · 5 phases, 5 gates\]

**Stuck rule:** 45 minutes stuck means you write the blocker in one line in `docs/decisions.md`, move to the next unchecked item, and come back later. Set a 90-minute timer per task; when it rings, commit what you have.

### Day 0: Tue 13 Oct evening (about 2.5 h)

- [ ] Azure: subscription in your new tenant with the Owner role (done); budget alerts are optional since the F2 capacity runs on Azure free credit (15 min)
- [ ] Tenant admin: create and assign the F2 capacity (hearingevidencef2, UK West); find the settings that allow Fabric items and service-principal API access **\[VERIFY\]** (20 min)
- [ ] AWS: bucket in eu-north-1, block public access, read-only IAM user (`fabric-shortcut-reader`), separate upload profile (`he-uploader`) (30 min)
- [ ] GitHub: `nbstripout` pre-commit added to the existing public repo (uv, ruff, pytest and CI already in place) (10 min)
- [ ] Tokens: CourtListener API token; Hugging Face token and accept the pyannote terms (10 min)
- [ ] Licences: read CourtListener and Oyez terms and write them into `contracts/*.yaml` (30 min)
- [ ] `fetch_and_stage.py`: the 4 Supreme Court hearings with Oyez transcripts (plus the appeals-court hearing later as an unscored addition), sha256 manifest, upload to S3 (45 min)

**Gate:** `aws s3 ls --recursive` shows the 4 audio files plus docket, Oyez and manifest files, and every audio hash is in the manifest.

### Day 1: Wed 14 Oct, ingest, quality gate, transcription

- [ ] Workspace assigned to the F2 capacity (hearingevidencef2) at creation; 3 lakehouses; connect Git to the `fabric/` folder (45 min)
- [ ] S3 shortcut `raw` with caching on; list it from a notebook (30 min)
- [ ] `nb_01_bronze_ingest`: manifest, JSON to Delta, lineage columns, hash check, contract schema check (90 min)
- [ ] `nb_02_audio_quality`: metrics, thresholds calibrated on the first 5 files, 3 planted bad files, quarantine (90 min)
- [ ] `colab/transcribe.ipynb`: small.en and large-v3 plus diarization on passing files, upload to S3 (90 min; runs while you do the next items)
- [ ] `nb_03_bronze_asr`: ingest ASR JSON, verify input audio hashes (45 min)
- [ ] Evening: label 40 citation utterances and 40 name mentions, in 10-minute blocks (40 min total)
- [ ] Three lines per Fabric item in `docs/decisions.md`; commit (20 min)

**Gate:** bronze counts reconcile to the manifest; quarantine holds exactly the 3 planted files plus any real failures; ASR JSON exists for the 4 hearings from 2 runs; everything is committed.

### Day 2: Thu 15 Oct, silver, evaluation, NLP, gold, gates (about 7 h, plus optional plots)

- [ ] `nb_04_silver_transcripts`: typed segments, Oyez turns, `slv_hearing` (75 min)
- [ ] `nb_05_eval`: WER, approximate DER, slices, `fact_asr_eval` (90 min)
- [ ] `nb_06_nlp`: speaker resolution, name matching, eyecite, LLM extraction, scoring against labels (120 min)
- [ ] `nb_07_gold_build`: dimensions with -1 members, facts, evidence index, `gold_eval_extraction` (90 min)
- [ ] `dq.py` checks, `dq_results`, and pipeline `pl_hearing_evidence` with Lookup, If condition and Fail activity (60 min)
- [ ] `nb_08_plots`: plots 1-5 saved to `docs/img/` (45 min, if time remains)

**Gate:** gold tables are non-empty and reconcile with silver; an injected empty batch turns the pipeline red.

### Day 3: Fri 16 Oct, Terraform (4 h box) then README (2 h)

- [ ] `terraform init`; fabric provider; workspace attached to the F2 capacity (hearingevidencef2) by name, role assignment (60 min)
- [ ] 3 lakehouses in Terraform (30 min)
- [ ] `create_shortcut.py` and `smoke_test.sql` (45 min)
- [ ] Rebuild test: destroy, apply, Git sync, shortcut, pipeline, smoke test (60 min)
- [ ] Record a screenshot or short video of the successful rebuild for the README (10 min)
- [ ] README with architecture image, plots, results table and limits; record a 3-minute demo GIF (2 h)

**Gate:** after the rebuild, gold row counts equal the pre-destroy counts and `dq_results` shows no failures; CI is green; the README renders with images.

### Buffer: Sat 17 Oct

- [ ] Semantic model with relationships, measures and the 2 RLS roles; report pages 1-3 (2 h)
- [ ] Deployment pipeline, dev to test (45 min)
- [ ] Denoise experiment, only if the 9.4 rule triggered (60 min)
- [ ] Final pass on `docs/decisions.md` and `docs/limitations.md`; write the 5-line project summary for your CV (30 min)

**Gate:** "View as GeneralCounsel" hides restricted cases; CI is green; repo is public.

## 18. Cost, risks and fallbacks

Expected spend is about £0 to £2 (S3 and LLM calls) against your £20 ceiling. All prices are my estimates; confirm them in the Azure pricing calculator **\[VERIFY\]**.

| Item | Estimate | Notes |
| --- | --- | --- |
| Azure subscription | £0 (credit) | Azure free credit funds the paid F2 capacity; add budget alerts for extra safety |
| Fabric F2 capacity (hearingevidencef2, UK West) | £0 (credit) | Paid capacity on Azure free credit, billed hourly while running. Pause it after every session |
| S3 storage and requests | under £1 | About 1 GB of audio |
| S3 egress through the shortcut | under £1 | About £0.07 per GB; shortcut caching reduces repeats |
| OneLake storage | under £1 | Same order as S3 |
| Colab GPU | £0 | Free tier; sessions are time-limited, so process in batches |
| LLM API for extraction | under £1 | 100 short utterances, a few retries |
| GitHub | £0 | Public repo, free Actions minutes |

**Risks and fallbacks:**

| Risk | Likelihood | Fallback |
| --- | --- | --- |
| Oyez terms forbid reuse, or no stable access | Medium | Use CourtListener transcripts as a weak reference only; relabel the evaluation as "agreement"; shrink scored set to what is allowed |
| CourtListener rate limits or audio missing for a hearing | Medium | Proceed with the hearings that are available; the design does not depend on the exact count |
| pyannote gating or token trouble | Medium | Drop DER and report WER and speaker-count error only; note it in limitations |
| S3 shortcut auth or URL error | Medium | Check bucket region, IAM actions and URL form first. Last resort: upload to OneLake Files and record the loss of the cross-cloud step |
| Fabric provider lacks a resource | High | Terraform for capacity and workspace, REST script for the rest, log the gap |
| Direct Lake or RLS behaves unexpectedly **\[VERIFY\]** | Low-Medium | Fall back to import mode for the semantic model; RLS rules stay the same |
| Day 2 overruns | High | Move the semantic model and report to the buffer day. Plots 1-5 from notebooks still satisfy the DS goal |
| Spark session or capacity throttling | Low | Run one notebook at a time; avoid parallel pipeline branches on the capacity |

## 19. Purpose test: skills and tools, in and out

Your rule: include something only if it has a clear purpose in the data flow. Verdicts for everything on your skills list that could plausibly apply.

| Item | Verdict | Reason |
| --- | --- | --- |
| Python, PySpark, SQL | In | All transforms, tests and queries |
| librosa | In | Quality gate metrics |
| faster-whisper (PyTorch underneath) | In | Transcription; PyTorch is not used directly |
| pyannote | In | Diarization, plus `pyannote.metrics` for DER |
| jiwer | In | WER computation |
| eyecite | In | Citation baseline, so the LLM has something to beat |
| LLM extraction plus pydantic | In | Measured against eyecite on hand labels |
| rapidfuzz | In | Name matching against a small closed candidate set |
| AWS S3 and IAM | In | The cross-cloud source and least-privilege read access |
| Terraform | In | Destroy and rebuild |
| GitHub Actions | In | CI tests and Terraform validation. Azure DevOps is skipped because the repo is on GitHub; both work with Fabric Git |
| Denoising, source separation | Conditional | Only under the decision rule in 9.4 |
| TCN, VAE | Out | No problem in this flow needs them; adding them would be for show |
| Fine-tuning a pretrained model | Out | No training data and no measured need |
| Embeddings and vector search | Out | The evidence index is searched by case and time, not semantic similarity. Add only with a retrieval question to evaluate |
| scikit-learn | Out | There is no model to train; do not invent a classifier |
| MongoDB | Out | Delta tables fit this data |
| Airflow | Out | Fabric pipelines orchestrate |
| Kubernetes | Out | No services to run |
| Databricks | Out | Fabric Spark does the same job; Delta and PySpark skills transfer one-to-one, which is a good interview point |
| MLflow | Out | `fact_asr_eval` already tracks runs |
| Great Expectations, Soda, dbt | Out | Custom checks suffice at this size |
| GCP | Out except Colab | Used only as a free GPU |

**Cut order if time runs short:** (1) deployment pipeline, (2) citation lookup guard, (3) email alert on failure, (4) hand labels down from 40 to 20 each, (5) the Git-in-Terraform stretch, (6) the second ASR model. **Never cut:** S3 shortcut, quality gate with quarantine, WER evaluation, star schema, zero-row failure test, RLS demo, rebuild test. Change this order now if you disagree, not on Day 2.

## 20. Interview talking points

Each line is a question you should expect and the short answer this project lets you give. Fill in the real numbers after the run.

| Question | Your answer, in one breath |
| --- | --- |
| Why a shortcut instead of copying S3 into OneLake? | One source of truth, no sync job, and I cached it to control egress. Trade-off: S3 shortcuts are read-only, so derived data is written back to S3 |
| Why three lakehouses, not one? | Visible layer boundaries and separate access per layer. At scale I would revisit schemas |
| Why no warehouse? | The lakehouse and SQL endpoint cover the read pattern. I would add a warehouse for multi-table T-SQL transactions |
| How did you measure transcription quality? | WER and approximate DER against Oyez on N hearings, sliced by quality and speaker role. Weak reference for the rest, labelled as such |
| Did you fine-tune or denoise? | No. I set a decision rule first, measured, and it did not trigger. (Change this line if it did) |
| How do you catch a pipeline that succeeds with zero rows? | Row-count floors, volume against last run, per-layer reconciliation, and a heartbeat row, all gating the pipeline |
| How does row-level security work here, and where does it fail? | Roles on the semantic model filter restricted cases. It does not apply to workspace members with write access, so consumers must be viewers |
| What is your data-product story? | Owner, contract, SLA, input and output ports, documented limits. No mesh, because I had no coordination problem |
| What changes at 10,000 hearings? | Incremental manifest-driven loads already exist; ASR moves to a batch GPU service; audio QC moves to a Spark UDF; evidence search needs an index; add monitoring and alerting |
| How does this connect to your Thomson Reuters work? | Same mindset as the agent-evaluation pipelines: define ground truth, measure, slice, report honestly, and model the results as a star schema |

**A story that works:** open with the measured result you are proudest of, not with the architecture. Architecture answers follow when they ask.

## 21. Fabric glossary

Terms in the order you will meet them.

| Term | Plain meaning | Where you use it |
| --- | --- | --- |
| Tenant | Your organisation's Microsoft account space; admin settings live here | Day 0: enable trial and API access |
| Capacity (F2 SKU, paid) | The pool of compute that Fabric items run on, billed by time while running | The F2 capacity (hearingevidencef2, UK West) on Azure free credit; Terraform attaches the workspace to it at creation; pause it after every session |
| Workspace | A folder-like container for Fabric items, assigned to one capacity | `ws-hearing-evidence-dev` |
| OneLake | Fabric's single storage layer, like one OneDrive for all data | Underneath everything |
| Lakehouse | OneLake folder with Files, Delta Tables and an auto SQL endpoint | `lh_bronze`, `lh_silver`, `lh_gold` |
| Files vs Tables | Files hold any raw format; Tables hold Delta tables | Shortcut under Files; Delta under Tables |
| Shortcut | A pointer making external or other-lakehouse data appear in a lakehouse without copying | `raw` to S3 |
| Delta table | Parquet files plus a transaction log; gives ACID, MERGE and time travel | Every layer's tables |
| Notebook | Code in cells that runs on Spark in your capacity | `nb_01` to `nb_08` |
| Starter pool | The default fast-starting Spark compute | All notebooks |
| Data pipeline | Orchestration of activities (notebooks, lookups, conditions) | `pl_hearing_evidence` |
| SQL analytics endpoint | Read-only T-SQL view over a lakehouse's Delta tables | DQ queries, evidence search, smoke test |
| Semantic model | A Power BI model: tables, relationships, measures, security | `sm_hearing_evidence` |
| Direct Lake | Semantic model mode reading Delta files straight from OneLake with no import copy | Gold to Power BI |
| Row-level security (RLS) | Rules that filter which rows a role can see | Restricted-matter demo |
| Git integration | Syncs workspace items to a Git branch or folder | `fabric/` folder |
| Deployment pipeline | Promotes items between workspaces such as dev to test | Optional day 3 or buffer |
| Lineage view | Graph of how items depend on each other | Governance screenshot |
| Domain and endorsement | Admin grouping of workspaces, and Promoted or Certified labels on items | Optional 10-minute governance win |
| Monitoring hub | Run history and status for notebooks and pipelines | Debugging, health evidence |

**Learning rule:** after each Day 1 and Day 2 session, write 3 lines in `docs/decisions.md`: what the item is, why you used it, what surprised you. That is the encoding practice you asked for, and it doubles as interview material.
