# Semantic model: `sm_hearing_evidence`

This page describes the TMDL files in
[`fabric/sm_hearing_evidence.SemanticModel/definition/`](../fabric/sm_hearing_evidence.SemanticModel/definition/).

## 1. What it is

`sm_hearing_evidence` is a Power BI semantic model in Direct Lake mode over the lakehouse
`lh_gold`. `expressions.tmdl` defines the source `DirectLake - lh_gold` (the OneLake path of
`lh_gold`), and every table's partition is `mode: directLake` on that source, schema `dbo`.

Direct Lake reads the gold Delta tables in OneLake directly, with no import copy.

## 2. Tables

| Table | Role | Grain |
| --- | --- | --- |
| `fact_utterance` | Fact | One speaker turn. Holds the keys, `side`, `is_amicus`, timings and `word_count`. |
| `dim_case` | Dimension | One case (docket). Holds `is_restricted`, the RLS flag. |
| `dim_hearing` | Dimension | One oral argument, with its MP3 link and durations. The dataset has one hearing per case. |
| `dim_speaker` | Dimension | One person (justice or advocate), case-independent, plus an `Unknown` row with key -1. |
| `gold_evidence_index` | Serving table | One utterance, denormalised for search: text, speaker, side, a timestamped `audio_link` and an `evidence_hash`. |

The grain of each table is set in [`nb_03_gold`](../fabric/nb_03_gold.Notebook/notebook-content.py).

## 3. Relationships

| From | To | Cardinality | Filter direction |
| --- | --- | --- | --- |
| `fact_utterance[case_key]` | `dim_case[case_key]` | Many-to-one | Single |
| `fact_utterance[speaker_key]` | `dim_speaker[speaker_key]` | Many-to-one | Single |
| `fact_utterance[hearing_key]` | `dim_hearing[hearing_key]` | Many-to-one | Single |
| `gold_evidence_index[docket]` | `dim_case[docket]` | Many-to-one | Single |

`relationships.tmdl` sets only the columns, so every relationship uses the TMDL defaults:
many-to-one, single direction, active.

Filters flow from the dimensions to the fact: a slicer on `dim_case` filters `fact_utterance`
and `gold_evidence_index`.

## 4. Measures

All four measures live on `fact_utterance`.

**Speaking Minutes**: total speaking time of the turns in the current filter, in minutes.

```dax
DIVIDE ( SUM ( fact_utterance[duration_s] ), 60 )
```

**Turns**: number of speaker turns in the current filter.

```dax
COUNTROWS ( fact_utterance )
```

**Share of Hearing** (format `0.00%`): the current selection's speaking minutes as a share of
all speaking minutes in the same case. `REMOVEFILTERS ()` clears every filter (speaker, side,
role) and `VALUES ( dim_case[docket] )` restores the case, so the denominator is the whole
hearing. The model has one hearing per case, so the case total is the hearing total.

```dax
DIVIDE (
    [Speaking Minutes],
    CALCULATE (
        [Speaking Minutes],
        REMOVEFILTERS (),
        VALUES ( dim_case[docket] )
    )
)
```

**Words per Minute**: words spoken divided by speaking minutes, for the current filter.

```dax
DIVIDE ( SUM ( fact_utterance[word_count] ), [Speaking Minutes] )
```

## 5. Hidden columns

| Table | Hidden columns |
| --- | --- |
| `fact_utterance` | `utterance_key`, `case_key`, `speaker_key`, `hearing_key`, `duration_s` |
| `dim_case` | `case_key` |
| `dim_hearing` | `hearing_key`, `case_key` |
| `dim_speaker` | `speaker_key` |

Surrogate keys and the raw `duration_s` are plumbing: the keys exist for relationships and
`duration_s` feeds the measures. Report users work with names (case, speaker, side) and with
the measures.

## 6. Security

The role `GeneralCounsel` (`roles/GeneralCounsel.tmdl`) has read permission and one table filter:

```dax
dim_case: [is_restricted] == false()
```

How the filter flows:

- `dim_case` shows unrestricted cases only.
- `fact_utterance` follows through `case_key`, so restricted turns drop out of every measure,
  including the denominator of Share of Hearing.
- `gold_evidence_index` follows through `docket`, so restricted snippets drop out of search.
- `dim_hearing` and `dim_speaker` sit on the "one" side of single-direction relationships to
  the fact, so the role leaves them unfiltered. `dim_hearing` lists every hearing's `docket`
  and `mp3_url`, restricted cases included.

The role has no members. It is checked with "Test as role" in Fabric.

Limit: RLS applies to users with Viewer access or read permission on the model. Workspace
Admin, Member and Contributor users see every row.

## 7. Version control

The semantic model is version-controlled as TMDL, so measures, relationships and RLS rules are
reviewed in pull requests like any other code. The model's logic isn't hidden inside a UI.

| File | Holds |
| --- | --- |
| `tables/*.tmdl` | One file per table: columns, types, hidden flags, measures, Direct Lake partition |
| `relationships.tmdl` | The relationships between tables |
| `roles/GeneralCounsel.tmdl` | The RLS role and its table filter |
| `expressions.tmdl` | The Direct Lake source: OneLake path of `lh_gold` |
| `model.tmdl` | Model settings and the list of tables and roles |
| `database.tmdl` | Compatibility level |

Changes flow: Fabric workspace -> `fabric-sync` (workspace commit through Source control) ->
pull request -> `main`. After the merge, `fabric-sync` is fast-forwarded to `main` and the
workspace pulls the update.

## 8. How to inspect it in Fabric

- Open the semantic model, choose **Open data model**, and browse the **Data** pane. Measures
  carry a calculator icon.
- Or open **DAX query view** and run:

```dax
EVALUATE INFO.VIEW.MEASURES()
```
