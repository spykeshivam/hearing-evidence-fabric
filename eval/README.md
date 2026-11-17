# eval

Evaluation set for the data agent `da_hearing_evidence`: fixed questions with known answers, and a log of how the agent answered them.

## Files

- `questions.yaml`: the questions. Each has an `id`, a `version`, the question `text`, the `expected_behaviour` (`answer` or `refuse`), the `ground_truth_dax` query, the structured `ground_truth` and `notes` on how to grade it.
- `results.csv`: one row per question per run.

## Adding a run

1. Ask the agent every question in `questions.yaml`.
2. Append one row per question to `results.csv`, all sharing the same `run_id` and `run_date`.
3. Rows are append only. Past rows stay as written.

Column conventions:

- `agent_version`: `<label>@<fabric-sync commit SHA>`, for example `v0-baseline@abc1234`.
- `question_version`: the `version` of the question as asked.
- `agent_answer` and `agent_dax`: newlines replaced with ` | `.
- `data_fingerprint`: the Delta version of `lh_gold.dbo.fact_utterance`, from `DESCRIBE HISTORY`.

## Verdict codes

| Code | Meaning |
|---|---|
| `correct` | Right and complete. |
| `partial` | Right but incomplete or badly formatted. |
| `wrong` | Wrong answer. |
| `refused_ok` | Correct refusal of an out-of-scope question. |

`tests/test_eval_files.py` checks both files in CI.
