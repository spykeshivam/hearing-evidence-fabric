# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "830a3ca1-2f11-4d7d-aac0-bdd1cf825f39",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "846c8817-872c-4a45-908c-5a04fc8d7ecd",
# META       "known_lakehouses": [
# META         {
# META           "id": "830a3ca1-2f11-4d7d-aac0-bdd1cf825f39"
# META         },
# META         {
# META           "id": "8bbd839c-04b7-4ca2-9ebc-20361dab5a60"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
"""
promotion gate.

Gold builds only from a silver build whose latest check run had no blocking failure.
Run-level gate (all or nothing): gold reconciles counts across tables, so a partially
valid silver would produce misleading totals. In a pipeline, a failed silver notebook
already stops this one; this gate also protects manual runs.
"""
from pyspark.sql import functions as F

SLV, GLD = "lh_silver.dbo", "lh_gold.dbo"

sdq = spark.table(f"{SLV}.dq_results")
last = sdq.orderBy(F.col("checked_at").desc()).first()["run_id"]   # most recent silver check run
blocking = (sdq.filter((F.col("run_id") == last) & (F.col("status") == "FAIL")
                       & (F.col("severity") == "error"))
               .select("check_id").collect())
if blocking:
    raise AssertionError(f"Gold blocked: silver run {last} failed {[r.check_id for r in blocking]}")
print(f"Silver run {last} passed its blocking checks.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
dimensions and the matter-access reference.

Dimensions describe the who and what (case, hearing, speaker). Surrogate keys are
deterministic hashes of natural keys, so a full rebuild reproduces identical keys and
every rerun is idempotent. Every dimension that a fact can miss carries an "unknown"
member (key -1), so an unresolved speaker never silently drops a fact row.
"""
def skey(*cols):
    """Deterministic 64-bit surrogate key from natural-key columns (same input -> same key)."""
    return F.xxhash64(*cols)

hearing = spark.table(f"{SLV}.slv_hearing")
part = spark.table(f"{SLV}.slv_case_participant")

# Simulated matter-access list: which matters are restricted to a cleared team.
# All data here is public; this seed demonstrates permission-aware serving. It is the
# input that row-level security reads: changing a row changes who sees the matter.
access = spark.createDataFrame([("23-970", True), ("21-1496", True)],
                               "docket string, is_restricted boolean")
access.write.mode("overwrite").format("delta").saveAsTable(f"{GLD}.ref_matter_access")

# dim_case: one row per case (docket). Unlisted matters default to unrestricted.
dim_case = (hearing.join(access, "docket", "left")
    .select(skey("docket").alias("case_key"), "docket", "case_name", "term",
            "first_party", "second_party", "licence_id",
            F.coalesce("is_restricted", F.lit(False)).alias("is_restricted")))

# dim_hearing: one row per oral argument. One hearing per case in this dataset; a real
# docket can be argued more than once, so its natural key would then add the argument date.
dim_hearing = hearing.select(skey("docket").alias("hearing_key"), skey("docket").alias("case_key"),
                             "docket", "mp3_url", "measured_duration_s", "reported_duration_s")

# dim_speaker: one row per PERSON, case-independent. Side is not here: it depends on the
# case, so it lives on the fact (one advocate can argue for different sides in different cases).
unknown = spark.createDataFrame([(-1, None, "Unknown", "unknown")],
                                "speaker_key bigint, speaker_id string, speaker_name string, role string")
dim_speaker = (part.select("speaker_id", "speaker_name", "role").dropDuplicates(["speaker_id"])
               .select(skey("speaker_id").alias("speaker_key"), "speaker_id", "speaker_name", "role")
               .unionByName(unknown))

for name, df in [("dim_case", dim_case), ("dim_hearing", dim_hearing), ("dim_speaker", dim_speaker)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{GLD}.{name}")
    print(name, spark.table(f"{GLD}.{name}").count())
display(spark.table(f"{GLD}.dim_case"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
fact_utterance and gold_evidence_index.

fact_utterance grain: one speaker turn; it holds the keys and the measures (duration,
words) every metric is computed from. Side depends on the case, so it is resolved per
(docket, speaker_id) and stored on the fact.
gold_evidence_index is a deliberately denormalised serving table for search: one row per
utterance with the text, who said it, for which side, and a link to the exact second of
the audio. evidence_hash fingerprints each snippet so any later change is detectable.
"""
turn = spark.table(f"{SLV}.slv_turn")

base = (turn.join(part.select("docket", "speaker_id", "side", "is_amicus"),
                  ["docket", "speaker_id"], "left")                 # left join: keep unresolved turns
    .withColumn("speaker_key", F.when(F.col("side").isNull(), F.lit(-1).cast("bigint"))
                                .otherwise(skey("speaker_id")))     # -1 = unknown member
    .withColumn("side", F.coalesce("side", F.lit("unknown")))
    .withColumn("utterance_key", skey("docket", "section_idx", "turn_idx"))
    .withColumn("hearing_key", skey("docket"))
    .withColumn("case_key", skey("docket"))
    .withColumn("duration_s", F.round(F.col("stop_s") - F.col("start_s"), 2))
    .withColumn("word_count", F.size(F.split(F.trim("text"), r"\s+"))))

fact_utterance = base.select("utterance_key", "hearing_key", "case_key", "speaker_key", "side",
                             "is_amicus", "start_s", "stop_s", "duration_s", "word_count")

gold_evidence_index = (base.drop("speaker_name")
    .join(dim_case.select("case_key", "case_name", "is_restricted"), "case_key")
    .join(dim_hearing.select("hearing_key", "mp3_url"), "hearing_key")
    .join(dim_speaker.select("speaker_key", "speaker_name", "role"), "speaker_key")
    .select("utterance_key", "docket", "case_name", "speaker_name", "role", "side",
            "start_s", "stop_s", "text",
            # Media-fragment link: a browser opens the MP3 at this second
            F.concat("mp3_url", F.lit("#t="), F.floor("start_s").cast("string")).alias("audio_link"),
            "is_restricted",
            F.sha2(F.concat_ws("|", "docket", "start_s", "stop_s", "text"), 256).alias("evidence_hash")))

# Overwrite with deterministic keys = idempotent rebuild; MERGE on the keys is the incremental option.
for name, df in [("fact_utterance", fact_utterance), ("gold_evidence_index", gold_evidence_index)]:
    df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{GLD}.{name}")
    print(name, spark.table(f"{GLD}.{name}").count())
display(spark.table(f"{GLD}.gold_evidence_index").limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
gold data-quality checks.

Gold is what people act on, so its checks are structural: keys unique, every foreign key
resolves (referential integrity), counts reconcile to silver, and restricted matters are
flagged consistently in the serving table. Results go to the ops schema, which separates
operational tables from business tables.
"""
import uuid
from datetime import datetime, timezone

spark.sql("CREATE SCHEMA IF NOT EXISTS lh_gold.ops")    # operational tables live apart from business data
run_id, now, results = str(uuid.uuid4()), datetime.now(timezone.utc), []

def record(check_id, table, ok, observed, expected, severity="error"):
    """Append one check result; severity 'error' blocks consumption, 'warn' is logged only."""
    results.append((check_id, "gold", table, "PASS" if ok else "FAIL",
                    str(observed), str(expected), severity, run_id, now))

g = lambda t: spark.table(f"{GLD}.{t}")

# Uniqueness: a surrogate key must identify exactly one row
for t, k in [("dim_case", "case_key"), ("dim_hearing", "hearing_key"), ("dim_speaker", "speaker_key"),
             ("fact_utterance", "utterance_key"), ("gold_evidence_index", "utterance_key")]:
    n, d = g(t).count(), g(t).select(k).distinct().count()
    record(f"pk_unique_{t}", t, n == d, d, n)

# Referential integrity: every fact foreign key exists in its dimension (left_anti = orphans)
f = g("fact_utterance")
for k, dim in [("speaker_key", "dim_speaker"), ("hearing_key", "dim_hearing"), ("case_key", "dim_case")]:
    orphans = f.join(g(dim), k, "left_anti").count()
    record(f"fk_resolves_{k}", "fact_utterance", orphans == 0, orphans, 0)

# Reconciliation: gold neither loses nor invents rows relative to silver
n_slv, n_fact, n_idx = spark.table(f"{SLV}.slv_turn").count(), f.count(), g("gold_evidence_index").count()
record("reconcile_fact_vs_silver", "fact_utterance", n_fact == n_slv, n_fact, n_slv)
record("reconcile_index_vs_fact", "gold_evidence_index", n_idx == n_fact, n_idx, n_fact)

# Resolution coverage: share of turns attributed to the Unknown speaker
unk = f.filter("speaker_key = -1").count() / n_fact
record("unknown_speaker_share", "fact_utterance", unk < 0.05, round(unk, 3), "<0.05", severity="warn")

# Security consistency: the serving table's restricted flag must match the access list
mismatch = (g("gold_evidence_index").select("docket", "is_restricted").distinct()
            .join(g("dim_case").select("docket", F.col("is_restricted").alias("ref")), "docket")
            .filter("is_restricted <> ref").count())
record("restricted_flag_consistent", "gold_evidence_index", mismatch == 0, mismatch, 0)

schema = ("check_id string, layer string, table_name string, status string, observed string, "
          "expected string, severity string, run_id string, checked_at timestamp")
out = spark.createDataFrame(results, schema)
out.write.mode("append").format("delta").saveAsTable("lh_gold.ops.dq_results")
display(out.select("check_id", "status", "severity", "observed", "expected"))

failed = [r[0] for r in results if r[3] == "FAIL" and r[6] == "error"]
if failed:
    raise AssertionError(f"Gold verification failed: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
preview every gold table.

Runs only after the checks cell passes (a failed check raises before this point)"""

for t in ["dim_case", "dim_hearing", "dim_speaker", "fact_utterance", "gold_evidence_index", "ref_matter_access"]:
    df = spark.table(f"{GLD}.{t}")
    print(f"=== {GLD}.{t}: {df.count()} rows | grain/columns: {df.columns}")
    # Lineage columns are hidden in the preview; they stay in the table
    display(df.drop(*[c for c in df.columns if c.startswith("_")]).limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
