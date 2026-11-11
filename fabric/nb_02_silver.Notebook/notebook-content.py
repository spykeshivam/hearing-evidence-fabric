# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c49ecf7c-5905-4428-bc8b-7f170bae4451",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "846c8817-872c-4a45-908c-5a04fc8d7ecd",
# META       "known_lakehouses": [
# META         {
# META           "id": "c49ecf7c-5905-4428-bc8b-7f170bae4451"
# META         },
# META         {
# META           "id": "830a3ca1-2f11-4d7d-aac0-bdd1cf825f39"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Session setup: install libraries first, because %pip restarts the Python process
# and would wipe any variables defined before it.
%pip install mutagen -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
promotion gate.

Silver is built only from bronze data that passed integrity verification.
Bronze accepts everything "as received"; promotion to silver is the controlled step.
A hearing whose audio failed its sha256 check never reaches silver, so downstream
consumers only ever see verified evidence.
"""
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *

BRZ = "lh_bronze.dbo"
SLV = "lh_silver.dbo"


def latest(table):
    """
    Return the latest version of each source file in a bronze table.

    Bronze is append-only: a source file re-fetched with different content (new sha256)
    adds a new row and never overwrites history. This window function keeps the newest
    row per file (the "latest record per key" deduplication pattern), and derives the
    docket number, the natural key that joins CourtListener and Oyez data.
    """
    w = Window.partitionBy("s3_key").orderBy(F.col("_ingested_at").desc())  # group per file, newest first
    return (spark.table(f"{BRZ}.{table}")
            .withColumn("_rn", F.row_number().over(w))      # 1 = newest version of the file
            .filter("_rn = 1").drop("_rn")
            .withColumn("docket", F.regexp_extract("s3_key", r"(\d{2}-\d+)", 1)))  # natural key, e.g. 23-970


# The data-quality log is the evidence used to decide promotion.
# Only the most recent verification run counts: an old PASS must not vouch for new bytes.
dq = spark.table(f"{BRZ}.dq_results")
last_run = dq.orderBy(F.col("checked_at").desc()).first()["run_id"]

# Blocking check: MP3 sha256 equals the fingerprint recorded at download time (chain of custody).
verified = (dq.filter((F.col("run_id") == last_run)
                      & F.col("check_id").startswith("audio_sha256_")
                      & (F.col("status") == "PASS"))
              .select(F.regexp_extract("check_id", r"(\d{2}-\d+)", 1).alias("docket")))

print("verified dockets:", [r.docket for r in verified.collect()])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
slv_turn, one row per speaker turn.

Grain: one speaker turn (docket, section_idx, turn_idx) in an oral-argument transcript.
Source: Oyez transcript JSON, stored verbatim in bronze and parsed here (schema-on-read).
Every row keeps the sha256 and path of the bronze file it came from (row-level lineage),
so any silver fact traces back to the exact source bytes.
"""

# Explicit schema = the subset of Oyez's JSON this layer depends on (a consumer-side contract).
# from_json ignores fields not listed, so new upstream fields never break the load (schema drift);
# a renamed field arrives as NULL, which a data-quality check must catch.
turn_t = StructType([
    StructField("speaker", StructType([StructField("name", StringType()),
                                       StructField("identifier", StringType())])),
    StructField("start", DoubleType()),                 # seconds from the start of the audio
    StructField("stop", DoubleType()),
    StructField("text_blocks", ArrayType(StructType([StructField("text", StringType())])))])
doc_t = StructType([StructField("transcript", StructType([
    StructField("sections", ArrayType(StructType([StructField("turns", ArrayType(turn_t))])))]))])

slv_turn = (
    latest("brz_oyez_transcript")
    .join(verified, "docket")                           # promotion gate: verified hearings only
    .withColumn("j", F.from_json("payload", doc_t))     # semi-structured text -> typed nested struct
    # posexplode turns an array into rows and keeps the position, which becomes part of the key
    .select("docket", "sha256", "_source_file",
            F.posexplode("j.transcript.sections").alias("section_idx", "sec"))
    .select("docket", "sha256", "_source_file", "section_idx",
            F.posexplode("sec.turns").alias("turn_idx", "t"))
    .select("docket", "section_idx", "turn_idx",
            F.col("t.speaker.name").alias("speaker_name"),
            F.col("t.speaker.identifier").alias("speaker_id"),        # Oyez's stable person id: used for entity resolution later
            F.col("t.start").alias("start_s"),
            F.col("t.stop").alias("stop_s"),
            F.concat_ws(" ", "t.text_blocks.text").alias("text"),   # join a turn's text blocks into one string
            F.col("sha256").alias("_source_sha256"),                # lineage: exact source bytes
            "_source_file")                                         # lineage: source location
    .withColumn("_processed_at", F.current_timestamp())             # lineage: when silver was built
)

# Overwrite = an idempotent full rebuild from bronze: re-running gives the same result, never duplicates.
# Fine at this size; at scale, an incremental MERGE on the natural key replaces it.
slv_turn.write.mode("overwrite").format("delta").saveAsTable(f"{SLV}.slv_turn")
display(spark.table(f"{SLV}.slv_turn").groupBy("docket").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
slv_hearing, one row per oral-argument hearing.

Grain: one hearing (docket). Conforms two independent sources on the docket number:
CourtListener gives the audio and its reported metadata; Oyez gives the case.
Audio duration is MEASURED from the MP3 itself: CourtListener's reported duration was
found to understate 2 of 4 hearings by ~22 minutes, so the provider's value is kept only
as a monitored signal, never as the source of truth.
"""
import mutagen

AUDIO_ROOT = "/lakehouse/default/Files/he-raw/"   # shortcut mounted as a local path (default lakehouse = lh_bronze)


def measure_duration(s3_key):
    """Read the MP3's frames/headers and return its real playing time in seconds."""
    return round(mutagen.File(AUDIO_ROOT + s3_key).info.length, 1)


# Measure only verified audio (4 small rows, so a driver-side loop is fine).
# Window keeps the newest manifest row per file, the same rule as everywhere else.
w_m = Window.partitionBy("s3_key").orderBy(F.col("batch_id").desc())
audio_rows = (spark.table(f"{BRZ}.brz_object_manifest")
              .withColumn("_rn", F.row_number().over(w_m)).filter("_rn = 1 AND kind = 'audio'")
              .withColumn("docket", F.regexp_extract("s3_key", r"(\d{2}-\d+)", 1))
              .join(verified, "docket").select("docket", "s3_key").collect())
measured = spark.createDataFrame(
    [(r.docket, measure_duration(r.s3_key)) for r in audio_rows],
    "docket string, measured_duration_s double")

# get_json_object pulls single scalar fields straight from JSON text by path.
cl_audio = (latest("brz_audio_meta").select(
    "docket",
    F.get_json_object("payload", "$.duration").cast("int").alias("reported_duration_s"),  # provider's claim
    F.concat(F.lit("https://storage.courtlistener.com/"),
             F.get_json_object("payload", "$.local_path_mp3")).alias("mp3_url"),  # deep-link target
    F.col("sha256").alias("_audio_meta_sha256")))

oyez_case = (latest("brz_oyez_case").select(
    "docket",
    F.get_json_object("payload", "$.name").alias("case_name"),
    F.get_json_object("payload", "$.term").alias("term"),
    F.get_json_object("payload", "$.first_party").alias("first_party"),
    F.get_json_object("payload", "$.second_party").alias("second_party"),
    "licence_id"))                                            # licence travels with the data

slv_hearing = (oyez_case.join(cl_audio, "docket").join(measured, "docket")   # inner joins = gate holds
               .withColumn("_processed_at", F.current_timestamp()))
slv_hearing.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").saveAsTable(f"{SLV}.slv_hearing")
display(spark.table(f"{SLV}.slv_hearing"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
slv_case_participant, one row per person per case.

Grain: (docket, speaker_id). Resolves every person who can speak in a hearing to a role
(justice or advocate) and, for advocates, to the side they argued for. The join key is
Oyez's stable person identifier, the same id carried by every transcript turn, so speaker
resolution here is an exact key match, not fuzzy name matching.
"""
# Consumer-side schema: only the fields this layer depends on (unlisted fields are ignored).
person_t = StructType([StructField("name", StringType()), StructField("identifier", StringType())])
case_t = StructType([
    StructField("advocates", ArrayType(StructType([
        StructField("advocate", person_t),
        StructField("advocate_description", StringType())]))),   # e.g. "for the petitioners"
    StructField("heard_by", ArrayType(StructType([
        StructField("members", ArrayType(person_t))])))])           # the bench that heard the case


cases = latest("brz_oyez_case").join(verified, "docket").withColumn("j", F.from_json("payload", case_t))

# Advocates: the side is derived from free text by keyword rules. Amici "supporting" a party
# are counted on that party's side and flagged, so speaking time by side stays meaningful.
desc = F.lower(F.col("a.advocate_description"))
advocates = (cases.select("docket", F.explode("j.advocates").alias("a"))
    .select("docket",
            F.col("a.advocate.identifier").alias("speaker_id"),
            F.col("a.advocate.name").alias("speaker_name"),
            F.lit("advocate").alias("role"),
            F.when(desc.contains("petitioner"), "petitioner")
             .when(desc.contains("respondent"), "respondent")
             .when(desc.contains("amicus"), "amicus_other")   # amicus supporting neither party (e.g. vacatur)
             .otherwise("unknown").alias("side"),
            desc.contains("amicus").alias("is_amicus"),
            F.col("a.advocate_description").alias("role_description")))   # kept for audit

justices = (cases.select("docket", F.explode("j.heard_by").alias("h"))
    .select("docket", F.explode("h.members").alias("m"))
    .select("docket",
            F.col("m.identifier").alias("speaker_id"),
            F.col("m.name").alias("speaker_name"),
            F.lit("justice").alias("role"),
            F.lit("bench").alias("side"),
            F.lit(False).alias("is_amicus"),
            F.lit(None).cast("string").alias("role_description")))

slv_case_participant = (advocates.unionByName(justices)
    .dropDuplicates(["docket", "speaker_id"])          # grain guard: one row per person per case
    .withColumn("_processed_at", F.current_timestamp()))
slv_case_participant.write.mode("overwrite").option("overwriteSchema", "true") \
    .format("delta").saveAsTable(f"{SLV}.slv_case_participant")

# Resolution rate: share of turns whose speaker is a known participant of that case.
turns = spark.table(f"{SLV}.slv_turn")
resolved = turns.join(slv_case_participant.select("docket", "speaker_id"), ["docket", "speaker_id"], "left_semi")
display(turns.groupBy("docket").count().withColumnRenamed("count", "turns")
        .join(resolved.groupBy("docket").count().withColumnRenamed("count", "resolved"), "docket")
        .withColumn("resolution_rate", F.round(F.col("resolved") / F.col("turns"), 3)))
display(slv_case_participant.filter("role = 'advocate'").select("docket", "speaker_name", "side", "is_amicus", "role_description"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
nb_02_silver | Checks cell: silver data-quality checks.

Proves the parsed transcript is internally valid, fits the measured audio, and that every
speaker resolves to a known participant of the case. Error-level failures stop the run
(fail loudly), so an invalid silver build is never consumed downstream. Results go to the
silver check log; each layer keeps its own log.
"""
import uuid
from datetime import datetime, timezone

run_id, now, results = str(uuid.uuid4()), datetime.now(timezone.utc), []

def record(check_id, table, ok, observed, expected, severity="error"):
    """Append one check result; severity 'error' blocks promotion, 'warn' is logged only."""
    results.append((check_id, "silver", table, "PASS" if ok else "FAIL",
                    str(observed), str(expected), severity, run_id, now))

# One aggregate row per hearing: compute in Spark, then bring 4 small rows to the driver.
stats = (spark.table(f"{SLV}.slv_turn").groupBy("docket").agg(
            F.count("*").alias("turns"),
            F.sum(F.when(F.col("stop_s") < F.col("start_s"), 1).otherwise(0)).alias("bad_order"),
            F.max("stop_s").alias("last_stop_s"),
            F.avg(F.col("speaker_name").isNull().cast("int")).alias("null_speaker_rate"))
         .join(spark.table(f"{SLV}.slv_hearing")
               .select("docket", "measured_duration_s", "reported_duration_s"), "docket"))

for r in stats.collect():
    d = r.docket
    record(f"turns_nonempty_{d}", "slv_turn", r.turns > 0, r.turns, ">0")
    # Validity: a turn cannot end before it starts
    record(f"turn_order_{d}", "slv_turn", r.bad_order == 0, r.bad_order, 0)
    # Integrity (blocking): the transcript timeline must fit the audio we actually hold
    record(f"duration_match_{d}", "slv_hearing",
           abs(r.measured_duration_s - r.last_stop_s) <= 30,
           round(r.last_stop_s, 1), f"{r.measured_duration_s} ±30s")
    # Source metadata quality (monitored): the provider's reported duration vs measured
    record(f"reported_duration_{d}", "slv_hearing",
           abs(r.measured_duration_s - r.reported_duration_s) <= 30,
           r.reported_duration_s, f"{r.measured_duration_s} ±30s", severity="warn")
    # Schema-drift sentinel: if Oyez renames the speaker field, from_json yields NULLs here
    record(f"speaker_null_rate_{d}", "slv_turn", r.null_speaker_rate < 0.05,
           round(r.null_speaker_rate, 3), "<0.05", severity="warn")

# NEW: entity-resolution quality. Every turn's speaker should be a known participant of its case.
part = spark.table(f"{SLV}.slv_case_participant")
turns = spark.table(f"{SLV}.slv_turn")
res = (turns.groupBy("docket").agg(F.count("*").alias("turns"))
       .join(turns.join(part, ["docket", "speaker_id"], "left_semi")   # left_semi = turns WITH a match
             .groupBy("docket").agg(F.count("*").alias("resolved")), "docket", "left")
       .fillna(0, ["resolved"]).collect())
for r in res:
    record(f"speaker_resolution_{r.docket}", "slv_case_participant",
           r.resolved / r.turns >= 0.95, round(r.resolved / r.turns, 3), ">=0.95")

# NEW: side attribution. An advocate with unrecognisable side text breaks speaking time by side.
unknown_side = part.filter("role = 'advocate' AND side = 'unknown'").count()
record("advocate_side_known", "slv_case_participant", unknown_side == 0, unknown_side, 0,
       severity="warn")

schema = ("check_id string, layer string, table_name string, status string, observed string, "
          "expected string, severity string, run_id string, checked_at timestamp")
out = spark.createDataFrame(results, schema)
out.write.mode("append").format("delta").saveAsTable(f"{SLV}.dq_results")
display(out.select("check_id", "status", "severity", "observed", "expected"))

failed = [r[0] for r in results if r[3] == "FAIL" and r[6] == "error"]
if failed:
    raise AssertionError(f"Silver verification failed: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

"""
preview every silver table.

Runs only after the checks cell passes (a failed check raises before this point), so what
is shown here is exactly what gold is allowed to consume.
"""

for t in ["slv_hearing", "slv_case_participant", "slv_turn", "dq_results"]:
    df = spark.table(f"{SLV}.{t}")
    print(f"=== {SLV}.{t}: {df.count()} rows | grain/columns: {df.columns}")
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
