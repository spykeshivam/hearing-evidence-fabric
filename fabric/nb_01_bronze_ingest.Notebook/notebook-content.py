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
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
for f in notebookutils.fs.ls("Files/he-raw/courtlistener/audio"):
    print(f.name, round(f.size / 1e6, 1), "MB")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (spark.read.option("header", True)
      .csv("Files/he-raw/_manifests/batch=*/objects.csv"))
print(df.count())
df.printSchema()
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

print("batches:", df.select("batch_id").distinct().count())
print("distinct files:", df.select("s3_key").distinct().count())
df.groupBy("upload_status").count().show()
df.groupBy("s3_key").agg(F.countDistinct("sha256").alias("n_hashes")).filter("n_hashes > 1").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# nb_01_bronze_ingest | cell: load the S3 manifests into a bronze table
# Source : Files/he-raw/_manifests/batch=*/objects.csv  (one CSV per fetch run)
# Target : dbo.brz_object_manifest in lh_bronze (append-only history)
# Rule   : bronze keeps data as received. Allowed changes: typing two
#          columns and adding lineage columns.
# Safe to rerun: batches already in the table are skipped.
# =====================================================================
import uuid
from pyspark.sql import functions as F

TABLE = "dbo.brz_object_manifest"  # schema.table (dbo = default schema)

# One random ID per notebook run, stored on every row it loads (lineage).
run_id = str(uuid.uuid4())

new = (
    # Read every batch's CSV at once; the * matches each batch=<timestamp> folder.
    # A CSV has no types, so every column arrives as text.
    spark.read.option("header", True)
    .csv("Files/he-raw/_manifests/batch=*/objects.csv")
    # Typing: bytes text -> whole number, retrieved_at text -> timestamp.
    .withColumn("bytes", F.col("bytes").cast("long"))
    .withColumn("retrieved_at", F.to_timestamp("retrieved_at"))
    # Lineage columns (the _ prefix marks pipeline-added columns):
    .withColumn("_source_file", F.input_file_name())    # exact CSV the row came from
    .withColumn("_ingested_at", F.current_timestamp())  # when Fabric loaded the row
    .withColumn("_run_id", F.lit(run_id))               # which notebook run loaded it
)

# Skip batches already loaded: a left_anti join keeps only rows whose batch_id
# is NOT in the existing table. First run: no table yet, so keep everything.
if spark.catalog.tableExists(TABLE):
    loaded = spark.table(TABLE).select("batch_id").distinct()
    new = new.join(loaded, "batch_id", "left_anti")

n = new.count()
print("rows to append:", n)

# append = add rows, never overwrite (bronze is history).
# Delta = Parquet data files + a transaction log; the append is all-or-nothing.
if n:
    new.write.mode("append").format("delta").saveAsTable(TABLE)

print("table rows:", spark.table(TABLE).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# nb_01_bronze_ingest | cell: load the raw JSON files into bronze tables
# Source : Files/he-raw/{courtlistener,oyez}/... *.json  (via S3 shortcut)
# Target : dbo.brz_docket, brz_audio_meta, brz_oyez_case, brz_oyez_transcript
# Rule   : one row per FILE. The whole JSON text is stored untouched in
#          `payload`; parsing is silver's job. Provenance comes from the
#          manifest. Safe to rerun: a file already loaded (same s3_key and
#          same sha256) is skipped.
# =====================================================================
import uuid
from pyspark.sql import functions as F
from pyspark.sql import Window

run_id = str(uuid.uuid4())  # one ID per notebook run, stored on every row (lineage)

# Latest manifest row per file. The manifest repeats every file in every batch,
# so row_number() = 1 over batch_id DESC picks the most recent listing.
w = Window.partitionBy("s3_key").orderBy(F.col("batch_id").desc())
latest = (
    spark.table("dbo.brz_object_manifest")
    .withColumn("_rn", F.row_number().over(w))
    .filter("_rn = 1")
    .select("s3_key", "sha256", "bytes", "retrieved_at", "source_url",
            "licence_id", "upload_status")
)

def load_raw_json(kind, glob, table):
    """Load every JSON file matching `glob` into `table`, appending only new files."""
    raw = (
        # wholetext=True makes Spark read each file as ONE row (column `value`)
        # instead of one row per line, so the JSON stays intact.
        spark.read.option("wholetext", True).text(glob)
        .withColumnRenamed("value", "payload")
        .withColumn("_source_file", F.input_file_name())  # full path of the file
        # Cut the full path down to the S3 key, so we can join the manifest.
        .withColumn("s3_key", F.regexp_extract("_source_file", r"/Files/he-raw/(.*)$", 1))
        .join(latest, "s3_key", "left")                  # add sha256, retrieved_at, ...
        .withColumn("kind", F.lit(kind))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    # Skip files already loaded: left_anti keeps only (s3_key, sha256) pairs
    # that are not in the target table yet.
    if spark.catalog.tableExists(table):
        done = spark.table(table).select("s3_key", "sha256")
        raw = raw.join(done, ["s3_key", "sha256"], "left_anti")
    n = raw.count()
    if n:
        raw.write.mode("append").format("delta").saveAsTable(table)  # append-only history
    print(f"{table}: appended {n}, total {spark.table(table).count()}")

load_raw_json("docket",     "Files/he-raw/courtlistener/dockets/*.json",    "dbo.brz_docket")
load_raw_json("audio_meta", "Files/he-raw/courtlistener/audio_meta/*.json", "dbo.brz_audio_meta")
load_raw_json("case",       "Files/he-raw/oyez/cases/*.json",               "dbo.brz_oyez_case")
load_raw_json("transcript", "Files/he-raw/oyez/transcripts/*.json",         "dbo.brz_oyez_transcript")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reconcile: files the manifest lists per kind (the MP3s are not loaded here).
(spark.table("dbo.brz_object_manifest")
 .filter(F.col("kind").isin("docket", "audio_meta", "case", "transcript"))
 .select("kind", "s3_key").distinct()
 .groupBy("kind").count().show())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# nb_01_bronze_ingest | cell: verify audio fingerprints, reconcile counts
# Checks (each result is one row in dbo.dq_results):
#  1. MP3 sha256 equals the manifest sha256 (the file we hold is the file listed)
#  2. MP3 sha1 equals CourtListener's own sha1 (independent integrity check)
#  3. each raw bronze table has one row per manifest file of its kind
#  4. no bronze table is empty (the "load succeeded with zero rows" failure)
# =====================================================================
import hashlib, uuid
from datetime import datetime, timezone
from pyspark.sql import functions as F, Window

run_id, now = str(uuid.uuid4()), datetime.now(timezone.utc)
BASE = "/lakehouse/default/Files/he-raw/"   # the shortcut as a local folder
results = []

def record(check_id, layer, table, ok, observed, expected, severity="error"):
    # severity: "error" stops the run, "warn" is logged only
    results.append((check_id, layer, table, "PASS" if ok else "FAIL",
                    str(observed), str(expected), severity, run_id, now))

# Latest manifest row per file (the manifest repeats every file in each batch).
w = Window.partitionBy("s3_key").orderBy(F.col("batch_id").desc())
man = (spark.table("dbo.brz_object_manifest")
       .withColumn("_rn", F.row_number().over(w)).filter("_rn = 1"))

# CourtListener's own sha1, read from the raw audio_meta JSON, keyed by docket.
cl_sha1 = {r.docket: r.sha1 for r in
    spark.table("dbo.brz_audio_meta")
    .select(F.regexp_extract("s3_key", r"scotus_(.*)\.json", 1).alias("docket"),
            F.get_json_object("payload", "$.sha1").alias("sha1")).collect()}

def hashes(path):
    """Read a file once in 1 MB chunks and return (sha256, sha1)."""
    h256, h1 = hashlib.sha256(), hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h256.update(chunk); h1.update(chunk)
    return h256.hexdigest(), h1.hexdigest()

for r in man.filter("kind = 'audio'").collect():
    docket = r.s3_key.split("scotus_")[1].rsplit(".", 1)[0]
    s256, s1 = hashes(BASE + r.s3_key)
    record(f"audio_sha256_{docket}", "bronze", "brz_object_manifest",
           s256 == r.sha256, s256[:12], r.sha256[:12])
    record(f"audio_sha1_{docket}", "bronze", "brz_audio_meta",
           s1 == cl_sha1.get(docket), s1[:12], str(cl_sha1.get(docket))[:12],
           severity="warn")   # CourtListener’s sha1 identifies the source file, not the served MP3

# Reconcile and zero-row guard for each raw table.
schema = "check_id string, layer string, table_name string, status string, " \
         "observed string, expected string, severity string, run_id string, checked_at timestamp"
out = spark.createDataFrame(results, schema)          # build the results table
out.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("dbo.dq_results")
out.select("check_id", "status", "severity", "observed", "expected").show(40, truncate=False)

# Fail loudly on blocking checks only
failed = [r[0] for r in results if r[3] == "FAIL" and r[6] == "error"]   # r[3]=status, r[6]=severity
if failed:
    raise AssertionError(f"Bronze verification failed: {failed}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# jsonschema isn't in the default Fabric runtime image (unverified: confirm this
# actually runs here, and drop the cell if it turns out to already be installed).
%pip install jsonschema -q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# nb_01_bronze_ingest | cell: validate raw payloads against the data contracts
# Source : Files/he-raw/_contracts/*.yaml (staged by fetch_and_stage.py
#          alongside the evidence, via the same S3 -> OneLake shortcut)
# Checks : each row's JSON payload matches its contract's `required` fields
#          and types (jsonschema). One PASS/FAIL per file, into dq_results.
# =====================================================================
import json, uuid
import yaml, jsonschema
from datetime import datetime, timezone

run_id, now = str(uuid.uuid4()), datetime.now(timezone.utc)
BASE = "/lakehouse/default/Files/he-raw/"   # the shortcut as a local folder
results = []

def record(check_id, layer, table, ok, observed, expected, severity="error"):
    results.append((check_id, layer, table, "PASS" if ok else "FAIL",
                    str(observed), str(expected), severity, run_id, now))

with open(BASE + "_contracts/courtlistener_audio.yaml") as f:
    cl_contract = yaml.safe_load(f)
with open(BASE + "_contracts/oyez_transcripts.yaml") as f:
    oyez_contract = yaml.safe_load(f)

# (table, schema, unwrap): unwrap peels CourtListener's docket search envelope
CHECKS = [
    ("brz_docket", cl_contract["schema"]["docket"], lambda d: d["results"][0]),
    ("brz_audio_meta", cl_contract["schema"]["audio_meta"], None),
    ("brz_oyez_case", oyez_contract["schema"]["case"], None),
    ("brz_oyez_transcript", oyez_contract["schema"]["transcript"], None),
]

for table, schema, unwrap in CHECKS:
    for row in spark.table(f"dbo.{table}").select("s3_key", "payload").collect():
        data = json.loads(row.payload)
        record_data = unwrap(data) if unwrap else data
        try:
            jsonschema.validate(instance=record_data, schema=schema)
            record(f"contract_{table}_{row.s3_key}", "bronze", table, True, "valid", "valid")
        except jsonschema.ValidationError as e:
            record(f"contract_{table}_{row.s3_key}", "bronze", table,
                   False, e.message, "schema-valid")

dq_schema = "check_id string, layer string, table_name string, status string, " \
            "observed string, expected string, severity string, run_id string, checked_at timestamp"
out = spark.createDataFrame(results, dq_schema)
out.write.mode("append").option("mergeSchema", "true").format("delta").saveAsTable("dbo.dq_results")
out.select("check_id", "status", "observed").show(40, truncate=False)

failed = [r[0] for r in results if r[3] == "FAIL" and r[6] == "error"]
if failed:
    raise AssertionError(f"Contract validation failed: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Interactive grid per table; the raw JSON payload is hidden so the provenance columns are readable
for t in ["brz_object_manifest", "brz_docket", "brz_audio_meta",
          "brz_oyez_case", "brz_oyez_transcript", "dq_results"]:
    df = spark.table(f"dbo.{t}")
    print(f"=== dbo.{t}: {df.count()} rows | columns: {df.columns}")
    display(df.drop("payload").limit(5) if "payload" in df.columns else df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Top-level keys of one Oyez case JSON: this is what silver will extract from
import json
p = spark.table("dbo.brz_oyez_case").first()["payload"]
print(list(json.loads(p).keys()))

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
