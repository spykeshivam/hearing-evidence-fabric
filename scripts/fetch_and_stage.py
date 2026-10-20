"""Stage SCOTUS (Supreme Court of the United States) hearing evidence for the pipeline.

For each docket in HEARINGS, this script:
  1. Looks up the docket on CourtListener and resolves its linked oral-argument audio.
  2. Downloads that audio's metadata JSON, then the MP3 itself from CourtListener's storage CDN.
  3. Fetches the matching Oyez case JSON and any transcripts tied to its oral arguments.
  4. Hashes and records provenance (source URL, retrieval time, sha256, licence) for every
     file written to data/stage/, per the contracts in contracts/*.yaml.
  5. Syncs the stage directory to S3 (unless --no-upload), then writes a per-batch
     manifest (objects.csv) recording each file's real upload_status ("uploaded",
     "size_mismatch", or "missing"), checked against S3 itself rather than trusted
     from the sync command's exit code.

Repeated runs are cheap: anything already present under data/stage/ is skipped (see
save_json/download caching), so only new files are fetched and appended to the manifest.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

from hearing_evidence.hashing import sha256_of
from hearing_evidence.s3_verify import verify_objects

# "Staging area" is a data-engineering term: a holding folder where files wait,
# already checked and fingerprinted, before they're loaded for real (here, before
# they're synced to S3).
STAGE = Path("data/stage")
CL_API = "https://www.courtlistener.com/api/rest/v4"
CL_STORAGE = "https://storage.courtlistener.com/"
OYEZ_API = "https://api.oyez.org"
# Identifies us honestly to the APIs we call (both run by non-profits) instead of
# posing as a browser. If something we do causes trouble, this lets them reach out
# and ask us to fix it rather than just banning the traffic.
UA = "hearing-evidence-portfolio/0.1 (research project; github.com/spykeshivam)"
CL_MIN_GAP = 15  # seconds between CourtListener API calls (limit is 5 per minute)
LICENCE = {"courtlistener": "public-domain-court-records", "oyez": "CC-BY-NC-4.0"}

HEARINGS = [
    {"term": 2024, "docket": "23-970", "audio_id": 95225},  # NVIDIA
    {"term": 2024, "docket": "23-980", "audio_id": None},  # Facebook
    {"term": 2022, "docket": "21-1333", "audio_id": None},  # Gonzalez v. Google
    {"term": 2022, "docket": "21-1496", "audio_id": None},  # Twitter v. Taamneh
]

# The provenance ledger: one entry per fetched file, recording where it came from
# (url), when it was fetched (retrieved_at), and which source/kind it is. See
# remember() for how entries get added.
PROV_FILE = STAGE / "_provenance.json"
prov = json.loads(PROV_FILE.read_text(encoding="utf-8")) if PROV_FILE.exists() else {}
_last_cl_call = 0.0


def now():
    """Return the current UTC time as an ISO-8601 string, truncated to whole seconds."""
    return datetime.now(UTC).isoformat(timespec="seconds")


def request(url, headers=None):
    """Open url with our User-Agent plus any extra headers.

    Returns the response context manager.
    """
    req = urllib.request.Request(url, headers={"User-Agent": UA, **(headers or {})})
    return urllib.request.urlopen(req, timeout=120)


def remember(key, url, source, kind):
    """Record provenance for a staged file and persist the ledger immediately."""
    prov[key] = {"url": url, "retrieved_at": now(), "source": source, "kind": kind}
    PROV_FILE.write_text(json.dumps(prov, indent=2), encoding="utf-8")


def save_json(key, url, source, kind, headers=None, cl=False):
    """Fetch a JSON endpoint into data/stage/<key>, or return the cached copy if it exists.

    cl=True paces requests to respect CourtListener's rate limit (CL_MIN_GAP between
    calls) and retries once on HTTP 429 using the server's Retry-After header.
    """
    global _last_cl_call
    path = STAGE / key
    if path.exists():
        print(f"  cached   {key}")
        return json.loads(path.read_text(encoding="utf-8"))
    if cl:
        # Wait out whatever's left of the minimum gap since the last CourtListener call.
        wait = CL_MIN_GAP - (time.time() - _last_cl_call)
        if wait > 0:
            time.sleep(wait)
    for attempt in (1, 2):
        try:
            with request(url, headers) as r:
                data = r.read()
            break
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt == 1:
                # Rate limited: sleep for the time the server asks for, then retry once.
                wait = int(e.headers.get("Retry-After", "60")) + 2
                print(f"  rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {e.code} for {url}") from e
    if cl:
        _last_cl_call = time.time()
    else:
        time.sleep(1)  # be polite to non-CourtListener sources too (Oyez has no documented limit)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    remember(key, url, source, kind)
    print(f"  fetched  {key}")
    return json.loads(data)


def download(key, url, source, kind):
    """Stream a binary file (e.g. an MP3) into data/stage/<key>, skipping it if already cached.

    Writes to a .part file first and renames on completion so a crash mid-download
    never leaves a truncated file at the final path.
    """
    path = STAGE / key
    if path.exists():
        print(f"  cached   {key}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".part")
    with request(url) as r, open(tmp, "wb") as f:
        while chunk := r.read(1 << 20):
            f.write(chunk)
    tmp.replace(path)
    remember(key, url, source, kind)
    print(f"  fetched  {key} ({path.stat().st_size / 1e6:.1f} MB)")


def main():
    """Fetch every hearing in HEARINGS, write a batch manifest, and sync to S3.

    Run with --no-upload to stage and build the manifest locally without
    touching S3 (also skips the HE_BUCKET requirement).
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-upload", action="store_true")
    args = ap.parse_args()

    # Credentials/config come from the environment, not CLI args, so they never
    # end up in shell history or a committed script.
    token, bucket = os.environ.get("CL_TOKEN"), os.environ.get("HE_BUCKET")
    if not token:
        sys.exit('Set the token: $env:CL_TOKEN = Read-Host "Paste CourtListener token"')
    if not bucket and not args.no_upload:
        sys.exit('Set the bucket: $env:HE_BUCKET = "your-bucket-name"')
    auth = {"Authorization": f"Token {token}"}
    STAGE.mkdir(parents=True, exist_ok=True)

    for h in HEARINGS:
        term, dn = h["term"], h["docket"]
        print(f"\n== {dn} (term {term})")

        try:
            # Step 1: look up the docket on CourtListener by docket number.
            # court=scotus restricts the lookup to the Supreme Court of the United States.
            res = save_json(
                f"courtlistener/dockets/scotus_{dn}.json",
                f"{CL_API}/dockets/?court=scotus&docket_number={dn}",
                "courtlistener",
                "docket",
                auth,
                cl=True,
            )
            if not res.get("results"):
                print("  no CourtListener docket found, skipping")
                continue

            # Step 2: resolve the audio reference from the docket's own audio_files field.
            audio_ref = None
            files = res["results"][0].get("audio_files") or []
            if files:
                first = files[0]
                audio_ref = first if isinstance(first, str) else first.get("resource_uri")
            if not audio_ref:
                # TODO: fallback (e.g. h["audio_id"]) not implemented yet
                print("  no audio linked to this docket, skipping")
                continue

            # Step 3: fetch the audio's metadata, then the MP3 itself if one is hosted.
            a = save_json(
                f"courtlistener/audio_meta/scotus_{dn}.json",
                audio_ref,
                "courtlistener",
                "audio_meta",
                auth,
                cl=True,
            )
            if a.get("local_path_mp3"):
                download(
                    f"courtlistener/audio/scotus_{dn}.mp3",
                    CL_STORAGE + a["local_path_mp3"],
                    "courtlistener",
                    "audio",
                )

            # Step 4: fetch the matching Oyez case and every transcript tied to its oral
            # arguments (used as the evaluation reference, per contracts/oyez_transcripts.yaml).
            case = save_json(
                f"oyez/cases/{term}_{dn}.json", f"{OYEZ_API}/cases/{term}/{dn}", "oyez", "case"
            )
            for entry in case.get("oral_argument_audio") or []:
                save_json(
                    f"oyez/transcripts/{term}_{dn}_{entry['id']}.json",
                    entry["href"],
                    "oyez",
                    "transcript",
                )
        except (RuntimeError, urllib.error.URLError, OSError) as e:
            # One hearing's bad response (e.g. a non-429 HTTP error, or a dropped
            # connection mid-download) shouldn't throw away work already staged
            # for the other hearings in this batch.
            print(f"  error fetching {dn}: {e}, skipping")
            continue

    # Build a manifest of every file currently on disk (not just ones fetched this
    # run), so reruns that only add new hearings still produce a complete listing.
    batch = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    mdir = STAGE / "_manifests" / f"batch={batch}"
    mdir.mkdir(parents=True, exist_ok=True)
    rows = []
    for key, p in sorted(prov.items()):
        path = STAGE / key
        if path.exists():
            rows.append(
                {
                    "batch_id": batch,
                    "s3_key": key,
                    "source": p["source"],
                    "kind": p["kind"],
                    "source_url": p["url"],
                    "retrieved_at": p["retrieved_at"],
                    "bytes": path.stat().st_size,
                    "sha256": sha256_of(path),
                    "licence_id": LICENCE[p["source"]],
                }
            )
    if not rows:
        sys.exit("Nothing was fetched.")

    if args.no_upload:
        for row in rows:
            row["upload_status"] = "not_attempted"
        write_manifest(mdir, rows)
        print(f"\nManifest written: {len(rows)} objects (--no-upload, nothing sent to S3)")
        return

    # Mirror the stage directory to S3; _provenance.json is local bookkeeping only.
    # check=False: a partial sync (e.g. one file timing out) must still leave a
    # manifest that's honest about what made it, rather than crashing before that
    # manifest is ever written.
    sync_result = subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            str(STAGE),
            f"s3://{bucket}/",
            "--profile",
            "he-uploader",
            "--exclude",
            "_provenance.json",
        ],
        check=False,
    )

    # Ask S3 itself what's actually there, rather than trusting sync's exit code,
    # so the manifest records ground truth even if sync reported success.
    statuses = verify_objects(bucket, "he-uploader", "eu-north-1", rows)
    for row in rows:
        row["upload_status"] = statuses.get(row["s3_key"], "missing")
    write_manifest(mdir, rows)

    # Push just the manifest (small, fast) so the copy in S3 reflects this run's
    # real outcome, even when the bulk sync above only partly succeeded.
    subprocess.run(
        [
            "aws",
            "s3",
            "cp",
            str(mdir / "objects.csv"),
            f"s3://{bucket}/_manifests/batch={batch}/objects.csv",
            "--profile",
            "he-uploader",
        ],
        check=False,
    )

    failed = [row["s3_key"] for row in rows if row["upload_status"] != "uploaded"]
    print(f"\nManifest written: {len(rows) - len(failed)}/{len(rows)} objects uploaded")
    if failed or sync_result.returncode != 0:
        sys.exit(f"Upload incomplete, not confirmed in S3: {failed}")


def write_manifest(mdir, rows):
    """Write rows to <mdir>/objects.csv, overwriting any manifest already there."""
    with open(mdir / "objects.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()
