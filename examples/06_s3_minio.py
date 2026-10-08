# S3 storage: the bucket is the source of truth; drop the local index, reopen cold, recall; then crypto-shred.
# Run: python examples/06_s3_minio.py   (needs: pip install "memd-engine[s3]" and an S3 API; MinIO: docker run -p 9000:9000 minio/minio server /data)
import os
import shutil
import tempfile
import uuid

import boto3

from memd import Memory
from memd.storage.crypto import KeyCustodyError

ENDPOINT = os.environ.get("MEMD_S3_ENDPOINT", "http://127.0.0.1:9000")
# memd reads MEMD_S3_ACCESS_KEY / MEMD_S3_SECRET_KEY (else the boto3 chain);
# the MinIO defaults keep this runnable against a stock container
os.environ.setdefault("MEMD_S3_ACCESS_KEY", "minioadmin")
os.environ.setdefault("MEMD_S3_SECRET_KEY", "minioadmin")
BUCKET = os.environ.get("MEMD_EXAMPLE_BUCKET", "memd-examples")
PREFIX = f"demo-{uuid.uuid4().hex[:8]}"  # a fresh root per run

s3 = boto3.client("s3", endpoint_url=ENDPOINT, region_name="us-east-1",
                  aws_access_key_id=os.environ["MEMD_S3_ACCESS_KEY"],
                  aws_secret_access_key=os.environ["MEMD_S3_SECRET_KEY"])
if BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
    s3.create_bucket(Bucket=BUCKET)

url = f"s3://{BUCKET}/{PREFIX}"
local = tempfile.mkdtemp(prefix="memd-s3-local-")
# local_dir holds the derived SQLite index (a cache) and, with the default
# `local` key provider, the envelope keys - back those up with the bucket
config = {"s3_endpoint_url": ENDPOINT, "local_dir": local}

mem = Memory(url, config=config)
mem.add("We deploy with `make ship`, never from CI", user_id="u1", session_id="s1")
mem.remember("The staging database is postgres 16", user_id="u1", entity_keys=["infra.staging_db"])
mem.close()
objects = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX)["Contents"]]
print(f"{len(objects)} objects under {url}, e.g.", objects[:3])

# a node without the keys refuses the store instead of reading it as empty
try:
    Memory(url, config={**config, "local_dir": tempfile.mkdtemp(prefix="memd-s3-nokeys-")})
    raise SystemExit("expected KeyCustodyError")
except KeyCustodyError as e:
    print("node without the keys:", type(e).__name__)

# drop the local index entirely: a cold open rebuilds it from the bucket
shutil.rmtree(os.path.join(local, "_cache"))
mem = Memory(url, config=config)
hits = mem.search("how do we deploy?", user_id="u1")
print("cold recall:", hits.items[0].content)
assert "make ship" in hits.items[0].content

# crypto-shred: the namespace's data key is destroyed and its objects deleted
assert mem.destroy_namespace()
mem.close()
left = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", [])]
print("after destroy_namespace:", left)
assert not any("/wal" in k or "/audit" in k for k in left)

# tidy up this run's root (what is left: the lease object and the manifest)
for key in left:
    s3.delete_object(Bucket=BUCKET, Key=key)
shutil.rmtree(local, ignore_errors=True)
