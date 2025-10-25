
# Ingest worker focused on fingerprinting and MusicBrainz lookup (simplified)
import os, time, json, subprocess
from pathlib import Path
import requests, boto3

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BUCKET = os.getenv('S3_BUCKET', 'spotify-mvp')
MB_API = 'https://musicbrainz.org/ws/2'

s3 = boto3.client('s3',
                  endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=MINIO_ACCESS_KEY,
                  aws_secret_access_key=MINIO_SECRET_KEY,
                  region_name='us-east-1')

def try_acoustid_lookup(local_path):
    # This demo uses fpcalc output; real production should call AcoustID web API with a key.
    try:
        out = subprocess.check_output(['fpcalc', '-json', str(local_path)], stderr=subprocess.DEVNULL)
        data = json.loads(out.decode())
        # fpcalc returns 'fingerprint' and 'duration' keys
        return data.get('fingerprint'), data.get('duration')
    except Exception as e:
        return None, None

def poll_and_enrich():
    qdir = Path('/tmp/ingest_queue')
    qdir.mkdir(parents=True, exist_ok=True)
    while True:
        items = list(qdir.iterdir())
        if not items:
            time.sleep(3)
            continue
        for it in items:
            ingest_id = it.name
            rowfile = Path(f'/tmp/ingest_rows/{ingest_id}.json')
            if not rowfile.exists():
                it.unlink()
                continue
            row = json.loads(rowfile.read_text())
            key = row['s3_path']
            local_dir = Path(f'/tmp/enrich/{ingest_id}')
            local_dir.mkdir(parents=True, exist_ok=True)
            local_file = local_dir / Path(key).name
            s3.download_file(BUCKET, key, str(local_file))
            fingerprint, duration = try_acoustid_lookup(local_file)
            if fingerprint:
                row['acoustid'] = fingerprint[:256]  # truncate in demo
            row['duration'] = duration
            # naive MusicBrainz search by recording search (title) if uploader provided metadata (omitted here)
            rowfile.write_text(json.dumps(row))
            # mark processed in this worker and leave for other workers
            it.unlink()
        time.sleep(1)

if __name__ == '__main__':
    poll_and_enrich()
