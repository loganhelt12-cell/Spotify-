
# A simple worker that polls the ingest queue directory and processes jobs.
# In production, you'd use Kafka/Redpanda + proper queues.
import os, time, json, subprocess, shutil
from pathlib import Path
import boto3

MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
BUCKET = os.getenv('S3_BUCKET', 'spotify-mvp')

s3 = boto3.client('s3',
                  endpoint_url=MINIO_ENDPOINT,
                  aws_access_key_id=MINIO_ACCESS_KEY,
                  aws_secret_access_key=MINIO_SECRET_KEY,
                  region_name='us-east-1')

def process_ingest(ingest_id):
    print('Processing', ingest_id)
    rowfile = Path(f'/tmp/ingest_rows/{ingest_id}.json')
    if not rowfile.exists():
        print('ingest row missing')
        return
    row = json.loads(rowfile.read_text())
    key = row['s3_path']
    local_dir = Path(f'/tmp/processing/{ingest_id}')
    local_dir.mkdir(parents=True, exist_ok=True)
    local_file = local_dir / Path(key).name
    # download
    s3.download_file(BUCKET, key, str(local_file))
    # probe duration via ffprobe
    try:
        out = subprocess.check_output(['ffprobe', '-v', 'error', '-show_entries',
                                       'format=duration', '-of',
                                       'default=noprint_wrappers=1:nokey=1', str(local_file)])
        duration = float(out.decode().strip())
    except Exception as e:
        duration = None
    # compute chromaprint (fpcalc) if available
    acoustid = None
    try:
        out = subprocess.check_output(['fpcalc', '-json', str(local_file)], stderr=subprocess.DEVNULL)
        acoustid = json.loads(out.decode()).get('fingerprint')
    except Exception as e:
        acoustid = None
    # For demo: create a transcoded 64kbps mp3 and upload as processed master
    processed_key = key.replace('/original/', '/processed/')
    transcode_file = local_dir / ('transcoded_64.mp3')
    subprocess.call(['ffmpeg', '-y', '-i', str(local_file), '-b:a', '64k', str(transcode_file)])
    s3.upload_file(str(transcode_file), BUCKET, processed_key)
    # update ingest row with processed info
    row['status'] = 'processed'
    row['processed_at'] = time.time()
    row['processed_key'] = processed_key
    rowfile.write_text(json.dumps(row))
    print('Done', ingest_id)

def poll_loop():
    qdir = Path('/tmp/ingest_queue')
    qdir.mkdir(parents=True, exist_ok=True)
    while True:
        items = list(qdir.iterdir())
        if not items:
            time.sleep(2)
            continue
        for it in items:
            ingest_id = it.name
            try:
                process_ingest(ingest_id)
            except Exception as e:
                print('Error processing', ingest_id, e)
            # remove queue marker
            try:
                it.unlink()
            except:
                pass

if __name__ == '__main__':
    poll_loop()
