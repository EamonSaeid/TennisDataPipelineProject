import requests
import logging
import uuid
import json
import re
from datetime import datetime
from google.cloud import storage

# Logging initialize
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger=logging.getLogger(__name__)

# Bucket initialise
client=storage.Client()
bucket=client.bucket('tennisdataengproject-landing')


 
# Get modified time and urls for all the available files
r=requests.get('https://stats.tennismylife.org/api/data-files', timeout=10)

# Pulling only the singles matches from 1970 to now 
files=[i for i in r.json()['files'] if re.fullmatch(r'\d{4}\.csv',i['name'])]

# Variables needed in ingestion
ingest_datetime=datetime.now()
ingest_date=ingest_datetime.strftime("%Y-%m-%d")

# First checking GCP to see when the last download was done/if there is a latest mtime
blobs=bucket.list_blobs(prefix='raw/tml/matches')
manifest_blobs=[i for i in blobs if i.name.endswith("_manifest.json")]

for file in files:
    batch_id=str(uuid.uuid4())
    last_modified=file['mtime']
    csv_url=file['url']
    file_name=file['name']
    data_path=f"raw/tml/matches/ingest_date={ingest_date}/batch_id={batch_id}/file={file_name}/{file_name}"
    file_manifest_blobs=[i for i in manifest_blobs if i.name.endswith("_manifest.json") and f"file={file_name}" in i.name]

    if not file_manifest_blobs:
        logger.info(f"No previous manifest found for {file_name} - First run")
        last_seen_mtime=None
    else:
        latest=max(file_manifest_blobs, key=lambda x: x.time_created)
        # Learning point - When I expanded this code from pulling a single file, to then pulling all the files one after the other, I hit a failure
        # type called "time of check to time of use". I went into the console and delted some of the previous runs, so the blob had been edited wehn I came to it
        # Generally this error won't be enountered but getting the latest_live version protects againts this sort of error, so why not
        latest_live=bucket.blob(latest.name)
        last_seen_mtime=json.loads(latest_live.download_as_text())['source_version']


    if last_seen_mtime is None or last_modified>last_seen_mtime:

        # Downloading the CSV
        response=requests.get(csv_url, timeout=30)
        response.raise_for_status()
        csv_bytes=response.content

        # row_count
        csv_row_count=len(csv_bytes.decode("utf_8").splitlines())-1


        # constants
        source_name='tml'
        entity='matches'


        manifest_dict={
            "source": source_name,
            "entity": entity,
            "file_name":file_name,
            "extraction_window":{'from':ingest_date, 'to':ingest_date},
            "row_count":csv_row_count,
            'source_version':last_modified,
            'ingested_at':ingest_datetime.isoformat(),
            'batch_id':batch_id
        }

        # Uploading the csv onto the GCP bucket
        manifest_path=f"raw/tml/matches/ingest_date={ingest_date}/batch_id={batch_id}/file={file_name}/_manifest.json"
        manifest_blob=bucket.blob(manifest_path)
        manifest_blob.upload_from_string(json.dumps(manifest_dict, indent=2), content_type='application/json')

        csv_blob=bucket.blob(data_path)
        csv_blob.upload_from_string(csv_bytes, content_type="text/csv")
