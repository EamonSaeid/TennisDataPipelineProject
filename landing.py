import requests
import logging
import uuid
import json
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



# Get modified time and urls for all the different files
r=requests.get('https://stats.tennismylife.org/api/data-files', timeout=10)
latest_2026_file_from_source=next(i for i in r.json()['files'] if "2026" in i['name'])
last_modified=latest_2026_file_from_source['mtime']
csv_url=latest_2026_file_from_source['url']

# Variables needed in ingestion
ingest_datetime=datetime.now()
ingest_date=ingest_datetime.strftime("%Y-%m-%d")
batch_id=str(uuid.uuid4())
data_path=f"raw/tml/matches/ingest_date={ingest_date}/batch_id={batch_id}/2026.csv"


# First checking GCP to see when the last download was done/if there is a latest mtime
blobs=bucket.list_blobs(prefix='raw/tml/matches')
manifest_blobs=[i for i in blobs if i.name.endswith("_manifest.json")]
if not manifest_blobs:
    logger.info("No previous manifest found - First run")
    last_seen_mtime=None
else:
    latest=max(manifest_blobs, key=lambda x: x.time_created)
    last_seen_mtime=json.loads(latest.download_as_text())['source_version']




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
        "extraction_window":{'from':ingest_date, 'to':ingest_date},
        "row_count":csv_row_count,
        'source_version':last_modified,
        'ingested_at':ingest_datetime.isoformat()
    }

    # Uploading the csv onto the GCP bucket
    manifest_path=f"raw/tml/matches/ingest_date={ingest_date}/batch_id={batch_id}/_manifest.json"
    manifest_blob=bucket.blob(manifest_path)
    manifest_blob.upload_from_string(json.dumps(manifest_dict, indent=2), content_type='application/json')

    csv_blob=bucket.blob(data_path)
    csv_blob.upload_from_string(csv_bytes, content_type="text/csv")
