import json
from google.cloud import storage
from google.cloud import bigquery
import logging
from datetime import datetime
import sys



# Logging initialize
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger=logging.getLogger(__name__)


# Bucket initialise
client=storage.Client()
bucket=client.bucket('tennisdataengproject-landing')

# Bigquery initialize
bq_client = bigquery.Client()

# Set up schema for the load
COLUMNS = [
    "tourney_id", "tourney_name", "surface", "draw_size", "tourney_level", "indoor",
    "tourney_date", "match_num", "winner_id", "winner_seed", "winner_entry", "winner_name",
    "winner_hand", "winner_ht", "winner_ioc", "winner_age", "winner_rank", "winner_rank_points",
    "loser_id", "loser_seed", "loser_entry", "loser_name", "loser_hand", "loser_ht",
    "loser_ioc", "loser_age", "loser_rank", "loser_rank_points", "score", "best_of", "round",
    "minutes", "w_ace", "w_df", "w_svpt", "w_1stIn", "w_1stWon", "w_2ndWon", "w_SvGms",
    "w_bpSaved", "w_bpFaced", "l_ace", "l_df", "l_svpt", "l_1stIn", "l_1stWon", "l_2ndWon",
    "l_SvGms", "l_bpSaved", "l_bpFaced",
]

schema = [bigquery.SchemaField(col, "STRING") for col in COLUMNS]
final_schema=schema+[
    bigquery.SchemaField("_batch_id", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]

# If the output table doesn't exist yet, build it!
table_id='tennisdataengproject.tml_raw.matches'
table=bigquery.Table(table_id, schema=final_schema)
bq_client.create_table(table, exists_ok=True)


# find the latest batch/manifest time
blobs=bucket.list_blobs(prefix='raw/tml/matches')
manifest_blobs=[i for i in blobs if i.name.endswith("_manifest.json")]
if not manifest_blobs:
    logger.info("No manifests found in the bucket yet - Nothing to load")
    sys.exit(0)

latest_manifest_blob=max(manifest_blobs, key=lambda x: x.time_created)

# Extract batch info for checking the load worked, and batch id to enforce idempotency
latest_manifest_dict=json.loads(latest_manifest_blob.download_as_text())
batch_id=latest_manifest_dict['batch_id']
ingested_at=latest_manifest_dict['ingested_at']
expected_row_count=latest_manifest_dict['row_count']




# Load the csv into a staging table 
csv_path=latest_manifest_blob.name.rsplit('/',1)[0]+'/2026.csv'
gcs_uri=f"gs://tennisdataengproject-landing/{csv_path}"
staging_table_id='tennisdataengproject.tml_raw._staging_matches'


load_job_config= bigquery.LoadJobConfig(
    schema=schema,
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

load_job=bq_client.load_table_from_uri(gcs_uri, staging_table_id, job_config=load_job_config)
load_job.result()



#  Add the _ingested_at and _batch_id and load that into the raw table
insert_sql="""
BEGIN TRANSACTION;

DELETE FROM `tennisdataengproject.tml_raw.matches`
WHERE _batch_id = @batch_id;

INSERT INTO `tennisdataengproject.tml_raw.matches`
SELECT *, @batch_id as _batch_id, @ingested_at as _ingested_at
FROM `tennisdataengproject.tml_raw._staging_matches`;

COMMIT TRANSACTION;
"""

insert_job_config=bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("batch_id","STRING",batch_id),
        bigquery.ScalarQueryParameter("ingested_at","TIMESTAMP",datetime.fromisoformat(ingested_at)),
    ]
)


insert_job=bq_client.query(insert_sql, job_config=insert_job_config)
insert_job.result()




# Check we have loaded the correct number of rows
verify_sql="""

SELECT COUNT(*) as row_count
FROM `tennisdataengproject.tml_raw.matches`
WHERE _batch_id=@batch_id


"""

verify_job_config=bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("batch_id","STRING",batch_id),
    ]
)
verify_result=bq_client.query(verify_sql, job_config=verify_job_config).result()
actual_row_count=list(verify_result)[0]['row_count']

if actual_row_count==expected_row_count:
    logger.info(f'Load verified: {actual_row_count} rows match manifest for batch {batch_id}')
else:
    logger.error(f'row count mismatch for {batch_id}: Rows expected {expected_row_count} vs actual rows: {actual_row_count}')
