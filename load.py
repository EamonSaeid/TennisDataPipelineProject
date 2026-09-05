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

# Table names
table_id='tennisdataengproject.tml_raw.matches'
staging_table_id='tennisdataengproject.tml_raw._staging_matches'



# If the output table doesn't exist yet, build it! Columns not defined as the data will guide which column exists
initial_schema=[
    bigquery.SchemaField("_batch_id", "STRING"),
    bigquery.SchemaField("_ingested_at", "TIMESTAMP"),
]
table=bigquery.Table(table_id, schema=initial_schema)
bq_client.create_table(table, exists_ok=True)





# Editing this to work across many files - First step is to see what batches exist in our staging table and compare this to what is in GCS
loaded_batches_sql="""
SELECT _batch_id FROM `tennisdataengproject.tml_raw.matches` GROUP BY 1
"""
loaded_batches_job=bq_client.query(loaded_batches_sql)
loaded_batch_ids={row['_batch_id'] for row in loaded_batches_job.result()}


# Check landed batches vs the matches table to see what needs loading
blobs=bucket.list_blobs(prefix='raw/tml/matches')
blob_records=[
    {'blob':i} | dict(g.split("=",1) for g in i.name.split("/") if "=" in g) 
    for i in blobs 
    ]
manifest_blobs=[i for i in blob_records if i['blob'].name.endswith("_manifest.json")]


# Temp running this for 1967
pending_records=[i for i in manifest_blobs if i['batch_id'] not in loaded_batch_ids]

if not pending_records:
    logger.info("No new batches to load")
    sys.exit(0)



for record in pending_records:
    # Extract batch info for checking the load worked, and batch id to enforce idempotency
    latest_manifest_dict=json.loads(record['blob'].download_as_text())
    batch_id=latest_manifest_dict['batch_id']
    ingested_at=latest_manifest_dict['ingested_at']
    expected_row_count=latest_manifest_dict['row_count']

    # Load the csv into a staging table 
    csv_path=f"{record['blob'].name.rsplit('/',1)[0]}/{record['file']}"
    gcs_uri=f"gs://tennisdataengproject-landing/{csv_path}"


    load_job_config= bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition. WRITE_TRUNCATE,
    )

    load_job=bq_client.load_table_from_uri(gcs_uri, staging_table_id, job_config=load_job_config)
    load_job.result()

    raw_table=bq_client.get_table(table_id)
    staging_table=bq_client.get_table(staging_table_id)

    raw_columns={i.name for i in raw_table.schema}
    staging_columns={i.name for i in staging_table.schema}

    new_columns=staging_columns-raw_columns

    if new_columns:
        logger.info(f"New columns from {record['file']}, extending raw schema to include: {new_columns}")
        raw_table.schema=raw_table.schema+[bigquery.SchemaField(i, "STRING") for i in new_columns]
        bq_client.update_table(raw_table, ['schema'])
        raw_columns|=new_columns

    target_columns=sorted(raw_columns-{'_batch_id','_ingested_at'})
    select_column_list=", \n".join(f"CAST({c} as STRING) as {c}" if c in staging_columns else f"CAST(NULL as STRING) as {c}" for c in target_columns )
    insert_columns=", ".join(target_columns + ['_batch_id','_ingested_at'])
    #  Add the _ingested_at and _batch_id and load that into the raw table
    insert_sql=f"""
    BEGIN TRANSACTION;

    DELETE FROM `tennisdataengproject.tml_raw.matches`
    WHERE _batch_id = @batch_id;

    INSERT INTO `tennisdataengproject.tml_raw.matches` ({insert_columns})
    SELECT {select_column_list}, @batch_id as _batch_id, @ingested_at as _ingested_at
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
    WHERE 
        _batch_id=@batch_id



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
