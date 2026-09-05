**Draft — edit freely before posting**

---

Built an ATP tennis data warehouse from scratch to learn the data engineering stack I don't touch in my day job (Senior Insight Analyst). GCP, dbt, Terraform, Airflow next.

What's in it:

- Ingestion pipeline (GCS → BigQuery) with a JSON manifest per file — row count, batch ID, source version, timestamp. Lets me prove a load was correct and replay it if something's wrong, rather than just hoping.
- Idempotent loads: delete-then-insert scoped to a batch ID, so a re-run after a failure can't duplicate data.
- Automatic schema reconciliation — historical files don't all have the same columns (a 1967 file is missing one the rest have), so the pipeline detects and adapts instead of breaking on load.
- dbt models with tiered test severity: known, expected data gaps warn; genuine integrity violations (e.g. a match where the winner and loser are the same player) fail the build.

Repo: [link]

#dataengineering #dbt #gcp #bigquery
