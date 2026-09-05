Buidling a data pipeline to ingest tennis match data from the TML data source and a tennis match data API. 

Why?
* The TML data has a full history with some retrospective fixes
* The API will provide info on changes in real time
* Reconciling both means learning and applying data engineering concepts

Step 1: Getting the TML data ingested into GCP, and building some data models off the back of the raw data.

Step 2L Loading the data into a GCP table (all stringss)

Step 3: Modelling that data with GCP to have sensible types




Decision log:

Started this a bit late (At the start of dbt)

Defining new staging dataset in terraform main.tf rather than letting dbt make it
The project should stand on its own and terraform is basically the contract for provisioning resources. 
It's cleaner that all the instructions live there

int_matches__by_player - Does that go in staging or intermediate? 
Theretically its more of an intermediate stage, I believe it satisfies a part of "assemeble history" even though it only uses one source.
Also I will want to resolve things between the tml data source and the API, and then I would need to propagate that through all the tables that use matches. Makes sense then for the shape change to occur further downstream

int_matches_by_player - player ID's null in 5 rows of the 2026 dataset
Without a player ID, they can't be identified and used for calculating playercentric metrics, and there also can't be a key per row
Therefore, it makes sense to exclude these rows entirely

Adapting the script now to work across all the singles files instead of just 2026, this means that for each file that is loaded into staging, the file is loaded to the staging table and then ingested and and batch ID are added on, that is loaded to the matches table, and then it loops through repeatedely adding data and appending the additional columns. It's not an elegant solution as we're doing multiple read and write transactions, and theoretically bigquery exposes a pseudo column _file_name on external tables which could be used to link back to the info in the manifest. But it would likely be overkill given the majority of changes will be just for the most recent year and occasionally for the historical years as corrections are made

Loading all the files shows that 1967 file is missing a column that the rest of the data does have, "Indoor". The Spec calls for schema changes to not make the whole thing fall over, so this is a good opportunity to build around this point on the spec. CUrrently the columns are defined explicitly, but instead we can read the columns in the "matches" table, and we can then compare with the columns in the staging table. If there's extras we can add a column and null everything else, and when inserting we can refer to the column names themselves. In DBT, the arrival of a new column out of nowhere should lead to a warning, but we can fix the output columns as part of the model itself.
