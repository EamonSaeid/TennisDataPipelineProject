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

Defining new staging dataset in terraform main.tf rather than letting dbt make it - The project should stand on its own and terraform is basically the contract for provisioning resources. It's cleaner that all the instructions live there

int_matches__by_player - Does that go in staging or intermediate. Theretically its more of an itnermediate stage, but it violates the spec as defined earlier