



terraform {
    required_version=">=1.15.8"
    required_providers {
        google={
            source="hashicorp/google"
            version="~> 5.0"
        }
    }
}



provider "google" {
    project="tennisdataengproject"
    region="us-west1"
    # Because only 3 buckets are free, us-west1, us-central1, us-east1
    # So picking this project to run in the US reduces cost in exchange for reduced redundancy
    # WHich matters less here given that the data is public and reproducible
}


# Both resources are in the US - I've done this as the bucket storage isn't free on a UK region, it then follows the dataset needs to be US too
# Its not a problem for an analytics project like this - latency will be a few millseconds to kick load jobs off etc
# In the real world, the issues would be: 
# Data residency i.e. laws now allowing pii to go abroad, thankfully this is a public dataset
# Durability, EU is a multireigon location so I would get better redundancy if I went for that



resource "google_storage_bucket" "landing" {
    name = "tennisdataengproject-landing"
    location="us-west1"
    force_destroy=false
    uniform_bucket_level_access=true
}

resource "google_bigquery_dataset" "raw" {
    dataset_id="tml_raw"
    location="us-west1"
    description="Raw layers - All strings, append only, loaded from gcs landing zone"
}