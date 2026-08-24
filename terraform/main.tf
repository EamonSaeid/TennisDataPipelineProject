



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
    region="europe-west2"
    # THe bucket won't use this - The UK apparently isnt elegible for free storage
}

resource "google_storage_bucket" "landing" {
    name = "tennisdataengproject-landing"
    location="us-west1"
    force_destroy=false
    uniform_bucket_level_access=true
}



