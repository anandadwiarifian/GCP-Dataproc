#!/bin/bash

echo "Uploading file to GCS Bucket"
gsutil -m cp data/covid_country_data.json gs://arifbucket-1/covid_country_data.json
echo "Create a cluster"
gcloud dataproc clusters create \
    --num-workers=2 \
	--master-machine-type=n1-standard-2 \
	--worker-machine-type=n1-standard-2 \
	--master-boot-disk-size=20 \
	--worker-boot-disk-size 20 \
	--enable-component-gateway \
	--optional-components=ANACONDA,JUPYTER \
	--zone=us-east1-b \
	--region=us-east1 \
	--image-version=1.3-ubuntu18 \
	arif01-cluster
echo "Submit Sparkjob"
gcloud dataproc jobs submit pyspark sparkjob.py \
	--cluster=arif01-cluster \
	--region=us-central1 \
	--jars=gs://spark-lib/bigquery/spark-bigquery-latest.jar
echo "Deleting the cluster"
gcloud dataproc clusters delete arif01-cluster --region=us-central1