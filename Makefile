help:
	@echo "Usage: make [help] [create] [shutdown] [reboot] [delete]\n"
	@echo "help           Print this help message"
	@echo "create         Create a dataproc on GKE cluster"
	@echo "shutdown       Delete dataproc cluster and resize GKE cluster to 0"
	@echo "reboot         Resize back the dataproc cluster and GKE cluster"
	@echo "delelte        Delete the GKE cluster completely"


create:
	gcloud container clusters create gke \
        --region asia-northeast3-a \
        --num-nodes 3 \
        --image-type UBUNTU \
        --machine-type e2-standard-2
	gcloud beta dataproc clusters create dataproc \
        --gke-cluster=gke \
        --region=asia-northeast3 \
        --image-version=1.4.27-beta \
        --bucket=matchy-bucket 

reboot:
	gcloud container clusters resize gke --num-nodes 1 \
        --region asia-northeast3 

	gcloud beta dataproc clusters create dataproc \
        --gke-cluster=gke \
        --region=asia-northeast3 \
        --image-version=1.4.27-beta \
        --bucket=matchy-bucket 

shutdown:
	gcloud beta dataproc clusters delete dataproc --region asia-northeast3 
	gcloud container clusters resize gke --num-nodes 0 --region asia-northeast3

delete:
	gcloud dataproc clusters delete dataproc --region asia-northeast3 
	gcloud container clusters resize gke --num-nodes 0 --region asia-northeast3

ls:
	gsutil ls gs://matchy-bucket/inputs/

file=input1
submit_spark:
	gcloud dataproc jobs submit pyspark \
        ./wordlettercount/spark-impl/sparkWordCount.py --cluster=dataproc \
        -- gs://matchy-bucket/inputs/$(file)