# Getting Started With Using GCP and GKE

- [Getting Started With Using GCP and GKE](#getting-started-with-using-gcp-and-gke)
  - [Install Google Cloud SDK](#install-google-cloud-sdk)
  - [Go through the basics](#go-through-the-basics)
    - [Create a GKE cluster](#create-a-gke-cluster)
    - [Link with `kubectl` command](#link-with-kubectl-command)
    - [Describe the GKE cluster](#describe-the-gke-cluster)
    - [Resize GKE cluster configuration](#resize-gke-cluster-configuration)
    - [Delete the GKE cluster](#delete-the-gke-cluster)

## Install Google Cloud SDK

Just follow the [guide](https://cloud.google.com/sdk/docs/install#deb) Google provided.

1. Add the Cloud SDK distribution URI as a package source

   ```bash
   echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
   sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
   ```

2. Import the Google Cloud public key

   ```bash
   curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
   sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
   ```

3. Update and install the Cloud SDK

   ```bash
   sudo apt-get update && sudo apt-get install google-cloud-sdk
   ```

4. Install additional components. For example:

   ```bash
   sudo apt-get install google-cloud-sdk-app-engine-java
   ```

5. Run `gcloud init` to get started

   ```bash
   gcloud init
   ```

   `gcloud init` would initialize a configuration called `default`, in which you will specify the account you would like to use to connect to GCP and the project you would like to interact with.

## Go through the basics

- Creation of the cluster on GCP
- Description of the `Kubernetes` cluster by specifying the number of nodes, instance type, region, zone, etc.
- Review and edit of the `Kubernetes` cluster definition/configuration
- Browsing the cluster information, including status of all nodes
- Extraction of the credentials required for connecting to the dashboard
- Deletion of the cluster and its information

### Create a GKE cluster

We can use `gcloud container create NAME` to create a default cluster with name `NAME`. We can also specify more options to configure our cluster.

```bash
gcloud container create example-cluster \
    --zone asia-northeast3-a \
    --num-nodes 3 \
    --image-type UBUNTU \
    --machine-type e2-standard-2
```

The command above can create a cluster with the following properties:

- Is a zonal cluster in compute zone `asia-northeast-3-a` (Seoul)
- With 1 master node, 3 worker nodes
- Of `Ubuntu` image type (which is the OS that the worker nodes running on)
- Of `e2-standard-2` machine type
  - The spec of the machine can be inspected via `gcloud compute machine-types list`

   ```bash
   $ gcloud compute machine-types list | \
     sed -n -e '1p' -e '/asia-northeast3-a/p' | \
     sed -n -e '1p' -e '/e2-standard-2/p'
   NAME              ZONE                       CPUS  MEMORY_GB  DEPRECATED
   e2-standard-2     asia-northeast3-a          2     8.00
   ```

### Link with `kubectl` command

After creating your cluster, you need to get authentication credentials to interact with the cluster:

```bash
gcloud container clusters get-credentials cluster-name
```

This command configures `kubectl` to use the cluster you created

### Describe the GKE cluster

After creating a cluster called `example-cluster` on GCP, we can look at the specs and info of it via the following command:

```bash
gcloud container clusters describe example-cluster
```

### Resize GKE cluster configuration

We can resize an existing cluster by running `gcloud container clusters resize NAME --size`.

For example:

```bash
gcloud container clusters resize example-cluster --size 4
```

This command will resize the cluster `example-cluster` to have 4 worker nodes instead of 3.

Note that the pods running on the cluster will all be terminated.

### Delete the GKE cluster

Simply run the following command to delete a cluster with name `example-cluster`

```bash
gcloud container clusters delete example-cluster
```
