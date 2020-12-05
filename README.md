# M1522.006300 Distributed Systems

This is the course project folder of [M1522.006300 Distributed Systems](http://dcslab.snu.ac.kr/courses/ds2020f/) of Group 17.

## Project Description

The goal of this project is to deploy and manage a prototype cloud cluster running batch processing `WordLetterCount` applications. There are two `WordLetterCount` applications implemented in different ways: one used the `Spark` API, the other used `WordCount` API and a self-designed resource scheduler.

## Developer Tutorials

Refer to the `docs` folder for useful guides.
``
The project specification is specified in [Specification.md](docs/Specification.md).

Refer to [GCP guide](/docs/GCP_guide.md) for a detailed tutorial on how to configure, access and use your GCP clusters.

Our project ID is `peaceful-fact-294309`, you can use the web-based dashboard [GCP Console](https://console.cloud.google.com/) to view our cluster, VMs and Pods.

## To-Dos

- [x] Deploy Google Dataproc on GKE (ref: [Dataproc on Google Kubernetes Engine](https://cloud.google.com/dataproc/docs/concepts/jobs/dataproc-gke))
- [x] Install `WordCount` locally to test
- [ ] Test `WordCount` on GKE
    - [x] Deploy `Hadoop` on GKE
    - [ ] Tweak `Hadoop` deployment, integration with GCS
