# Kafka Scale

**kafka-scale** is a Go language demo application that demonstrates parallelizing and distributing a compute task across several Pods in a Kubernetes cluster. The app reads and summarizes census data. Census data was chosen for this demo app because it is readily available, and there is a reasonable volume of it to test the scalability of the application and observe the impact of increasing the number of compute replicas. The goal of the project is to demonstrate a scalability pattern. So the actual data being consumed is less important. Presumably the pattern can be applied to any data set in which independent computation threads can run in parallel without needing to access to each other's data to do their work.

The app reads census data sets from the US Census site, and chunks the data into Kafka. Other instances of the program consume data from Kafka, perform a computation on it and put it back into Kafka. A third instance of the program reads the computed data from Kafka and summarizes the results in memory which can then be queried via a REST call:



![](resources/design.jpg)

The blue components are open source or publicly available. The green components are what this project provides.

One of the goals of the project was to simplify the components in green as much as possible. Therefore, all concurrency, scaling, deployment, synchronization, etc. are delegated to Kubernetes and Kafka.

<u>Diagram Narrative</u>:

1. A *Reader* pod reads census Gzips from the US Census site. The reader breaks the gzips into chunks and writes the chunks into a Kafka **compute** topic. If the topic does not exist when the app starts, the app creates the topic. The partitions and replication factors of the topic are configurable through command line params
2. Multiple *Computation* pods consume the **compute** topic as part of a Kafka consumer group. These pods perform a calculation on the message chunks, and write the data into a Kafka **results** topic. Again - if the results topic does not exist it is created automatically. Multiple replicas are supported. This is where the parallelization comes from. You can scale your Kafka cluster and this deployment up in parallel
3. A single *Results* pod reads from the Kafka **results** topic, summarizes the data, and holds the data in memory where it can be viewed via HTTP using curl or the browser. The project contains a manifest for a NodePort service that enables access to the results endpoint. Of course depending on the Kubernetes environment that runs this demo, access to the endpoint could be achieved in various different ways
4. Prometheus and Grafana are used to provide observability. A Grafana dashboard is included in the project for this purpose. It can be imported into the Grafana instance running in the cluster, and it has a *Prometheus* data source in it

### Environment

This demo assumes you have a Kubernetes cluster available to you. I use [desktop-kubernetes](https://github.com/aceeric/desktop-kubernetes) for local testing, which is a project I built that stands up a three-node Kubernetes dev cluster on your desktop using VirtualBox. Desktop Kubernetes is derivative of Kelsy Hightower's *Kubernetes the hard way*.

It also assumes you have an observability stack. I use [kube-prometheus](https://github.com/prometheus-operator/kube-prometheus). This project includes a Grafana dashboard JSON file that you can import into Grafana in the cluster to monitor the *kafka-scale* application as it runs.

Finally, this demo assumes you have Kafka running in cluster. I use [Strimzi](https://strimzi.io/) for this. In fact, in my development cluster I use the following to deploy my Strimzi Kafka cluster: [Simple Strimzi Workload](https://github.com/aceeric/desktop-kubernetes/tree/master/test-workloads/strimzi)

### The Code

The application code consists of a single Go package, and compiles to a single Go binary. The binary - `kafka-scale` - accepts a positional param (along with other configuration params) to control the role that it plays in the cluster. The following positional params are supported.

| Param     | Role                                                         |
| --------- | ------------------------------------------------------------ |
| read      | Reads from the census website, chunks the data, writes to the **compute** Kafka topic |
| compute   | Reads from the **compute** Kafka topic, performs some basic computation on the data, writes the computed result to the **results** Kafka topic |
| results   | Reads the **results** Kafka topic, summarizes to an in-memory data structure, and serves the data structure as JSON via a **/results** endpoint. E.g.: `curl --silent -H "Accept: application/json"  http://192.168.0.46:32099/results` |
| topiclist | Lists all the Kafka topics. Same as `kubectl get kafkatopics` if you're running Strimzi |
| offsets   | Lists the offsets for a Kafka topic - lets you see the lags for a topic |
| rmtopics  | Removes topics. If you're running Strimzi, then `kubectl delete kafkatopic <mytopic>` because otherwise Strimzi will see the topic removal as a reconciliation event, and re-create the topic for you |

The code makes use of the [kafka-go](https://github.com/segmentio/kafka-go) Kafka client library from [Segment](https://segment.com/).

### Observability

The application includes the Prometheus client libraries and exposes Prometheus metrics on a configurable port. The following metrics are exposed:

| Metric                                |                                                              |
| ------------------------------------- | ------------------------------------------------------------ |
| `kafka_scale_downloaded_gzips`        | The Count of census gzip files downloaded from the US Census website |
| `kafka_scale_chunks_written`          | The Count of Census data chunks written by the read command to the compute topic |
| `kafka_scale_compute_messages_read`   | The Count of messages read by the compute command from the compute topic |
| `kafka_scale_result_messages_written` | The Count of messages written by the compute command to the results topic |
| `kafka_scale_result_messages_read`    | The Count of messages read by the result command from the results topic |

### How To Run The App

First - as discussed above - you need a Kafka cluster running in your Kubernetes cluster.

Then, the manifests directory contains several manifests that run the application. Once you apply these manifests, census data will immediately start coming down into your cluster.

```shell
$ kubectl create namespace kafka
$ find manifests -maxdepth 1 -name '*.yaml' | xargs kubectl apply -f
```

The manifests create the compute topic with 10 partitions. The compute deployment is configured for two replicas, but can be modified in cluster to scale up to 10 replicas to observe the performance improvement from increased parallelization. As discussed further down in the README, if you have Grafana running in the cluster you can import the dashboard `manifests/kafka-scale-grafana-dashboard.json` to observe the read/compute metrics.

### Running the App on your desktop

If you've built the `kafka-scale` binary on your desktop (see *Building* below), you can run the app from your desktop for testing / debugging. These steps assume that you've provisioned your Kafka cluster using Strimzi Kafka and have specified a NodePort service that makes it possible to access your Kafka broker that is running in your Kubernetes cluster from outside the cluster.

In this example the Kafka cluster named `my-cluster` consists of a single broker, exposed via a NodePort service in the `kafka` namespace named `my-cluster-kafka-nodeport-0`. Further, since this is a NodePort service, it is accessible via any node in the cluster. In this example, I have a Kubernetes cluster node named `ham` for which I need the IP:

```shell
IP=$(kubectl -n kafka get node ham\
  -o=jsonpath='{range .status.addresses[?(@.type == "InternalIP")]}{.address}{"\n"}')
PORT=$(kubectl -n kafka get svc my-cluster-kafka-nodeport-0\
  -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
```

This example also assumes that you've already created the two Kafka topics used by the app using the `compute-topic.yaml` in the manifests directory. So:

```shell
$ kubectl -n kafka get kafkatopics
NAME      CLUSTER     PARTITIONS   REPLICATION FACTOR   READY
...
compute  my-cluster   10           1                    True
results  my-cluster   1            1                    True
...
```

Having verified that the two topics exist, and having gotten the IP address and port number for the Kafka broker NodePort service, you run the app on the desktop like so:

```shell
./kafka-scale --kafka=$IP:$PORT --years=2018 --months=jan --chunks=10 --verbose read
```

The example reads only the month of January from year 2018 and writes only ten chunks into the compute topic for test purposes. (Each chunk consists of multiple messages.) If should produce output something like the following. The ellipses are elided verbose content:

```shell
oneGz processing url https://www2.census.gov/programs-surveys/cps/datasets/2018/basic/jan18pub.dat.gz with current value of chunks: 0
Getting gzip: https://www2.census.gov/programs-surveys/cps/datasets/2018/basic/jan18pub.dat.gz
chunk: 2018
000004795110719 12018 120100-1 1 1-1 1 9-1-1-1 ...
000004795110719 12018 120100-1 1 1-1 1 9-1-1-1 ...
...
writing message with key c4652fcc to topic compute
chunk: 2018
000110327856469 12018 120100-1 1 1-1 1 5-1-1-1 ...
...
writing message with key 422c993 to topic compute
chunk: 2018
000110370885915 12018 220100-1 1 1-1 115-1-1-1 ...
000110405887199 12018 120100-1 1 2 2 0 2-1-1-1 ...
...
writing message with key 4954dbfb to topic compute
chunk: 2018
000110478587527 12018 120100-1 1 1-1 1 9-1-1-1 ...
000110509947170 12018-121600-1 1-1-1 0-1 1-1-1 ...
...
writing message with key 836ec5d6 to topic compute
chunk count met: 10. Stopping
no errors were encountered processing census data. 10 chunks were processed
```

If everything worked correctly, you can run the `offsets` command to see the offsets for the `compute` topic:

```shell
$ ./kafka-scale --kafka=$IP:$PORT --topic=compute offsets
Listing offsets for topic: compute

Partition           CommittedOffset     LastOffset
0                   -1                  10
1                   -1                  10
2                   -1                  10
3                   -1                  10
4                   -1                  10
5                   -1                  10
6                   -1                  10
7                   -1                  10
8                   -1                  10
9                   -1                  10
```

The above output shows that - since you've only run the reader, you've only written to the `compute` topic. You would run the `kafka-scale` binary with the `compute` command to read from the `compute` topic, perform the computation, and write the results to the `results` topic:

```shell
./kafka-scale --kafka=$IP:$PORT compute
```

Note that the `kafka-scale` binary's `compute` command blocks if there's nothing available any more in the `compute` topic so, just terminate it with CTRL-C after a minute. then:

```shell
$ ./kafka-scale --kafka=$IP:$PORT --topic=compute offsets
Listing offsets for topic: compute

Partition           CommittedOffset     LastOffset
0                   10                  10
1                   10                  10
2                   10                  10
3                   10                  10
4                   10                  10
5                   10                  10
6                   10                  10
7                   10                  10
8                   10                  10
9                   10                  10

$ ./kafka-scale --kafka=$IP:$PORT --topic=results offsets
Listing offsets for topic: results

Partition           CommittedOffset     LastOffset
0                   -1                  100
```

So you can see that the `compute` topic was fully consumed and the results topic is fully populated. You would run next the `kafka-scale` utility with the `results` command to read the `results` topic and tabulate the results in memory. From there - while the results command is running you would curl the results endpoint on localhost to observe the tabulated results. So:

```shell
$ ./kafka-scale --kafka=$IP:$PORT --verbose --results-port=8888 results
beginning read message from topic: results
Starting http server on port: 8888
read message from topic results - message: 2018:1,1,1,1,1,1,1,1,1,1
read message from topic results - message: 2018:1,1,1,1,1,1,1,1,1,1
read message from topic results - message: 2018:1,1,1,1,1,1,1,1,1,1
...
```

Then, from another command shell:

```shell
$ curl -s http://localhost:8888/results | jq
{
  "2018": {
    "0": {
      "Description": "OTHER UNIT",
      "Count": 0
    },
    "1": {
      "Description": "HOUSE, APARTMENT, FLAT",
      "Count": 1011
    },
    "10": {
      "Description": "UNOCCUPIED TENT SITE OR TRLR SITE",
      "Count": 4
    },
    "11": {
      "Description": "STUDENT QUARTERS IN COLLEGE DORM",
      "Count": 0
    },
    "12": {
      "Description": "OTHER UNIT NOT SPECIFIED ABOVE",
      "Count": 3
    },
    "2": {
      "Description": "HU IN NONTRANSIENT HOTEL, MOTEL, ETC.",
      "Count": 0
    },
    "3": {
      "Description": "HU PERMANENT IN TRANSIENT HOTEL, MOTEL",
      "Count": 0
    },
    "4": {
      "Description": "HU IN ROOMING HOUSE",
      "Count": 0
    },
    "5": {
      "Description": "MOBILE HOME OR TRAILER W/NO PERM. ROOM ADDED",
      "Count": 65
    },
    "6": {
      "Description": "MOBILE HOME OR TRAILER W/1 OR MORE PERM. ROOMS ADDED",
      "Count": 2
    },
    "7": {
      "Description": "HU NOT SPECIFIED ABOVE",
      "Count": 1
    },
    "8": {
      "Description": "QUARTERS NOT HU IN ROOMING OR BRDING HS",
      "Count": 0
    },
    "9": {
      "Description": "UNIT NOT PERM. IN TRANSIENT HOTL, MOTL",
      "Count": 4
    }
  }
}
```
And then finally, observe that all the results topic entries have been consumed to summarize the results:

```shell
$ ./kafka-scale --kafka=$IP:$PORT --topic=results offsets
Listing offsets for topic: results

Partition           CommittedOffset     LastOffset          
0                   100                 100    
```

The individual commands shown above are  exactly what occurs in the cluster when you deploy the manifests in the `manifests` directory.

### Manifests

The `manifests` directory contains the following manifest files that comprise the application, when it is run in-cluster:

| Manifest                           | Description                                                  |
| ---------------------------------- | ------------------------------------------------------------ |
| compute-deployment.yaml            | Creates a deployment with as many replicas as you specify. Reads from the compute topic populated by the read job, performs a simple computation, and writes the computed result to the results topic. The compute topic is consumed as part of a consumer group. So typically the number of replicas of this deployment will be less than or equal to the number of partitions of the compute topic. If there are more replicas of this deployment than partitions, then the consumer group will not provide those replicas with data - meaning the replicas will do nothing |
| compute-podmonitor.yaml            | Creates a `PodMonitor` CR that wires Prometheus up to scrape the application pods for exposed metrics. You'll need to configure the Prometheus Operator to watch for `PodMonitor` CRs in all namespaces. From my experience, this is the default configuration for `kube-prometheus` |
| compute-topic.yaml | Since my testing is done with Strimzi Kafka - this CR will create the Kafka compute topic via the Strimzi operator |
| kafka-scale-grafana-dashboard.json | This is a Grafana Dashboard that provides visibility into what the application is doing. The kube-prometheus stack doesn't deploy the Grafana Operator. So - to use kube-prometheus with custom dashboards like this it is easiest just to import the dashboard JSON into Grafana via the Grafana UI |
| prometheus-rbac.yaml               | RBACs to allow Prometheus to scrape the application pods for metrics |
| read-job.yaml                      | Reads census gzips as configured by command-line params. Chunks the data and writes the chunks to the compute topic. Creates the compute topic if it does not already exist with configurable partitions and replicas. Typically, you will create one partition for each replica of the compute deployment. The manifest is a Job, because once it reads and chunks all the data, it has nothing else to do |
| results-deployment.yaml            | Creates a deployment with one replica. This pod reads from the results topic and summarizes the data into a Go `struct` which it serves via a configurable port on the **/results** endpoint |
| results-service.yaml               | A NodePort service that enables you to access the results endpoint without port-forwarding. Again - I test this in a desktop cluster so this may or may not be useful or necessary or even possible depending on your Kubernetes environment |
| results-topic.yaml | Just like `compute-topic.yaml` - this CR will create the Kafka results topic via the Strimzi operator |


### Building

The project `Makefile` is how you build. There are two top-level targets:

`local-build`

This target builds the app to the desktop.  This supports desktop testing of the app against Kafka in the cluster.

`quay`

This target does a containerized build and push using **podman**, with the resulting image going to: `quay.io/appzygy/kafka-scale` with the version specified in the Make file (probably 1.0.1).
