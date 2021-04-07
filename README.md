### consume the results endpoint
curl --silent -H "Accept: application/json"  http://192.168.0.46:32099/results | json_pp


### TODO

-) Grafana dashboard for kafka-scale - how to inject it into the kube-prometheus stack
-) How to add Strimzi monitoring to existing kube-prometheus stack
-) Test with one broker and two - I think the net.Addr only supports a single endpoint...

### DONE
-) Add podman push image to appzygy in Makefile
-) Add a manifests dir
-) Created and tested a Pod
-) look at the redhat quay thing
-) dont use 9090 for metrics
-) Need: three deployments: read/chunk, compute, results
-) Create 1) PodMonitor 2) Role 3) Rolebinding to enable Promtheus to scrape worker
