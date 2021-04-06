



port forward to results pod
curl localhost:8888/results - empty result set
  thats ok - because no results yet
but:
  browser
    http://192.168.0.45:8888/results
  unable to connct


kc port-forward -nkafka kafka-scale-results-5cbd84d7c9-jq56n 8888
   OK !!!
kc port-forward -nkafka svc/kafka-scale-results 8888
   OK !!!
curl -i -H "Accept: application/json" 192.168.0.45:32099/results
   NO FAIL!

    conclusion -- servicve is no reaching pod?????????????????



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
