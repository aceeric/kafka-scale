## To deploy to the cluster

```shell
find . -maxdepth 1 -name '*.yaml' | xargs kubectl apply -f
```