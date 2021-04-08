package main

var command string
var dryRun bool
var years string
var months string
var kafkaBrokers string
var chunkCount int
var yearsArr []int
var monthsArr []string
var partitionCnt int
var replicationFactor int
var fromFile string
var topic string
var verbose bool
var port int
var withMetrics bool
var MetricsPort string
var printVersion bool
var writeTo string

const (
	// supported commands

	// read a dataset, and chunk it into the 'compute' queue
	read      = "read"
	// read the 'compute` queue, compute results, write to the 'results' queue
	compute   = "compute"
	// read the 'results' queue, summarize results into memory, serve the results as JSON via a REST call
	results   = "results"
	// list all topics to the console
	topiclist = "topiclist"
	// remove comma-separated list of topics
	rmtopics  = "rmtopics"
	// list offsets of a specified topic to the console
	offsets   = "offsets"

	// Readers of the compute topic all read as part of this consumer group
	computeConsumer = "kafka-scale-consumer-group"
	resultConsumer = "kafka-scale-results-consumer-group"

	writeToKafka = "kafka"
	writeToStdout = "stdout"
	WriteToNull = "null"
)

// maps consumer groups to topics. The code always reads from a topic as part of a consumer group because
// that enables using the Kafka.NewReader functionality with auto-commit, etc.
var consumerGrpForTopic = map[string]string {
	compute: computeConsumer,
	results: resultConsumer,
}

// Some example usages:
//
// IP=$(kubectl -n kafka get node ham -o=jsonpath='{range .status.addresses[?(@.type == "InternalIP")]}{.address}{"\n"}')
// PORT=$(kubectl -n kafka get svc my-cluster-kafka-external-0 -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
//
// ./kafka-scale --kafka=$IP:$PORT --years=2018,2019 --months=jan --compute-topic-partitions=10 --chunks=1 --verbose read
// ./kafka-scale --years=2019 --months='*' --write-to=stdout read
// ./kafka-scale --kafka=$IP:$PORT --from-file=/home/eace/Downloads/dec20pub.dat.gz --years=2019 --compute-topic-partitions=10 --chunks=1 read
// ./kafka-scale --kafka=$IP:$PORT --write-to=stdout --verbose compute
// ./kafka-scale --kafka=$IP:$PORT --verbose --port=8888 results
// ./kafka-scale --kafka=$IP:$PORT topiclist
// ./kafka-scale --kafka=$IP:$PORT --topic=compute offsets
// ./kafka-scale --kafka=$IP:$PORT --topic=compute,results rmtopics
func main() {
	if !validateCmdline() {
		return
	}
	if dryRun {
		// print how the app interprets the command line but don't do anything
		doDryRun()
		return
	}
	if withMetrics {
		startMetrics(MetricsPort)
		defer stopMetrics()
	}
	switch command {
	case read:
		readCmd(kafkaBrokers, partitionCnt, replicationFactor, fromFile, chunkCount, yearsArr, monthsArr, writeTo, verbose)
	case compute:
		computeCmd(kafkaBrokers, partitionCnt, replicationFactor, verbose, writeTo)
	case results:
		resultsCmd(kafkaBrokers, port, verbose)
	case topiclist:
		topicListCmd(kafkaBrokers)
	case offsets:
		offsetsCmd(kafkaBrokers, topic)
	case rmtopics:
		rmTopicsCmd(kafkaBrokers, topic)
	}
}
