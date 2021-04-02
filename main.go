package main

import (
	"log"

	"github.com/segmentio/kafka-go"
)

var command string
var dryRun bool
var stdout bool
var writeKafka bool
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

const (
	// supported commands

	// read a dataset, and chunk it into the 'compute' queue
	read      = "read"
	// read the 'compute` queue, compute results, write to the 'results' queue
	compute   = "compute"
	// read the 'results' queue, summarize results into memory, serve the results as JSON via a REST call
	results   = "results"
	// list all topics
	topiclist = "topiclist"
	// remove comma-separated list of topics
	rmtopics  = "rmtopics"
	// list offsets of one specified topic
	offsets   = "offsets"

	// Readers of the compute topic all read as part of this consumer group
	computeConsumer = "kafka-scale-consumer-group"
	resultConsumer = "kafka-scale-results-consumer-group"
)

// maps consumer groups to topics. The code always reads from a topic as part of a consumer group because
// that enables using the Kafka.NewReader functionality with auto-commit, etc.
var consumerGrpForTopic = map[string]string {
	compute: computeConsumer,
	results: resultConsumer,
}

// Usage:
//
// IP=$(kubectl -n kafka get node ham -o=jsonpath='{range .status.addresses[?(@.type == "InternalIP")]}{.address}{"\n"}')
// PORT=$(kubectl -n kafka get svc my-cluster-kafka-external-0 -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
//
// ./kafka-scale --kafka=$IP:$PORT --years=2019 --months=jan,feb --chunks 1000 read
// ./kafka-scale --years=2019 --months='*' --stdout read
// ./kafka-scale --from-file=/home/eace/Downloads/dec20pub.dat.gz --kafka=$IP:$PORT --partitions=10 --chunks=100 read
// ./kafka-scale --kafka=$IP:$PORT --stdout compute
// ./kafka-scale --kafka=$IP:$PORT --verbose compute
// ./kafka-scale --kafka=$IP:$PORT --verbose results
// ./kafka-scale --kafka=$IP:$PORT topiclist
// ./kafka-scale --kafka=$IP:$PORT --topic=compute offsets
// ./kafka-scale --kafka=$IP:$PORT --topic=compute,result rmtopics
func main() {
	if !validateCmdline() {
		return
	}
	if dryRun {
		doDryRun()
		return
	}
	withMetrics = true
	if withMetrics {
		startMetrics()
		defer stopMetrics()
	}
	var conn *kafka.Conn
	var writer *kafka.Writer
	var err error

	switch command {
	case read:
		if kafkaBrokers != "" {
			conn, err = connectKakfa(kafkaBrokers)
			if err != nil {
				log.Printf("error connecting to Kafka at: %v, error is: %v\n", kafkaBrokers, err)
				return
			}
			if err := createTopicIfNotExists(conn, compute_topic, partitionCnt, replicationFactor); err != nil {
				log.Printf("error creating topic %v, error is:%v\n", compute_topic, err)
				return
			}
			writer = newKafkaWriter(kafkaBrokers, compute_topic)
			defer conn.Close()
			defer writer.Close()
		}
		if chunks, ok := readAndChunk(writer, fromFile); !ok {
			log.Printf("error processing census data. %v chunks were processed before stopping\n", chunks)
		} else {
			log.Printf("no errors were encountered processing census data. %v chunks were processed\n", chunks)
		}
		select {}
	case compute:
		conn, err = connectKakfa(kafkaBrokers)
		if err != nil {
			log.Printf("error connecting to Kafka at: %v, error is: %v\n", kafkaBrokers, err)
			return
		}
		if err := createTopicIfNotExists(conn, results_topic, partitionCnt, replicationFactor); err != nil {
			log.Printf("error creating topic %v, error is:%v\n", results_topic, err)
			return
		}
		writer = newKafkaWriter(kafkaBrokers, results_topic)
		defer conn.Close()
		defer writer.Close()
		calc(writer, kafkaBrokers)
		select {}

	case results:
		accumulateAndServeResults(kafkaBrokers, port)
	case topiclist:
		getTopics(kafkaBrokers)
	case offsets:
		listOffsets(kafkaBrokers, topic)
	case rmtopics:
		deleteTopics(kafkaBrokers, topic)
	}
}
