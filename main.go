package main

import (
	"log"

	"github.com/segmentio/kafka-go"
)

var command string
var dryRun bool
var stdout bool
var quiet bool
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

const (
	read      = "read"
	compute   = "compute"
	results   = "results"
	topiclist = "topiclist"
	rmtopics  = "rmtopics"
	offsets   = "offsets"
)

// Usage:
//
// IP=$(kubectl -n kafka get node ham -o=jsonpath='{range .status.addresses[?(@.type == "InternalIP")]}{.address}{"\n"}')
// PORT=$(kubectl -n kafka get svc my-cluster-kafka-external-0 -o=jsonpath='{.spec.ports[0].nodePort}{"\n"}')
//
// ./kafka-scale --kafka=$IP:$PORT --years=2015 --months=jan,feb --chunks 10 read
// ./kafka-scale --from-file=/home/eace/Downloads/dec20pub.dat.gz --kafka=$IP:$PORT --partitions=10 --chunks=100 read
// ./kafka-scale --kafka=$IP:$PORT --stdout compute
// ./kafka-scale --kafka=$IP:$PORT compute
// ./kafka-scale --kafka=$IP:$PORT results
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
	var conn *kafka.Conn
	var writer *kafka.Writer
	var err error

	// TODO CREATE THE TOPICS HERE IF NEEDED!!!

	switch command {
	case rmtopics:
		deleteTopics(kafkaBrokers, topic)
	case read:
		if kafkaBrokers != "" {
			conn, err = connectKakfa(kafkaBrokers)
			if err != nil {
				log.Printf("error connecting to Kafka at: %v, error is: %v\n", kafkaBrokers, err)
				return
			}
			writer = newKafkaWriter(kafkaBrokers, compute_topic)
			defer conn.Close()
			defer writer.Close()
		}
		if err := createTopicIfNotExists(conn, compute_topic, partitionCnt, replicationFactor); err != nil {
			log.Printf("error creating topic %v, error is:%v\n", compute_topic, err)
			return
		}
		if chunks, ok := readAndChunk(writer, fromFile); !ok {
			log.Printf("error processing census data. %v chunks were processed before stopping\n", chunks)
		} else {
			log.Printf("no errors were encountered processing census data. %v chunks were processed\n", chunks)
		}
	case compute:
		conn, err = connectKakfa(kafkaBrokers)
		if err != nil {
			log.Printf("error connecting to Kafka at: %v, error is: %v\n", kafkaBrokers, err)
			return
		}
		writer = newKafkaWriter(kafkaBrokers, results_topic)
		defer conn.Close()
		defer writer.Close()
		if err := createTopicIfNotExists(conn, results_topic, partitionCnt, replicationFactor); err != nil {
			log.Printf("error creating topic %v, error is:%v\n", results_topic, err)
			return
		}
		calc(writer, kafkaBrokers)
	case topiclist:
		getTopics(kafkaBrokers)
	case offsets:
		listOffsets(kafkaBrokers, topic)
	}
}
