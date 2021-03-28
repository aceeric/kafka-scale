package main

import (
	"context"
	"crypto/md5"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// valid topics. The tabulation topic holds chunks for computation. The results topic holds the results
// of a computation performed with data from the tabulation topic
const (
	compute_topic = "compute"
	results_topic = "results"
)

// Creates the passed topic if it does not already exist. Then writes the passed chunk to topic
func writeMessage(writer *kafka.Writer, message string) error {
	h := md5.New()
	k := h.Sum([]byte(message))
	if verbose {
		log.Printf("writing message with key: %v, topic: %v\n", k, writer.Topic)
	}
	err := writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   k,
			Value: []byte(message),
		},
	)
	if err != nil {
		log.Printf("error writing message, error is: %v\n", err)
		return err
	}
	return nil
}

// Creates and returns a new Kafka writer
func newKafkaWriter(url string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(url),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

// Creates and returns a connection to Kafka
func connectKakfa(url string) (*kafka.Conn, error) {
	conn, err := kafka.Dial("tcp", url)
	if err != nil {
		log.Printf("error connecting to Kafka url: %v, error is: %v\n", url, err)
		return nil, err
	}
	return conn, nil
}

// Creates a topic if it does not already exist. If topic exists, then no change is made to Kafka
func createTopicIfNotExists(conn *kafka.Conn, topic string, partitionCnt int, replFactorCnt int) error {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("error reading partitions. error is: %v\n", err)
		return err
	}
	for _, p := range partitions {
		if p.Topic == topic {
			return nil
		}
	}
	controller, err := conn.Controller()
	if err != nil {
		log.Printf("error getting controller. error is: %v\n", err)
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		log.Printf("error getting controller connection. error is: %v\n", err)
		return err
	}
	defer controllerConn.Close()
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     partitionCnt,
			ReplicationFactor: replFactorCnt,
		},
	}
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		log.Printf("error creating topic. error is: %v\n", err)
		return err
	}
	return nil
}

// Just a smoke test function - lists the topics in kafka. If this works, then assume we have
// a good Kafka instance and connectivity configuration. This creates a Kafka connection and then
// closes it.
func getTopics(url string) {
	conn, err := connectKakfa(url)
	if err != nil {
		log.Printf("error connecting to Kafka url: %v, error is: %v\n", url, err)
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("error reading partitions. error is: %v\n", err)
		return
	}
	sort.Slice(partitions[:], func(i, j int) bool {
		switch strings.Compare(partitions[i].Topic, partitions[j].Topic) {
		case -1: return true
		case 0: return partitions[i].ID < partitions[j].ID
		case 1: fallthrough
		default:return false
		}
	})

	fmt.Printf("Listing topics\n\n")
	format := "%-70v%-15v%-20v\n"
	fmt.Printf(format, "Topic", "Partition ID", "Leader")
	for _, p := range partitions {
		fmt.Printf(format, p.Topic, p.ID, p.Leader.Host)
	}
}

// Gets all the partitions for a topic as an array of int, sorted ascending
func getPartitionsForTopic(url string, topic string) ([]int, error) {
	conn, err := connectKakfa(url)
	if err != nil {
		log.Printf("error connecting to Kafka url: %v, error is: %v\n", url, err)
		return nil, err
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions()
	if err != nil {
		log.Printf("error reading partitions. error is: %v\n", err)
		return nil, err
	}
	var topicPartitions []int
	for _, p := range partitions {
		if p.Topic == topic {
			topicPartitions = append(topicPartitions, p.ID)
		}
	}
	sort.Ints(topicPartitions)
	return topicPartitions, nil
}

// Lists the offsets for all partitions in the passed topic
func listOffsets(url string, topic string) {
	client, shutdown := newClient(kafka.TCP(url))
	defer shutdown()
	partitions, err := getPartitionsForTopic(url, topic)
	if err != nil {
		log.Printf("error getting partitions for topic: %v, error is: %v\n", topic, err)
		return
	}
	var offsetRequests []kafka.OffsetRequest
	for _, partition := range partitions {
		offsetRequests = append(offsetRequests, kafka.FirstOffsetOf(partition), kafka.LastOffsetOf(partition))
	}
	res, err := client.ListOffsets(context.Background(), &kafka.ListOffsetsRequest{
		Topics: map[string][]kafka.OffsetRequest{
			topic: offsetRequests,
		},
	})
	if err != nil {
		log.Printf("error listing offsets for topic: %v, error is: %v\n", url, err)
		return
	}
	partitionOffsets, ok := res.Topics[topic]
	if !ok {
		log.Printf("error getting partition offsets for topic: %v, error is: %v\n", url, err)
		return
	}
	sort.Slice(partitionOffsets[:], func(i, j int) bool {
		if partitionOffsets[i].Partition < partitionOffsets[j].Partition {
			return true
		}
		return false
	})
	fmt.Printf("Listing offsets for topic: %v\n\n", topic)
	format := "%-20v%-20v%-20v\n"
	fmt.Printf(format, "Partition", "FirstOffset", "LastOffset")
	for _, partition := range partitionOffsets {
		fmt.Printf(format, partition.Partition, partition.FirstOffset, partition.LastOffset)
	}
}

// Creates a new Kafka client which can be used to query topic partitions and offsets
func newClient(addr net.Addr) (*kafka.Client, func()) {
	transport := &kafka.Transport{
		Dial:     (&net.Dialer{}).DialContext,
		Resolver: kafka.NewBrokerResolver(nil),
	}
	client := &kafka.Client{
		Addr:      addr,
		Timeout:   5 * time.Second,
		Transport: transport,
	}
	type ConnWaitGroup struct {
		sync.WaitGroup
	}
	conns := &ConnWaitGroup{}
	return client, func() { transport.CloseIdleConnections(); conns.Wait() }
}

func deleteTopics(url string, topics string) {
	client, shutdown := newClient(kafka.TCP(url))
	defer shutdown()

	topicArray := strings.Split(topics, ",")
	res, err := client.DeleteTopics(context.Background(), &kafka.DeleteTopicsRequest{
		Topics: topicArray,
	})
	if err != nil {
		log.Printf("error deleting topics: %v, error is: %v\n", topics, err)
		return
	}
	for _, topic := range topicArray {
		if err := res.Errors[topic]; err != nil {
			log.Printf("error deleting topic: %v, error is: %v\n", topic, err)
		}
	}
}
