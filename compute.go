package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Reads from the 'compute' topic, calculates results, and writes to the 'results' topic. Blocks reading from the
// compute topic indefinitely. So once the topic is emptied, this function will block indefinitely. On the other hand
// since it is sitting blocking, you can add more results using the read command and processing here will just resume
func computeCmd(kafkaBrokers string, partitionCnt int, replicationFactor int, verbose bool, writeTo string, delay int) {
	if err := createTopicIfNotExists(kafkaBrokers, results_topic, partitionCnt, replicationFactor); err != nil {
		fmt.Printf("error creating topic %v, error is:%v\n", results_topic, err)
		return
	}
	writer := newKafkaWriter(kafkaBrokers, results_topic)
	defer writer.Close()
	calc(writer, kafkaBrokers, verbose, writeTo, delay)
}

func calc(writer *kafka.Writer, url string, verbose bool, writeTo string, delay int) bool {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       strings.Split(url, ","),
		GroupID:       consumerGrpForTopic[compute_topic],
		Topic:         compute_topic,
		QueueCapacity: 1,
		MinBytes:      10e3, // 10KB
		MaxBytes:      10e6, // 10MB
	})
	defer r.Close()

	for {
		// ReadMessage blocks
		if verbose {
			fmt.Printf("reading message from topic: %v\n", compute_topic)
		}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			if err == io.EOF {
				fmt.Printf("reader for topic %v has been closed\n", compute_topic)
				return false
			}
			fmt.Printf("error getting chunk from topic: %v, error is: %v\n", compute_topic, err)
			return false
		}
		computeMessagesRead.Inc()
		if verbose {
			fmt.Printf("message was read. key: %v, topic: %v, part: %v, offset: %v\n", m.Key, m.Topic, m.Partition, m.Offset)
		}
		scanner := bufio.NewScanner(strings.NewReader(string(m.Value)))
		codes := ""
		separator := ""
		lineCnt := 0
		for scanner.Scan() {
			// get the HEHOUSUT value which is a one or two character code at position 30, zero-relative. So
			// like " 1" and up to "12". This is the housing code. Just string the codes together into a comma-
			// separated list like nnnn:1,1,1,2,12,3 where nnnn is the year (year is always the first line in
			// the message)
			line := scanner.Text()
			if lineCnt == 0 {
				// first line is the year
				codes = strings.TrimSpace(line) + ":"
			} else {
				codes += separator + strings.TrimSpace(line[30:32])
				separator = ","
			}
			lineCnt++
		}
		if delay > 0 {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		}
		if verbose {
			fmt.Printf("Message: %v\n", codes)
		}
		if writeTo == writeToStdout {
			fmt.Printf("codes: %v\n", codes)
		} else if writeTo == writeToKafka {
			if err := writeMessage(writer, codes, verbose); err != nil {
				fmt.Printf("error writing codes to Kafka - error is: %v\n", err)
				return false
			}
			resultMessagesWritten.Inc()
		}
	}
}
