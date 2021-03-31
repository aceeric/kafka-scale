package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

// Reads from the 'compute' topic, calculates results, and writes to the 'results' topic. Blocks reading from the
// compute topic indefinitely. So once the topic is emptied, this function will block indefinitely
func calc(writer *kafka.Writer, url string) bool {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  strings.Split(url, ","),
		GroupID:  computeConsumer,
		Topic:    compute_topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
	})
	defer r.Close()

	for {
		// ReadMessage blocks
		if verbose {
			log.Printf("reading message from topic: %v\n", compute_topic)
		}
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			if err == io.EOF {
				log.Printf("reader for topic %v has been closed\n", compute_topic)
				return false
			}
			log.Printf("error getting chunk from topic: %v, error is: %v\n", compute_topic, err)
			return false
		}
		if verbose {
			log.Printf("message was read. key: %v, topic: %v, part: %v, offset: %v\n", m.Key, m.Topic, m.Partition, m.Offset)
		}
		scanner := bufio.NewScanner(strings.NewReader(string(m.Value)))
		codes := ""
		separator := ""
		for scanner.Scan() {
			// get the HEHOUSUT value which is a one or two character code at position 30, zero-relative. So
			// like " 1" and up to "12". This is the housing code. Just string the codes together into a comma-
			// separated list like 1,1,1,2,12,3
			line := scanner.Text()
			codes += separator + strings.TrimSpace(line[30:32])
			separator = ","
		}
		// todo simulate some compute time
		//time.Sleep(300 * time.Millisecond)
		if stdout {
			log.Printf("codes: %v\n", codes)
		} else if !quiet {
			if err := writeMessage(writer, codes); err != nil {
				log.Printf("error writing codes to Kafka - error is: %v\n", err)
				return false
			}
		}
	}
}
