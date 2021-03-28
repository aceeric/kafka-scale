package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"

	"github.com/segmentio/kafka-go"
)

// Reads from the 'compute' topic, calcs results, and writes to the 'results' topic. If any error, just
// goes to sleep and waits for the program to be terminated
func calc(writer *kafka.Writer, url string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   strings.Split(url, ","),
		GroupID:   "kafka-scale-consumer-group",
		Topic:     compute_topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	defer r.Close()

	for {
		// ReadMessage blocks
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			if err == io.EOF {
				log.Printf("reader for topic %v has been closed\n. Going to sleep permanently", compute_topic)
				select{}
			}
			log.Printf("error getting chunk from topic: %v, error is: %v\n", compute_topic, err)
			break
		}
		if verbose {
			log.Printf("read message: %v, topic: %v, part: %v, offset: %v\n", m.Key, m.Topic, m.Partition, m.Offset)
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
		if stdout {
			log.Printf("codes: %v\n", codes)
		} else if !quiet {
			if err := writeMessage(writer, codes); err != nil {
				log.Printf("error writing codes to Kafka - error is: %v. Going to sleep permanently\n", err)
				select {}
			}
		}
	}
}
