package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Reads from the 'compute' topic, calculates results, and writes to the 'results' topic. Blocks reading from the
// compute topic indefinitely. So once the topic is emptied, this function will block indefinitely. On the other hand
// since it is sitting blocking, you can add more results using the chunker and processing will just resume
func calc(writer *kafka.Writer, url string) bool {
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
		lineCnt := 0
		for scanner.Scan() {
			// get the HEHOUSUT value which is a one or two character code at position 30, zero-relative. So
			// like " 1" and up to "12". This is the housing code. Just string the codes together into a comma-
			// separated list like nnnn:1,1,1,2,12,3 where nnnn is the year (year is always the first line in
			// the message)
			line := scanner.Text()
			if lineCnt == 0 {
				// first line is the year so we'
				codes = strings.TrimSpace(line) + ":"
			} else {
				codes += separator + strings.TrimSpace(line[30:32])
				separator = ","
			}
			lineCnt++
		}
		// todo simulate some compute time
		time.Sleep(100 * time.Millisecond)
		if stdout {
			log.Printf("codes: %v\n", codes)
		} else if writeKafka {
			if err := writeMessage(writer, codes); err != nil {
				log.Printf("error writing codes to Kafka - error is: %v\n", err)
				return false
			}
		}
	}
}
