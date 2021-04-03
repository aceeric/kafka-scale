package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

const gzurl = "https://www2.census.gov/programs-surveys/cps/datasets/%v/basic/%v%vpub.dat.gz"

// Reads census data and chunks the data into the 'compute' topic. Operates in two modes based on args:
//
// If fromFile arg is not empty, then assumes this is the FQPN of a downloaded census gzip. In this case,
// processes the file via the oneGz func.
//
// If fromFile is empty, uses package level 'yearsArr' and 'monthsArr' initialized from the command line.
// In a nested for-loop, builds a URL to a census gzip with the year and month encoded the way the CPS
// website requires and calls oneGz to process each GZIP.
//
// In both scenarios, returns the number of chunks processed and true if success, else false if error. Also,
// supports throttling via the package-level 'chunkCount' variable initialized from the command line.

func readCmd(kafkaBrokers string, partitionCnt int, replicationFactor int, fromFile string, chunkCount int,
	yearsArr []int, monthsArr []string, writeTo string, verbose bool) {
	var writer *kafka.Writer
	if kafkaBrokers != "" {
		if err := createTopicIfNotExists(kafkaBrokers, compute_topic, partitionCnt, replicationFactor); err != nil {
			fmt.Printf("error creating topic %v, error is:%v\n", compute_topic, err)
			return
		}
		writer = newKafkaWriter(kafkaBrokers, compute_topic)
		defer writer.Close()
	}
	if chunks, ok := readAndChunk(writer, fromFile, chunkCount, yearsArr, monthsArr, writeTo, verbose); !ok {
		fmt.Printf("error processing census data. %v chunks were processed before stopping\n", chunks)
	} else {
		fmt.Printf("no errors were encountered processing census data. %v chunks were processed\n", chunks)
	}
}

func readAndChunk(writer *kafka.Writer, fromFile string, chunkCount int, yearsArr []int, monthsArr []string, writeTo string,
	verbose bool) (int, bool) {
	chunks := 0
	var ok bool
	if fromFile != "" {
		return oneGz(writer, chunkCount, chunks, fromFile, yearsArr[0], writeTo, verbose)
	}
	for _, year := range yearsArr {
		for _, month := range monthsArr {
			if chunks, ok = oneGz(writer, chunkCount, chunks, fmt.Sprintf(gzurl, year, month, strconv.Itoa(year)[2:]), year, writeTo, verbose); !ok {
				return chunks, false
			} else if chunkCount >= 0 && chunks >= chunkCount {
				return chunks, true
			}
		}
	}
	return chunks, true
}

// Processes one census gzip dataset. Can take either a file (mostly for testing), or an http URL to the census
// site. Either way streams the GZIP, chunks the output to Kafka, or to stdout, or doesn't chunk depending on
// the command line. If chunking, each 10 lines of input is concatenated into a chunk and written to the
// compute topic in Kafka. The first line is the year. (Can also chunk to the console if the package-level
// 'stdout' var is set to true from the command line.)
//
// Returns the cumulative number of chunks processed so far (including chunks from prior calls) and true if success,
// else false if error. Returns if package var 'chunkCount' count is met.
func oneGz(writer *kafka.Writer, chunkCount int, chunks int, url string, year int, writeTo string, verbose bool) (int, bool) {
	var rdr io.Reader
	var err error

	fmt.Printf("oneGz processing url %v with current value of chunks: %v\n", url, chunks)

	if strings.HasPrefix(url, "http") {
		fmt.Printf("Getting gzip: %v", url)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("error getting gzip: %v, error is: %v\n", url, err)
			return chunks, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			fmt.Printf("error getting gzip: %v, status code is: %v\n", url, resp.StatusCode)
			return chunks, false
		}
		rdr, err = gzip.NewReader(resp.Body)
		if err != nil {
			fmt.Printf("error creating gzip reader over url: %v, error is: %v\n", url, err)
			return chunks, false
		}
	} else {
		rdr, err = os.Open(url)
		if err != nil {
			fmt.Printf("error opening file: %v, error is: %v\n", url, err)
			return chunks, false
		}
		rdr, err = gzip.NewReader(rdr)
		if err != nil {
			fmt.Printf("error creating gzip reader over filesystem object: %v, error is: %v\n", url, err)
			return chunks, false
		}
	}
	scanner := bufio.NewScanner(rdr)
	chunk := strconv.Itoa(year) + "\n"
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		chunk += line + "\n"
		cnt++
		// chunk every ten lines
		if cnt >= 10 {
			if writeTo == writeToStdout {
				fmt.Printf("chunk: %v\n", chunk)
			} else if writeTo == writeToKafka {
				if err := writeMessage(writer, chunk, verbose); err != nil {
					fmt.Printf("error writing chunk to Kafka - error is: %v\n", err)
					return chunks, false
				}
			}
			chunks++
			cnt = 0
			chunk = ""
			if chunkCount >= 0 && chunks >= chunkCount {
				fmt.Printf("chunk count met: %v. Stopping\n", chunks)
				return chunks, true
			}
		}
	}
	return chunks, true
}
