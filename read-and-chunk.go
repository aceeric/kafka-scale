package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"log"
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
func readAndChunk(writer *kafka.Writer, fromFile string) (int, bool) {
	chunks := 0
	if fromFile != "" {
		return oneGz(writer, chunks, fromFile)
	}
	for _, year := range yearsArr {
		for _, month := range monthsArr {
			if chunks, ok := oneGz(writer, chunks, fmt.Sprintf(gzurl, year, month, strconv.Itoa(year)[2:])); !ok {
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
// compute topic in Kafka. (Can also chunk to the console if the package-level 'stdout' var is set to true
// from the command line.)
//
// Returns the cumulative number of chunks processed so far (including chunks from prior calls) and true if success,
// else false if error. Returns if package var 'chunkCount' count is met.
func oneGz(writer *kafka.Writer, chunks int, url string) (int, bool) {
	var rdr io.Reader
	var err error
	if strings.HasPrefix(url, "http") {
		log.Printf("Getting gzip: %v", url)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("error getting gzip: %v, error is: %v\n", url, err)
			return chunks, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			log.Printf("error getting gzip: %v, status code is: %v\n", url, resp.StatusCode)
			return chunks, false
		}
		rdr, err = gzip.NewReader(resp.Body)
		if err != nil {
			log.Printf("error creating gzip reader over url: %v, error is: %v\n", url, err)
			return chunks, false
		}
	} else {
		rdr, err = os.Open(url)
		if err != nil {
			log.Printf("error opening file: %v, error is: %v\n", url, err)
			return chunks, false
		}
		rdr, err = gzip.NewReader(rdr)
		if err != nil {
			log.Printf("error creating gzip reader over filesystem object: %v, error is: %v\n", url, err)
			return chunks, false
		}
	}
	scanner := bufio.NewScanner(rdr)
	chunk := ""
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		chunk += line + "\n"
		cnt++
		if cnt >= 10 {
			if stdout {
				log.Printf("chunk: %v\n", chunk)
			} else if !quiet {
				if err := writeMessage(writer, chunk); err != nil {
					log.Printf("error writing chunk to Kafka - error is: %v\n", err)
					return chunks, false
				}
			}
			chunks++
			cnt = 0
			chunk = ""
		}
		if chunkCount >= 0 && chunks >= chunkCount {
			log.Printf("chunk count met: %v. Stopping\n", chunks)
			return chunks, true
		}
	}
	return chunks, true
}
