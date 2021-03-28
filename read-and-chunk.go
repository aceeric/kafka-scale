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

// Iterates the years and months arrays. Builds a URL with the year and month encoded the way the CPS
// website requires. Calls oneGz to process the GZIP. Returns the number of chunks processed and true if
// success, else false if error, Stops if command line configurable chunk count met
func readAndChunk(writer *kafka.Writer, fromFile string) (int, bool) {
	chunks := 0
	if fromFile != "" {
		return oneGz(writer, chunks, fromFile)
	}
	for _, year := range yearsArr {
		for _, month := range monthsArr {
			if chunks, ok := oneGz(writer, chunks, fmt.Sprintf(gzurl, year, month, strconv.Itoa(year)[2:])); !ok {
				return chunks, false
			} else if chunks >= chunkCount {
				return chunks, true
			}
		}
	}
	return chunks, true
}

// Processes a census gzip dataset. Can take either a file (mostly for testing), or an http URL to the censs
// site. Either way gets the GZIP, extracts to memory, chunks the output to Kafka, or to stdout, or doesn't chunk
// depending on the command line. If chunking, each 10 lines of input is concatenated into a chunk. Returns the
// number of chunks processed and true if success, else false if error. Stops if command line configurable
// chunk count met
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
		if chunks >= chunkCount {
			log.Printf("chunk count met: %v. Stopping\n", chunks)
			return chunks, true
		}
	}
	return chunks, true
}