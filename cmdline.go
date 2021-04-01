package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
)

func init() {
	flag.StringVar(&years, "years", "", "Years. E.g. --years=2015,2016. Ignored unless role is 'read'")
	flag.StringVar(&months, "months", "", "Months. E.g. --months=jan,feb. Ignored unless role is 'read'")
	flag.StringVar(&kafkaBrokers, "kafka", "", "Kafka broker URLs. E.g. 192.168.0.45:32355,192.168.0.46:32355,192.168.0.47:32355")
	flag.IntVar(&chunkCount, "chunks", -1, "Chunk count - the number of chunks of 'dataSrc' to read or calculate. If omitted, or -1, then all")
	flag.BoolVar(&dryRun, "dry-run", false, "Displays how the command would run, but doesn't actually run it")
	flag.BoolVar(&stdout, "stdout", false, "Instead of writing to Kafka, write the stdout")
	flag.BoolVar(&quiet, "quiet", false, "Don't write to Kafka or stdout, just present some summary stats")
	flag.IntVar(&partitionCnt, "partitions", 1, "Partitions for the tabulation topic. Defaults to 1")
	flag.IntVar(&replicationFactor, "replication-factor", 1, "Replication factor. Defaults to 1")
	flag.StringVar(&fromFile, "from-file", "", "FQPN of census file to load (don't download - use filesystem)")
	flag.StringVar(&topic, "topic", "", "If listing offsets, this is the topic for which to list offsets. If deleting topics, this is a comma-separated list of topics to delete")
	flag.BoolVar(&verbose, "verbose", false, "Prints verbose diagnostic messages")
	flag.IntVar(&port, "port", 8888, "REST endpoint port - defaults to 8888")
	flag.BoolVar(&withMetrics, "with-metrics", false, "Enables metrics exposition on port 9090")
}

var validCommands = []string {read, compute, results, topiclist, offsets, rmtopics}

func validateCmdline() bool {
	flag.Parse()
	tmp := flag.Args()

	if len(tmp) != 1 {
		log.Printf("invalid command: %v\n", tmp)
		return false
	} else {
		for _, cmd := range validCommands {
			if tmp[0] == cmd {
				command = tmp[0]
				break
			}
		}
		if command == "" {
			log.Printf("invalid command: %v\n", tmp)
			return false
		}
	}
	if !(stdout || quiet) && kafkaBrokers == "" {
		log.Printf("need a Kafka cluster bootstrap URL\n")
		return false
	} else if command == read && fromFile == "" && (years == "" || months == "") {
		log.Printf("if command is 'read' then '--years' and '--months' are both required unless --from-file is specified\n")
		return false
	}
	if months != "" && !parseMonths() {
		log.Printf("Can't parse months: %v. Must be comma separated and abbreviated like '--months=jan,feb,mar' etc.\n", months)
		return false
	}
	if years != "" && !parseYears() {
		log.Printf("Can't parse years: %v. Must be comma separated and each year between 1970 and 2020 inclusive like --years=2019,2020\n", years)
		return false
	}

	return true
}

func doDryRun() {
	fmt.Printf("Command: %v\n", command)
	if command == read {
		fmt.Printf("Years: %v\n", years)
		fmt.Printf("Months: %v\n", months)
	}
	if !(stdout || quiet) {
		fmt.Printf("Kafka bootstrap URL: %v\n", kafkaBrokers)
		fmt.Printf("Partitions: %v\n", partitionCnt)
		fmt.Printf("Replication Factor: %v\n", replicationFactor)
		fmt.Printf("From File: %v\n", fromFile)
	} else if stdout {
		fmt.Printf("Write to stdout rather than writing to Kafka\n")
	} else if quiet {
		fmt.Printf("Run silently (don't write to Kafka or stdout)\n")
	}
	fmt.Printf("Chunk count: %v\n", chunkCount)
}

func parseYears() bool {
	for _, s := range strings.Split(years, ",") {
		yr, _ := strconv.Atoi(s)
		if yr < 1970 && yr > 2020 {
			return false
		}
		yearsArr = append(yearsArr, yr)
	}
	return true
}

func parseMonths() bool {
	valid := map[string]int {"jan":0,"feb":0,"mar":0,"apr":0,"may":0,"jun":0,"jul":0,"aug":0,"sep":0,"oct":0,"nov":0,"dec":0}
	for _, s := range strings.Split(months, ",") {
		if _, ok := valid[s]; !ok {
			return false
		}
		monthsArr = append(monthsArr, s)
	}
	return true
}
