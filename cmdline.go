package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
)

// initialize command line args and default values
func init() {
	flag.StringVar(&years, "years", "", "Years. E.g. --years=2015,2016. Ignored unless role is 'read'")
	flag.StringVar(&months, "months", "", "Months. E.g. --months=jan,feb. Ignored unless role is 'read'. '*' is also allowed (use single ticks to exclude from shell parsing)")
	flag.StringVar(&kafkaBrokers, "kafka", "", "Kafka broker URLs. E.g. 192.168.0.45:32355,192.168.0.46:32355,192.168.0.47:32355")
	flag.IntVar(&chunkCount, "chunks", -1, "Chunk count - the number of chunks of 'dataSrc' to read or calculate. If omitted, or -1, then all")
	flag.BoolVar(&dryRun, "dry-run", false, "Displays how the command would run, but doesn't actually run it")
	flag.BoolVar(&stdout, "stdout", false, "Instead of writing to Kafka, write the stdout. Overrides 'write-kafka'")
	flag.BoolVar(&writeKafka, "write-kafka", true, "Write to Kafka. Defaults true. For debugging, set to false to disable writing to the Kafka topics")
	flag.IntVar(&partitionCnt, "partitions", 1, "Partitions for the compute topic. Defaults to 1. Tune to the number of compute pods")
	flag.IntVar(&replicationFactor, "replication-factor", 1, "Replication factor. Defaults to 1. Tune to your cluster size")
	flag.StringVar(&fromFile, "from-file", "", "FQPN of census file to load (i.e. don't download from the census site - use a file on the filesystem)")
	flag.StringVar(&topic, "topic", "", "If listing offsets, this is the topic for which to list offsets. If deleting topics, this is a comma-separated list of topics to delete")
	flag.BoolVar(&verbose, "verbose", false, "Prints verbose diagnostic messages")
	flag.IntVar(&port, "port", 8888, "REST endpoint port for results - defaults to 8888")
	flag.BoolVar(&withMetrics, "with-metrics", false, "Enables Prometheus metrics exposition on port 9090")
}

var validCommands = []string {read, compute, results, topiclist, offsets, rmtopics}

// Validate the command line
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
	// stdout means write to stdout not to Kafka
	if stdout {
		writeKafka = false
	}
	needKafkaUrl := false
	if (command == results || command == rmtopics || command == topiclist || command == compute) || (command == read && writeKafka) {
		needKafkaUrl = true
	}
	if needKafkaUrl && kafkaBrokers == "" {
		log.Printf("need Kafka cluster broker URL(s)\n")
		return false
	} else if command == read && fromFile == "" && (years == "" || months == "") {
		log.Printf("if command is 'read' then '--years' and '--months' are both required\n")
		return false
	} else if command == read && fromFile != "" && years == "" {
		log.Printf("if command is 'read' and --from-file is specified, then '--years' is required with one value like --years=2018 - that being the year of the file\n")
		return false
	} else if months != "" && !parseMonths() {
		log.Printf("Can't parse months: %v. Must be comma-separated and abbreviated like '--months=jan,feb,mar' etc. ('*' is also allowed)\n", months)
		return false
	} else if years != "" && !parseYears() {
		log.Printf("Can't parse years: %v. Must be comma-separated and each year between 1970 and 2020 inclusive like --years=2019,2020\n", years)
		return false
	}
	return true
}

// Print how the program is interpreting the command line
func doDryRun() {
	fmt.Printf("Command: %v\n", command)
	if command == read {
		fmt.Printf("Years: %v\n", years)
		fmt.Printf("Months: %v\n", months)
		fmt.Printf("From file: %v\n", fromFile)
		fmt.Printf("Chunk count: %v\n", chunkCount)
		fmt.Printf("Months: %v\n", monthsArr)
		fmt.Printf("Year: %v\n", yearsArr)
	}
	if command == read || command == compute {
		if writeKafka {
			fmt.Printf("Kafka bootstrap URL: %v\n", kafkaBrokers)
			fmt.Printf("Partitions: %v\n", partitionCnt)
			fmt.Printf("Replication Factor: %v\n", replicationFactor)
		} else if stdout {
			fmt.Printf("Write to stdout rather than writing to Kafka\n")
		} else {
			fmt.Printf("Don't write to Kafka or stdout (silently discard the outputs)\n")
		}
	}
	if command == results {
		fmt.Printf("Kafka bootstrap URL: %v\n", kafkaBrokers)
		fmt.Printf("Results port: %v\n", port)
	}
	if command == topiclist || command == rmtopics || command == offsets {
		fmt.Printf("Topic: %v\n", topic)
	}
	if command == read || command == compute || command == results {
		fmt.Printf("With metrics exposition: %v\n", withMetrics)
	}
}

// parses the --years command line param
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

// parses the --months command line param
func parseMonths() bool {
	valid := map[string]int {"jan":0,"feb":0,"mar":0,"apr":0,"may":0,"jun":0,"jul":0,"aug":0,"sep":0,"oct":0,"nov":0,"dec":0}
	if months == "*" {
		for k := range valid {
			monthsArr = append(monthsArr, k)
		}
		return true
	}
	for _, s := range strings.Split(months, ",") {
		if _, ok := valid[s]; !ok {
			return false
		}
		monthsArr = append(monthsArr, s)
	}
	return true
}
