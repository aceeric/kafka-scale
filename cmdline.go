package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// initialize command line args and default values
func init() {
	flag.StringVar(&years, "years", "", "Years. E.g. --years=2015,2016. Ignored unless role is 'read'")
	flag.StringVar(&months, "months", "", "Months. E.g. --months=jan,feb. Ignored unless role is 'read'. Asterisk (*) is also allowed, meaning 'all'")
	flag.StringVar(&kafkaBrokers, "kafka", "", "Kafka broker URLs. E.g. 192.168.0.45:32355,192.168.0.46:32355,192.168.0.47:32355")
	flag.IntVar(&chunkCount, "chunks", -1, "Chunk count - the number of chunks of census data to read or calculate. If omitted, or -1, then all")
	flag.BoolVar(&dryRun, "dry-run", false, "Displays how the command would run, but doesn't actually run it")
	flag.StringVar(&writeTo, "write-to", writeToKafka, "Where to send the output of the read and compute commands. Valid values are: 'kafka', 'stdout', and 'null'")
	flag.IntVar(&partitionCnt, "compute-topic-partitions", 1, "Partitions for the compute topic. Tune to the number of compute pods")
	flag.IntVar(&replicationFactor, "compute-topic-replfactor", 1, "Replication factor for the compute topic. Tune to your Kafka cluster size")
	flag.StringVar(&fromFile, "from-file", "", "FQPN of census file to load (i.e. don't download from the census site - use a file on the filesystem). Also requires you to specify a year via the --years option")
	flag.StringVar(&topic, "topic", "", "If listing offsets, this is the topic for which to list offsets. If deleting topics, this is a comma-separated list of topics to delete")
	flag.BoolVar(&verbose, "verbose", false, "Prints verbose diagnostic messages")
	flag.IntVar(&resultsPort, "results-port", 8888, "REST endpoint port for results")
	flag.IntVar(&delay, "delay", 0, "slows down processing by introducing a delay in the processing loops. Value is millis. Supports testing")
	flag.BoolVar(&withMetrics, "with-metrics", false, "Enables Prometheus metrics exposition")
	flag.StringVar(&metricsPort, "metrics-port", "9123", "The Prometheus metrics exposition port")
	flag.BoolVar(&printVersion, "version", false, "Prints the version number and exits")
	flag.BoolVar(&noShutdownReader, "no-shutdown-reader", false, "If true, leaves the reader running (inactive) after all gzips have been processed and chunked")
	flag.BoolVar(&force, "force", false, "Forces some commands. So far - only applies to the rmtopics command")
}

var validCommands = []string {read, compute, results, topiclist, offsets, rmtopics}

var version = "1.0.1"

// Validate the command line
func validateCmdline() bool {
	flag.Parse()
	tmp := flag.Args()

	if len(os.Args) == 1 || printVersion {
		fmt.Printf("kakfa-scale version: %v\n", version)
		return false
	}

	if len(tmp) == 0 {
		fmt.Printf("no command specified\n")
		return false
	} else if len(tmp) != 1 {
		fmt.Printf("only one command supported: %v\n", tmp)
		return false
	} else {
		for _, cmd := range validCommands {
			if tmp[0] == cmd {
				command = tmp[0]
				break
			}
		}
		if command == "" {
			fmt.Printf("unknown command: %v\n", tmp)
			return false
		}
	}
	if writeTo != writeToKafka && writeTo != writeToStdout && writeTo != WriteToNull {
		fmt.Printf("unknown value %v for --write-to\n", writeTo)
		return false
	}
	needKafkaUrl := false
	if (command == results || command == rmtopics || command == topiclist || command == compute) || (command == read && writeTo == writeToKafka) {
		needKafkaUrl = true
	}
	if needKafkaUrl && kafkaBrokers == "" {
		fmt.Printf("need Kafka cluster broker URL(s)\n")
		return false
	} else if command == read && fromFile == "" && (years == "" || months == "") {
		fmt.Printf("if command is 'read' then '--years' and '--months' are both required\n")
		return false
	} else if command == read && fromFile != "" && years == "" {
		fmt.Printf("if command is 'read' and --from-file is specified, then '--years' is required with one value like --years=2018 - that being the year of the file\n")
		return false
	} else if months != "" && !parseMonths() {
		fmt.Printf("Can't parse months: %v. Must be comma-separated and abbreviated like '--months=jan,feb,mar' etc. ('*' is also allowed)\n", months)
		return false
	} else if years != "" && !parseYears() {
		fmt.Printf("Can't parse years: %v. Must be comma-separated and each year between 1970 and 2020 inclusive like --years=2019,2020\n", years)
		return false
	} else if (command == rmtopics || command == offsets) && topic == "" {
		fmt.Printf("Must specify --topic with 'rmtopics' nad 'offsets' commands\n")
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
		fmt.Printf("Write to: %v\n", writeTo)
		fmt.Printf("Kafka bootstrap URL: %v\n", kafkaBrokers)
		if command == read && writeTo == writeToKafka {
			fmt.Printf("Compute Topic Partitions: %v\n", partitionCnt)
			fmt.Printf("Compute Topic Replication Factor: %v\n", replicationFactor)
		}
	}
	if command == results {
		fmt.Printf("Kafka bootstrap URL: %v\n", kafkaBrokers)
		fmt.Printf("Results port: %v\n", resultsPort)
	}
	if command == topiclist || command == rmtopics || command == offsets {
		fmt.Printf("Topic: %v\n", topic)
	}
	if command == read || command == compute || command == results {
		fmt.Printf("Metrics exposition: %v\n", withMetrics)
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
