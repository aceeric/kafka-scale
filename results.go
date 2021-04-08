package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

// synchronize access to the in-mem results struct since it can be updated and requested
// via separate goroutines
var mu sync.Mutex

// says how many of a given housing type was accumulated
type HousingResult struct {
	Description string
	Count       int
}

// HousingResults is a map. The main key is year. For each year, there is a map. The key of that
// nested map is a housing code (int) and the value of the map is the description and count for that housing code
var HousingResults = map[int]map[int]HousingResult{}

// Reads from the 'results' topic indefinitely, blocking until a result is available. Each result message
// is a comma-separated list of housing codes like: nnnn:1,1,1,6,5,1,4,1,1,1,12 etc. where nnnn is a year, and
// the values are housing codes. The function splits the message into its codes, and the for each code, increments
// the count of the `housingResult` item in the `housingResults' map whose entry is identified by the code. Invalid
// codes are simply ignored. Modifications to the `housingResults' variable are guarded by a mutex since this data
// is also available for consumption via a REST endpoint.
func resultsCmd(kafkaBrokers string, port int, verbose bool) {
	go serveResults(port)
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       strings.Split(kafkaBrokers, ","),
		GroupID:       consumerGrpForTopic[results_topic],
		Topic:         results_topic,
		QueueCapacity: 1,
		MinBytes:      10e3, // 10KB
		MaxBytes:      10e6, // 10MB
	})
	defer r.Close()

	if verbose {
		fmt.Printf("beginning read message from topic: %v\n", results_topic)
	}
	for {
		// ReadMessage blocks
		m, err := r.ReadMessage(context.Background())
		if verbose {
			fmt.Printf("read message from topic %v - message: %v\n", results_topic, string(m.Value))
		}
		resultMessagesRead.Inc()
		if err == nil {
			messageParts := strings.Split(string(m.Value), ":")
			year, _ := strconv.Atoi(messageParts[0])
			var housingResult map[int]HousingResult
			var ok = false
			if housingResult, ok = HousingResults[year]; !ok {
				housingResult = newHousingResults(year)
			}
			for _, codeStr := range strings.Split(string(messageParts[1]), ",") {
				if code, err := strconv.Atoi(codeStr); err == nil {
					if result, ok := housingResult[code]; ok {
						result.Count++
						housingResult[code] = result
					}
					// if the code isn't valid, just ignore it
				}
			}
			mu.Lock()
			HousingResults[year] = housingResult
			mu.Unlock()
		}
	}
}

func newHousingResults(year int) map[int]HousingResult {
	HousingResults[year] = map[int]HousingResult{
		0:  {"OTHER UNIT", 0},
		1:  {"HOUSE, APARTMENT, FLAT", 0},
		2:  {"HU IN NONTRANSIENT HOTEL, MOTEL, ETC.", 0},
		3:  {"HU PERMANENT IN TRANSIENT HOTEL, MOTEL", 0},
		4:  {"HU IN ROOMING HOUSE", 0},
		5:  {"MOBILE HOME OR TRAILER W/NO PERM. ROOM ADDED", 0},
		6:  {"MOBILE HOME OR TRAILER W/1 OR MORE PERM. ROOMS ADDED", 0},
		7:  {"HU NOT SPECIFIED ABOVE", 0},
		8:  {"QUARTERS NOT HU IN ROOMING OR BRDING HS", 0},
		9:  {"UNIT NOT PERM. IN TRANSIENT HOTL, MOTL", 0},
		10: {"UNOCCUPIED TENT SITE OR TRLR SITE", 0},
		11: {"STUDENT QUARTERS IN COLLEGE DORM", 0},
		12: {"OTHER UNIT NOT SPECIFIED ABOVE", 0},
	}
	return HousingResults[year]
}

func serveResults(port int) {
	fmt.Printf("Starting http server on port: %v\n", port)

	r := mux.NewRouter()
	r.HandleFunc("/results", resultsHandler)

	// address can't be loopback - does not work in cluster - possibly I need to configure the pod
	// networking to handle that? Anyway - the ":PORT" form used below works
	srv := &http.Server{
		Handler:      r,
		Addr:         ":" + strconv.Itoa(port),
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	fmt.Printf("Results server terminated with result: %v\n", srv.ListenAndServe())
}

func resultsHandler(w http.ResponseWriter, r *http.Request) {
	// todo don't ref global var
	if verbose {
		fmt.Printf("Http response handler invoked\n")
	}
	mu.Lock()
	js, err := json.Marshal(HousingResults)
	defer mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(js)
}
