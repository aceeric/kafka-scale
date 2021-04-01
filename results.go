package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"
)

type HousingResult struct {
	Description string
	Count       int
}

var mu sync.Mutex

var housingResults = map[int]HousingResult{
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

// Reads from the 'results' topic indefinitely, blocking until a result is available. Each result message
// is a comma-separated list of housing codes like: 1,1,1,6,5,1,4,1,1,1,12 etc. The function splits the message
// into its codes, and the for each code, increments the count of the `housingResult` item in the `housingResults'
// map whose entry is identified by the code. Invalid codes are simply ignored. Modifications to the `housingResults'
// variable are guarded by a mutex since this data is also available for consumption via a REST endpoint.
func accumulateAndServeResults(url string) {
	go serveResults()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:       strings.Split(url, ","),
		GroupID:       consumerGrpForTopic[results_topic],
		Topic:         results_topic,
		QueueCapacity: 1,
		MinBytes:      10e3, // 10KB
		MaxBytes:      10e6, // 10MB
	})
	defer r.Close()

	if verbose {
		log.Printf("beginning read message from topic: %v\n", results_topic)
	}
	for {
		// ReadMessage blocks
		m, err := r.ReadMessage(context.Background())
		if verbose {
			log.Printf("read message from topic: %v\n", string(m.Value))
		}
		if err == nil {
			for _, code := range strings.Split(string(m.Value), ",") {
				if intCode, err := strconv.Atoi(code); err == nil {
					if result, ok := housingResults[intCode]; ok {
						mu.Lock()
						result.Count++
						housingResults[intCode] = result
						mu.Unlock()
					}
					// if the code isn't valid, just ignore it
				}
			}

		}
		//if err := r.CommitMessages(context.Background(), m); err != nil {
		//	log.Printf("error committing message: %v. the error was: %v\n", m.Key, err)
		//}
		// slow it down a tad for testing
		//time.Sleep(500 * time.Millisecond)
	}
}

func serveResults() {
	r := mux.NewRouter()
	r.HandleFunc("/results", resultsHandler)

	srv := &http.Server{
		Handler:      r,
		Addr:         "127.0.0.1:8888",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}

	log.Fatal(srv.ListenAndServe())
}

func resultsHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	js, err := json.Marshal(housingResults)
	mu.Unlock()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}
