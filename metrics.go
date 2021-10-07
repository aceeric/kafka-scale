package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var downloadedGZips Counter
var chunksWritten Counter
var computeMessagesRead Counter
var resultMessagesWritten Counter
var resultMessagesRead Counter

// these are just to have handy to clone
//var TestCounterVec CounterVec
//var TestGauge Gauge
//var TestGaugeVec GaugeVec

var server *http.Server

// starts the prometheus metrics server
func startMetrics(MetricsPort string, command string) {
	server = &http.Server{Addr: ":" + MetricsPort, Handler: promhttp.Handler()}
	fmt.Printf("starting prometheus metrics http server on port %v\n", MetricsPort)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			fmt.Printf("metrics server exited with result: %v\n", err)
		}
	}()
	fmt.Printf("metrics server is running\n")
}

// stops the prometheus metrics server
func stopMetrics() {
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			fmt.Print("instrumentation was unable to stop the Prometheus HTTP server")
		}
		server = nil
	}
}

//func testMetrics() {
//	for i := 0; i < 10000; i++ {
//		TestCounterVec.With(prometheus.Labels{"thread": "t1"}).Inc()
//		TestCounterVec.With(prometheus.Labels{"thread": "t2"}).Inc()
//		TestGauge.Set(rand.Float64())
//		TestGaugeVec.With(prometheus.Labels{"thread": "t1"}).Set(rand.Float64())
//		TestGaugeVec.With(prometheus.Labels{"thread": "t2"}).Set(rand.Float64())
//		time.Sleep(500 * time.Millisecond)
//	}
//}

func initMetrics(command string) {
	switch command {
	case read:
		downloadedGZips = NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_scale_downloaded_gzips",
				Help: "The Count of census gzip files downloaded from the US Census website",
			},
		)
		chunksWritten = NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_scale_chunks_written",
				Help: fmt.Sprintf("The Count of Census data chunks written by the read command to the %v topic", compute_topic),
			},
		)
	case compute:
		computeMessagesRead = NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_scale_compute_messages_read",
				Help: fmt.Sprintf("The Count of messages read by the compute command from the %v topic", compute_topic),
			},
		)
		resultMessagesWritten = NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_scale_result_messages_written",
				Help: fmt.Sprintf("The Count of messages written by the compute command to the %v topic", results_topic),
			},
		)
	case results:
		resultMessagesRead = NewCounter(
			prometheus.CounterOpts{
				Name: "kafka_scale_result_messages_read",
				Help: fmt.Sprintf("The Count of messages read by the result command from the %v topic", results_topic),
			},
		)
	}

	//TestCounterVec = NewCounterVec(
	//	prometheus.CounterOpts{
	//		Name: "test_counter_vec",
	//		Help: "Test Counter Vec",
	//	},
	//	[]string{"thread"},
	//)
	//TestGauge = NewGauge(
	//	prometheus.GaugeOpts{
	//		Name: "test_gauge",
	//		Help: "Test Gauge",
	//	},
	//)
	//TestGaugeVec = NewGaugeVec(
	//	prometheus.GaugeOpts{
	//		Name: "test_gauge_vec",
	//		Help: "Test Gauge Vec",
	//	},
	//	[]string{"thread"},
	//)
}

// below is the thin interface layer to prometheus metrics

type Counter interface {
	Inc()
}

type CounterVec interface {
	With(labels prometheus.Labels) Counter
}

type Gauge interface {
	Set(val float64)
}

type GaugeVec interface {
	With(labels prometheus.Labels) Gauge
}

type promCounterVec struct {
	vec *prometheus.CounterVec
}

type promGaugeVec struct {
	vec *prometheus.GaugeVec
}

func (cv *promGaugeVec) With(labels prometheus.Labels) Gauge {
	return cv.vec.With(labels)
}

func (cv *promCounterVec) With(labels prometheus.Labels) Counter {
	return cv.vec.With(labels)
}

func NewCounter(opts prometheus.CounterOpts) Counter {
	return promauto.NewCounter(opts)
}

func NewCounterVec(opts prometheus.CounterOpts, labelNames []string) CounterVec {
	return &promCounterVec{
		promauto.NewCounterVec(opts, labelNames),
	}
}

func NewGauge(opts prometheus.GaugeOpts) Gauge {
	return promauto.NewGauge(opts)
}

func NewGaugeVec(opts prometheus.GaugeOpts, labelNames []string) GaugeVec {
	return &promGaugeVec{
		promauto.NewGaugeVec(opts, labelNames),
	}
}
