package main

import (
	"context"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var TestCounter Counter
var TestCounterVec CounterVec
var TestGauge Gauge
var TestGaugeVec GaugeVec
var server *http.Server

const address = ":9090"

func startMetrics() {
	server = &http.Server{Addr: address, Handler: promhttp.Handler()}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatalf("Instrumentation was unable to start Prometheus HTTP server. The error is: %v\n", err)
		}
	}()
}

func stopMetrics() {
	if server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Print("[ERROR] Unable to stop Prometheus HTTP")
		}
		server = nil
	}
}

func testMetrics() {
	for  i := 0; i < 10000; i++ {
		TestCounter.Inc()
		TestCounterVec.With(prometheus.Labels{"thread": "t1"}).Inc()
		TestCounterVec.With(prometheus.Labels{"thread": "t2"}).Inc()
		TestGauge.Set(rand.Float64())
		TestGaugeVec.With(prometheus.Labels{"thread": "t1"}).Set(rand.Float64())
		TestGaugeVec.With(prometheus.Labels{"thread": "t2"}).Set(rand.Float64())
		time.Sleep(500 * time.Millisecond)
	}
}

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

func init() {
	InitMetrics()
}

func InitMetrics() {
	TestCounter = NewCounter(
		prometheus.CounterOpts{
			Name: "test_counter",
			Help: "Test Counter",
		},
	)
	TestCounterVec = NewCounterVec(
		prometheus.CounterOpts{
			Name: "test_counter_vec",
			Help: "Test Counter Vec",
		},
		[]string{"thread"},
	)

	TestGauge = NewGauge(
		prometheus.GaugeOpts{
			Name: "test_gauge",
			Help: "Test Gauge",
		},
	)
	TestGaugeVec = NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "test_gauge_vec",
			Help: "Test Gauge Vec",
		},
		[]string{"thread"},
	)
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
