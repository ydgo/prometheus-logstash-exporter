package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/ydgo/prometheus-logstash-exporter/collectors"
	"net/http"
)

func main() {
	reg := prometheus.NewRegistry()
	exporter := collectors.NewLogstashCollector("localhost:9600")
	reg.MustRegister(exporter)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	_ = http.ListenAndServe(":8080", nil)

}
