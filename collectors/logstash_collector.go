package collectors

import (
	"encoding/json"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

type logstashCollector struct {
	nodeStatsUri         string
	up                   *prometheus.Desc
	eventsIn             *prometheus.Desc
	eventsFiltered       *prometheus.Desc
	eventsOut            *prometheus.Desc
	flowInputThroughput  *prometheus.Desc
	flowFilterThroughput *prometheus.Desc
	flowOutputThroughput *prometheus.Desc
}

type Stats map[string]interface{}

func NewLogstashCollector(host string) prometheus.Collector {
	fqName := func(name string) string {
		return "logstash_" + name
	}
	return &logstashCollector{
		nodeStatsUri: "http://" + host + "/_node/stats",
		up: prometheus.NewDesc(
			"up",
			"Was the last query successful.",
			nil, nil,
		),
		eventsIn: prometheus.NewDesc(
			fqName("events_in"),
			"Number of logstash input events.",
			nil, nil,
		),
		eventsFiltered: prometheus.NewDesc(
			fqName("events_filtered"),
			"Number of logstash filtered events.",
			nil, nil,
		),
		eventsOut: prometheus.NewDesc(
			fqName("events_out"),
			"Number of logstash output events.",
			nil, nil,
		),
		flowInputThroughput: prometheus.NewDesc(
			fqName("flow_input_throughput"),
			"Throughput of logstash event input.",
			nil, nil,
		),
		flowFilterThroughput: prometheus.NewDesc(
			fqName("flow_filter_throughput"),
			"Throughput of logstash event filter.",
			nil, nil,
		),
		flowOutputThroughput: prometheus.NewDesc(
			fqName("flow_output_throughput"),
			"Throughput of logstash event output.",
			nil, nil,
		),
	}

}

func (c *logstashCollector) Collect(ch chan<- prometheus.Metric) {

	stats, err := c.fetchStats()
	if err != nil {
		log.Println("ERROR:", err)
	} else {
		c.collectMetrics(stats, ch)
	}
	ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, 1)
}

func (c *logstashCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.eventsIn
	ch <- c.eventsFiltered
	ch <- c.eventsOut
	ch <- c.flowInputThroughput
	ch <- c.flowFilterThroughput
	ch <- c.flowOutputThroughput
}

func (c *logstashCollector) fetchStats() (*Stats, error) {
	body, err := c.fetch(c.nodeStatsUri)
	if err != nil {
		return nil, err
	}

	var stats Stats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

func (c *logstashCollector) fetch(uri string) ([]byte, error) {
	client := http.Client{
		Timeout: time.Second * 3,
	}

	resp, err := client.Get(uri)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Println("ERROR:", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (c *logstashCollector) collectMetrics(stats *Stats, ch chan<- prometheus.Metric) {
	if tree, ok := (*stats)["events"]; ok {
		c.collectEvents(tree, prometheus.Labels{}, ch)
	}
	if tree, ok := (*stats)["flow"]; ok {
		c.collectFlow(tree, prometheus.Labels{}, ch)
	}
}

type Event struct {
	In       int `json:"in"`
	Out      int `json:"out"`
	Filtered int `json:"filtered"`
}

type Flow struct {
	InputThroughput  Throughput `json:"input_throughput"`
	OutputThroughput Throughput `json:"output_throughput"`
	FilterThroughput Throughput `json:"filter_throughput"`
}

type Throughput struct {
	Current float64 `json:"current"`
}

func (c *logstashCollector) collectEvents(data interface{}, labels prometheus.Labels, ch chan<- prometheus.Metric) {
	labelNames := make([]string, 0)
	for k := range labels {
		labelNames = append(labelNames, k)
	}
	body, err := json.Marshal(data)
	if err != nil {
		log.Println("ERROR:", err)
		return
	}
	event := &Event{}
	err = json.Unmarshal(body, event)
	if err != nil {
		log.Println("ERROR:", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.eventsIn, prometheus.GaugeValue, float64(event.In), labelNames...)
	ch <- prometheus.MustNewConstMetric(c.eventsOut, prometheus.GaugeValue, float64(event.Out), labelNames...)
	ch <- prometheus.MustNewConstMetric(c.eventsFiltered, prometheus.GaugeValue, float64(event.Filtered), labelNames...)
}

func (c *logstashCollector) collectFlow(data interface{}, labels prometheus.Labels, ch chan<- prometheus.Metric) {
	labelNames := make([]string, 0)
	for k := range labels {
		labelNames = append(labelNames, k)
	}
	body, err := json.Marshal(data)
	if err != nil {
		log.Println("ERROR:", err)
		return
	}
	flow := &Flow{}
	err = json.Unmarshal(body, flow)
	if err != nil {
		log.Println("ERROR:", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.flowInputThroughput, prometheus.GaugeValue, flow.InputThroughput.Current, labelNames...)
	ch <- prometheus.MustNewConstMetric(c.flowFilterThroughput, prometheus.GaugeValue, flow.OutputThroughput.Current, labelNames...)
	ch <- prometheus.MustNewConstMetric(c.flowOutputThroughput, prometheus.GaugeValue, flow.FilterThroughput.Current, labelNames...)
}
