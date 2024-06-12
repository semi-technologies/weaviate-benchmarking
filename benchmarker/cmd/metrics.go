package cmd

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/common/expfmt"
	log "github.com/sirupsen/logrus"
)

type Memstats struct {
	HeapAllocBytes float64 `json:"heap_alloc_bytes"`
	HeapInuseBytes float64 `json:"heap_inuse_bytes"`
	HeapSysBytes   float64 `json:"heap_sys_bytes"`
}

func readMemoryMetrics(cfg *Config) (*Memstats, error) {
	prometheusURL := fmt.Sprintf("http://%s/metrics", strings.Replace(cfg.HttpOrigin, "8080", "2112", -1))
	response, err := http.Get(prometheusURL)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code %d", response.StatusCode)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	bodyReader := strings.NewReader(string(body))
	parser := expfmt.TextParser{}
	metrics, err := parser.TextToMetricFamilies(bodyReader)
	if err != nil {
		return nil, err
	}

	var memstats Memstats

	if metric, ok := metrics["go_memstats_heap_alloc_bytes"]; ok {
		memstats.HeapAllocBytes = metric.Metric[0].GetGauge().GetValue()
	}

	if metric, ok := metrics["go_memstats_heap_inuse_bytes"]; ok {
		memstats.HeapInuseBytes = metric.Metric[0].GetGauge().GetValue()
	}

	if metric, ok := metrics["go_memstats_heap_sys_bytes"]; ok {
		memstats.HeapSysBytes = metric.Metric[0].GetGauge().GetValue()
	}

	return &memstats, nil
}

func waitTombstonesEmpty(cfg *Config) error {

	prometheusURL := fmt.Sprintf("http://%s/metrics", strings.Replace(cfg.HttpOrigin, "8080", "2112", -1))
	metricName := "vector_index_tombstones"

	log.Printf("Waiting to allow for tombstone cleanup\n")

	start := time.Now()

	for {
		response, err := http.Get(prometheusURL)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("HTTP request failed with status code %d", response.StatusCode)
		}

		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			return err
		}
		bodyReader := strings.NewReader(string(body))

		parser := expfmt.TextParser{}
		metrics, err := parser.TextToMetricFamilies(bodyReader)
		if err != nil {
			return err
		}

		var totalSum float64 = 0
		if vectorMetric, ok := metrics[metricName]; ok {
			for _, m := range vectorMetric.Metric {
				value := m.GetGauge().GetValue()
				totalSum += value
			}
		}

		if totalSum == 0 {
			break
		}

		time.Sleep(time.Second * 10)
	}

	log.WithFields(log.Fields{"duration": time.Since(start)}).Infof("Tombstones empty\n")

	return nil
}
