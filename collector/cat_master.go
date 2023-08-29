package collector

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
)

type CatMaster struct {
	logger log.Logger
	client *http.Client
	url    *url.URL

	masterVec CatMasterMetric
}

func (c *CatMaster) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.masterVec.Desc
}

type CatMasterMetric struct {
	Type  prometheus.ValueType
	Desc  *prometheus.Desc
	Value float64
}

func NewCatMaster(logger log.Logger, client *http.Client, url *url.URL) *CatMaster {
	res := CatMasterMetric{
		Type: prometheus.GaugeValue,
		Desc: prometheus.NewDesc(
			prometheus.BuildFQName("elasticsearch", "cluster_role", "master"),
			"The master role in the master node.",
			[]string{"master_role_name"}, nil,
		),
		Value: 1,
	}

	return &CatMaster{
		logger:    logger,
		client:    client,
		url:       url,
		masterVec: res,
	}
}

func (c *CatMaster) fetchAndDecodeCatMaster() (string, error) {
	u := *c.url
	u.Path = path.Join(u.Path, "/_cat/master")
	res, err := c.client.Get(u.String())
	if err != nil {
		return "", fmt.Errorf("failed to get cluster health from %s://%s:%s%s: %s",
			u.Scheme, u.Hostname(), u.Port(), u.Path, err)
	}

	defer func() {
		err = res.Body.Close()
		if err != nil {
			_ = level.Warn(c.logger).Log(
				"msg", "failed to close http.Client",
				"err", err,
			)
		}
	}()

	if res.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP Request failed with code %d", res.StatusCode)
	}

	bts, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %s", err)
	}
	responseString := string(bts)
	fields := strings.Fields(responseString)
	if len(fields) >= 4 {
		return fields[3], nil
	} else {
		return "", fmt.Errorf("unexpected response format: %s", responseString)
	}
}
func (c *CatMaster) Collect(ch chan<- prometheus.Metric) {

	catMasterInfo, err := c.fetchAndDecodeCatMaster()
	if err != nil {
		//c.masterVec.Set(0)
		_ = level.Warn(c.logger).Log(
			"msg", "failed to fetch and decode cat master",
			"err", err,
		)
		return
	}

	ch <- prometheus.MustNewConstMetric(
		c.masterVec.Desc,
		c.masterVec.Type,
		c.masterVec.Value,
		catMasterInfo,
	)
}
