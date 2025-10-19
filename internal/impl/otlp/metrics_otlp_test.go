package otlp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
)

type captureMetricsServer struct {
	collectormetricspb.UnimplementedMetricsServiceServer
	received chan *collectormetricspb.ExportMetricsServiceRequest
}

func newCaptureMetricsServer() *captureMetricsServer {
	return &captureMetricsServer{
		received: make(chan *collectormetricspb.ExportMetricsServiceRequest, 10),
	}
}

func (c *captureMetricsServer) Export(ctx context.Context, req *collectormetricspb.ExportMetricsServiceRequest) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	select {
	case c.received <- req:
	default:
	}
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

func waitForRequest(t *testing.T, srv *captureMetricsServer) *collectormetricspb.ExportMetricsServiceRequest {
	select {
	case req := <-srv.received:
		return req
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for OTLP metrics request")
	}
	return nil
}

func TestOTLPMetricsExport(t *testing.T) {
	t.Parallel()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer lis.Close()

	server := grpc.NewServer()
	t.Cleanup(server.GracefulStop)

	capture := newCaptureMetricsServer()
	collectormetricspb.RegisterMetricsServiceServer(server, capture)

	go func() {
		_ = server.Serve(lis)
	}()

	cfg := metricsConfig{
		protocol:       mmProtocolGRPC,
		endpoint:       lis.Addr().String(),
		insecure:       true,
		headers:        map[string]string{},
		exportInterval: 50 * time.Millisecond,
		temporality:    mmTemporalityCumulative,
		resourceAttrs: map[string]string{
			"service.name": "test-service",
			"env":          "test",
		},
		histogram: histogramConfig{
			mode:    mmHistogramModeExplicit,
			buckets: []float64{0.25, 0.5, 1},
		},
		seriesTTL:     time.Minute,
		maxSeries:     100,
		engineVersion: "unit-test",
	}

	metrics, err := newOtlpMetrics(cfg, nil)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, metrics.Close(context.Background()))
	}()

	counterCtor := metrics.NewCounterCtor("test_counter", "label")
	counter := counterCtor("alpha")
	counter.Incr(5)
	if ctr, ok := counter.(interface{ IncrFloat64(float64) }); ok {
		ctr.IncrFloat64(1.5)
	}

	timerCtor := metrics.NewTimerCtor("test_timing", "label")
	timer := timerCtor("alpha")
	timer.Timing(int64(750 * time.Millisecond))

	gaugeCtor := metrics.NewGaugeCtor("test_gauge", "label")
	gauge := gaugeCtor("alpha")
	gauge.Set(10)
	if inc, ok := gauge.(interface{ Incr(int64) }); ok {
		inc.Incr(5)
	} else {
		gauge.Set(15)
	}
	if g, ok := gauge.(interface{ SetFloat64(float64) }); ok {
		g.SetFloat64(12.5)
	}

	require.NoError(t, metrics.forceFlush(context.Background()))

	req := waitForRequest(t, capture)
	require.NotNil(t, req)
	require.Len(t, req.ResourceMetrics, 1)

	resMetrics := req.ResourceMetrics[0]
	require.NotNil(t, resMetrics.Resource)
	assertResourceAttributes(t, resMetrics.Resource.Attributes, map[string]string{
		"service.name":    "test-service",
		"service.version": "unit-test",
		"env":             "test",
	})

	require.Len(t, resMetrics.ScopeMetrics, 1)
	scope := resMetrics.ScopeMetrics[0]
	require.NotNil(t, scope.Scope)
	assert.Equal(t, "warpstreamlabs/bento", scope.Scope.Name)

	metricsByName := map[string]*metricspb.Metric{}
	for _, m := range scope.Metrics {
		metricsByName[m.GetName()] = m
	}

	counterMetric, ok := metricsByName["test_counter"]
	require.True(t, ok)
	verifySumMetric(t, counterMetric, 6.5, map[string]string{"label": "alpha"})

	gaugeMetric, ok := metricsByName["test_gauge"]
	require.True(t, ok)
	verifyGaugeMetric(t, gaugeMetric, 12.5, map[string]string{"label": "alpha"})

	histMetric, ok := metricsByName["test_timing"]
	require.True(t, ok)
	verifyHistogramMetric(t, histMetric, 0.75, map[string]string{"label": "alpha"})
}

func TestSeriesCacheTTLAndLimit(t *testing.T) {
	base := time.Unix(0, 0)
	cache := newSeriesCache(time.Second, 1, nil, func() time.Time { return base })

	cache.set("a", nil, 1)

	cache.now = func() time.Time { return base.Add(500 * time.Millisecond) }
	snap := cache.snapshot()
	require.Len(t, snap, 1)

	cache.now = func() time.Time { return base.Add(2 * time.Second) }
	_ = cache.snapshot()
	cache.mu.Lock()
	size := len(cache.entries)
	cache.mu.Unlock()
	assert.Equal(t, 0, size)

	cache.now = func() time.Time { return base }
	cache.set("a", nil, 2)
	cache.add("a", nil, 3)

	cache.set("b", nil, 5)
	cache.mu.Lock()
	size = len(cache.entries)
	cache.mu.Unlock()
	assert.Equal(t, 1, size)

	cache.now = func() time.Time { return base.Add(10 * time.Second) }
	_ = cache.snapshot()
	cache.mu.Lock()
	size = len(cache.entries)
	cache.mu.Unlock()
	assert.Equal(t, 0, size)
}

func assertResourceAttributes(t *testing.T, attrs []*commonpb.KeyValue, expected map[string]string) {
	actual := map[string]string{}
	for _, attr := range attrs {
		actual[attr.GetKey()] = attr.GetValue().GetStringValue()
	}
	for k, v := range expected {
		assert.Equal(t, v, actual[k], "attribute %s", k)
	}
}

func verifySumMetric(t *testing.T, metric *metricspb.Metric, expected float64, labels map[string]string) {
	sum := metric.GetSum()
	require.NotNil(t, sum)
	require.Len(t, sum.DataPoints, 1)
	dp := sum.DataPoints[0]
	assert.InDelta(t, expected, dp.GetAsDouble(), 1e-6)
	assertAttributes(t, dp.Attributes, labels)
	assert.True(t, sum.GetIsMonotonic())
	assert.Equal(t, metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE, sum.GetAggregationTemporality())
}

func verifyGaugeMetric(t *testing.T, metric *metricspb.Metric, expected float64, labels map[string]string) {
	gauge := metric.GetGauge()
	require.NotNil(t, gauge)
	require.Len(t, gauge.DataPoints, 1)
	dp := gauge.DataPoints[0]
	assert.InDelta(t, expected, dp.GetAsDouble(), 1e-6)
	assertAttributes(t, dp.Attributes, labels)
}

func verifyHistogramMetric(t *testing.T, metric *metricspb.Metric, expectedSum float64, labels map[string]string) {
	hist := metric.GetHistogram()
	require.NotNil(t, hist)
	require.Len(t, hist.DataPoints, 1)
	dp := hist.DataPoints[0]
	assert.InDelta(t, expectedSum, dp.GetSum(), 1e-6)
	assert.Equal(t, uint64(1), dp.GetCount())
	assertAttributes(t, dp.Attributes, labels)
}

func assertAttributes(t *testing.T, attrs []*commonpb.KeyValue, expected map[string]string) {
	actual := map[string]string{}
	for _, attr := range attrs {
		actual[attr.GetKey()] = attr.GetValue().GetStringValue()
	}
	for k, v := range expected {
		assert.Equal(t, v, actual[k], "attribute %s", k)
	}
}
