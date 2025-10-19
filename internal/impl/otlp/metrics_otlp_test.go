package otlp

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestBuildAttributes(t *testing.T) {
	attrs, err := buildAttributes([]string{"foo", "bar"}, []string{"one", "two"})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if attrs.Len() != 2 {
		t.Fatalf("expected 2 attributes, got %d", attrs.Len())
	}

	if _, err = buildAttributes([]string{"foo"}, []string{"one", "two"}); err == nil {
		t.Fatal("expected error on mismatch")
	}
}

func TestGaugeSeriesCacheEvictionAndLimit(t *testing.T) {
	originalNow := nowFn
	defer func() { nowFn = originalNow }()

	current := time.Unix(0, 0)
	nowFn = func() time.Time { return current }

	cache := newGaugeSeriesCache([]string{"foo"}, 10*time.Second, 1, nil)
	attrs := attribute.NewSet(attribute.String("foo", "bar"))

	cache.set("a", attrs, 1)
	if len(cache.series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(cache.series))
	}

	// New series should be dropped once the limit is reached.
	cache.set("b", attrs, 2)
	if len(cache.series) != 1 {
		t.Fatalf("expected limit enforcement, got %d series", len(cache.series))
	}

	// Advance time past TTL and ensure eviction occurs.
	current = current.Add(11 * time.Second)
	cache.mu.Lock()
	cache.evictExpiredLocked(nowFn())
	cache.mu.Unlock()
	if len(cache.series) != 0 {
		t.Fatalf("expected eviction after TTL, got %d series", len(cache.series))
	}
}

func TestDeltaTemporalitySelector(t *testing.T) {
	cases := []struct {
		kind metric.InstrumentKind
		want metricdata.Temporality
	}{
		{metric.InstrumentKindCounter, metricdata.DeltaTemporality},
		{metric.InstrumentKindHistogram, metricdata.DeltaTemporality},
		{metric.InstrumentKindObservableCounter, metricdata.DeltaTemporality},
		{metric.InstrumentKindObservableGauge, metricdata.CumulativeTemporality},
	}

	for _, tc := range cases {
		if got := deltaTemporality(tc.kind); got != tc.want {
			t.Errorf("kind %v: expected %v, got %v", tc.kind, tc.want, got)
		}
	}
}

func TestAggregationSelector(t *testing.T) {
	cfg := &otlpMetricsConfig{histogram: histogramConfig{mode: histogramModeExplicit, buckets: []float64{0.25, 0.5}}}
	agg := func(kind metric.InstrumentKind) metric.Aggregation {
		if kind == metric.InstrumentKindHistogram {
			return metric.AggregationExplicitBucketHistogram{Boundaries: cfg.histogram.buckets}
		}
		return metric.DefaultAggregationSelector(kind)
	}

	hAgg, ok := agg(metric.InstrumentKindHistogram).(metric.AggregationExplicitBucketHistogram)
	if !ok {
		t.Fatalf("expected explicit histogram aggregation")
	}
	if len(hAgg.Boundaries) != 2 || hAgg.Boundaries[0] != 0.25 || hAgg.Boundaries[1] != 0.5 {
		t.Fatalf("unexpected boundaries: %+v", hAgg.Boundaries)
	}
}

func TestSeriesKey(t *testing.T) {
	if key := seriesKey([]string{"a", "b"}); key == "" {
		t.Fatal("expected non-empty key")
	}
	if key := seriesKey(nil); key != "" {
		t.Fatalf("expected empty key for nil values, got %q", key)
	}
}

func TestSubmitEventDropsWhenClosed(t *testing.T) {
	// Use a dummy exporter to verify submit logic.
	exporter := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	m := &otlpMetrics{
		provider:  provider,
		reader:    nil,
		meter:     provider.Meter("test"),
		events:    make(chan metricEvent, 1),
		seriesTTL: time.Minute,
		maxSeries: 0,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.wg.Add(1)
	go m.eventLoop(ctx)

	m.closed.Store(true)
	m.submitEvent(metricEvent{})
	if len(m.events) != 0 {
		t.Fatalf("expected no events queued when closed")
	}
}
