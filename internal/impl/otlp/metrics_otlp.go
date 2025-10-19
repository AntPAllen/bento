package otlp

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	metricsFieldProtocol           = "protocol"
	metricsFieldEndpoint           = "endpoint"
	metricsFieldHeaders            = "headers"
	metricsFieldInsecure           = "insecure"
	metricsFieldExportInterval     = "export_interval"
	metricsFieldTemporality        = "temporality"
	metricsFieldResourceAttributes = "resource_attributes"
	metricsFieldHistogram          = "histogram"
	metricsFieldSeriesTTL          = "series_ttl"
	metricsFieldMaxSeries          = "max_series"

	histogramFieldMode    = "mode"
	histogramFieldBuckets = "buckets"
)

const (
	protocolGRPC = "grpc"
	protocolHTTP = "http"

	temporalityCumulative = "cumulative"
	temporalityDelta      = "delta"

	histogramModeExponential = "exponential"
	histogramModeExplicit    = "explicit"

	defaultSeriesTTL = 5 * time.Minute
)

var nowFn = time.Now

type otlpMetricsConfig struct {
	engineVersion  string
	protocol       string
	endpoint       string
	headers        map[string]string
	insecure       bool
	exportInterval time.Duration
	temporality    string
	resourceAttrs  map[string]string
	histogram      histogramConfig
	seriesTTL      time.Duration
	maxSeries      int
}

type histogramConfig struct {
	mode    string
	buckets []float64
}

func metricsSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary("Export Bento metrics to an [OpenTelemetry collector](https://opentelemetry.io/docs/collector/) using the OTLP protocol.").
		Description("This exporter uses the OpenTelemetry metrics SDK. Timings are converted from nanoseconds into seconds before they are recorded, matching Bento's other histogram exporters.").
		Fields(
			service.NewStringEnumField(metricsFieldProtocol, protocolGRPC, protocolHTTP).
				Description("Protocol to use when sending OTLP metrics. `grpc` targets the OTLP gRPC endpoint (default), whilst `http` targets the OTLP HTTP/protobuf endpoint.").
				Default(protocolGRPC),
			service.NewStringField(metricsFieldEndpoint).
				Description("Endpoint of the collector to send metrics to. For gRPC this should be host:port (e.g. `localhost:4317`). For HTTP the value should either be host:port or a full URL (e.g. `http://localhost:4318`).").
				Default(""),
			service.NewStringMapField(metricsFieldHeaders).
				Description("Additional headers to attach to requests. Applies to both gRPC metadata and HTTP headers.").
				Advanced().
				Default(map[string]string{}),
			service.NewBoolField(metricsFieldInsecure).
				Description("Disable TLS when connecting to the collector. When using HTTP this will switch to HTTP instead of HTTPS.").
				Advanced().
				Default(false),
			service.NewDurationField(metricsFieldExportInterval).
				Description("How frequently metrics should be exported to the collector. Defaults to the OpenTelemetry SDK default of one minute when left empty.").
				Advanced().
				Optional(),
			service.NewStringEnumField(metricsFieldTemporality, temporalityCumulative, temporalityDelta).
				Description("Aggregation temporality used for counters and histograms. `cumulative` is the OTLP default. `delta` emits deltas for counter and histogram instruments whilst keeping gauges cumulative.").
				Advanced().
				Default(temporalityCumulative),
			service.NewStringMapField(metricsFieldResourceAttributes).
				Description("Additional OpenTelemetry resource attributes to attach to every metric. By default `service.name` is set to `bento` and `service.version` is populated from the running engine version when not overridden.").
				Advanced().
				Default(map[string]string{}),
			service.NewObjectField(metricsFieldHistogram,
				service.NewStringEnumField(histogramFieldMode, histogramModeExponential, histogramModeExplicit).
					Description("Histogram aggregation mode to use for timing metrics. `exponential` uses the OpenTelemetry exponential histogram (default). `explicit` allows providing explicit bucket boundaries.").
					Default(histogramModeExponential),
				service.NewFloatListField(histogramFieldBuckets).
					Description("Explicit histogram bucket boundaries (in seconds) used when `mode` is `explicit`. Leave empty to use the OpenTelemetry defaults.").
					Default([]any{}).
					Advanced(),
			).
				Description("Histogram aggregation behaviour.").
				Advanced().
				Default(map[string]any{}),
			service.NewDurationField(metricsFieldSeriesTTL).
				Description("How long a gauge series should be retained without updates before being evicted. Set to `0s` to disable eviction.").
				Advanced().
				Default(defaultSeriesTTL.String()),
			service.NewIntField(metricsFieldMaxSeries).
				Description("Maximum number of concurrent gauge series cached for callbacks. Additional series are dropped once the cache is full. Set to 0 to disable the limit.").
				Advanced().
				Default(0),
		)
}

func init() {
	if err := service.RegisterMetricsExporter("otlp", metricsSpec(), func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
		cfg, err := metricsConfigFromParsed(conf)
		if err != nil {
			return nil, err
		}
		return newOtlpMetrics(cfg, log)
	}); err != nil {
		panic(err)
	}
}

func metricsConfigFromParsed(conf *service.ParsedConfig) (*otlpMetricsConfig, error) {
	protocol, err := conf.FieldString(metricsFieldProtocol)
	if err != nil {
		return nil, err
	}
	protocol = strings.ToLower(protocol)
	if protocol != protocolGRPC && protocol != protocolHTTP {
		return nil, fmt.Errorf("metrics.otlp protocol '%s' not recognised", protocol)
	}

	endpoint, err := conf.FieldString(metricsFieldEndpoint)
	if err != nil {
		return nil, err
	}
	endpoint = strings.TrimSpace(endpoint)

	headers, err := conf.FieldStringMap(metricsFieldHeaders)
	if err != nil {
		return nil, err
	}

	insecure, err := conf.FieldBool(metricsFieldInsecure)
	if err != nil {
		return nil, err
	}

	var exportInterval time.Duration
	if conf.Contains(metricsFieldExportInterval) {
		if exportInterval, err = conf.FieldDuration(metricsFieldExportInterval); err != nil {
			return nil, err
		}
	}

	temporality, err := conf.FieldString(metricsFieldTemporality)
	if err != nil {
		return nil, err
	}
	temporality = strings.ToLower(temporality)
	if temporality != temporalityCumulative && temporality != temporalityDelta {
		return nil, fmt.Errorf("metrics.otlp temporality '%s' not recognised", temporality)
	}

	resourceAttrs, err := conf.FieldStringMap(metricsFieldResourceAttributes)
	if err != nil {
		return nil, err
	}

	histParsed := conf.Namespace(metricsFieldHistogram)
	mode := histogramModeExponential
	if histParsed.Contains(histogramFieldMode) {
		if mode, err = histParsed.FieldString(histogramFieldMode); err != nil {
			return nil, err
		}
		mode = strings.ToLower(mode)
		if mode != histogramModeExponential && mode != histogramModeExplicit {
			return nil, fmt.Errorf("metrics.otlp histogram mode '%s' not recognised", mode)
		}
	}

	var buckets []float64
	if histParsed.Contains(histogramFieldBuckets) {
		if buckets, err = histParsed.FieldFloatList(histogramFieldBuckets); err != nil {
			return nil, err
		}
	}

	seriesTTL, err := conf.FieldDuration(metricsFieldSeriesTTL)
	if err != nil {
		return nil, err
	}
	if seriesTTL < 0 {
		seriesTTL = 0
	}

	maxSeries, err := conf.FieldInt(metricsFieldMaxSeries)
	if err != nil {
		return nil, err
	}
	if maxSeries < 0 {
		maxSeries = 0
	}

	if endpoint == "" {
		switch protocol {
		case protocolGRPC:
			endpoint = "localhost:4317"
		case protocolHTTP:
			endpoint = "localhost:4318"
		}
	}

	return &otlpMetricsConfig{
		engineVersion:  conf.EngineVersion(),
		protocol:       protocol,
		endpoint:       endpoint,
		headers:        headers,
		insecure:       insecure,
		exportInterval: exportInterval,
		temporality:    temporality,
		resourceAttrs:  resourceAttrs,
		histogram: histogramConfig{
			mode:    mode,
			buckets: buckets,
		},
		seriesTTL: seriesTTL,
		maxSeries: maxSeries,
	}, nil
}

type otlpMetrics struct {
	provider   *metric.MeterProvider
	reader     *metric.PeriodicReader
	meter      otelmetric.Meter
	log        *service.Logger
	events     chan metricEvent
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	dropLogged atomic.Bool
	closed     atomic.Bool

	gaugesMu sync.Mutex
	gauges   map[string]*gaugeRegistration

	seriesTTL time.Duration
	maxSeries int
}

type gaugeRegistration struct {
	labelKeys    []string
	instrument   otelmetric.Float64ObservableGauge
	registration otelmetric.Registration
	cache        *gaugeSeriesCache
}

type eventKind int

const (
	eventKindCounter eventKind = iota
	eventKindHistogram
)

type metricEvent struct {
	kind      eventKind
	counter   otelmetric.Float64Counter
	histogram otelmetric.Float64Histogram
	value     float64
	attrs     attribute.Set
}

func newOtlpMetrics(cfg *otlpMetricsConfig, log *service.Logger) (*otlpMetrics, error) {
	exporter, err := createExporter(cfg)
	if err != nil {
		return nil, err
	}

	readerOpts := []metric.PeriodicReaderOption{}
	if cfg.exportInterval > 0 {
		readerOpts = append(readerOpts, metric.WithInterval(cfg.exportInterval))
	}
	reader := metric.NewPeriodicReader(exporter, readerOpts...)

	res, err := buildResource(cfg)
	if err != nil {
		return nil, err
	}

	provider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(reader),
	)

	ctx, cancel := context.WithCancel(context.Background())
	m := &otlpMetrics{
		provider:  provider,
		reader:    reader,
		meter:     provider.Meter("warpstreamlabs/bento"),
		log:       log,
		events:    make(chan metricEvent, 1024),
		cancel:    cancel,
		gauges:    map[string]*gaugeRegistration{},
		seriesTTL: cfg.seriesTTL,
		maxSeries: cfg.maxSeries,
	}

	m.wg.Add(1)
	go m.eventLoop(ctx)

	return m, nil
}

func buildResource(cfg *otlpMetricsConfig) (*sdkresource.Resource, error) {
	attrs := make([]attribute.KeyValue, 0, len(cfg.resourceAttrs)+2)
	for k, v := range cfg.resourceAttrs {
		attrs = append(attrs, attribute.String(k, v))
	}

	if _, exists := cfg.resourceAttrs[string(semconv.ServiceNameKey)]; !exists {
		attrs = append(attrs, semconv.ServiceNameKey.String("bento"))
		if _, ok := cfg.resourceAttrs[string(semconv.ServiceVersionKey)]; !ok {
			attrs = append(attrs, semconv.ServiceVersionKey.String(cfg.engineVersion))
		}
	}

	return sdkresource.NewWithAttributes(semconv.SchemaURL, attrs...), nil
}

func createExporter(cfg *otlpMetricsConfig) (metric.Exporter, error) {
	aggSelector := func(kind metric.InstrumentKind) metric.Aggregation {
		if kind == metric.InstrumentKindHistogram {
			switch cfg.histogram.mode {
			case histogramModeExplicit:
				return metric.AggregationExplicitBucketHistogram{Boundaries: append([]float64(nil), cfg.histogram.buckets...)}
			case histogramModeExponential:
				return metric.AggregationBase2ExponentialHistogram{}
			}
		}
		return metric.DefaultAggregationSelector(kind)
	}

	temporalitySelector := cumulativeTemporality
	if cfg.temporality == temporalityDelta {
		temporalitySelector = deltaTemporality
	}

	switch cfg.protocol {
	case protocolGRPC:
		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(cfg.endpoint),
			otlpmetricgrpc.WithAggregationSelector(aggSelector),
			otlpmetricgrpc.WithTemporalitySelector(temporalitySelector),
		}
		if cfg.insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		if len(cfg.headers) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.headers))
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return otlpmetricgrpc.New(ctx, opts...)
	case protocolHTTP:
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithAggregationSelector(aggSelector),
			otlpmetrichttp.WithTemporalitySelector(temporalitySelector),
		}
		if strings.Contains(cfg.endpoint, "://") {
			opts = append(opts, otlpmetrichttp.WithEndpointURL(cfg.endpoint))
		} else {
			opts = append(opts, otlpmetrichttp.WithEndpoint(cfg.endpoint))
		}
		if cfg.insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		if len(cfg.headers) > 0 {
			opts = append(opts, otlpmetrichttp.WithHeaders(cfg.headers))
		}
		return otlpmetrichttp.New(context.Background(), opts...)
	default:
		return nil, fmt.Errorf("metrics.otlp protocol '%s' not supported", cfg.protocol)
	}
}

func cumulativeTemporality(metric.InstrumentKind) metricdata.Temporality {
	return metricdata.CumulativeTemporality
}

func deltaTemporality(kind metric.InstrumentKind) metricdata.Temporality {
	switch kind {
	case metric.InstrumentKindCounter,
		metric.InstrumentKindHistogram,
		metric.InstrumentKindObservableCounter:
		return metricdata.DeltaTemporality
	default:
		return metricdata.CumulativeTemporality
	}
}

func (m *otlpMetrics) eventLoop(ctx context.Context) {
	defer m.wg.Done()
	background := context.Background()
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-m.events:
			switch ev.kind {
			case eventKindCounter:
				ev.counter.Add(background, ev.value, otelmetric.WithAttributeSet(ev.attrs))
			case eventKindHistogram:
				ev.histogram.Record(background, ev.value, otelmetric.WithAttributeSet(ev.attrs))
			}
		}
	}
}

func (m *otlpMetrics) submitEvent(ev metricEvent) {
	if m.closed.Load() {
		return
	}
	select {
	case m.events <- ev:
	default:
		if !m.dropLogged.Load() && !m.dropLogged.Swap(true) {
			m.log.Warnf("metrics.otlp dropping metric events due to backpressure")
		}
	}
}

func (m *otlpMetrics) Close(ctx context.Context) error {
	if m.closed.CompareAndSwap(false, true) {
		m.cancel()
		m.wg.Wait()
		m.gaugesMu.Lock()
		for _, reg := range m.gauges {
			if reg.registration != nil {
				_ = reg.registration.Unregister()
			}
		}
		m.gaugesMu.Unlock()
	}
	return m.provider.Shutdown(ctx)
}

func (m *otlpMetrics) NewCounterCtor(name string, labelKeys ...string) service.MetricsExporterCounterCtor {
	counter, err := m.meter.Float64Counter(name)
	if err != nil {
		m.log.Errorf("metrics.otlp failed to create counter '%s': %v", name, err)
		return func(...string) service.MetricsExporterCounter { return noopCounter{} }
	}
	keys := append([]string(nil), labelKeys...)
	return func(labelValues ...string) service.MetricsExporterCounter {
		attrs, err := buildAttributes(keys, labelValues)
		if err != nil {
			m.log.Errorf("metrics.otlp counter '%s': %v", name, err)
			return noopCounter{}
		}
		return &otlpCounter{metrics: m, counter: counter, attrs: attrs}
	}
}

func (m *otlpMetrics) NewTimerCtor(name string, labelKeys ...string) service.MetricsExporterTimerCtor {
	histogram, err := m.meter.Float64Histogram(name, otelmetric.WithUnit("s"))
	if err != nil {
		m.log.Errorf("metrics.otlp failed to create histogram '%s': %v", name, err)
		return func(...string) service.MetricsExporterTimer { return noopTimer{} }
	}
	keys := append([]string(nil), labelKeys...)
	return func(labelValues ...string) service.MetricsExporterTimer {
		attrs, err := buildAttributes(keys, labelValues)
		if err != nil {
			m.log.Errorf("metrics.otlp histogram '%s': %v", name, err)
			return noopTimer{}
		}
		return &otlpTimer{metrics: m, histogram: histogram, attrs: attrs}
	}
}

func (m *otlpMetrics) NewGaugeCtor(name string, labelKeys ...string) service.MetricsExporterGaugeCtor {
	reg, err := m.getOrCreateGauge(name, labelKeys)
	if err != nil {
		m.log.Errorf("metrics.otlp failed to create gauge '%s': %v", name, err)
		return func(...string) service.MetricsExporterGauge { return noopGauge{} }
	}
	keys := append([]string(nil), reg.labelKeys...)
	return func(labelValues ...string) service.MetricsExporterGauge {
		attrs, err := buildAttributes(keys, labelValues)
		if err != nil {
			m.log.Errorf("metrics.otlp gauge '%s': %v", name, err)
			return noopGauge{}
		}
		return &otlpGauge{
			cache: reg.cache,
			key:   seriesKey(labelValues),
			attrs: attrs,
		}
	}
}

func (m *otlpMetrics) getOrCreateGauge(name string, labelKeys []string) (*gaugeRegistration, error) {
	m.gaugesMu.Lock()
	defer m.gaugesMu.Unlock()
	if existing, ok := m.gauges[name]; ok {
		if !equalStringSlices(existing.labelKeys, labelKeys) {
			return nil, fmt.Errorf("gauge '%s' re-registered with different label set", name)
		}
		return existing, nil
	}

	instrument, err := m.meter.Float64ObservableGauge(name)
	if err != nil {
		return nil, err
	}

	cache := newGaugeSeriesCache(labelKeys, m.seriesTTL, m.maxSeries, m.log)
	registration, err := m.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		cache.observe(observer, instrument)
		return nil
	}, instrument)
	if err != nil {
		return nil, err
	}

	reg := &gaugeRegistration{
		labelKeys:    append([]string(nil), labelKeys...),
		instrument:   instrument,
		registration: registration,
		cache:        cache,
	}
	m.gauges[name] = reg
	return reg, nil
}

type otlpCounter struct {
	metrics *otlpMetrics
	counter otelmetric.Float64Counter
	attrs   attribute.Set
}

func (c *otlpCounter) Incr(count int64) {
	c.metrics.submitEvent(metricEvent{kind: eventKindCounter, counter: c.counter, value: float64(count), attrs: c.attrs})
}

func (c *otlpCounter) IncrFloat64(count float64) {
	c.metrics.submitEvent(metricEvent{kind: eventKindCounter, counter: c.counter, value: count, attrs: c.attrs})
}

type otlpTimer struct {
	metrics   *otlpMetrics
	histogram otelmetric.Float64Histogram
	attrs     attribute.Set
}

func (t *otlpTimer) Timing(delta int64) {
	seconds := float64(delta) / float64(time.Second)
	t.metrics.submitEvent(metricEvent{kind: eventKindHistogram, histogram: t.histogram, value: seconds, attrs: t.attrs})
}

type otlpGauge struct {
	cache *gaugeSeriesCache
	key   string
	attrs attribute.Set
}

func (g *otlpGauge) Set(value int64) {
	g.cache.set(g.key, g.attrs, float64(value))
}

func (g *otlpGauge) SetFloat64(value float64) {
	g.cache.set(g.key, g.attrs, value)
}

type gaugeSeries struct {
	attrs   attribute.Set
	value   float64
	updated time.Time
}

type gaugeSeriesCache struct {
	labelKeys []string
	ttl       time.Duration
	maxSeries int
	log       *service.Logger

	mu       sync.Mutex
	series   map[string]*gaugeSeries
	dropOnce atomic.Bool
}

func newGaugeSeriesCache(labelKeys []string, ttl time.Duration, maxSeries int, log *service.Logger) *gaugeSeriesCache {
	return &gaugeSeriesCache{
		labelKeys: append([]string(nil), labelKeys...),
		ttl:       ttl,
		maxSeries: maxSeries,
		log:       log,
		series:    make(map[string]*gaugeSeries),
	}
}

func (c *gaugeSeriesCache) set(key string, attrs attribute.Set, value float64) {
	now := nowFn()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictExpiredLocked(now)
	if entry, exists := c.series[key]; exists {
		entry.value = value
		entry.updated = now
		return
	}

	if c.maxSeries > 0 && len(c.series) >= c.maxSeries {
		if !c.dropOnce.Load() && !c.dropOnce.Swap(true) {
			c.log.Warnf("metrics.otlp gauge series limit reached (%d), dropping new series", c.maxSeries)
		}
		return
	}

	c.series[key] = &gaugeSeries{
		attrs:   attrs,
		value:   value,
		updated: now,
	}
}

func (c *gaugeSeriesCache) observe(observer otelmetric.Observer, instrument otelmetric.Float64ObservableGauge) {
	now := nowFn()
	c.mu.Lock()
	c.evictExpiredLocked(now)
	snaps := make([]gaugeSeries, 0, len(c.series))
	for _, series := range c.series {
		snaps = append(snaps, *series)
	}
	c.mu.Unlock()

	for _, snap := range snaps {
		observer.ObserveFloat64(instrument, snap.value, otelmetric.WithAttributeSet(snap.attrs))
	}
}

func (c *gaugeSeriesCache) evictExpiredLocked(now time.Time) {
	if c.ttl <= 0 {
		return
	}
	cutoff := now.Add(-c.ttl)
	for key, series := range c.series {
		if series.updated.Before(cutoff) {
			delete(c.series, key)
		}
	}
}

func buildAttributes(keys, values []string) (attribute.Set, error) {
	if len(keys) != len(values) {
		return attribute.Set{}, fmt.Errorf("label/value count mismatch: %d keys, %d values", len(keys), len(values))
	}
	kvs := make([]attribute.KeyValue, len(keys))
	for i := range keys {
		kvs[i] = attribute.String(keys[i], values[i])
	}
	return attribute.NewSet(kvs...), nil
}

func seriesKey(values []string) string {
	if len(values) == 0 {
		return ""
	}
	var b strings.Builder
	for i, v := range values {
		if i > 0 {
			b.WriteRune('\uffff')
		}
		b.WriteString(v)
	}
	return b.String()
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type noopCounter struct{}

func (noopCounter) Incr(int64)          {}
func (noopCounter) IncrFloat64(float64) {}

type noopTimer struct{}

func (noopTimer) Timing(int64) {}

type noopGauge struct{}

func (noopGauge) Set(int64)          {}
func (noopGauge) SetFloat64(float64) {}
