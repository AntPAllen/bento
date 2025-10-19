package otlp

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"

	"github.com/warpstreamlabs/bento/public/service"
)

const (
	mmFieldProtocol           = "protocol"
	mmFieldEndpoint           = "endpoint"
	mmFieldHeaders            = "headers"
	mmFieldInsecure           = "insecure"
	mmFieldExportInterval     = "export_interval"
	mmFieldTemporality        = "temporality"
	mmFieldResourceAttributes = "resource_attributes"
	mmFieldHistogram          = "histogram"
	mmFieldHistogramMode      = "mode"
	mmFieldHistogramBuckets   = "buckets"
	mmFieldSeriesTTL          = "series_ttl"
	mmFieldMaxSeries          = "max_series"

	mmProtocolGRPC = "grpc"
	mmProtocolHTTP = "http"

	mmTemporalityCumulative = "cumulative"
	mmTemporalityDelta      = "delta"

	mmHistogramModeExponential = "exponential"
	mmHistogramModeExplicit    = "explicit"
)

func metricsSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Summary("Exports metrics using the OpenTelemetry Protocol (OTLP). Tested with the OpenTelemetry Collector.").
		Description("The OTLP exporter uses the official OpenTelemetry Go SDK for metrics and supports both gRPC and HTTP protocols.").
		Fields(
			service.NewStringEnumField(mmFieldProtocol, mmProtocolGRPC, mmProtocolHTTP).
				Description("The transport protocol to use when exporting metrics.").
				Default(mmProtocolGRPC),
			service.NewStringField(mmFieldEndpoint).
				Description("The collector endpoint to send metrics to. For gRPC the value should be host:port, for HTTP it should be host:port or a full URL depending on your collector configuration.").
				Default("localhost:4317"),
			service.NewBoolField(mmFieldInsecure).
				Description("Disable transport security when connecting to the collector.").
				Default(false),
			service.NewStringMapField(mmFieldHeaders).
				Description("Additional headers to attach to export requests.").
				Default(map[string]any{}).
				Advanced(),
			service.NewDurationField(mmFieldExportInterval).
				Description("How frequently metrics should be exported.").
				Default("10s"),
			service.NewStringEnumField(mmFieldTemporality, mmTemporalityCumulative, mmTemporalityDelta).
				Description("Controls whether cumulative or delta data is exported for counter and histogram instruments.").
				Default(mmTemporalityCumulative).
				Advanced(),
			service.NewStringMapField(mmFieldResourceAttributes).
				Description("Additional resource attributes to associate with exported metrics.").
				Default(map[string]any{}).
				Advanced(),
			service.NewObjectField(mmFieldHistogram,
				service.NewStringEnumField(mmFieldHistogramMode, mmHistogramModeExponential, mmHistogramModeExplicit).
					Description("Controls the aggregation used for histogram metrics.").
					Default(mmHistogramModeExponential).
					Advanced(),
				service.NewFloatListField(mmFieldHistogramBuckets).
					Description("Explicit histogram bucket boundaries (seconds) used when mode is 'explicit'.").
					Optional().
					Advanced(),
			).
				Optional().
				Advanced(),
			service.NewDurationField(mmFieldSeriesTTL).
				Description("The amount of time a label set is kept in memory without new updates before it is evicted.").
				Default("5m").
				Advanced(),
			service.NewIntField(mmFieldMaxSeries).
				Description("Maximum number of active label sets per metric before new series are dropped. Set to 0 for unlimited.").
				Default(5000).
				Advanced(),
		)
}

func init() {
	if err := service.RegisterMetricsExporter("otlp", metricsSpec(), func(conf *service.ParsedConfig, log *service.Logger) (service.MetricsExporter, error) {
		cfg, err := metricsConfigFromParsed(conf)
		if err != nil {
			return nil, err
		}
		cfg.engineVersion = conf.EngineVersion()
		return newOtlpMetrics(cfg, log)
	}); err != nil {
		panic(err)
	}
}

type histogramConfig struct {
	mode    string
	buckets []float64
}

type metricsConfig struct {
	protocol       string
	endpoint       string
	insecure       bool
	headers        map[string]string
	exportInterval time.Duration
	temporality    string
	resourceAttrs  map[string]string
	histogram      histogramConfig
	seriesTTL      time.Duration
	maxSeries      int
	engineVersion  string
}

func metricsConfigFromParsed(conf *service.ParsedConfig) (metricsConfig, error) {
	cfg := metricsConfig{
		protocol:       mmProtocolGRPC,
		endpoint:       "localhost:4317",
		headers:        map[string]string{},
		exportInterval: 10 * time.Second,
		temporality:    mmTemporalityCumulative,
		resourceAttrs:  map[string]string{},
		histogram: histogramConfig{
			mode: mmHistogramModeExponential,
		},
		seriesTTL: 5 * time.Minute,
		maxSeries: 5000,
	}

	if conf.Contains(mmFieldProtocol) {
		protocol, err := conf.FieldString(mmFieldProtocol)
		if err != nil {
			return cfg, err
		}
		cfg.protocol = strings.ToLower(protocol)
	}

	if conf.Contains(mmFieldEndpoint) {
		endpoint, err := conf.FieldString(mmFieldEndpoint)
		if err != nil {
			return cfg, err
		}
		cfg.endpoint = endpoint
	}

	if conf.Contains(mmFieldInsecure) {
		insecure, err := conf.FieldBool(mmFieldInsecure)
		if err != nil {
			return cfg, err
		}
		cfg.insecure = insecure
	}

	headers, err := conf.FieldStringMap(mmFieldHeaders)
	if err != nil {
		return cfg, err
	}
	cfg.headers = headers

	if conf.Contains(mmFieldExportInterval) {
		interval, err := conf.FieldDuration(mmFieldExportInterval)
		if err != nil {
			return cfg, err
		}
		cfg.exportInterval = interval
	}

	if conf.Contains(mmFieldTemporality) {
		temporality, err := conf.FieldString(mmFieldTemporality)
		if err != nil {
			return cfg, err
		}
		cfg.temporality = strings.ToLower(temporality)
	}

	resourceAttrs, err := conf.FieldStringMap(mmFieldResourceAttributes)
	if err != nil {
		return cfg, err
	}
	cfg.resourceAttrs = resourceAttrs

	if conf.Contains(mmFieldHistogram) {
		hconf := conf.Namespace(mmFieldHistogram)
		mode, err := hconf.FieldString(mmFieldHistogramMode)
		if err != nil {
			return cfg, err
		}
		cfg.histogram.mode = strings.ToLower(mode)
		if hconf.Contains(mmFieldHistogramBuckets) {
			buckets, err := hconf.FieldFloatList(mmFieldHistogramBuckets)
			if err != nil {
				return cfg, err
			}
			cfg.histogram.buckets = buckets
		}
	}

	if conf.Contains(mmFieldSeriesTTL) {
		ttl, err := conf.FieldDuration(mmFieldSeriesTTL)
		if err != nil {
			return cfg, err
		}
		cfg.seriesTTL = ttl
	}

	if conf.Contains(mmFieldMaxSeries) {
		maxSeries, err := conf.FieldInt(mmFieldMaxSeries)
		if err != nil {
			return cfg, err
		}
		cfg.maxSeries = maxSeries
	}

	return cfg, nil
}

type otlpMetrics struct {
	log    *service.Logger
	cfg    metricsConfig
	meter  otelmetric.Meter
	reader *sdkmetric.PeriodicReader
	mp     *sdkmetric.MeterProvider

	countersMu sync.Mutex
	counters   map[string]otelmetric.Float64Counter
	histMu     sync.Mutex
	histograms map[string]otelmetric.Float64Histogram
	gaugesMu   sync.Mutex
	gauges     map[string]*gaugeInstrument

	now func() time.Time
}

type gaugeInstrument struct {
	instrument   otelmetric.Float64ObservableGauge
	cache        *seriesCache
	registration otelmetric.Registration
}

func newOtlpMetrics(cfg metricsConfig, log *service.Logger) (*otlpMetrics, error) {
	ctx := context.Background()

	exp, err := buildExporter(ctx, cfg)
	if err != nil {
		return nil, err
	}

	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(cfg.exportInterval))

	res, err := buildResource(cfg)
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	m := &otlpMetrics{
		log:        log,
		cfg:        cfg,
		reader:     reader,
		mp:         mp,
		meter:      mp.Meter("warpstreamlabs/bento"),
		counters:   map[string]otelmetric.Float64Counter{},
		histograms: map[string]otelmetric.Float64Histogram{},
		gauges:     map[string]*gaugeInstrument{},
		now:        time.Now,
	}

	return m, nil
}

func buildExporter(ctx context.Context, cfg metricsConfig) (sdkmetric.Exporter, error) {
	temporality := selectTemporality(cfg.temporality)
	aggregation := selectAggregation(cfg.histogram)

	switch cfg.protocol {
	case mmProtocolGRPC:
		opts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(cfg.endpoint),
			otlpmetricgrpc.WithTemporalitySelector(temporality),
			otlpmetricgrpc.WithAggregationSelector(aggregation),
		}
		if cfg.insecure {
			opts = append(opts, otlpmetricgrpc.WithInsecure())
		}
		if len(cfg.headers) > 0 {
			opts = append(opts, otlpmetricgrpc.WithHeaders(cfg.headers))
		}
		return otlpmetricgrpc.New(ctx, opts...)
	case mmProtocolHTTP:
		opts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(cfg.endpoint),
			otlpmetrichttp.WithTemporalitySelector(temporality),
			otlpmetrichttp.WithAggregationSelector(aggregation),
		}
		if cfg.insecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
		if len(cfg.headers) > 0 {
			opts = append(opts, otlpmetrichttp.WithHeaders(cfg.headers))
		}
		return otlpmetrichttp.New(ctx, opts...)
	default:
		return nil, fmt.Errorf("unrecognized protocol %q", cfg.protocol)
	}
}

func buildResource(cfg metricsConfig) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{}
	serviceNameKey := string(semconv.ServiceNameKey)
	serviceName := "bento"
	if v, ok := cfg.resourceAttrs[serviceNameKey]; ok {
		serviceName = v
	}
	attrs = append(attrs, semconv.ServiceNameKey.String(serviceName))

	serviceVersionKey := string(semconv.ServiceVersionKey)
	version := cfg.engineVersion
	if v, ok := cfg.resourceAttrs[serviceVersionKey]; ok {
		version = v
	}
	if version != "" {
		attrs = append(attrs, semconv.ServiceVersionKey.String(version))
	}
	for k, v := range cfg.resourceAttrs {
		if k == serviceNameKey || k == serviceVersionKey {
			continue
		}
		attrs = append(attrs, attribute.String(k, v))
	}
	defaultRes := resource.Default()
	customRes := resource.NewWithAttributes(defaultRes.SchemaURL(), attrs...)
	return resource.Merge(defaultRes, customRes)
}

func selectTemporality(temporality string) sdkmetric.TemporalitySelector {
	if temporality == mmTemporalityDelta {
		return func(kind sdkmetric.InstrumentKind) metricdata.Temporality {
			switch kind {
			case sdkmetric.InstrumentKindCounter, sdkmetric.InstrumentKindHistogram:
				return metricdata.DeltaTemporality
			default:
				return metricdata.CumulativeTemporality
			}
		}
	}
	return sdkmetric.DefaultTemporalitySelector
}

func selectAggregation(hist histogramConfig) sdkmetric.AggregationSelector {
	return func(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
		if kind != sdkmetric.InstrumentKindHistogram {
			return sdkmetric.AggregationDefault{}
		}
		switch hist.mode {
		case mmHistogramModeExplicit:
			if len(hist.buckets) == 0 {
				return sdkmetric.AggregationExplicitBucketHistogram{}
			}
			return sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: hist.buckets,
			}
		case mmHistogramModeExponential:
			return sdkmetric.AggregationBase2ExponentialHistogram{}
		default:
			return sdkmetric.AggregationDefault{}
		}
	}
}

func (m *otlpMetrics) NewCounterCtor(name string, labelKeys ...string) service.MetricsExporterCounterCtor {
	counter, err := m.getCounter(name)
	if err != nil {
		if m.log != nil {
			m.log.Errorf("failed to create OTLP counter %q: %v", name, err)
		}
		return func(labelValues ...string) service.MetricsExporterCounter {
			return noopCounter{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterCounter {
		attrs := buildAttributes(labelKeys, labelValues)
		return &otlpCounter{instrument: counter, attrs: attrs}
	}
}

func (m *otlpMetrics) NewTimerCtor(name string, labelKeys ...string) service.MetricsExporterTimerCtor {
	histogram, err := m.getHistogram(name)
	if err != nil {
		if m.log != nil {
			m.log.Errorf("failed to create OTLP histogram %q: %v", name, err)
		}
		return func(labelValues ...string) service.MetricsExporterTimer {
			return noopTimer{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterTimer {
		attrs := buildAttributes(labelKeys, labelValues)
		return &otlpHistogram{instrument: histogram, attrs: attrs}
	}
}

func (m *otlpMetrics) NewGaugeCtor(name string, labelKeys ...string) service.MetricsExporterGaugeCtor {
	gauge, err := m.getGauge(name)
	if err != nil {
		if m.log != nil {
			m.log.Errorf("failed to create OTLP gauge %q: %v", name, err)
		}
		return func(labelValues ...string) service.MetricsExporterGauge {
			return noopGauge{}
		}
	}
	return func(labelValues ...string) service.MetricsExporterGauge {
		attrs := buildAttributes(labelKeys, labelValues)
		key := seriesKey(labelValues)
		return &otlpGauge{
			gauge: gauge,
			attrs: attrs,
			key:   key,
		}
	}
}

func (m *otlpMetrics) Close(ctx context.Context) error {
	m.gaugesMu.Lock()
	for _, g := range m.gauges {
		if g.registration != nil {
			_ = g.registration.Unregister()
		}
	}
	m.gaugesMu.Unlock()
	return m.mp.Shutdown(ctx)
}

func (m *otlpMetrics) forceFlush(ctx context.Context) error {
	return m.mp.ForceFlush(ctx)
}

func (m *otlpMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (m *otlpMetrics) getCounter(name string) (otelmetric.Float64Counter, error) {
	m.countersMu.Lock()
	defer m.countersMu.Unlock()
	if ctr, ok := m.counters[name]; ok {
		return ctr, nil
	}
	ctr, err := m.meter.Float64Counter(name)
	if err != nil {
		return nil, err
	}
	m.counters[name] = ctr
	return ctr, nil
}

func (m *otlpMetrics) getHistogram(name string) (otelmetric.Float64Histogram, error) {
	m.histMu.Lock()
	defer m.histMu.Unlock()
	if h, ok := m.histograms[name]; ok {
		return h, nil
	}
	opts := []otelmetric.Float64HistogramOption{otelmetric.WithUnit("s")}
	hist, err := m.meter.Float64Histogram(name, opts...)
	if err != nil {
		return nil, err
	}
	m.histograms[name] = hist
	return hist, nil
}

func (m *otlpMetrics) getGauge(name string) (*gaugeInstrument, error) {
	m.gaugesMu.Lock()
	defer m.gaugesMu.Unlock()
	if g, ok := m.gauges[name]; ok {
		return g, nil
	}
	inst, err := m.meter.Float64ObservableGauge(name)
	if err != nil {
		return nil, err
	}
	cache := newSeriesCache(m.cfg.seriesTTL, m.cfg.maxSeries, m.log, m.now)
	reg, err := m.meter.RegisterCallback(func(ctx context.Context, observer otelmetric.Observer) error {
		observations := cache.snapshot()
		for _, obs := range observations {
			observer.ObserveFloat64(inst, obs.value, otelmetric.WithAttributes(obs.attrs...))
		}
		return nil
	}, inst)
	if err != nil {
		return nil, err
	}
	gauge := &gaugeInstrument{
		instrument:   inst,
		cache:        cache,
		registration: reg,
	}
	m.gauges[name] = gauge
	return gauge, nil
}

type otlpCounter struct {
	instrument otelmetric.Float64Counter
	attrs      []attribute.KeyValue
}

func (c *otlpCounter) Incr(count int64) {
	c.instrument.Add(context.Background(), float64(count), otelmetric.WithAttributes(c.attrs...))
}

func (c *otlpCounter) IncrFloat64(count float64) {
	c.instrument.Add(context.Background(), count, otelmetric.WithAttributes(c.attrs...))
}

type otlpHistogram struct {
	instrument otelmetric.Float64Histogram
	attrs      []attribute.KeyValue
}

func (h *otlpHistogram) Timing(delta int64) {
	seconds := float64(delta) / float64(time.Second)
	h.instrument.Record(context.Background(), seconds, otelmetric.WithAttributes(h.attrs...))
}

type otlpGauge struct {
	gauge *gaugeInstrument
	attrs []attribute.KeyValue
	key   string
}

func (g *otlpGauge) Set(value int64) {
	g.gauge.cache.set(g.key, g.attrs, float64(value))
}

func (g *otlpGauge) SetFloat64(value float64) {
	g.gauge.cache.set(g.key, g.attrs, value)
}

func (g *otlpGauge) Incr(count int64) {
	g.gauge.cache.add(g.key, g.attrs, float64(count))
}

func (g *otlpGauge) IncrFloat64(count float64) {
	g.gauge.cache.add(g.key, g.attrs, count)
}

func (g *otlpGauge) Decr(count int64) {
	g.gauge.cache.add(g.key, g.attrs, -float64(count))
}

func (g *otlpGauge) DecrFloat64(count float64) {
	g.gauge.cache.add(g.key, g.attrs, -count)
}

type noopCounter struct{}

func (noopCounter) Incr(int64)          {}
func (noopCounter) IncrFloat64(float64) {}

type noopTimer struct{}

func (noopTimer) Timing(int64) {}

type noopGauge struct{}

func (noopGauge) Set(int64)           {}
func (noopGauge) SetFloat64(float64)  {}
func (noopGauge) Incr(int64)          {}
func (noopGauge) IncrFloat64(float64) {}
func (noopGauge) Decr(int64)          {}
func (noopGauge) DecrFloat64(float64) {}

func buildAttributes(labelKeys, labelValues []string) []attribute.KeyValue {
	if len(labelKeys) == 0 || len(labelValues) == 0 {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, len(labelKeys))
	for i, key := range labelKeys {
		if i >= len(labelValues) {
			break
		}
		attrs = append(attrs, attribute.String(key, labelValues[i]))
	}
	return attrs
}

func seriesKey(labelValues []string) string {
	if len(labelValues) == 0 {
		return "__empty__"
	}
	var b strings.Builder
	for i, v := range labelValues {
		if i > 0 {
			b.WriteRune('\x00')
		}
		b.WriteString(v)
	}
	return b.String()
}

type seriesObservation struct {
	attrs []attribute.KeyValue
	value float64
}

type seriesCache struct {
	mu         sync.Mutex
	entries    map[string]*seriesEntry
	ttl        time.Duration
	max        int
	log        *service.Logger
	now        func() time.Time
	warnedDrop atomic.Bool
}

type seriesEntry struct {
	attrs    []attribute.KeyValue
	value    float64
	lastSeen time.Time
}

func newSeriesCache(ttl time.Duration, max int, log *service.Logger, now func() time.Time) *seriesCache {
	return &seriesCache{
		entries: make(map[string]*seriesEntry),
		ttl:     ttl,
		max:     max,
		log:     log,
		now:     now,
	}
}

func (c *seriesCache) set(key string, attrs []attribute.KeyValue, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, exists := c.entries[key]; exists {
		entry.value = value
		entry.lastSeen = c.now()
		return
	}
	if c.max > 0 && len(c.entries) >= c.max {
		if !c.warnedDrop.Load() && c.log != nil {
			c.warnedDrop.Store(true)
			c.log.Warnf("otlp metrics gauge reached max_series limit (%d), dropping new label sets", c.max)
		}
		return
	}
	c.entries[key] = &seriesEntry{
		attrs:    attrs,
		value:    value,
		lastSeen: c.now(),
	}
}

func (c *seriesCache) add(key string, attrs []attribute.KeyValue, delta float64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, exists := c.entries[key]
	if !exists {
		if c.max > 0 && len(c.entries) >= c.max {
			if !c.warnedDrop.Load() && c.log != nil {
				c.warnedDrop.Store(true)
				c.log.Warnf("otlp metrics gauge reached max_series limit (%d), dropping new label sets", c.max)
			}
			return
		}
		entry = &seriesEntry{
			attrs: attrs,
		}
		c.entries[key] = entry
	}
	entry.value += delta
	entry.lastSeen = c.now()
}

func (c *seriesCache) snapshot() []seriesObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	result := make([]seriesObservation, 0, len(c.entries))
	for key, entry := range c.entries {
		if c.ttl > 0 && now.Sub(entry.lastSeen) > c.ttl {
			delete(c.entries, key)
			continue
		}
		result = append(result, seriesObservation{
			attrs: entry.attrs,
			value: entry.value,
		})
	}
	return result
}
