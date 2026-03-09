package config


import (
	"net/http"

	"go.uber.org/zap"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    RequestLatency = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name: "proxy_request_latency_seconds",
            Help: "HTTP request latency",
            Buckets: []float64{0.05, 0.1, 0.5, 1, 5, 10},
        },
        []string{"upstream"},
    )
    UpstreamTimeouts = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "proxy_upstream_errors_total",
            Help: "Upstream timeouts",
        },
        []string{"upstream"},
    )
    PoolTimeouts = prometheus.NewCounter(
        prometheus.CounterOpts{
            Name: "proxy_pool_errors_total",
            Help: "Pool timeouts",
        },
    )
    PoolLatency = prometheus.NewHistogram(
        prometheus.HistogramOpts{
            Name: "pool_latency_seconds",
            Help: "pool latency",
            Buckets: []float64{0.05, 0.1, 0.5, 1, 5, 10},
        },
    )
)

func StartMetricsServer(logger *zap.Logger) error{
    prometheus.MustRegister(RequestLatency)
    prometheus.MustRegister(UpstreamTimeouts)
    prometheus.MustRegister(PoolTimeouts)
    prometheus.MustRegister(PoolLatency)
    
    http.Handle("/metrics", promhttp.Handler())
    logger.Sugar().Info("Starting metrics server...")

    return http.ListenAndServe("0.0.0.0:9100", nil)
}