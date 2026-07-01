package reaper

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics bundles all Prometheus collectors so they can be injected (and
// registered against a private registry in tests without global state).
type Metrics struct {
	Deletions      *prometheus.CounterVec
	DeletionErrors *prometheus.CounterVec
	TaskRuns       *prometheus.CounterVec
	TaskLastRun    *prometheus.GaugeVec
	TaskDuration   *prometheus.HistogramVec
	IsLeader       prometheus.Gauge
}

// NewMetrics constructs the collector set. Call Register to expose them.
func NewMetrics() *Metrics {
	return &Metrics{
		Deletions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_deletions_total",
			Help: "Total number of successful deletions.",
		}, []string{"kind", "namespace", "reason"}),
		DeletionErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_deletion_errors_total",
			Help: "Total number of failed deletion attempts, by error type.",
		}, []string{"kind", "namespace", "error_type"}),
		TaskRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "reaper_task_runs_total",
			Help: "Total number of reap task runs, by outcome.",
		}, []string{"task", "outcome"}),
		TaskLastRun: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "reaper_task_last_run_timestamp_seconds",
			Help: "Unix timestamp of the last completion per task.",
		}, []string{"task"}),
		TaskDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "reaper_task_duration_seconds",
			Help:    "Reap task duration in seconds.",
			Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
		}, []string{"task"}),
		IsLeader: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "reaper_is_leader",
			Help: "1 if this replica currently holds the leader lease, 0 otherwise.",
		}),
	}
}

// Register adds all collectors to the given registerer.
func (m *Metrics) Register(r prometheus.Registerer) {
	r.MustRegister(
		m.Deletions,
		m.DeletionErrors,
		m.TaskRuns,
		m.TaskLastRun,
		m.TaskDuration,
		m.IsLeader,
	)
}

// classifyError maps an API error to a low-cardinality label value so that
// deletion_errors_total stays cheap while remaining useful for alerting.
func classifyError(err error) string {
	switch {
	case apierrors.IsNotFound(err):
		return "not_found"
	case apierrors.IsForbidden(err):
		return "forbidden"
	case apierrors.IsUnauthorized(err):
		return "unauthorized"
	case apierrors.IsConflict(err):
		return "conflict"
	case apierrors.IsTooManyRequests(err):
		return "rate_limited"
	case apierrors.IsServerTimeout(err), apierrors.IsTimeout(err):
		return "timeout"
	default:
		return "other"
	}
}
