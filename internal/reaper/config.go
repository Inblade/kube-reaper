package reaper

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime settings. Everything is sourced from environment
// variables (12-factor) so the same image can be tuned per-cluster without a rebuild.
type Config struct {
	// How often each reap task runs.
	TerminatingInterval  time.Duration
	EvictedInterval      time.Duration
	FailedPodInterval    time.Duration
	SucceededJobInterval time.Duration
	FailedJobInterval    time.Duration

	// A Job with no status/conditions older than StuckJobGrace is treated as stuck.
	StuckJobGrace time.Duration

	ListTimeout   time.Duration
	DeleteTimeout time.Duration
	PageSize      int64

	// Namespaces that are never touched (e.g. kube-system).
	DenyNamespaces map[string]struct{}

	DryRun bool

	// Leader election.
	EnableLeaderElection bool
	Namespace            string
	Identity             string
	LeaseName            string
	LeaseDuration        time.Duration
	RenewDeadline        time.Duration
	RetryPeriod          time.Duration

	MetricsAddr string
	HealthAddr  string

	DeleteQPS   float64
	DeleteBurst int
}

// Load builds a Config from environment variables, falling back to sane defaults.
func Load(log *slog.Logger, dryRunFlag bool) *Config {
	c := &Config{
		TerminatingInterval:  envDuration(log, "REAPER_TERMINATING_INTERVAL", 20*time.Minute),
		EvictedInterval:      envDuration(log, "REAPER_EVICTED_INTERVAL", 15*time.Minute),
		FailedPodInterval:    envDuration(log, "REAPER_FAILED_POD_INTERVAL", 30*time.Minute),
		SucceededJobInterval: envDuration(log, "REAPER_SUCCEEDED_JOB_INTERVAL", time.Hour),
		FailedJobInterval:    envDuration(log, "REAPER_FAILED_JOB_INTERVAL", 5*time.Minute),
		StuckJobGrace:        envDuration(log, "REAPER_STUCK_JOB_GRACE", 2*time.Minute),

		ListTimeout:   envDuration(log, "REAPER_LIST_TIMEOUT", 60*time.Second),
		DeleteTimeout: envDuration(log, "REAPER_DELETE_TIMEOUT", 30*time.Second),
		PageSize:      envInt64(log, "REAPER_LIST_PAGE_SIZE", 500),

		DryRun: dryRunFlag || envBool(log, "REAPER_DRY_RUN", false),

		EnableLeaderElection: envBool(log, "REAPER_LEADER_ELECTION", true),
		Namespace:            envString("REAPER_NAMESPACE", "kube-reaper"),
		Identity:             envString("REAPER_IDENTITY", os.Getenv("HOSTNAME")),
		LeaseName:            envString("REAPER_LEASE_NAME", "kube-reaper"),
		LeaseDuration:        envDuration(log, "REAPER_LEASE_DURATION", 30*time.Second),
		RenewDeadline:        envDuration(log, "REAPER_RENEW_DEADLINE", 20*time.Second),
		RetryPeriod:          envDuration(log, "REAPER_RETRY_PERIOD", 5*time.Second),

		MetricsAddr: envString("REAPER_METRICS_ADDR", ":8080"),
		HealthAddr:  envString("REAPER_HEALTH_ADDR", ":8081"),

		DeleteQPS:   envFloat(log, "REAPER_DELETE_QPS", 10),
		DeleteBurst: int(envInt64(log, "REAPER_DELETE_BURST", 20)),
	}

	if c.Identity == "" {
		c.Identity = "kube-reaper-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	c.DenyNamespaces = parseSet(envString("REAPER_DENY_NAMESPACES", "kube-system"))
	return c
}

func parseSet(raw string) map[string]struct{} {
	out := make(map[string]struct{})
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out[item] = struct{}{}
		}
	}
	return out
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(log *slog.Logger, key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Warn("invalid duration, using default", "key", key, "value", v, "default", def)
		return def
	}
	return d
}

func envInt64(log *slog.Logger, key string, def int64) int64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		log.Warn("invalid int, using default", "key", key, "value", v, "default", def)
		return def
	}
	return n
}

func envFloat(log *slog.Logger, key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Warn("invalid float, using default", "key", key, "value", v, "default", def)
		return def
	}
	return f
}

func envBool(log *slog.Logger, key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		log.Warn("invalid bool, using default", "key", key, "value", v, "default", def)
		return def
	}
	return b
}
