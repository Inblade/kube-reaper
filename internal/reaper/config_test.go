package reaper

import (
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestLoadDefaults(t *testing.T) {
	// t.Setenv on a key that is not set still isolates the test from a
	// developer's shell, which is where "works on my machine" starts.
	for _, key := range []string{
		"REAPER_TERMINATING_INTERVAL", "REAPER_EVICTED_INTERVAL",
		"REAPER_FAILED_POD_INTERVAL", "REAPER_SUCCEEDED_JOB_INTERVAL",
		"REAPER_FAILED_JOB_INTERVAL", "REAPER_STUCK_JOB_GRACE",
		"REAPER_LIST_TIMEOUT", "REAPER_DELETE_TIMEOUT", "REAPER_LIST_PAGE_SIZE",
		"REAPER_DRY_RUN", "REAPER_LEADER_ELECTION", "REAPER_NAMESPACE",
		"REAPER_IDENTITY", "REAPER_LEASE_NAME", "REAPER_LEASE_DURATION",
		"REAPER_RENEW_DEADLINE", "REAPER_RETRY_PERIOD", "REAPER_METRICS_ADDR",
		"REAPER_HEALTH_ADDR", "REAPER_DELETE_QPS", "REAPER_DELETE_BURST",
		"REAPER_DENY_NAMESPACES", "HOSTNAME",
	} {
		t.Setenv(key, "")
	}

	cfg := Load(testLogger(), false)

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"TerminatingInterval", cfg.TerminatingInterval, 20 * time.Minute},
		{"EvictedInterval", cfg.EvictedInterval, 15 * time.Minute},
		{"FailedPodInterval", cfg.FailedPodInterval, 30 * time.Minute},
		{"SucceededJobInterval", cfg.SucceededJobInterval, time.Hour},
		{"FailedJobInterval", cfg.FailedJobInterval, 5 * time.Minute},
		{"StuckJobGrace", cfg.StuckJobGrace, 2 * time.Minute},
		{"ListTimeout", cfg.ListTimeout, 60 * time.Second},
		{"DeleteTimeout", cfg.DeleteTimeout, 30 * time.Second},
		{"PageSize", cfg.PageSize, int64(500)},
		{"DryRun", cfg.DryRun, false},
		{"EnableLeaderElection", cfg.EnableLeaderElection, true},
		{"Namespace", cfg.Namespace, "kube-reaper"},
		{"LeaseName", cfg.LeaseName, "kube-reaper"},
		{"LeaseDuration", cfg.LeaseDuration, 30 * time.Second},
		{"RenewDeadline", cfg.RenewDeadline, 20 * time.Second},
		{"RetryPeriod", cfg.RetryPeriod, 5 * time.Second},
		{"MetricsAddr", cfg.MetricsAddr, ":8080"},
		{"HealthAddr", cfg.HealthAddr, ":8081"},
		{"DeleteQPS", cfg.DeleteQPS, float64(10)},
		{"DeleteBurst", cfg.DeleteBurst, 20},
	}

	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// kube-system in the deny list by default is the single most important
// default in this operator: without it a misconfigured reaper deletes control
// plane pods.
func TestKubeSystemIsDeniedByDefault(t *testing.T) {
	t.Setenv("REAPER_DENY_NAMESPACES", "")

	cfg := Load(testLogger(), false)

	if _, denied := cfg.DenyNamespaces["kube-system"]; !denied {
		t.Fatalf("kube-system is not in the default deny list: %v", cfg.DenyNamespaces)
	}
}

// The leader-election lease default of true is the other dangerous one: two
// replicas both reaping is how a rate limit becomes a thundering herd.
func TestLeaderElectionDefaultsToOn(t *testing.T) {
	t.Setenv("REAPER_LEADER_ELECTION", "")

	if !Load(testLogger(), false).EnableLeaderElection {
		t.Fatal("leader election must default to enabled")
	}
}

func TestLoadReadsTheEnvironment(t *testing.T) {
	t.Setenv("REAPER_TERMINATING_INTERVAL", "90s")
	t.Setenv("REAPER_LIST_PAGE_SIZE", "42")
	t.Setenv("REAPER_DELETE_QPS", "2.5")
	t.Setenv("REAPER_DELETE_BURST", "7")
	t.Setenv("REAPER_LEADER_ELECTION", "false")
	t.Setenv("REAPER_NAMESPACE", "platform")
	t.Setenv("REAPER_METRICS_ADDR", "127.0.0.1:9999")

	cfg := Load(testLogger(), false)

	if cfg.TerminatingInterval != 90*time.Second {
		t.Errorf("TerminatingInterval = %v, want 90s", cfg.TerminatingInterval)
	}
	if cfg.PageSize != 42 {
		t.Errorf("PageSize = %d, want 42", cfg.PageSize)
	}
	if cfg.DeleteQPS != 2.5 {
		t.Errorf("DeleteQPS = %v, want 2.5", cfg.DeleteQPS)
	}
	if cfg.DeleteBurst != 7 {
		t.Errorf("DeleteBurst = %d, want 7", cfg.DeleteBurst)
	}
	if cfg.EnableLeaderElection {
		t.Error("EnableLeaderElection = true, want false")
	}
	if cfg.Namespace != "platform" {
		t.Errorf("Namespace = %q, want %q", cfg.Namespace, "platform")
	}
	if cfg.MetricsAddr != "127.0.0.1:9999" {
		t.Errorf("MetricsAddr = %q, want %q", cfg.MetricsAddr, "127.0.0.1:9999")
	}
}

// A typo in a duration must not take the operator down, and must not silently
// become zero either — a zero interval is a tight loop against the API server.
func TestGarbageValuesFallBackToDefaults(t *testing.T) {
	t.Setenv("REAPER_TERMINATING_INTERVAL", "twenty minutes")
	t.Setenv("REAPER_LIST_PAGE_SIZE", "many")
	t.Setenv("REAPER_DELETE_QPS", "fast")
	t.Setenv("REAPER_LEADER_ELECTION", "yes-please")

	cfg := Load(testLogger(), false)

	if cfg.TerminatingInterval != 20*time.Minute {
		t.Errorf("TerminatingInterval = %v, want the 20m default", cfg.TerminatingInterval)
	}
	if cfg.PageSize != 500 {
		t.Errorf("PageSize = %d, want the 500 default", cfg.PageSize)
	}
	if cfg.DeleteQPS != 10 {
		t.Errorf("DeleteQPS = %v, want the default of 10", cfg.DeleteQPS)
	}
	if !cfg.EnableLeaderElection {
		t.Error("an unparseable bool must keep the safe default, not become false")
	}
}

// --dry-run on the command line must win over the environment. The reverse
// would mean an operator typing --dry-run and still deleting things.
func TestDryRunFlagOverridesTheEnvironment(t *testing.T) {
	t.Setenv("REAPER_DRY_RUN", "false")

	if !Load(testLogger(), true).DryRun {
		t.Fatal("the --dry-run flag must win over REAPER_DRY_RUN=false")
	}
}

func TestDryRunCanBeSetFromTheEnvironment(t *testing.T) {
	t.Setenv("REAPER_DRY_RUN", "true")

	if !Load(testLogger(), false).DryRun {
		t.Fatal("REAPER_DRY_RUN=true was ignored")
	}
}

func TestIdentityFallsBackToHostname(t *testing.T) {
	t.Setenv("REAPER_IDENTITY", "")
	t.Setenv("HOSTNAME", "kube-reaper-7d9f8-abcde")

	if got := Load(testLogger(), false).Identity; got != "kube-reaper-7d9f8-abcde" {
		t.Errorf("Identity = %q, want the hostname", got)
	}
}

// Two replicas sharing an identity would fight over the same lease forever, so
// an empty hostname has to produce something unique rather than an empty string.
func TestIdentityIsNeverEmpty(t *testing.T) {
	t.Setenv("REAPER_IDENTITY", "")
	t.Setenv("HOSTNAME", "")

	first := Load(testLogger(), false).Identity
	if first == "" {
		t.Fatal("Identity is empty; leader election would be undefined")
	}
	if !strings.HasPrefix(first, "kube-reaper-") {
		t.Errorf("Identity = %q, want the generated kube-reaper- prefix", first)
	}

	// Uniqueness is the whole point of the fallback.
	if second := Load(testLogger(), false).Identity; first == second {
		t.Error("two generated identities collided")
	}
}

func TestParseSet(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{"single value", "kube-system", []string{"kube-system"}},
		{"comma separated", "kube-system,istio-system", []string{"kube-system", "istio-system"}},
		{"surrounding spaces are trimmed", " kube-system , monitoring ", []string{"kube-system", "monitoring"}},
		{"empty entries are dropped", "kube-system,,monitoring,", []string{"kube-system", "monitoring"}},
		{"empty string yields nothing", "", nil},
		{"only separators yield nothing", ",,,", nil},
		{"whitespace only yields nothing", "  ,  ", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSet(tt.raw)

			if len(got) != len(tt.want) {
				t.Fatalf("parseSet(%q) = %v, want %v", tt.raw, keysOf(got), tt.want)
			}
			for _, want := range tt.want {
				if _, ok := got[want]; !ok {
					t.Errorf("parseSet(%q) is missing %q", tt.raw, want)
				}
			}
		})
	}
}

func TestEnvHelpersTreatEmptyAsUnset(t *testing.T) {
	// An env var set to the empty string is what a Kubernetes manifest
	// produces from an unset ConfigMap key. It must mean "use the default",
	// not "use the zero value".
	t.Setenv("REAPER_TEST_STRING", "")
	t.Setenv("REAPER_TEST_DURATION", "")
	t.Setenv("REAPER_TEST_INT", "")
	t.Setenv("REAPER_TEST_FLOAT", "")
	t.Setenv("REAPER_TEST_BOOL", "")

	log := testLogger()

	if got := envString("REAPER_TEST_STRING", "fallback"); got != "fallback" {
		t.Errorf("envString = %q, want the default", got)
	}
	if got := envDuration(log, "REAPER_TEST_DURATION", time.Minute); got != time.Minute {
		t.Errorf("envDuration = %v, want the default", got)
	}
	if got := envInt64(log, "REAPER_TEST_INT", 5); got != 5 {
		t.Errorf("envInt64 = %d, want the default", got)
	}
	if got := envFloat(log, "REAPER_TEST_FLOAT", 1.5); got != 1.5 {
		t.Errorf("envFloat = %v, want the default", got)
	}
	if got := envBool(log, "REAPER_TEST_BOOL", true); !got {
		t.Error("envBool did not return the default")
	}
}

func TestEnvBoolAcceptsTheUsualSpellings(t *testing.T) {
	for _, truthy := range []string{"true", "TRUE", "True", "1", "t", "T"} {
		t.Run(truthy, func(t *testing.T) {
			t.Setenv("REAPER_TEST_BOOL", truthy)
			if !envBool(testLogger(), "REAPER_TEST_BOOL", false) {
				t.Errorf("envBool(%q) = false, want true", truthy)
			}
		})
	}
	for _, falsy := range []string{"false", "FALSE", "0", "f", "F"} {
		t.Run(falsy, func(t *testing.T) {
			t.Setenv("REAPER_TEST_BOOL", falsy)
			if envBool(testLogger(), "REAPER_TEST_BOOL", true) {
				t.Errorf("envBool(%q) = true, want false", falsy)
			}
		})
	}
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
