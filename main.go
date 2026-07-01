// Command kube-reaper is a cluster-wide janitor that reclaims stuck/failed pods
// and finished/stuck Jobs on independent timed loops. It runs a single active
// leader (with hot standbys for HA), rate-limits deletions, exposes Prometheus
// metrics and health probes, and never touches denied namespaces.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"github.com/Inblade/kube-reaper/internal/reaper"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "log deletions without executing them")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	cfg := reaper.Load(log, *dryRun)

	client, err := buildClient()
	if err != nil {
		log.Error("failed to build kubernetes client", "err", err)
		os.Exit(1)
	}

	metrics := reaper.NewMetrics()
	registry := prometheus.NewRegistry()
	metrics.Register(registry)

	rp := reaper.New(client, cfg, metrics, log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var ready atomic.Bool
	go serveMetrics(ctx, log, cfg.MetricsAddr, registry)
	go serveHealth(ctx, log, cfg.HealthAddr, &ready)
	ready.Store(true)

	log.Info("kube-reaper starting",
		"identity", cfg.Identity, "namespace", cfg.Namespace,
		"dry_run", cfg.DryRun, "leader_election", cfg.EnableLeaderElection)

	if !cfg.EnableLeaderElection {
		rp.Run(ctx)
		return
	}
	if err := runWithLeaderElection(ctx, client, cfg, metrics, rp, log); err != nil {
		log.Error("leader election failed", "err", err)
		os.Exit(1)
	}
}

// buildClient prefers in-cluster config and falls back to the local kubeconfig
// (KUBECONFIG or ~/.kube/config), which makes the binary runnable outside the
// cluster for development.
func buildClient() (kubernetes.Interface, error) {
	if cfg, err := rest.InClusterConfig(); err == nil {
		return kubernetes.NewForConfig(cfg)
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

func runWithLeaderElection(
	ctx context.Context,
	client kubernetes.Interface,
	cfg *reaper.Config,
	metrics *reaper.Metrics,
	rp *reaper.Reaper,
	log *slog.Logger,
) error {
	lock, err := resourcelock.New(
		resourcelock.LeasesResourceLock,
		cfg.Namespace,
		cfg.LeaseName,
		client.CoreV1(),
		client.CoordinationV1(),
		resourcelock.ResourceLockConfig{Identity: cfg.Identity},
	)
	if err != nil {
		return err
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   cfg.LeaseDuration,
		RenewDeadline:   cfg.RenewDeadline,
		RetryPeriod:     cfg.RetryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				log.Info("acquired leadership", "identity", cfg.Identity)
				metrics.IsLeader.Set(1)
				rp.Run(leaderCtx)
			},
			OnStoppedLeading: func() {
				log.Info("lost leadership", "identity", cfg.Identity)
				metrics.IsLeader.Set(0)
			},
			OnNewLeader: func(identity string) {
				if identity != cfg.Identity {
					log.Info("observed leader", "leader", identity)
				}
			},
		},
	})
	return nil
}

func serveMetrics(ctx context.Context, log *slog.Logger, addr string, registry *prometheus.Registry) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{Registry: registry}))
	serve(ctx, log, "metrics", addr, mux)
}

func serveHealth(ctx context.Context, log *slog.Logger, addr string, ready *atomic.Bool) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	serve(ctx, log, "health", addr, mux)
}

func serve(ctx context.Context, log *slog.Logger, name, addr string, handler http.Handler) {
	srv := &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	log.Info("http server listening", "server", name, "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server error", "server", name, "err", err)
	}
}
