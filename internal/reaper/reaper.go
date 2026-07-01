// Package reaper implements a cluster-wide janitor: it periodically reclaims
// pods stuck in terminating, evicted or failed states and cleans up finished or
// stuck Jobs. Each concern runs on its own interval, deletions are rate-limited,
// and a namespace denylist protects critical workloads.
package reaper

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"golang.org/x/time/rate"
)

// Reaper performs the actual cleanup work. It depends on kubernetes.Interface
// (not *kubernetes.Clientset) so the logic is unit-testable with a fake client.
type Reaper struct {
	client  kubernetes.Interface
	cfg     *Config
	metrics *Metrics
	limiter *rate.Limiter
	log     *slog.Logger
}

// New wires a Reaper. A single rate limiter is shared across all tasks so the
// aggregate delete rate against the API server stays bounded.
func New(client kubernetes.Interface, cfg *Config, m *Metrics, log *slog.Logger) *Reaper {
	return &Reaper{
		client:  client,
		cfg:     cfg,
		metrics: m,
		limiter: rate.NewLimiter(rate.Limit(cfg.DeleteQPS), cfg.DeleteBurst),
		log:     log,
	}
}

func (r *Reaper) denied(ns string) bool {
	_, ok := r.cfg.DenyNamespaces[ns]
	return ok
}

// deletePod removes a single pod, honouring dry-run and the rate limiter.
// Delete is idempotent: a NotFound is treated as success (something else already
// removed it), which keeps concurrent or repeated runs safe.
func (r *Reaper) deletePod(ctx context.Context, ns, name, reason string) {
	if r.cfg.DryRun {
		r.log.Info("would delete pod", "namespace", ns, "name", name, "reason", reason)
		r.metrics.Deletions.WithLabelValues("pod", ns, reason+"_dryrun").Inc()
		return
	}
	if err := r.limiter.Wait(ctx); err != nil {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, r.cfg.DeleteTimeout)
	defer cancel()

	zero := int64(0)
	err := r.client.CoreV1().Pods(ns).Delete(dctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	switch {
	case err == nil:
		r.log.Info("deleted pod", "namespace", ns, "name", name, "reason", reason)
		r.metrics.Deletions.WithLabelValues("pod", ns, reason).Inc()
	case apierrors.IsNotFound(err):
		// already gone
	default:
		r.log.Warn("failed to delete pod", "namespace", ns, "name", name, "reason", reason, "err", err)
		r.metrics.DeletionErrors.WithLabelValues("pod", ns, classifyError(err)).Inc()
	}
}

// deleteJob removes a Job with foreground propagation so its child pods are
// garbage-collected as part of the delete.
func (r *Reaper) deleteJob(ctx context.Context, ns, name, reason string) {
	if r.cfg.DryRun {
		r.log.Info("would delete job", "namespace", ns, "name", name, "reason", reason)
		r.metrics.Deletions.WithLabelValues("job", ns, reason+"_dryrun").Inc()
		return
	}
	if err := r.limiter.Wait(ctx); err != nil {
		return
	}
	dctx, cancel := context.WithTimeout(ctx, r.cfg.DeleteTimeout)
	defer cancel()

	zero := int64(0)
	fg := metav1.DeletePropagationForeground
	err := r.client.BatchV1().Jobs(ns).Delete(dctx, name, metav1.DeleteOptions{
		GracePeriodSeconds: &zero,
		PropagationPolicy:  &fg,
	})
	switch {
	case err == nil:
		r.log.Info("deleted job", "namespace", ns, "name", name, "reason", reason)
		r.metrics.Deletions.WithLabelValues("job", ns, reason).Inc()
	case apierrors.IsNotFound(err):
		// already gone
	default:
		r.log.Warn("failed to delete job", "namespace", ns, "name", name, "reason", reason, "err", err)
		r.metrics.DeletionErrors.WithLabelValues("job", ns, classifyError(err)).Inc()
	}
}

// forEachPod lists pods across all namespaces in pages, skipping denied
// namespaces, and calls visit for each. Paging bounds the memory and the load
// a single List places on the API server / etcd.
func (r *Reaper) forEachPod(ctx context.Context, visit func(corev1.Pod)) error {
	cont := ""
	for {
		lctx, cancel := context.WithTimeout(ctx, r.cfg.ListTimeout)
		list, err := r.client.CoreV1().Pods(metav1.NamespaceAll).List(lctx, metav1.ListOptions{
			Limit:    r.cfg.PageSize,
			Continue: cont,
		})
		cancel()
		if err != nil {
			return err
		}
		for i := range list.Items {
			p := list.Items[i]
			if r.denied(p.Namespace) {
				continue
			}
			visit(p)
		}
		if list.Continue == "" {
			return nil
		}
		cont = list.Continue
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func (r *Reaper) forEachJob(ctx context.Context, visit func(batchv1.Job)) error {
	cont := ""
	for {
		lctx, cancel := context.WithTimeout(ctx, r.cfg.ListTimeout)
		list, err := r.client.BatchV1().Jobs(metav1.NamespaceAll).List(lctx, metav1.ListOptions{
			Limit:    r.cfg.PageSize,
			Continue: cont,
		})
		cancel()
		if err != nil {
			return err
		}
		for i := range list.Items {
			j := list.Items[i]
			if r.denied(j.Namespace) {
				continue
			}
			visit(j)
		}
		if list.Continue == "" {
			return nil
		}
		cont = list.Continue
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

// ReapTerminatingPods force-deletes pods that are stuck with a deletion
// timestamp set (e.g. because of a dead node or a hung finalizer).
func (r *Reaper) ReapTerminatingPods(ctx context.Context) error {
	return r.forEachPod(ctx, func(p corev1.Pod) {
		if p.DeletionTimestamp != nil {
			r.deletePod(ctx, p.Namespace, p.Name, "terminating")
		}
	})
}

// ReapEvictedPods removes pods evicted by node-pressure, which the kubelet
// leaves behind as tombstones.
func (r *Reaper) ReapEvictedPods(ctx context.Context) error {
	return r.forEachPod(ctx, func(p corev1.Pod) {
		if p.Status.Reason == "Evicted" {
			r.deletePod(ctx, p.Namespace, p.Name, "evicted")
		}
	})
}

// ReapFailedPods removes pods in the Failed phase.
func (r *Reaper) ReapFailedPods(ctx context.Context) error {
	return r.forEachPod(ctx, func(p corev1.Pod) {
		if p.Status.Phase == corev1.PodFailed {
			r.deletePod(ctx, p.Namespace, p.Name, "failed")
		}
	})
}

// ReapSucceededJobs deletes Jobs that have completed successfully and have no
// active pods.
func (r *Reaper) ReapSucceededJobs(ctx context.Context) error {
	return r.forEachJob(ctx, func(j batchv1.Job) {
		if j.Status.Succeeded > 0 && j.Status.Active == 0 {
			r.deleteJob(ctx, j.Namespace, j.Name, "succeeded")
		}
	})
}

// ReapFailedJobs deletes failed Jobs and "stuck" Jobs — ones that have carried
// no status/conditions for longer than StuckJobGrace (typically a scheduling or
// admission problem). It also cleans up the Job's leftover pods explicitly.
func (r *Reaper) ReapFailedJobs(ctx context.Context) error {
	now := time.Now()
	return r.forEachJob(ctx, func(j batchv1.Job) {
		age := now.Sub(j.CreationTimestamp.Time)
		isFailed := j.Status.Failed > 0 && j.Status.Active == 0
		isStuck := j.Status.Failed == 0 && j.Status.Succeeded == 0 &&
			j.Status.Active == 0 && len(j.Status.Conditions) == 0 && age > r.cfg.StuckJobGrace

		if !isFailed && !isStuck {
			return
		}
		reason := "failed"
		if isStuck {
			reason = "stuck"
		}
		r.log.Info("reaping job", "namespace", j.Namespace, "name", j.Name, "reason", reason, "age", age.String())

		lctx, cancel := context.WithTimeout(ctx, r.cfg.ListTimeout)
		pods, err := r.client.CoreV1().Pods(j.Namespace).List(lctx, metav1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", j.Name),
		})
		cancel()
		if err != nil {
			r.log.Warn("failed to list job pods", "namespace", j.Namespace, "job", j.Name, "err", err)
		} else {
			for i := range pods.Items {
				p := pods.Items[i]
				r.deletePod(ctx, p.Namespace, p.Name, reason+"_job_pod")
			}
		}
		r.deleteJob(ctx, j.Namespace, j.Name, reason)
	})
}

// Task is a named periodic reap job.
type Task struct {
	Name     string
	Interval time.Duration
	Run      func(context.Context) error
}

// Tasks returns the configured task set in a deterministic order.
func (r *Reaper) Tasks() []Task {
	return []Task{
		{"terminating_pods", r.cfg.TerminatingInterval, r.ReapTerminatingPods},
		{"evicted_pods", r.cfg.EvictedInterval, r.ReapEvictedPods},
		{"failed_pods", r.cfg.FailedPodInterval, r.ReapFailedPods},
		{"succeeded_jobs", r.cfg.SucceededJobInterval, r.ReapSucceededJobs},
		{"failed_jobs", r.cfg.FailedJobInterval, r.ReapFailedJobs},
	}
}

// Run starts every task on its own goroutine and blocks until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context) {
	for _, t := range r.Tasks() {
		go r.runLoop(ctx, t)
	}
	<-ctx.Done()
}

// runLoop executes a task immediately, then on each tick, recording metrics.
func (r *Reaper) runLoop(ctx context.Context, t Task) {
	exec := func() {
		start := time.Now()
		if err := t.Run(ctx); err != nil {
			r.log.Warn("task error", "task", t.Name, "err", err)
			r.metrics.TaskRuns.WithLabelValues(t.Name, "error").Inc()
		} else {
			r.metrics.TaskRuns.WithLabelValues(t.Name, "success").Inc()
		}
		r.metrics.TaskDuration.WithLabelValues(t.Name).Observe(time.Since(start).Seconds())
		r.metrics.TaskLastRun.WithLabelValues(t.Name).SetToCurrentTime()
	}

	r.log.Info("starting task", "task", t.Name, "interval", t.Interval.String())
	exec()

	ticker := time.NewTicker(t.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			r.log.Info("stopping task", "task", t.Name)
			return
		case <-ticker.C:
			exec()
		}
	}
}
