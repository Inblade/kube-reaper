package reaper

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

func testReaper(t *testing.T, objs ...runtime.Object) (*Reaper, kubernetes.Interface) {
	t.Helper()
	client := fake.NewSimpleClientset(objs...)
	cfg := &Config{
		StuckJobGrace:  2 * time.Minute,
		ListTimeout:    5 * time.Second,
		DeleteTimeout:  5 * time.Second,
		PageSize:       100,
		DenyNamespaces: map[string]struct{}{"kube-system": {}},
		DeleteQPS:      1e6,
		DeleteBurst:    1e6,
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(client, cfg, NewMetrics(), log), client
}

func pod(ns, name string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func podExists(t *testing.T, c kubernetes.Interface, ns, name string) bool {
	t.Helper()
	_, err := c.CoreV1().Pods(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected get error: %v", err)
	}
	return err == nil
}

func jobExists(t *testing.T, c kubernetes.Interface, ns, name string) bool {
	t.Helper()
	_, err := c.BatchV1().Jobs(ns).Get(context.Background(), name, metav1.GetOptions{})
	return err == nil
}

func TestReapFailedPods(t *testing.T) {
	good := pod("default", "running", corev1.PodRunning)
	bad := pod("default", "failed", corev1.PodFailed)
	protected := pod("kube-system", "sys-failed", corev1.PodFailed)

	r, c := testReaper(t, good, bad, protected)
	if err := r.ReapFailedPods(context.Background()); err != nil {
		t.Fatalf("ReapFailedPods: %v", err)
	}

	if podExists(t, c, "default", "failed") {
		t.Error("failed pod should have been deleted")
	}
	if !podExists(t, c, "default", "running") {
		t.Error("running pod must be kept")
	}
	if !podExists(t, c, "kube-system", "sys-failed") {
		t.Error("denied-namespace pod must be kept")
	}
}

func TestReapEvictedPods(t *testing.T) {
	evicted := pod("default", "evicted", corev1.PodFailed)
	evicted.Status.Reason = "Evicted"
	other := pod("default", "oom", corev1.PodFailed)
	other.Status.Reason = "OOMKilled"

	r, c := testReaper(t, evicted, other)
	if err := r.ReapEvictedPods(context.Background()); err != nil {
		t.Fatalf("ReapEvictedPods: %v", err)
	}
	if podExists(t, c, "default", "evicted") {
		t.Error("evicted pod should have been deleted")
	}
	if !podExists(t, c, "default", "oom") {
		t.Error("non-evicted pod must be kept")
	}
}

func TestReapTerminatingPods(t *testing.T) {
	stuck := pod("default", "stuck", corev1.PodRunning)
	now := metav1.Now()
	stuck.DeletionTimestamp = &now
	stuck.Finalizers = []string{"example.com/finalizer"} // fake requires finalizers with a deletion timestamp
	live := pod("default", "live", corev1.PodRunning)

	r, c := testReaper(t, stuck, live)
	if err := r.ReapTerminatingPods(context.Background()); err != nil {
		t.Fatalf("ReapTerminatingPods: %v", err)
	}
	if podExists(t, c, "default", "stuck") {
		t.Error("terminating pod should have been force-deleted")
	}
	if !podExists(t, c, "default", "live") {
		t.Error("live pod must be kept")
	}
}

func TestReapSucceededJobs(t *testing.T) {
	done := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "done"},
		Status:     batchv1.JobStatus{Succeeded: 1, Active: 0},
	}
	running := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "busy"},
		Status:     batchv1.JobStatus{Active: 1},
	}
	r, c := testReaper(t, done, running)
	if err := r.ReapSucceededJobs(context.Background()); err != nil {
		t.Fatalf("ReapSucceededJobs: %v", err)
	}
	if jobExists(t, c, "default", "done") {
		t.Error("succeeded job should have been deleted")
	}
	if !jobExists(t, c, "default", "busy") {
		t.Error("active job must be kept")
	}
}

func TestReapFailedJobs_FailedAndStuck(t *testing.T) {
	failed := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "failed"},
		Status:     batchv1.JobStatus{Failed: 1, Active: 0},
	}
	stuck := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "stuck",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Status: batchv1.JobStatus{}, // no status at all, old -> stuck
	}
	fresh := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "fresh",
			CreationTimestamp: metav1.Now(),
		},
		Status: batchv1.JobStatus{}, // no status but young -> keep
	}

	r, c := testReaper(t, failed, stuck, fresh)
	if err := r.ReapFailedJobs(context.Background()); err != nil {
		t.Fatalf("ReapFailedJobs: %v", err)
	}
	if jobExists(t, c, "default", "failed") {
		t.Error("failed job should have been deleted")
	}
	if jobExists(t, c, "default", "stuck") {
		t.Error("old status-less job should have been deleted as stuck")
	}
	if !jobExists(t, c, "default", "fresh") {
		t.Error("young status-less job must be kept (within grace)")
	}
}

func TestDryRunDeletesNothing(t *testing.T) {
	bad := pod("default", "failed", corev1.PodFailed)
	r, c := testReaper(t, bad)
	r.cfg.DryRun = true
	if err := r.ReapFailedPods(context.Background()); err != nil {
		t.Fatalf("ReapFailedPods: %v", err)
	}
	if !podExists(t, c, "default", "failed") {
		t.Error("dry-run must not delete anything")
	}
}
