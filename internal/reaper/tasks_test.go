package reaper

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// The task set is what the operator actually does. A task dropped during a
// refactor produces no error and no failing test anywhere else — the reaper
// just quietly stops cleaning one class of debris.
func TestTasksCoverEveryReapFunction(t *testing.T) {
	reaper, _ := testReaper(t)

	tasks := reaper.Tasks()
	if len(tasks) != 5 {
		t.Fatalf("got %d tasks, want 5", len(tasks))
	}

	want := []string{
		"terminating_pods", "evicted_pods", "failed_pods",
		"succeeded_jobs", "failed_jobs",
	}
	for i, name := range want {
		if tasks[i].Name != name {
			t.Errorf("task[%d] = %q, want %q", i, tasks[i].Name, name)
		}
		if tasks[i].Run == nil {
			t.Errorf("task %q has no Run function", name)
		}
	}
}

// Metric label values are derived from task names, so a duplicate would make
// two tasks share a counter and silently halve both.
func TestTaskNamesAreUnique(t *testing.T) {
	reaper, _ := testReaper(t)

	seen := map[string]bool{}
	for _, task := range reaper.Tasks() {
		if seen[task.Name] {
			t.Errorf("duplicate task name %q", task.Name)
		}
		seen[task.Name] = true
	}
}

// A zero interval means time.NewTicker panics and the operator crash-loops.
func TestTaskIntervalsComeFromTheConfig(t *testing.T) {
	reaper, _ := testReaper(t)
	reaper.cfg.TerminatingInterval = 7 * time.Minute
	reaper.cfg.FailedJobInterval = 90 * time.Second

	byName := map[string]time.Duration{}
	for _, task := range reaper.Tasks() {
		byName[task.Name] = task.Interval
	}

	if byName["terminating_pods"] != 7*time.Minute {
		t.Errorf("terminating_pods interval = %v, want 7m", byName["terminating_pods"])
	}
	if byName["failed_jobs"] != 90*time.Second {
		t.Errorf("failed_jobs interval = %v, want 90s", byName["failed_jobs"])
	}
}

func TestRunLoopExecutesImmediatelyAndRecordsSuccess(t *testing.T) {
	reaper, _ := testReaper(t)

	ran := make(chan struct{}, 1)
	task := Task{
		Name:     "test_task",
		Interval: time.Hour, // long enough that only the eager run can fire
		Run: func(context.Context) error {
			select {
			case ran <- struct{}{}:
			default:
			}
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go reaper.runLoop(ctx, task)

	select {
	case <-ran:
	case <-time.After(5 * time.Second):
		t.Fatal("the task did not run before its first tick")
	}
	cancel()

	// Waiting for the metric rather than asserting immediately: the counter is
	// incremented after Run returns.
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := testutil.ToFloat64(reaper.metrics.TaskRuns.WithLabelValues("test_task", "success"))
		if got == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("success counter = %v, want 1", got)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// A failing task must be counted as an error and must not stop the loop:
// a transient API failure should not take a task offline until the next
// restart.
func TestRunLoopRecordsErrorsAndKeepsRunning(t *testing.T) {
	reaper, _ := testReaper(t)

	calls := make(chan struct{}, 10)
	task := Task{
		Name:     "failing_task",
		Interval: 20 * time.Millisecond,
		Run: func(context.Context) error {
			select {
			case calls <- struct{}{}:
			default:
			}
			return errors.New("api server said no")
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go reaper.runLoop(ctx, task)

	// Two executions means the loop survived the first failure.
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d execution(s) before timing out", i)
		}
	}
	cancel()

	deadline := time.Now().Add(5 * time.Second)
	for {
		errCount := testutil.ToFloat64(reaper.metrics.TaskRuns.WithLabelValues("failing_task", "error"))
		if errCount >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("error counter = %v, want at least 2", errCount)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got := testutil.ToFloat64(reaper.metrics.TaskRuns.WithLabelValues("failing_task", "success")); got != 0 {
		t.Errorf("success counter = %v, want 0", got)
	}
}

func TestRunLoopStopsOnContextCancellation(t *testing.T) {
	reaper, _ := testReaper(t)

	stopped := make(chan struct{})
	task := Task{
		Name:     "cancellable",
		Interval: 10 * time.Millisecond,
		Run:      func(context.Context) error { return nil },
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		reaper.runLoop(ctx, task)
		close(stopped)
	}()

	cancel()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("runLoop ignored context cancellation")
	}
}

// Run starts every task and blocks until cancelled. A Run that returned early
// would leave the operator alive but doing nothing.
func TestRunBlocksUntilCancelled(t *testing.T) {
	reaper, _ := testReaper(t)
	reaper.cfg.TerminatingInterval = time.Hour
	reaper.cfg.EvictedInterval = time.Hour
	reaper.cfg.FailedPodInterval = time.Hour
	reaper.cfg.SucceededJobInterval = time.Hour
	reaper.cfg.FailedJobInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	returned := make(chan struct{})
	go func() {
		reaper.Run(ctx)
		close(returned)
	}()

	select {
	case <-returned:
		t.Fatal("Run returned before the context was cancelled")
	case <-time.After(100 * time.Millisecond):
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}
