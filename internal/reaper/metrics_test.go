package reaper

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// classifyError keeps deletion_errors_total cheap. If it ever started
// returning the raw error text, the label would explode into one series per
// pod name and take the whole metrics pipeline with it.
func TestClassifyError(t *testing.T) {
	pods := schema.GroupResource{Resource: "pods"}

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "already deleted",
			err:  apierrors.NewNotFound(pods, "web-1"),
			want: "not_found",
		},
		{
			name: "missing RBAC",
			err:  apierrors.NewForbidden(pods, "web-1", errors.New("no permission")),
			want: "forbidden",
		},
		{
			name: "bad credentials",
			err:  apierrors.NewUnauthorized("token expired"),
			want: "unauthorized",
		},
		{
			name: "resource version conflict",
			err:  apierrors.NewConflict(pods, "web-1", errors.New("modified")),
			want: "conflict",
		},
		{
			name: "api server throttling",
			err:  apierrors.NewTooManyRequests("slow down", 5),
			want: "rate_limited",
		},
		{
			name: "server timeout",
			err:  apierrors.NewServerTimeout(pods, "delete", 1),
			want: "timeout",
		},
		{
			name: "request timeout",
			err:  apierrors.NewTimeoutError("timed out", 1),
			want: "timeout",
		},
		{
			name: "anything else collapses to one bucket",
			err:  errors.New("connection reset by peer"),
			want: "other",
		},
		{
			name: "an internal error is not misfiled",
			err:  apierrors.NewInternalError(errors.New("boom")),
			want: "other",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyError(tt.err); got != tt.want {
				t.Errorf("classifyError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

// Every label value must come from the fixed set above; a stray value means
// unbounded cardinality.
func TestClassifyErrorReturnsOnlyKnownLabels(t *testing.T) {
	allowed := map[string]bool{
		"not_found": true, "forbidden": true, "unauthorized": true,
		"conflict": true, "rate_limited": true, "timeout": true, "other": true,
	}

	errs := []error{
		errors.New("x"),
		apierrors.NewBadRequest("nope"),
		apierrors.NewServiceUnavailable("down"),
		apierrors.NewResourceExpired("expired"),
		&apierrors.StatusError{ErrStatus: metav1.Status{Code: 418}},
	}

	for _, err := range errs {
		if got := classifyError(err); !allowed[got] {
			t.Errorf("classifyError(%v) = %q, which is outside the allowed label set", err, got)
		}
	}
}

func TestMetricsRegisterWithAPedanticRegistry(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()

	// MustRegister panics on a duplicate or inconsistent collector, so a
	// successful call here is the assertion.
	NewMetrics().Register(registry)

	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, f := range families {
		if f.GetHelp() == "" {
			t.Errorf("metric %s has no help text", f.GetName())
		}
	}
}

// Registering the same metric set twice must fail rather than silently
// double-count — this is what catches a collector accidentally registered in
// both main() and a constructor.
func TestDuplicateRegistrationIsRejected(t *testing.T) {
	registry := prometheus.NewRegistry()
	NewMetrics().Register(registry)

	defer func() {
		if recover() == nil {
			t.Error("registering a second identical metric set did not fail")
		}
	}()
	NewMetrics().Register(registry)
}
