IMAGE ?= ghcr.io/inblade/kube-reaper:dev

.PHONY: build test vet lint tidy docker run-dry clean

build: ## Build the binary
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o bin/kube-reaper .

test: ## Run unit tests
	go test -race -count=1 ./...

vet: ## Run go vet
	go vet ./...

tidy: ## Sync go.mod/go.sum
	go mod tidy

docker: ## Build the container image
	docker build -t $(IMAGE) .

run-dry: build ## Run locally against your kubeconfig in dry-run (no leader election)
	REAPER_LEADER_ELECTION=false ./bin/kube-reaper --dry-run

clean:
	rm -rf bin
