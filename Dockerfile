# --- build stage ---
FROM golang:1.26 AS build
WORKDIR /src

# cache module downloads
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# static, stripped binary
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/kube-reaper .

# --- runtime stage ---
FROM gcr.io/distroless/static:nonroot
LABEL org.opencontainers.image.source="https://github.com/Inblade/kube-reaper"
LABEL org.opencontainers.image.description="Cluster-wide janitor for stuck/failed pods and finished Jobs"
COPY --from=build /out/kube-reaper /kube-reaper
USER nonroot:nonroot
EXPOSE 8080 8081
ENTRYPOINT ["/kube-reaper"]
