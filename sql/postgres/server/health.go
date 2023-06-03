package server

import (
	"context"
	"log"

	health "google.golang.org/grpc/health/grpc_health_v1"
)

type HealthChecker struct {
	isDatabaseReady bool
}

func NewHealthChecker() *HealthChecker {
	return &HealthChecker{}
}

func (s *HealthChecker) setDatabaseReady() {
	s.isDatabaseReady = true
}

func (s *HealthChecker) Check(ctx context.Context, req *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	log.Println("Serving the Check request for health check")
	status := health.HealthCheckResponse_NOT_SERVING
	if s.isDatabaseReady {
		status = health.HealthCheckResponse_SERVING
	}

	return &health.HealthCheckResponse{
		Status: status,
	}, nil
}

func (s *HealthChecker) Watch(req *health.HealthCheckRequest, server health.Health_WatchServer) error {
	log.Println("Serving the Watch request for health check")
	status := health.HealthCheckResponse_NOT_SERVING
	if s.isDatabaseReady {
		status = health.HealthCheckResponse_SERVING
	}

	return server.Send(&health.HealthCheckResponse{
		Status: status,
	})
}
