package sql

import (
	"context"
	"time"
)

// Observer receives a callback for every SQL statement the engine executes,
// carrying the query, its arguments, how long it took, and any error. It is the
// integration point for logging, metrics, and tracing (e.g. an OpenTelemetry
// adapter) without styx depending on any of them. Implementations must be safe
// for concurrent use and should not block.
type Observer interface {
	// OnQuery is called once per executed statement, after it completes.
	OnQuery(ctx context.Context, query string, args []any, dur time.Duration, err error)
}
