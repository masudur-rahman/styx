package sql

// Config holds resolved engine construction options. Engine implementations read
// it to wire up cross-cutting behaviour like observation and statement caching.
type Config struct {
	// Observer, when set, receives a callback for every executed statement.
	Observer Observer
	// StmtCache enables prepared-statement caching on the non-transaction path.
	StmtCache bool
}

// Option configures engine construction.
type Option func(*Config)

// WithObserver attaches an Observer that is notified for every executed statement.
func WithObserver(o Observer) Option {
	return func(c *Config) { c.Observer = o }
}

// WithStmtCache enables prepared-statement caching for recurring queries executed
// outside a transaction.
func WithStmtCache() Option {
	return func(c *Config) { c.StmtCache = true }
}

// BuildConfig resolves the given options into a Config.
func BuildConfig(opts ...Option) Config {
	var c Config
	for _, o := range opts {
		o(&c)
	}
	return c
}
