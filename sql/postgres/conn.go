package postgres

import (
	"context"
	"database/sql"
	"fmt"

	isql "github.com/masudur-rahman/styx/v2/sql"

	_ "github.com/lib/pq"
)

type PostgresConfig struct {
	Name     string `json:"name" yaml:"name"`
	Host     string `json:"host" yaml:"host"`
	Port     string `json:"port" yaml:"port"`
	User     string `json:"user" yaml:"user"`
	Password string `json:"password" yaml:"password"`
	SSLMode  string `json:"sslmode" yaml:"sslmode"`
}

func (cp PostgresConfig) String() string {
	return fmt.Sprintf("user=%v password=%v dbname=%v host=%v port=%v sslmode=%v", cp.User, cp.Password, cp.Name, cp.Host, cp.Port, cp.SSLMode)
}

// GetPostgresConnection opens a PostgreSQL database and returns a *sql.DB
// connection pool. An optional PoolConfig tunes the pool's size and connection
// lifetimes.
func GetPostgresConnection(cfg PostgresConfig, pool ...isql.PoolConfig) (*sql.DB, error) {
	db, err := sql.Open("postgres", cfg.String())
	if err != nil {
		return nil, err
	}
	if len(pool) > 0 {
		pool[0].Apply(db)
	}

	if err = db.PingContext(context.Background()); err != nil {
		return nil, err
	}

	return db, nil
}

// IsZeroValue checks if a value is its type's zero value.
// Deprecated: Use dberr.IsZeroValue instead.
