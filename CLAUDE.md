# Project: styx

## Overview
Go ORM/database engine library providing unified interfaces for multiple SQL and NoSQL databases, with a Unit of Work pattern for transaction coordination.

## Stack
Go 1.20, gRPC, Protocol Buffers, Docker (for build/test/fmt via containerized Go toolchain)

## Architecture
- `sql/`           — SQL Engine interface + implementations (PostgreSQL, SQLite, Supabase)
- `sql/postgres/`  — PostgreSQL: direct lib + gRPC-based remote access (pb/, pg-grpc/, server/)
- `sql/sqlite/`    — SQLite implementation (lib/ for low-level, sqlite.go for engine)
- `sql/supabase/`  — Supabase REST-based implementation (experimental, incomplete — most methods panic; deprecated)
- `sql/mock/`      — Mock SQL engine for testing
- `nosql/`         — NoSQL Engine interface + implementations (ArangoDB, MongoDB)
- `nosql/mock/`    — Mock NoSQL engine for testing
- `mock/`          — Generic mock engine interface (map-based in mock/map/)
- `dberr/`         — Shared database error types (DataNotFound, RequirementMissing)
- `pkg/`           — Shared utilities (struct decoding/reflection)
- `uow.go`         — Unit of Work: coordinates SQL + NoSQL transactions
- `examples/`      — Usage examples

## Commands
```bash
# build (containerized)
make build

# test (containerized)
make test

# lint/format (containerized)
make fmt

# run grpc server
make start-server

# run grpc client
make start-client

# generate protobuf
make proto-gen

# generate mocks
make mockgen

# update go modules
make modules

# verify (modules + fmt)
make verify
```

## Conventions
- Struct tags use `db:"column_name,constraints"` (pk, autoincr, uq, uqs)
- Engine methods are chainable: `db.Table("x").ID(1).FindOne(&result)`
- SQL and NoSQL engines share similar method signatures (FindOne, FindMany, InsertOne, etc.)
- Vendor directory is used for dependencies
- Builds run inside Docker containers via Makefile

## Active Context

## Learned

<!-- code-review-graph MCP tools -->
## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.
