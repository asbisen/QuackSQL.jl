"""
    QueryDF

A Julia library for querying tabular data sources—`DataFrame`s, CSV files,
Parquet files, and DuckDB databases—using standard SQL via DuckDB.

# Quick start

```julia
using QueryDF, DataFrames

# ── In-memory work ──────────────────────────────────────────────────────────
ctx = QueryContext()                         # or QueryContext(":memory:")
register!(ctx, "orders", df_orders)
register!(ctx, "customers", "customers.parquet")

df = execute(ctx, \"""
    SELECT c.name, SUM(o.amount) AS total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY c.name
    ORDER BY total DESC
    LIMIT 20
\""")

close!(ctx)

# ── Automatic resource management ───────────────────────────────────────────
with_context("warehouse.duckdb"; threads=8, memory_limit="8GB") do ctx
    register!(ctx, "events", "s3://bucket/events/*.parquet")
    execute(ctx, "SELECT date_trunc('day', ts), count(*) FROM events GROUP BY 1")
end

# ── Parameterized queries ────────────────────────────────────────────────────
df = execute(ctx, "SELECT * FROM orders WHERE status = :status AND amount > :min",
             status="shipped", min=100.0)

# ── Transactions ─────────────────────────────────────────────────────────────
transaction(ctx) do tx
    execute!(tx, "INSERT INTO log VALUES (now(), ?)", "job_start")
    execute!(tx, "UPDATE jobs SET started_at = now() WHERE id = ?", job_id)
end

# ── Streaming large results ──────────────────────────────────────────────────
for batch in stream(ctx, "SELECT * FROM huge_table"; batch_size=50_000)
    append!(result_buffer, batch)
end

# ── Connection pool for concurrent use ──────────────────────────────────────
pool = ConnectionPool("analytics.duckdb"; size=8, readonly=true)
Threads.@threads for q in queries
    with_connection(pool) do conn
        execute(conn, pool.config, q)
    end
end
close!(pool)
```

# Logging

QueryDF uses Julia's standard `Logging` module.  Enable debug output with:

```julia
using Logging
Logging.global_logger(ConsoleLogger(stderr, Logging.Debug))
```
"""
module QueryDF

using DuckDB
using DataFrames
using Logging
using Tables
using Dates

include("types.jl")     # QueryError, QueryResult
include("config.jl")    # QueryConfig, _open_db, _apply_config!
include("params.jl")    # normalise_params, _bind_named
include("pool.jl")      # ConnectionPool, acquire!, release!, with_connection
include("context.jl")   # QueryContext, with_context, _with_conn
include("sources.jl")   # register!, deregister!, list_sources, _register_source!
include("query.jl")     # execute, execute!, query, transaction, stream, explain

# ── Public exports ────────────────────────────────────────────────────────────

# Error / result types
export QueryError, QueryResult, elapsed_ms

# Configuration
export QueryConfig

# Connection pool
export ConnectionPool, acquire!, release!, with_connection, close!

# Context
export QueryContext, with_context, close!

# Source management
export register!, deregister!, list_sources

# Query execution
export execute, execute!, query, transaction, stream, explain

end # module QueryDF
