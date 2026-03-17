# QuackSQL.jl

A Julia library for querying tabular data sources—DataFrames, CSV files,
Parquet files, and DuckDB databases—using standard SQL, powered by
[DuckDB.jl](https://github.com/duckdb/duckdb/tree/main/tools/juliapkg).

---

## Installation

```julia
using Pkg
Pkg.add(path="path/to/QuackSQL.jl")   # local checkout
```

---

## Quick start

```julia
using QuackSQL, DataFrames

# ── 1. Simplest usage — one-shot query ───────────────────────────────────────
with_context() do ctx
    df = execute(ctx, "SELECT 42 AS answer, 'hello' AS world")
    println(df)
end

# ── 2. Register sources, then query ─────────────────────────────────────────
ctx = QueryContext()

customers = DataFrame(id=1:3, name=["Alice","Bob","Charlie"], country=["US","UK","US"])
orders    = DataFrame(order_id=101:104, customer_id=[1,2,1,3], amount=[120.,80.,230.,55.])

register!(ctx, "customers" => customers, "orders" => orders)

df = execute(ctx, """
    SELECT c.name, SUM(o.amount) AS total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE c.country = 'US'
    GROUP BY c.name
    ORDER BY total DESC
""")

close!(ctx)
```

---

## API reference

### `QueryContext`

The central state object.  Create it once, register your data sources, execute
as many queries as you need, then close it.

```julia
# In-memory (default)
ctx = QueryContext()
ctx = QueryContext(":memory:")

# Persistent file
ctx = QueryContext("warehouse.duckdb")

# With DuckDB settings
ctx = QueryContext("warehouse.duckdb";
    threads      = 8,
    memory_limit = "8GB",
    readonly     = false,
    extensions   = ["httpfs", "spatial"],
    init_sql     = ["SET timezone='UTC'"],
    on_error     = :throw              # :throw | :empty | :missing
)

# Connection pool (for multi-threaded use)
ctx = QueryContext("warehouse.duckdb"; pool_size=8, readonly=true)

close!(ctx)   # always close when done
```

Use `with_context` for automatic cleanup:

```julia
with_context("warehouse.duckdb"; threads=4) do ctx
    execute(ctx, "SELECT count(*) FROM events")
end
```

---

### Registering data sources

```julia
# Single registration
register!(ctx, "customers", df_customers)          # DataFrame
register!(ctx, "trips",     "trips.parquet")        # Parquet file or glob
register!(ctx, "events",    "logs/*.csv")           # CSV glob
register!(ctx, "archive",   "archive.duckdb")       # Attached DuckDB file

# Bulk registration via pairs
register!(ctx,
    "customers" => df_customers,
    "trips"     => "trips.parquet",
    "events"    => "events/*.csv"
)

# Inspect and remove
list_sources(ctx)             # DataFrame with name / type / info columns
deregister!(ctx, "events")
```

---

### Executing queries

```julia
# Returns a DataFrame
df = execute(ctx, "SELECT * FROM customers")

# DDL / DML — fire-and-forget variant
execute!(ctx, "CREATE TABLE summary AS SELECT * FROM customers")
execute!(ctx, "INSERT INTO log VALUES (now(), 'started')")

# Sequence of statements (returns result of last one)
df = execute(ctx, [
    "CREATE TEMP TABLE t AS SELECT generate_series AS n FROM generate_series(1, 100)",
    "SELECT avg(n), sum(n) FROM t"
])
```

#### Parameterized queries

**Positional (`?` placeholders)**

```julia
df = execute(ctx, "SELECT * FROM orders WHERE status = ? AND amount > ?",
             "shipped", 100.0)
```

**Named (`:param` placeholders)**

```julia
df = execute(ctx, "SELECT * FROM orders WHERE status = :st AND amount > :min",
             st="shipped", min=100.0)
```

> Positional and named parameters cannot be mixed in the same query.

---

### `QueryResult` — with metadata

```julia
r = query(ctx, "SELECT * FROM large_table")   # returns QueryResult, not DataFrame

println(r)                    # QueryResult — N rows × M cols (1.23 ms)
println(elapsed_ms(r))        # execution time in milliseconds
df = DataFrame(r)             # explicit DataFrame conversion
r[1, :col]                    # direct indexing delegates to inner DataFrame
```

`QueryResult` satisfies the `Tables.jl` interface so it can be passed directly
to any function that accepts a table source (e.g. `CSV.write`, `Arrow.write`).

---

### Transactions

```julia
transaction(ctx) do tx
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 1, 1_000.0)
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 2, 2_000.0)
    # Committed automatically if no exception; rolled back otherwise
end
```

---

### Streaming large results

```julia
for batch in stream(ctx, "SELECT * FROM billion_row_table"; batch_size=50_000)
    # batch is a plain DataFrame
    process(batch)
end
```

---

### Connection pool

```julia
pool = ConnectionPool("analytics.duckdb";
    size     = 8,
    readonly = true,
    threads  = 4
)

register!(pool, "events", "events.parquet")   # applied lazily per connection

Threads.@threads for q in queries
    df = with_connection(pool) do conn
        execute(conn, pool.config, q)
    end
end

close!(pool)
```

---

### Query plan

```julia
plan = explain(ctx, "SELECT * FROM trips WHERE passenger_count > 2")
println(plan)

# Include actual execution stats (runs the query)
plan = explain(ctx, sql; analyze=true)
```

---

## Logging

QuackSQL uses Julia's standard `Logging` module.  By default only warnings and
errors are emitted.  Enable verbose output with:

```julia
using Logging
Logging.global_logger(ConsoleLogger(stderr, Logging.Debug))
```

Structured log fields (SQL text, elapsed time, table name, etc.) are attached
as key-value pairs, making it easy to filter with packages like
[LoggingExtras.jl](https://github.com/JuliaLogging/LoggingExtras.jl).

---

## Error handling

| `on_error` value | Behaviour on query failure |
|---|---|
| `:throw` (default) | Raises `QueryError` with SQL, params, and root cause |
| `:empty` | Returns an empty `DataFrame`; logs `@error` |
| `:missing` | Returns `DataFrame(result=[missing])`; logs `@error` |

```julia
ctx = QueryContext(on_error=:empty)
df  = execute(ctx, "SELECT * FROM table_that_might_not_exist")
# df is empty rather than throwing
```

---

## File structure

```
QuackSQL.jl/
├── Project.toml
├── README.md
├── src/
│   ├── QuackSQL.jl      # module, exports
│   ├── types.jl        # QueryError, QueryResult
│   ├── config.jl       # QueryConfig, DuckDB connection helpers
│   ├── pool.jl         # ConnectionPool
│   ├── context.jl      # QueryContext, with_context
│   ├── sources.jl      # register!, deregister!, list_sources
│   ├── params.jl       # parameterized query binding
│   └── query.jl        # execute, execute!, query, transaction, stream, explain
├── test/
│   └── runtests.jl
└── examples/
    └── quickstart.jl
```
