# QuackSQL.jl — Quickstart

QuackSQL wraps [DuckDB](https://duckdb.org/) with a clean Julia API: parameterized queries, DataFrame sources, streaming, transactions, and connection pooling — all through a single `QueryContext`.

---

## Contents

1. [Installation](#1-installation)
2. [Opening a context](#2-opening-a-context)
3. [Running queries](#3-running-queries)
4. [Parameterized queries](#4-parameterized-queries)
5. [Registering data sources](#5-registering-data-sources)
6. [Transactions](#6-transactions)
7. [Streaming large results](#7-streaming-large-results)
8. [Query plans](#8-query-plans)
9. [Error handling modes](#9-error-handling-modes)
10. [Connection pooling](#10-connection-pooling)
11. [Configuration reference](#11-configuration-reference)

---

## 1. Installation

```julia
using Pkg
Pkg.add("QuackSQL")
```

```julia
using QuackSQL
```

---

## 2. Opening a context

`QueryContext` is the central object. Create it once, use it for all queries, close it when done.

```julia
# In-memory database (default)
ctx = QueryContext()

# Persistent file database
ctx = QueryContext("analytics.duckdb")

# Always clean up
close!(ctx)
```

Use `with_context` for automatic cleanup — the database is closed even if an error is thrown:

```julia
with_context("analytics.duckdb") do ctx
    execute(ctx, "SELECT count(*) FROM orders")
end
```

---

## 3. Running queries

### `execute` — returns a DataFrame

```julia
df = execute(ctx, "SELECT * FROM customers")
```

### `execute!` — discard the result (DDL / DML)

```julia
execute!(ctx, "CREATE TABLE orders (id INTEGER, amount DOUBLE)")
execute!(ctx, "INSERT INTO orders VALUES (1, 99.50)")
execute!(ctx, "DROP TABLE IF EXISTS tmp")
```

### `query` — returns a `QueryResult` with metadata

```julia
r = query(ctx, "SELECT * FROM orders")

println(r.elapsed_ms)   # execution time in milliseconds
println(nrow(r))        # number of rows
df = DataFrame(r)       # convert to plain DataFrame
r[1, :amount]           # index directly — works like a DataFrame
```

### Batch execution — share one connection across multiple statements

```julia
df = execute(ctx, [
    "CREATE TEMP TABLE t AS SELECT generate_series AS n FROM generate_series(1, 100)",
    "SELECT avg(n) AS mean FROM t",
])
# df contains the result of the last statement
```

---

## 4. Parameterized queries

Always use parameters instead of string interpolation — QuackSQL passes them to DuckDB's prepared statement engine.

### Positional parameters (`?`)

```julia
df = execute(ctx, "SELECT * FROM orders WHERE status = ? AND amount > ?", "shipped", 50.0)
```

### Named parameters (`:name`)

```julia
df = execute(ctx, "SELECT * FROM orders WHERE status = :status AND amount > :min",
             status="shipped", min=50.0)
```

Both styles work identically with `execute!`, `query`, `stream`, and `explain`.
Mixing positional and named parameters in the same query is an error.

---

## 5. Registering data sources

Register external data under a SQL name so you can query it with standard SQL.

### DataFrame

```julia
df_customers = DataFrame(id=[1,2,3], name=["Alice","Bob","Charlie"], country=["US","UK","US"])

register!(ctx, "customers", df_customers)

execute(ctx, "SELECT * FROM customers WHERE country = ?", "US")
```

### CSV file or glob

```julia
register!(ctx, "events", "data/events_2024.csv")
register!(ctx, "all_events", "data/events_*.csv")   # glob

execute(ctx, "SELECT count(*) FROM events")
```

### Parquet file or glob

```julia
register!(ctx, "trips", "yellow_tripdata_2024.parquet")
register!(ctx, "logs",  "logs/*.parquet")

# Merge files with different schemas by column name
register!(ctx, "logs", "logs/*.parquet"; union_by_name=true)
```

### Attached DuckDB database

```julia
register!(ctx, "archive", "archive.duckdb")

# Cross-database query
execute(ctx, "SELECT * FROM archive.main.orders WHERE year = ?", 2023)
```

### Bulk registration with pairs

```julia
register!(ctx,
    "customers" => df_customers,
    "trips"     => "trips.parquet",
    "events"    => "events/*.csv",
)
```

### Inspecting and removing sources

```julia
list_sources(ctx)        # DataFrame with columns: name, type, info

deregister!(ctx, "trips")
```

---

## 6. Transactions

Wrap multiple writes in a transaction. On success it commits; on any error it rolls back and re-throws.

```julia
transaction(ctx) do tx
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 1, 1_000.0)
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 2, 2_000.0)
    execute!(tx, "UPDATE balances SET total = total + ? WHERE id = ?", 500.0, 1)
end
```

The `tx` handle supports the same `execute` and `execute!` signatures as a regular context.

---

## 7. Streaming large results

`stream` executes a query once and yields results as a `Channel` of DataFrames. Memory usage is bounded to `batch_size` rows regardless of total result size.

```julia
for batch in stream(ctx, "SELECT * FROM huge_table ORDER BY ts"; batch_size=5_000)
    process(batch)   # each batch is a plain DataFrame
end
```

Parameterized streaming works the same way:

```julia
for batch in stream(ctx, "SELECT * FROM events WHERE user_id = ?", user_id; batch_size=10_000)
    process(batch)
end
```

Collect all batches into one DataFrame when size allows:

```julia
df = vcat(collect(stream(ctx, "SELECT * FROM medium_table"))...)
```

---

## 8. Query plans

`explain` returns the DuckDB query plan as a formatted string — useful for tuning slow queries.

```julia
println(explain(ctx, "SELECT * FROM trips WHERE passenger_count > ?", 2))
```

Pass `analyze=true` to include actual row counts and timing (runs the query):

```julia
println(explain(ctx, "SELECT avg(fare) FROM trips GROUP BY vendor_id"; analyze=true))
```

---

## 9. Error handling modes

Control what happens when a query fails by setting `on_error` on the context.

| Mode        | Behaviour                                              |
|-------------|--------------------------------------------------------|
| `:throw`    | Raises `QueryError` (default)                          |
| `:empty`    | Returns an empty `DataFrame` (or zero stream batches)  |
| `:missing`  | Returns a one-row `DataFrame` with a `missing` sentinel|

```julia
# Strict (default)
ctx = QueryContext(on_error=:throw)
execute(ctx, "SELECT * FROM nonexistent")   # throws QueryError

# Graceful degradation — useful in pipelines that tolerate missing data
ctx = QueryContext(on_error=:empty)
df = execute(ctx, "SELECT * FROM nonexistent")   # → empty DataFrame

# Sentinel value — lets callers detect failure without try/catch
ctx = QueryContext(on_error=:missing)
df = execute(ctx, "SELECT * FROM nonexistent")
ismissing(df[1, :result])   # → true
```

`QueryError` carries the original SQL, bound parameters, and the underlying DuckDB exception:

```julia
try
    execute(ctx, "SELECT * FROM nonexistent")
catch e::QueryError
    println(e.sql)     # the failing SQL
    println(e.cause)   # the DuckDB exception
end
```

---

## 10. Connection pooling

Use a pooled context when multiple Julia tasks will query concurrently. Each task gets its own DuckDB connection; registered sources are applied automatically to every connection.

```julia
# pool_size connections, all pointing at the same database
ctx = QueryContext("analytics.duckdb"; pool_size=8, threads=4, readonly=true)

# Each @spawn task gets its own connection from the pool
results = map(1:20) do i
    Threads.@spawn execute(ctx, "SELECT * FROM trips WHERE vendor_id = ?", i)
end

df = vcat(fetch.(results)...)
close!(ctx)
```

Sources registered on a pooled context propagate to every connection automatically:

```julia
register!(ctx, "customers", df_customers)   # available to all pool connections
```

---

## 11. Configuration reference

All options can be passed as keyword arguments to `QueryContext` or `QueryConfig`.

```julia
ctx = QueryContext("data.duckdb";
    threads      = 4,           # DuckDB worker threads (0 = auto)
    memory_limit = "4GB",       # cap DuckDB's memory use
    readonly     = true,        # open file in read-only mode
    extensions   = ["httpfs",   # DuckDB extensions to INSTALL and LOAD
                    "spatial"],
    init_sql     = [            # SQL run on every new connection
        "SET timezone = 'UTC'",
        "SET enable_progress_bar = false",
    ],
    on_error     = :empty,      # :throw | :empty | :missing
    pool_size    = 4,           # >1 enables connection pooling
)
```

Or build a `QueryConfig` separately and reuse it:

```julia
cfg = QueryConfig(threads=8, memory_limit="8GB", on_error=:empty)

with_context("warehouse.duckdb"; threads=cfg.threads,
             memory_limit=cfg.memory_limit, on_error=cfg.on_error) do ctx
    execute(ctx, "SELECT sum(revenue) FROM sales")
end
```
