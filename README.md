# QuackSQL.jl — Quickstart

QuackSQL wraps [DuckDB](https://duckdb.org/) with a clean Julia API: parameterized queries,
DataFrame sources, streaming, transactions, and connection pooling — all through a single
`QueryContext`.

> **How to use this guide**
> The examples in sections 2–8 are a single continuous session. Paste them top-to-bottom
> in a Julia REPL (or a notebook) and every line will work. Sections 9 and 10 are
> self-contained and can be run independently.

---

## Contents

1. [Installation](#1-installation)
2. [Setup — sample data and context](#2-setup--sample-data-and-context)
3. [Running queries](#3-running-queries)
4. [Parameterized queries](#4-parameterized-queries)
5. [Registering data sources](#5-registering-data-sources)
6. [Transactions](#6-transactions)
7. [Streaming large results](#7-streaming-large-results)
8. [Query plans](#8-query-plans)
9. [Error handling modes](#9-error-handling-modes)  ← self-contained
10. [Connection pooling](#10-connection-pooling)      ← self-contained
11. [Configuration reference](#11-configuration-reference)
12. [SQL macros — @query, @query!, @stream](#12-sql-macros--query-query-stream)  ← self-contained

---

## 1. Installation

```julia
using Pkg
# Once (and if) the package is registered
# Pkg.add("QuackSQL")
Pkg.add("https://github.com/asbisen/QuackSQL.jl.git")
```

```julia
using QuackSQL, DataFrames
```

---

## 2. Setup — sample data and context

Paste this block once. It creates the in-memory context and sample DataFrames used
throughout sections 3–8.

```julia
using QuackSQL, DataFrames

# ── Sample data ──────────────────────────────────────────────────────────────
customers = DataFrame(
    id      = [1, 2, 3, 4, 5],
    name    = ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    country = ["US", "UK", "US", "CA", "UK"],
    age     = [32,  45,  28,  35,  52],
)

orders = DataFrame(
    id          = [101, 102, 103, 104, 105, 106],
    customer_id = [1,   2,   1,   3,   4,   2  ],
    status      = ["shipped", "pending", "shipped", "cancelled", "shipped", "shipped"],
    amount      = [120.50, 89.00, 340.00, 55.75, 210.25, 67.80],
    year        = [2023,  2023,  2024,  2024,  2024,  2024 ],
)

# ── Open an in-memory context ────────────────────────────────────────────────
ctx = QueryContext()

# Register both DataFrames as queryable SQL tables
register!(ctx, "customers", customers)
register!(ctx, "orders",    orders)
```

---

## 3. Running queries

### `execute` — returns a DataFrame

```julia
execute(ctx, "SELECT * FROM customers")
# 5×4 DataFrame
#  Row │ id     name     country  age
# ─────┼──────────────────────────────
#    1 │  1  Alice    US        32
#  ...
```

```julia
execute(ctx, "SELECT c.name, sum(o.amount) AS total
              FROM customers c
              JOIN orders o ON o.customer_id = c.id
              GROUP BY c.name
              ORDER BY total DESC")
```

### `execute!` — discard the result (DDL / DML)

```julia
execute!(ctx, "CREATE TABLE revenue_by_year AS
               SELECT year, round(sum(amount), 2) AS revenue
               FROM orders
               GROUP BY year
               ORDER BY year")

execute!(ctx, "INSERT INTO revenue_by_year VALUES (2022, 0.0)")
```

### `query` — returns a `QueryResult` with timing metadata

```julia
r = query(ctx, "SELECT * FROM revenue_by_year ORDER BY year")

println("Took $(r.elapsed_ns) ns, got $(nrow(r)) rows")
# Took 0.12 ms, got 3 rows

r[1, :year]          # → 2022    (direct indexing, no conversion needed)
df = DataFrame(r)    # convert to a plain DataFrame when required
```

### Batch execution — multiple statements on one connection

```julia
# DDL and query share the same connection, so the temp table is visible
df = execute(ctx, [
    "CREATE TEMP TABLE top_customers AS
         SELECT customer_id, sum(amount) AS spend
         FROM orders GROUP BY customer_id ORDER BY spend DESC LIMIT 3",
    "SELECT c.name, t.spend
     FROM top_customers t JOIN customers c ON c.id = t.customer_id
     ORDER BY t.spend DESC",
])
# df is the result of the last statement
```

---

## 4. Parameterized queries

Always use parameters instead of string interpolation — QuackSQL passes them to
DuckDB's prepared statement engine, eliminating SQL injection risk.

### Positional parameters (`?`)

```julia
# Single filter
execute(ctx, "SELECT * FROM orders WHERE status = ?", "shipped")

# Multiple filters
execute(ctx, "SELECT * FROM orders WHERE status = ? AND amount > ?",
        "shipped", 100.0)
```

### Named parameters (`:name`)

```julia
execute(ctx, "SELECT * FROM orders WHERE status = :status AND amount > :min",
        status="shipped", min=100.0)
```

### Parameters with `execute!` and `query`

```julia
execute!(ctx, "INSERT INTO revenue_by_year VALUES (:yr, :rev)", yr=2021, rev=4321.00)

r = query(ctx, "SELECT * FROM revenue_by_year WHERE year >= ?", 2022)
println("$(nrow(r)) rows returned in $(r.elapsed_ms) ms")
```

> Mixing positional and named parameters in the same query raises a `QueryError`.

---

## 5. Registering data sources

### DataFrame (already shown in Setup)

```julia
# Replace an existing source by registering under the same name
updated_customers = DataFrame(
    id      = [1, 2, 3, 4, 5, 6],
    name    = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank"],
    country = ["US", "UK", "US", "CA", "UK", "US"],
    age     = [32, 45, 28, 35, 52, 41],
)
register!(ctx, "customers", updated_customers)

execute(ctx, "SELECT count(*) AS n FROM customers")
# n = 6
```

### CSV file

```julia
# Write a temp CSV using DuckDB, then register it back as a view
csv_path = joinpath(tempdir(), "orders_export.csv")
execute!(ctx, "COPY orders TO '$(csv_path)' (FORMAT CSV, HEADER true)")

register!(ctx, "orders_csv", csv_path)
execute(ctx, "SELECT count(*) AS n FROM orders_csv")
# n = 6
```

### Parquet file

```julia
parquet_path = joinpath(tempdir(), "orders_export.parquet")
execute!(ctx, "COPY orders TO '$(parquet_path)' (FORMAT PARQUET)")

register!(ctx, "orders_parquet", parquet_path)
execute(ctx, "SELECT avg(amount) AS avg_amount FROM orders_parquet")
```

### Parquet glob with `union_by_name`

```julia
# Write two parquet files with slightly different schemas
parquet_a = joinpath(tempdir(), "orders_2023.parquet")
parquet_b = joinpath(tempdir(), "orders_2024.parquet")
execute!(ctx, "COPY (SELECT * FROM orders WHERE year = 2023) TO '$(parquet_a)' (FORMAT PARQUET)")
execute!(ctx, "COPY (SELECT * FROM orders WHERE year = 2024) TO '$(parquet_b)' (FORMAT PARQUET)")

glob_pattern = joinpath(tempdir(), "orders_*.parquet")
register!(ctx, "all_orders_pq", glob_pattern; union_by_name=true)
execute(ctx, "SELECT year, count(*) AS n FROM all_orders_pq GROUP BY year ORDER BY year")
```

### Bulk registration with pair syntax

```julia
extra = DataFrame(id=[7,8], name=["Grace","Hank"], country=["DE","FR"], age=[29,38])

register!(ctx,
    "customers"  => extra,               # replaces current customers source
    "orders_csv" => csv_path,
)
```

### Inspecting and removing sources

```julia
list_sources(ctx)
# 5×3 DataFrame with columns: name | type | info

deregister!(ctx, "orders_csv")
deregister!(ctx, "all_orders_pq")
list_sources(ctx)
```

---

## 6. Transactions

Wrap multiple writes in a transaction. Commits on success; rolls back and re-throws on
any error.

```julia
# Setup: create an accounts table
execute!(ctx, "CREATE TABLE accounts (id INTEGER, balance DOUBLE)")

# Successful transaction — both inserts commit together
transaction(ctx) do tx
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 1, 1_000.0)
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 2, 2_500.0)
end

execute(ctx, "SELECT * FROM accounts")
# 2 rows: id=1 balance=1000.0, id=2 balance=2500.0
```

```julia
# Failed transaction — rollback leaves accounts unchanged
try
    transaction(ctx) do tx
        execute!(tx, "UPDATE accounts SET balance = balance - ? WHERE id = ?", 500.0, 1)
        error("payment gateway timeout")   # simulated failure
    end
catch e
    println("Transaction rolled back: $(e.msg)")
end

execute(ctx, "SELECT * FROM accounts ORDER BY id")
# Still 2 rows with original balances — rollback preserved state
```

---

## 7. Streaming large results

`stream` executes the query once and yields successive `DataFrame` batches through a
`Channel`. Memory stays bounded to roughly `batch_size` rows regardless of total
result size.

```julia
# Create a 50 000-row table to stream
execute!(ctx, "CREATE TABLE events AS
               SELECT
                   (random() * 1000)::INTEGER AS user_id,
                   (now()::TIMESTAMP - INTERVAL (random() * 365) DAY) AS ts,
                   ['click','view','purchase'][1 + (random()*2)::INTEGER] AS action
               FROM generate_series(1, 50_000)")
```

```julia
# Count total rows across all batches
total_rows = sum(nrow(batch) for batch in stream(ctx, "SELECT * FROM events"; batch_size=10_000))
println("Processed $total_rows rows")   # → 50000
```

```julia
# Accumulate per-action counts across batches
action_counts = Dict{String,Int}()
for batch in stream(ctx, "SELECT * FROM events"; batch_size=10_000)
    for action in batch.action
        action_counts[action] = get(action_counts, action, 0) + 1
    end
end
println(action_counts)
```

```julia
# Parameterized streaming — filter inside the query
user_id = 42
batches = collect(stream(ctx,
    "SELECT * FROM events WHERE user_id = ? ORDER BY ts",
    user_id; batch_size=5_000))

println("$(sum(nrow, batches)) events for user $user_id")
```

```julia
# Collect all batches when the result fits in memory
df = vcat(collect(stream(ctx, "SELECT action, count(*) AS n
                               FROM events GROUP BY action"))...)
```

---

## 8. Query plans

`explain` returns the DuckDB query plan as a formatted string — useful for diagnosing
slow queries.

```julia
# Static plan (does not run the query)
println(explain(ctx,
    "SELECT c.name, sum(o.amount)
     FROM customers c JOIN orders o ON o.customer_id = c.id
     WHERE o.status = ?
     GROUP BY c.name",
    "shipped"))
```

```julia
# Annotated plan with actual row counts and timing (runs the query)
println(explain(ctx,
    "SELECT action, count(*) FROM events GROUP BY action";
    analyze=true))
```

```julia
# Close the session when done with sections 3–8
close!(ctx)
```

---

## 9. Error handling modes

*Self-contained — paste from `using QuackSQL, DataFrames` below.*

Control what happens when a query fails via the `on_error` keyword.

| Mode       | Behaviour                                             |
|------------|-------------------------------------------------------|
| `:throw`   | Raises `QueryError` (default)                         |
| `:empty`   | Returns an empty `DataFrame` (or zero stream batches) |
| `:missing` | Returns a one-row `DataFrame` with a `missing` value  |

```julia
using QuackSQL, DataFrames

# ── :throw (default) ─────────────────────────────────────────────────────────
ctx = QueryContext(on_error=:throw)

try
    execute(ctx, "SELECT * FROM nonexistent_table")
catch e
    println(typeof(e))   # QuackSQL.QueryError
    println(e.sql)       # SELECT * FROM nonexistent_table
    println(e.cause)     # the underlying DuckDB error
end

close!(ctx)
```

```julia
# ── :empty ────────────────────────────────────────────────────────────────────
ctx = QueryContext(on_error=:empty)

df = execute(ctx, "SELECT * FROM nonexistent_table")
println(nrow(df))   # 0
println(ncol(df))   # 0

batches = collect(stream(ctx, "SELECT * FROM nonexistent_table"))
println(length(batches))   # 0

close!(ctx)
```

```julia
# ── :missing ──────────────────────────────────────────────────────────────────
ctx = QueryContext(on_error=:missing)

df = execute(ctx, "SELECT * FROM nonexistent_table")
println(size(df))              # (1, 1)
println(names(df))             # ["result"]
println(ismissing(df[1,1]))   # true

close!(ctx)
```

---

## 10. Connection pooling

*Self-contained — paste from `using QuackSQL, DataFrames` below.*

Use a pooled context when multiple Julia tasks query concurrently. Each task gets its own
DuckDB connection; registered sources are applied automatically to every connection in the
pool.

```julia
using QuackSQL, DataFrames

# ── Build a persistent database to share across tasks ────────────────────────
db_path = joinpath(tempdir(), "pool_demo.duckdb")

setup_ctx = QueryContext(db_path)
execute!(setup_ctx, "CREATE TABLE IF NOT EXISTS sales AS
                     SELECT
                         (random() * 10 + 1)::INTEGER AS region_id,
                         round((random() * 1000)::NUMERIC, 2) AS amount
                     FROM generate_series(1, 100_000)")
close!(setup_ctx)
```

```julia
# ── Open the same database with a 4-connection pool ───────────────────────────
ctx = QueryContext(db_path; pool_size=4)

# Register a DataFrame source — propagates to all pool connections automatically
regions = DataFrame(id=[1,2,3,4,5,6,7,8,9,10],
                    name=["North","South","East","West","Central",
                          "NE","NW","SE","SW","Mid"])
register!(ctx, "regions", regions)
```

```julia
# ── Run 20 queries concurrently across the pool ───────────────────────────────
tasks = map(1:10) do region_id
    Threads.@spawn execute(ctx,
        "SELECT r.name, count(*) AS n, round(sum(s.amount), 2) AS revenue
         FROM sales s JOIN regions r ON r.id = s.region_id
         WHERE s.region_id = ?
         GROUP BY r.name",
        region_id)
end

results = vcat(fetch.(tasks)...)
sort!(results, :revenue; rev=true)
println(results)

close!(ctx)
```

---

## 11. Configuration reference

All options are keyword arguments to `QueryContext`.

```julia
ctx = QueryContext("data.duckdb";
    threads      = 4,            # DuckDB worker threads (0 = auto-detect)
    memory_limit = "4GB",        # cap DuckDB's memory use
    readonly     = true,         # open file in read-only mode
    extensions   = ["httpfs",    # DuckDB extensions to INSTALL + LOAD on connect
                    "spatial"],
    init_sql     = [             # SQL executed on every new connection
        "SET timezone = 'UTC'",
        "SET enable_progress_bar = false",
    ],
    on_error     = :empty,       # :throw | :empty | :missing
    pool_size    = 4,            # >1 enables connection pooling
)
close!(ctx)
```

| Option         | Default    | Description                                      |
|----------------|------------|--------------------------------------------------|
| `threads`      | `0`        | DuckDB worker threads; `0` = DuckDB default      |
| `memory_limit` | `""`       | e.g. `"4GB"`; empty = DuckDB default             |
| `readonly`     | `false`    | Open file databases in read-only mode            |
| `extensions`   | `String[]` | Extensions to `INSTALL` and `LOAD`               |
| `init_sql`     | `String[]` | SQL run on every new connection                  |
| `on_error`     | `:throw`   | `:throw`, `:empty`, or `:missing`                |
| `pool_size`    | `1`        | Connection pool size; `1` = single connection    |

---

## 12. SQL macros — @query, @query!, @stream

*Self-contained — paste from `using QuackSQL, DataFrames` below.*

The `@query`, `@query!`, and `@stream` macros let you write SQL with standard
Julia `$variable` / `$(expression)` interpolation. Each interpolation is replaced
with a `?` placeholder at **compile time** and the value is passed to DuckDB's
prepared statement engine at **run time** — injection-safe by construction, with
no manual placeholder counting.

> **Note:** `$interpolations` work for SQL *values* (strings, numbers, dates).
> They cannot be used for *identifiers* such as table or column names, because
> SQL prepared statements do not support parameterized identifiers.

### `@query` — returns a DataFrame

```julia
using QuackSQL, DataFrames

ctx = QueryContext()
execute!(ctx, "CREATE TABLE orders AS
    SELECT i AS id,
           ['shipped','pending','cancelled'][1+(i%3)] AS status,
           round(random()*500, 2) AS amount
    FROM generate_series(1, 50) t(i)")

status  = "shipped"
min_amt = 100.0

df = @query ctx """
    SELECT id, status, amount
    FROM   orders
    WHERE  status = $status
      AND  amount > $min_amt
    ORDER  BY amount DESC
"""
println(df)
```

Multiline strings, arbitrary expressions, and multiple variables all work:

```julia
lo, hi = 10, 30

df = @query ctx "SELECT * FROM orders WHERE id BETWEEN $lo AND $hi"

# Expressions are evaluated at the call site
df = @query ctx "SELECT * FROM orders WHERE id > $(lo * 2 - 5)"
```

### `@query!` — discard result (DML)

Use `@query!` for INSERT, UPDATE, DELETE, and other statements where the
return value is not needed.

```julia
msg  = "nightly_run"
code = 0
execute!(ctx, "CREATE TABLE log (msg VARCHAR, code INTEGER)")

@query! ctx "INSERT INTO log VALUES ($msg, $code)"

execute(ctx, "SELECT * FROM log")
```

### `@stream` — streaming with interpolation

`@stream` accepts the same `batch_size` keyword as `stream`.

```julia
# Stream all rows for a specific status in 15-row batches
status     = "shipped"
batch_size = 15

row_count = 0
for batch in @stream ctx "SELECT * FROM orders WHERE status = $status" batch_size=batch_size
    global row_count += nrow(batch)
end
println("Shipped orders: $row_count")
```

```julia
# batch_size can itself be a variable
bs = 20
total = sum(nrow(b) for b in @stream ctx "SELECT * FROM orders" batch_size=bs)
println("Total orders: $total")

close!(ctx)
```

### Comparison with explicit parameters

The three styles are exactly equivalent — choose what reads best:

```julia
status = "shipped"
min_amt = 100.0

# Macro — reads like plain SQL
df = @query ctx "SELECT * FROM orders WHERE status = $status AND amount > $min_amt"

# Positional — explicit but requires counting ?
df = execute(ctx, "SELECT * FROM orders WHERE status = ? AND amount > ?", status, min_amt)

# Named — verbose but self-documenting
df = execute(ctx, "SELECT * FROM orders WHERE status = :status AND amount > :min",
             status=status, min=min_amt)
```
