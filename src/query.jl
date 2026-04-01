# ─── Query execution ──────────────────────────────────────────────────────────
#
# Public functions
# ────────────────
#   execute(ctx, sql, args...; kwargs...)   → DataFrame
#   execute!(ctx, sql, args...; kwargs...)  → nothing   (DDL / DML)
#   query(ctx, sql, args...; kwargs...)     → QueryResult (with metadata)
#   execute(ctx, sqls::Vector)              → DataFrame (result of last query)
#   transaction(f, ctx)                     → result of f
#   stream(ctx, sql; batch_size)            → Channel{DataFrame}
#   explain(ctx, sql)                       → String
#
# All execute/query variants accept either positional parameters
#   execute(ctx, "SELECT * FROM t WHERE id = ?", 42)
# or named keyword parameters
#   execute(ctx, "SELECT * FROM t WHERE id = :id", id=42)

# ─── Core low-level executor ──────────────────────────────────────────────────

"""
    _run(conn, config, sql, params) → DataFrame

Run a single SQL statement on `conn`, binding `params` (a `Vector` or
`nothing`).  Timing and error handling are done here.
"""
function _run(
    conn   ::DuckDB.DB,
    config ::QueryConfig,
    sql    ::String,
    params ::Union{Vector, Nothing} = nothing
)::Tuple{DataFrame, Int64}
    t0 = time_ns()
    try
        result_df = if params === nothing
            DuckDB.execute(conn, sql) |> DataFrame
        else
            DuckDB.execute(conn, sql, params) |> DataFrame
        end
        elapsed = Int64(time_ns() - t0)
        @debug "Query executed" elapsed_ms=round(elapsed/1e6;digits=2) rows=nrow(result_df) sql=sql
        return (result_df, elapsed)
    catch e
        @error "Query failed" sql=sql params=params exception=e
        if config.on_error === :throw
            throw(QueryError("Query execution failed", sql, params, e))
        elseif config.on_error === :empty
            return (DataFrame(), Int64(time_ns() - t0))
        else  # :missing
            return (DataFrame(result=[missing]), Int64(time_ns() - t0))
        end
    end
end

# ─── execute ──────────────────────────────────────────────────────────────────

"""
    execute(ctx, sql, args...; kwargs...) → DataFrame
    execute(conn, sql, args...; kwargs...) → DataFrame

Execute a SQL query and return the result as a `DataFrame`.

Positional parameters map to `?` placeholders.
Named keyword parameters map to `:name` placeholders.

```julia
# Plain query
df = execute(ctx, "SELECT * FROM customers")

# Positional params
df = execute(ctx, "SELECT * FROM customers WHERE country = ? AND age > ?", "US", 18)

# Named params
df = execute(ctx, "SELECT * FROM orders WHERE status = :status", status="shipped")

# DDL (return value is an empty DataFrame — can be discarded)
execute(ctx, "CREATE TABLE archive AS SELECT * FROM orders")
```
"""
function execute(ctx::QueryContext, sql::String, args...; kwargs...)::DataFrame
    processed_sql, params = normalise_params(sql, args, kwargs)
    _with_conn(ctx) do conn
        df, _ = _run(conn, ctx.config, processed_sql, params)
        df
    end
end

# Direct-connection variant (used inside transaction blocks)
function execute(conn::DuckDB.DB, config::QueryConfig, sql::String, args...; kwargs...)::DataFrame
    processed_sql, params = normalise_params(sql, args, kwargs)
    df, _ = _run(conn, config, processed_sql, params)
    df
end

# ─── execute! ─────────────────────────────────────────────────────────────────

"""
    execute!(ctx, sql, args...; kwargs...) → nothing

Execute a SQL statement and discard the result.  Intended for DDL/DML
(`CREATE`, `INSERT`, `DROP`, etc.) where a return value is not needed.

```julia
execute!(ctx, "DROP TABLE IF EXISTS tmp")
execute!(ctx, "INSERT INTO log VALUES (?, now())", "job_started")
```
"""
function execute!(ctx::QueryContext, sql::String, args...; kwargs...)::Nothing
    execute(ctx, sql, args...; kwargs...)
    return nothing
end

function execute!(conn::DuckDB.DB, config::QueryConfig, sql::String, args...; kwargs...)::Nothing
    execute(conn, config, sql, args...; kwargs...)
    return nothing
end

# ─── query (with metadata) ────────────────────────────────────────────────────

"""
    query(ctx, sql, args...; kwargs...) → QueryResult

Like `execute` but returns a `QueryResult` that wraps the DataFrame with
execution metadata (elapsed time, original SQL).

```julia
r = query(ctx, "SELECT * FROM trips WHERE passenger_count > ?", 2)
println("Took \$(r.elapsed_ms) ms, got \$(nrow(r)) rows")
df = DataFrame(r)
```
"""
function query(ctx::QueryContext, sql::String, args...; kwargs...)::QueryResult
    processed_sql, params = normalise_params(sql, args, kwargs)
    _with_conn(ctx) do conn
        df, elapsed = _run(conn, ctx.config, processed_sql, params)
        QueryResult(df, elapsed, sql)
    end
end

# ─── Batch execution ──────────────────────────────────────────────────────────

"""
    execute(ctx, sqls::Vector{String}) → DataFrame

Execute a sequence of SQL statements on a single connection and return the
result of the last one.  All statements share the same connection so DDL
created in earlier statements is visible to later ones.

```julia
df = execute(ctx, [
    "CREATE TEMP TABLE t AS SELECT generate_series AS n FROM generate_series(1,100)",
    "SELECT avg(n) FROM t"
])
```
"""
function execute(ctx::QueryContext, sqls::Vector{String})::DataFrame
    isempty(sqls) && return DataFrame()
    _with_conn(ctx) do conn
        last_df = DataFrame()
        for (i, sql) in enumerate(sqls)
            @debug "Batch" step=i total=length(sqls) sql=sql
            last_df, _ = _run(conn, ctx.config, sql, nothing)
        end
        last_df
    end
end

# ─── transaction ──────────────────────────────────────────────────────────────

"""
    transaction(f, ctx) → result of f

Execute `f` inside a DuckDB transaction.  If `f` completes without error,
the transaction is committed.  If `f` throws, the transaction is rolled back
and the exception is re-raised.

The block receives a `Transaction` handle that supports `execute` and `execute!`.

```julia
transaction(ctx) do tx
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 1, 1000.0)
    execute!(tx, "INSERT INTO accounts VALUES (?, ?)", 2, 2000.0)
end
```
"""
function transaction(f::Function, ctx::QueryContext)
    _with_conn(ctx) do conn
        DuckDB.execute(conn, "BEGIN TRANSACTION")
        tx = Transaction(conn, ctx.config)
        try
            result = f(tx)
            DuckDB.execute(conn, "COMMIT")
            @debug "Transaction committed"
            return result
        catch e
            try DuckDB.execute(conn, "ROLLBACK") catch end
            @warn "Transaction rolled back" exception=e
            rethrow(e)
        end
    end
end

"""
    Transaction

A lightweight handle to an open DuckDB transaction.  Obtained via the
`transaction(f, ctx)` do-block; do not construct directly.
"""
struct Transaction
    _conn  ::DuckDB.DB
    _config::QueryConfig
end

function execute(tx::Transaction, sql::String, args...; kwargs...)::DataFrame
    execute(tx._conn, tx._config, sql, args...; kwargs...)
end

function execute!(tx::Transaction, sql::String, args...; kwargs...)::Nothing
    execute!(tx._conn, tx._config, sql, args...; kwargs...)
end

# ─── stream ───────────────────────────────────────────────────────────────────

"""
    stream(ctx, sql; batch_size=10_000) → Channel{DataFrame}

Execute `sql` and return a `Channel` that emits successive `DataFrame` batches.
The channel is closed automatically when all rows have been consumed or when an
error occurs.

The SQL is executed once and results are consumed via DuckDB's native chunk
iterator, so streaming is O(N) regardless of result size.

Each batch contains at least `batch_size` rows (it may be slightly larger
because whole DuckDB-internal chunks of ~2048 rows are accumulated before
emitting).

```julia
for batch in stream(ctx, "SELECT * FROM huge_table"; batch_size=5_000)
    process(batch)
end
```
"""
function stream(ctx::QueryContext, sql::String, args...; batch_size::Int=10_000, kwargs...)::Channel{DataFrame}
    processed_sql, params = normalise_params(sql, args, kwargs)
    Channel{DataFrame}(2) do ch
        _with_conn(ctx) do conn
            result = try
                if params === nothing
                    DuckDB.execute(conn, processed_sql)
                else
                    DuckDB.execute(conn, processed_sql, params)
                end
            catch e
                @error "Query failed" sql=processed_sql exception=e
                if ctx.config.on_error === :throw
                    throw(QueryError("Query execution failed", processed_sql, params, e))
                elseif ctx.config.on_error === :empty
                    return                                 # close channel, zero batches
                else  # :missing
                    put!(ch, DataFrame(result=[missing]))
                    return
                end
            end

            pending      = DataFrame[]
            pending_rows = 0
            for chunk in Tables.partitions(result)
                chunk_df = DataFrame(chunk)
                push!(pending, chunk_df)
                pending_rows += nrow(chunk_df)
                if pending_rows >= batch_size
                    put!(ch, vcat(pending...))
                    empty!(pending)
                    pending_rows = 0
                end
            end
            pending_rows > 0 && put!(ch, vcat(pending...))
        end
    end
end

# ─── explain ──────────────────────────────────────────────────────────────────

"""
    explain(ctx, sql, args...; analyze=false, kwargs...) → String

Return the DuckDB query plan for `sql` as a formatted string.
Set `analyze=true` to include actual execution statistics (runs the query).

Positional and named parameters are supported the same way as `execute`.

```julia
println(explain(ctx, "SELECT * FROM trips WHERE passenger_count > ?", 2))
```
"""
function explain(ctx::QueryContext, sql::String, args...; analyze::Bool=false, kwargs...)::String
    processed_sql, params = normalise_params(sql, args, kwargs)
    prefix = analyze ? "EXPLAIN ANALYZE " : "EXPLAIN "
    _with_conn(ctx) do conn
        df, _ = _run(conn, ctx.config, "$prefix$processed_sql", params)
        (ncol(df) == 0 || nrow(df) == 0) && return ""
        join(df[!, end], "\n")
    end
end
