# ─── QueryContext ─────────────────────────────────────────────────────────────
#
# QueryContext is the primary object users interact with.  It holds:
#   • a DuckDB.DB connection (or a ConnectionPool for multi-threaded use)
#   • a source registry (DataFrames, CSV/Parquet paths, attached databases)
#   • the QueryConfig
#
# Lifecycle
# ─────────
#   ctx = QueryContext(":memory:")          single-connection, in-memory
#   ctx = QueryContext("data.duckdb")       single-connection, persistent
#   ctx = QueryContext("data.duckdb"; pool_size=8)   pooled
#   close!(ctx)                             explicit close
#
# Or use the do-block form for automatic cleanup:
#   with_context("data.duckdb") do ctx
#       ...
#   end

"""
    QueryContext

The central state object for QuackSQL.  Create it once, register your sources,
then execute as many queries as you need.

```julia
ctx = QueryContext(":memory:"; threads=4, memory_limit="2GB")
register!(ctx, "customers", df)
df_result = execute(ctx, "SELECT * FROM customers WHERE country = ?", "US")
close!(ctx)
```

Use `with_context` for automatic resource cleanup.
"""
mutable struct QueryContext
    db_path::String
    config::QueryConfig
    _conn::Union{DuckDB.DB, Nothing}         # nil when pool_size > 1
    _pool::Union{ConnectionPool, Nothing}    # nil when pool_size == 1
    sources::Dict{String, Any}
    _closed::Bool
    _lock::ReentrantLock                     # guards sources and _closed

    function QueryContext(
        db_path::String = ":memory:";
        pool_size::Int = 1,
        kwargs...
    )
        config = QueryConfig(; kwargs...)
        _validate_config(config)

        if pool_size == 1
            conn = _open_db(db_path, config)
            _apply_config!(conn, config)
            ctx = new(db_path, config, conn, nothing, Dict{String,Any}(), false, ReentrantLock())
        else
            pool = ConnectionPool(db_path; size=pool_size, kwargs...)
            ctx = new(db_path, config, nothing, pool, Dict{String,Any}(), false, ReentrantLock())
        end

        finalizer(_finalizer_close!, ctx)
        return ctx
    end
end

function _finalizer_close!(ctx::QueryContext)
    try close!(ctx) catch end
end

# ─── Lifecycle ────────────────────────────────────────────────────────────────

"""
    close!(ctx)

Release all DuckDB connections held by this context.  After calling `close!`,
the context must not be used.
"""
function close!(ctx::QueryContext)
    conn, pool = lock(ctx._lock) do
        ctx._closed && return (nothing, nothing)
        ctx._closed = true
        c, p = ctx._conn, ctx._pool
        ctx._conn = nothing
        ctx._pool = nothing
        (c, p)
    end
    conn !== nothing && try close(conn) catch end
    pool !== nothing && close!(pool)
    @debug "QueryContext closed" db_path=ctx.db_path
end

"""
    with_context([f,] db_path; kwargs...) → result of f

Create a `QueryContext`, pass it to `f`, then close it — even if `f` throws.

```julia
with_context("sales.duckdb"; threads=4) do ctx
    register!(ctx, "orders", df_orders)
    execute(ctx, "SELECT sum(amount) FROM orders")
end
```
"""
function with_context(f::Function, db_path::String = ":memory:"; kwargs...)
    ctx = QueryContext(db_path; kwargs...)
    try
        return f(ctx)
    finally
        close!(ctx)
    end
end

# ─── Internal: connection borrowing ───────────────────────────────────────────

"""
    _with_conn(f, ctx) → result of f(conn)

Internal helper: obtain a connection (either the single persistent one or one
from the pool), ensure all registered sources are applied to it, call `f`,
and release the connection back to the pool if needed.
"""
function _with_conn(f::Function, ctx::QueryContext)
    ctx._closed && throw(QueryError("QueryContext has been closed."))
    if ctx._pool !== nothing
        # Pool path: acquire → apply sources → call f → release
        with_connection(ctx._pool) do conn
            # Snapshot ctx.sources under the lock so the iteration below does
            # not race with concurrent register!/deregister! calls.
            snapshot = lock(ctx._lock) do
                collect(ctx.sources)
            end
            for (name, src) in snapshot
                needs_apply = lock(ctx._pool._lock) do
                    !(name in get(ctx._pool._applied, conn, Set{String}()))
                end
                if needs_apply
                    _register_source!(conn, name, src)
                    lock(ctx._pool._lock) do
                        push!(get!(ctx._pool._applied, conn, Set{String}()), name)
                    end
                end
            end
            f(conn)
        end
    else
        # Single connection path
        f(ctx._conn)
    end
end
