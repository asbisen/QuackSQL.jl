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
    QueryContext(db_path=":memory:"; pool_size=1, kwargs...)

Central state object for QuackSQL: holds the database connection (or pool),
registered sources, and configuration. Create once, reuse for all queries.

# Arguments
- `db_path`: Path to a DuckDB file, or `":memory:"` for an in-memory database.
- `pool_size`: Number of connections. Use `> 1` for concurrent/multi-threaded workloads.

# Configuration kwargs (forwarded to `QueryConfig`)
- `threads::Int`: DuckDB thread count (default: `1`).
- `memory_limit::String`: e.g. `"2GB"` (default: `""`).
- `readonly::Bool`: Open in read-only mode (default: `false`).
- `extensions::Vector{String}`: DuckDB extensions to auto-load.
- `init_sql::Vector{String}`: SQL statements run on each new connection.
- `on_error::Symbol`: `:throw` (default), `:empty`, or `:missing`.

# Fields
- `db_path`: The database path this context was opened with.
- `config`: The [`QueryConfig`](@ref) snapshot (immutable after construction).
- `sources`: `Dict{String,Any}` of registered named sources.

# Examples
```julia
# In-memory, default settings
ctx = QueryContext()

# File-based with tuning
ctx = QueryContext("analytics.duckdb"; threads=4, memory_limit="4GB")

# Pooled for concurrent use
ctx = QueryContext("data.duckdb"; pool_size=8)

# Automatic cleanup via do-block
with_context("data.duckdb"; threads=2) do ctx
    register!(ctx, "sales", df)
    execute(ctx, "SELECT region, SUM(amount) FROM sales GROUP BY region")
end
```

See also [`register!`](@ref), [`execute`](@ref), [`query`](@ref),
[`close!`](@ref), [`with_context`](@ref).
"""
mutable struct QueryContext
    db_path::String
    config::QueryConfig
    _conn::Union{DuckDB.DB, Nothing}         # nil when pool_size > 1
    _pool::Union{ConnectionPool, Nothing}    # nil when pool_size == 1
    sources::Dict{String, Any}
    _closed::Bool

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
            ctx = new(db_path, config, conn, nothing, Dict{String,Any}(), false)
        else
            pool = ConnectionPool(db_path; size=pool_size, kwargs...)
            ctx = new(db_path, config, nothing, pool, Dict{String,Any}(), false)
        end

        finalizer(_finalizer_close!, ctx)
        return ctx
    end
end

function Base.show(io::IO, ctx::QueryContext)
    status = ctx._closed ? "closed" : "open"
    mode   = ctx._pool !== nothing ? "pool($(ctx._pool.size))" : "single"
    nsrc   = length(ctx.sources)
    print(io, "QueryContext(\"$(ctx.db_path)\", $nsrc sources, $mode, $status)")
end

function Base.show(io::IO, ::MIME"text/plain", ctx::QueryContext)
    status = ctx._closed ? "closed" : "open"
    mode   = ctx._pool !== nothing ? "pool (size=$(ctx._pool.size))" : "single connection"
    nsrc   = length(ctx.sources)
    src_str = isempty(ctx.sources) ? "none" : join(sort(collect(keys(ctx.sources))), ", ")
    cfg    = ctx.config
    cfg_str = "threads=$(cfg.threads), memory_limit=$(isempty(cfg.memory_limit) ? "default" : cfg.memory_limit), on_error=:$(cfg.on_error)"
    println(io, "QueryContext:")
    println(io, "  database : $(ctx.db_path)")
    println(io, "  mode     : $mode")
    println(io, "  sources  : $nsrc registered [$src_str]")
    println(io, "  config   : $cfg_str")
    print(io,   "  status   : $status")
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
    ctx._closed && return
    ctx._closed = true
    if ctx._conn !== nothing
        try close(ctx._conn) catch end
        ctx._conn = nothing
    end
    if ctx._pool !== nothing
        close!(ctx._pool)
        ctx._pool = nothing
    end
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
            # Sync pool's source registry with context's registry
            # (register! on ctx also adds to pool, but belt-and-suspenders)
            for (name, src) in ctx.sources
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
