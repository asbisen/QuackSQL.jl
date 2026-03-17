# ─── ConnectionPool ───────────────────────────────────────────────────────────
#
# A thread-safe pool of DuckDB.DB connections backed by a Julia Channel.
# Each slot holds an independent DuckDB.DB handle so connections are fully
# isolated and safe to use from different tasks concurrently.
#
# Design notes
# ─────────────
# • `acquire!` blocks until a connection is free (Channel semantics).
# • `release!` returns the connection to the pool for reuse.
# • `with_connection(f, pool)` is the recommended do-block API; it always
#   releases the connection even if `f` throws.
# • Pending sources (DataFrames, views) are tracked per-connection so they are
#   applied exactly once after the connection is first reused.
# • `close!(pool)` drains the channel and closes all connections.

"""
    ConnectionPool

A bounded pool of `DuckDB.DB` connections.  Useful for concurrent or
multi-threaded workloads where a single shared connection would be a
bottleneck.

```julia
pool = ConnectionPool("analytics.duckdb"; size=8, threads=4, readonly=true)

Threads.@threads for q in queries
    df = with_connection(pool) do conn
        execute(conn, q)
    end
end

close!(pool)
```
"""
mutable struct ConnectionPool
    db_path::String
    config::QueryConfig
    channel::Channel{DuckDB.DB}
    size::Int

    # Per-connection set of source names already registered on that connection.
    # Guarded by _lock so it is safe to read/write from multiple tasks.
    _applied::IdDict{DuckDB.DB, Set{String}}
    _lock::ReentrantLock

    # Shared source registry (name → raw source object).
    # Sources added via register!(pool, name, src) go here and will be lazily
    # applied to each connection on first use.
    sources::Dict{String, Any}

    function ConnectionPool(
        db_path::String = ":memory:";
        size::Int        = 4,
        kwargs...
    )
        config = QueryConfig(; kwargs...)
        _validate_config(config)
        ch = Channel{DuckDB.DB}(size)
        pool = new(
            db_path, config, ch, size,
            IdDict{DuckDB.DB, Set{String}}(), ReentrantLock(),
            Dict{String, Any}()
        )
        # Pre-warm all connections
        for _ in 1:size
            conn = _new_pool_conn(pool)
            put!(ch, conn)
        end
        finalizer(_finalizer_close!, pool)
        return pool
    end
end

# ─── Internal helpers ──────────────────────────────────────────────────────────

function _new_pool_conn(pool::ConnectionPool)::DuckDB.DB
    conn = _open_db(pool.db_path, pool.config)
    _apply_config!(conn, pool.config)
    lock(pool._lock) do
        pool._applied[conn] = Set{String}()
    end
    @debug "Pool: opened new connection" db_path=pool.db_path
    return conn
end

# Apply any not-yet-registered sources to this connection.
function _ensure_sources_applied!(pool::ConnectionPool, conn::DuckDB.DB)
    to_apply = lock(pool._lock) do
        applied = get!(pool._applied, conn, Set{String}())
        [(n, s) for (n, s) in pool.sources if n ∉ applied]
    end
    for (name, src) in to_apply
        _register_source!(conn, name, src)
        lock(pool._lock) do
            push!(pool._applied[conn], name)
        end
    end
end

function _finalizer_close!(pool::ConnectionPool)
    try close!(pool) catch end   # never throw from a finalizer
end

# ─── Public API ────────────────────────────────────────────────────────────────

"""
    acquire!(pool) → DuckDB.DB

Borrow a connection from the pool, blocking until one is available.
You **must** call `release!` when done, or preferably use `with_connection`.
"""
function acquire!(pool::ConnectionPool)::DuckDB.DB
    conn = take!(pool.channel)
    _ensure_sources_applied!(pool, conn)
    @debug "Pool: acquired connection" pool_size=pool.size
    return conn
end

"""
    release!(pool, conn)

Return a previously acquired connection to the pool.
"""
function release!(pool::ConnectionPool, conn::DuckDB.DB)
    put!(pool.channel, conn)
    @debug "Pool: released connection"
end

"""
    with_connection(f, pool) → result of f

Acquire a connection, call `f(conn)`, then release — always, even on error.

```julia
df = with_connection(pool) do conn
    execute(conn, "SELECT count(*) FROM trips")
end
```
"""
function with_connection(f::Function, pool::ConnectionPool)
    conn = acquire!(pool)
    try
        return f(conn)
    finally
        release!(pool, conn)
    end
end

"""
    close!(pool)

Drain the pool channel and close every connection.
"""
function close!(pool::ConnectionPool)
    # Drain and close all connections currently in the channel
    while isready(pool.channel)
        conn = take!(pool.channel)
        try close(conn) catch end
    end
    @debug "Pool: closed all connections"
end

"""
    register!(pool, name, source)

Register a named data source with the pool.
The source will be lazily applied to each connection on first use.

See `register!` on `QueryContext` for supported source types.
"""
function register!(pool::ConnectionPool, name::String, source)
    pool.sources[name] = source
    # Invalidate per-connection tracking so all existing connections
    # will re-apply the full source list on next acquisition.
    lock(pool._lock) do
        for conn in keys(pool._applied)
            delete!(pool._applied[conn], name)
        end
    end
    @debug "Pool: registered source" name=name type=typeof(source)
end
