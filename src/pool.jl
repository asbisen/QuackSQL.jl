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
    _closed::Bool

    # Per-connection set of source names already registered on that connection.
    # Guarded by _lock so it is safe to read/write from multiple tasks.
    _applied::IdDict{DuckDB.DB, Set{String}}
    _in_use::IdDict{DuckDB.DB, Bool}
    _pending_drops::IdDict{DuckDB.DB, Dict{String, Any}}
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
            db_path, config, ch, size, false,
            IdDict{DuckDB.DB, Set{String}}(), IdDict{DuckDB.DB, Bool}(),
            IdDict{DuckDB.DB, Dict{String, Any}}(), ReentrantLock(),
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

function _apply_pending_drops!(pool::ConnectionPool, conn::DuckDB.DB)
    pending = lock(pool._lock) do
        haskey(pool._pending_drops, conn) || return Pair{String, Any}[]
        queued = collect(pool._pending_drops[conn])
        delete!(pool._pending_drops, conn)
        queued
    end

    for (name, src) in pending
        try
            _deregister_source!(conn, name, src)
        catch e
            @warn "Pool: failed pending source deregistration" name=name exception=e
        end
        lock(pool._lock) do
            delete!(get!(pool._applied, conn, Set{String}()), name)
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
    lock(pool._lock) do
        pool._closed && throw(QueryError("ConnectionPool has been closed."))
    end

    conn = take!(pool.channel)
    try
        lock(pool._lock) do
            if pool._closed
                throw(QueryError("ConnectionPool has been closed."))
            end
            pool._in_use[conn] = true
        end
        _apply_pending_drops!(pool, conn)
        _ensure_sources_applied!(pool, conn)
        @debug "Pool: acquired connection" pool_size=pool.size
        return conn
    catch
        should_close = lock(pool._lock) do
            haskey(pool._in_use, conn) && delete!(pool._in_use, conn)
            pool._closed
        end
        if should_close
            try close(conn) catch end
        else
            put!(pool.channel, conn)
        end
        rethrow()
    end
end

"""
    release!(pool, conn)

Return a previously acquired connection to the pool.
"""
function release!(pool::ConnectionPool, conn::DuckDB.DB)
    was_in_use, is_closed = lock(pool._lock) do
        in_use = haskey(pool._in_use, conn)
        in_use && delete!(pool._in_use, conn)
        in_use, pool._closed
    end

    if is_closed
        try close(conn) catch end
        @debug "Pool: released connection after close (closed instead of re-pooled)"
        return
    end

    was_in_use || throw(QueryError("Attempted to release a connection that is not checked out from this pool."))
    _apply_pending_drops!(pool, conn)
    _ensure_sources_applied!(pool, conn)
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
    borrowed_count = lock(pool._lock) do
        pool._closed && return 0
        pool._closed = true
        length(pool._in_use)
    end

    # Drain and close all connections currently in the channel
    while isready(pool.channel)
        conn = take!(pool.channel)
        try close(conn) catch end
        lock(pool._lock) do
            delete!(pool._applied, conn)
            delete!(pool._pending_drops, conn)
        end
    end
    @debug "Pool: closed all available connections" borrowed=borrowed_count
end

"""
    register!(pool, name, source)

Register a named data source with the pool.
The source will be lazily applied to each connection on first use.

See `register!` on `QueryContext` for supported source types.
"""
function register!(pool::ConnectionPool, name::String, source)
    lock(pool._lock) do
        pool._closed && throw(QueryError("ConnectionPool has been closed."))
        pool.sources[name] = source
        # Invalidate per-connection tracking so all existing connections
        # will re-apply the full source list on next acquisition.
        for conn in keys(pool._applied)
            delete!(pool._applied[conn], name)
        end
    end
    @debug "Pool: registered source" name=name type=typeof(source)
end
