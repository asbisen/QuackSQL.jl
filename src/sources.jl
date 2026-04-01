# ─── Source registration ───────────────────────────────────────────────────────
#
# Sources can be:
#   • DataFrame          → registered as an in-memory scan via register_data_frame
#   • String (*.csv)     → CREATE OR REPLACE VIEW using read_csv_auto
#   • String (*.parquet / *.pq)  → CREATE OR REPLACE VIEW using read_parquet
#   • String (*.duckdb / *.db)   → ATTACH as a named catalog
#   • String (other)     → ATTACH; user is responsible for the path being valid

const PARQUET_EXTS = (".parquet", ".pq")
const CSV_EXTS     = (".csv", ".tsv", ".csv.gz")
const DUCKDB_EXTS  = (".duckdb", ".db")

"""
    ParquetSource(path; union_by_name=false)

Internal wrapper that pairs a Parquet path (or glob) with read options.  Created
automatically by `register!` when keyword options are supplied.
"""
struct ParquetSource
    path::String
    union_by_name::Bool
end

ParquetSource(path::String; union_by_name::Bool=false) =
    ParquetSource(path, union_by_name)

"""Return true when `s` looks like a parquet source (extension or glob)."""
_looks_like_parquet(s::String) =
    any(e -> endswith(lowercase(s), e), PARQUET_EXTS) ||
    occursin('*', s) || occursin('?', s)

"""
    register!(ctx, name, source)
    register!(ctx, name => source, ...)

Register a named data source so it is available in SQL queries executed against
`ctx`.  The source is applied immediately for single-connection contexts, and
lazily (on first connection use) for pooled contexts.

# Supported source types

| Type                     | SQL name              | How it is registered               |
|--------------------------|-----------------------|------------------------------------|
| `DataFrame`              | `name`                | `register_data_frame`             |
| CSV / TSV path or glob   | `name`                | `CREATE VIEW … AS read_csv_auto`  |
| Parquet path or glob     | `name`                | `CREATE VIEW … AS read_parquet`   |
| DuckDB/SQLite file       | `name` (catalog)      | `ATTACH`                          |

# Keyword options (Parquet only)

- `union_by_name=false` — pass `union_by_name=true` to `read_parquet` so that
  multiple files whose schemas differ are merged by column name rather than by
  position.  Mirrors the DuckDB `read_parquet(…, union_by_name=true)` option.
  Using this with a non-Parquet source throws an `ArgumentError`.

# Examples
```julia
register!(ctx, "customers", df_customers)
register!(ctx, "trips",     "yellow_tripdata_2024.parquet")
register!(ctx, "events",    "events/*.csv")
register!(ctx, "archive",   "archive.duckdb")

# Glob over heterogeneous parquet files — merge columns by name
register!(ctx, "logs", "logs/*.parquet"; union_by_name=true)

# Bulk registration using pairs
register!(ctx,
    "customers" => df_customers,
    "trips"     => "trips.parquet"
)
```
"""
function register!(ctx::QueryContext, name::String, source; union_by_name::Bool=false)
    ctx._closed && throw(QueryError("QueryContext has been closed."))

    # Compute actual_source before acquiring the lock (pure logic, no I/O).
    actual_source = if union_by_name && source isa String
        _looks_like_parquet(source) || throw(ArgumentError(
            "union_by_name is only supported for Parquet/glob sources, got: $source"
        ))
        ParquetSource(source, true)
    else
        source
    end

    if ctx._pool !== nothing
        pool = ctx._pool

        # ctx.sources IS pool.sources (shared object); use pool._lock as the
        # single authoritative guard so _ensure_sources_applied! stays consistent.
        had_previous, previous_source = lock(pool._lock) do
            prev = haskey(ctx.sources, name)
            prev_src = prev ? ctx.sources[name] : nothing
            ctx.sources[name] = actual_source
            (prev, prev_src)
        end

        available = DuckDB.DB[]
        while isready(pool.channel)
            push!(available, take!(pool.channel))
        end

        try
            # Apply changes immediately to currently idle connections.
            for conn in available
                if had_previous
                    try
                        _deregister_source!(conn, name, previous_source)
                    catch e
                        @warn "Failed to deregister replaced pooled source" name=name exception=e
                    end
                    lock(pool._lock) do
                        delete!(get!(pool._applied, conn, Set{String}()), name)
                    end
                end

                _register_source!(conn, name, actual_source)
                lock(pool._lock) do
                    push!(get!(pool._applied, conn, Set{String}()), name)
                end
            end

            # For checked-out connections, queue replacement cleanup and force
            # source re-application when they are next released/acquired.
            lock(pool._lock) do
                for conn in keys(pool._in_use)
                    if had_previous
                        pending = get!(pool._pending_drops, conn, Dict{String, Any}())
                        pending[name] = previous_source
                    end
                    delete!(get!(pool._applied, conn, Set{String}()), name)
                end
            end
        finally
            for conn in available
                put!(pool.channel, conn)
            end
        end

        @debug "Source registered (pooled context)" name=name type=typeof(actual_source)
        return
    end

    # Single-connection path: ctx.sources is its own dict, guarded by ctx._lock.
    had_previous, previous_source = lock(ctx._lock) do
        prev = haskey(ctx.sources, name)
        prev_src = prev ? ctx.sources[name] : nothing
        ctx.sources[name] = actual_source
        (prev, prev_src)
    end

    if ctx._conn !== nothing
        if had_previous
            try
                _deregister_source!(ctx._conn, name, previous_source)
            catch e
                @warn "Failed to deregister replaced source" name=name exception=e
            end
        end
        _register_source!(ctx._conn, name, actual_source)
    end
    @debug "Source registered" name=name type=typeof(actual_source)
end

# Variadic pair form
function register!(ctx::QueryContext, pairs::Pair{String}...)
    for (name, source) in pairs
        register!(ctx, name, source)
    end
end

"""
    deregister!(ctx, name)

Remove a previously registered source.  For DataFrames and views this drops the
VIEW; for attached databases this issues DETACH.
"""
function deregister!(ctx::QueryContext, name::String)
    ctx._closed && throw(QueryError("QueryContext has been closed."))

    if ctx._pool !== nothing
        pool = ctx._pool

        # ctx.sources IS pool.sources; use pool._lock as the single guard.
        src, found = lock(pool._lock) do
            haskey(ctx.sources, name) || return (nothing, false)
            (pop!(ctx.sources, name), true)
        end

        if !found
            @warn "Attempted to deregister unknown source" name=name
            return
        end

        available = DuckDB.DB[]
        while isready(pool.channel)
            push!(available, take!(pool.channel))
        end

        try
            # Apply deregistration immediately to idle pooled connections.
            for conn in available
                _deregister_source!(conn, name, src)
                lock(pool._lock) do
                    delete!(get!(pool._applied, conn, Set{String}()), name)
                end
            end

            # Queue deregistration for currently checked-out connections.
            lock(pool._lock) do
                for conn in keys(pool._in_use)
                    pending = get!(pool._pending_drops, conn, Dict{String, Any}())
                    pending[name] = src
                    delete!(get!(pool._applied, conn, Set{String}()), name)
                end
            end
        finally
            for conn in available
                put!(pool.channel, conn)
            end
        end

        @debug "Source deregistered (pooled context)" name=name
        return
    end

    # Single-connection path: ctx.sources is its own dict, guarded by ctx._lock.
    src, found = lock(ctx._lock) do
        haskey(ctx.sources, name) || return (nothing, false)
        (pop!(ctx.sources, name), true)
    end

    if !found
        @warn "Attempted to deregister unknown source" name=name
        return
    end

    if ctx._conn !== nothing
        _deregister_source!(ctx._conn, name, src)
    end
    @debug "Source deregistered" name=name
end

"""
    list_sources(ctx) → DataFrame

Return a one-row-per-source `DataFrame` with columns `name`, `type`, and `info`.
"""
function list_sources(ctx::QueryContext)::DataFrame
    lk = ctx._pool !== nothing ? ctx._pool._lock : ctx._lock
    snapshot = lock(lk) do
        collect(ctx.sources)
    end
    rows = [(
        name = k,
        type = _source_type_label(v),
        info = _source_info(v)
    ) for (k, v) in snapshot]
    isempty(rows) && return DataFrame(name=String[], type=String[], info=String[])
    return DataFrame(rows)
end

# ─── Internal helpers ──────────────────────────────────────────────────────────

"""
    _register_source!(conn, name, source)

Apply a single source to an open DuckDB connection.
"""
function _register_source!(conn::DuckDB.DB, name::String, source)
    if source isa ParquetSource
        _register_parquet_view!(conn, name, source.path; union_by_name=source.union_by_name)
    elseif source isa DataFrame
        _register_dataframe!(conn, name, source)
    elseif source isa String
        lower_source = lowercase(source)
        if any(e -> endswith(lower_source, e), CSV_EXTS)
            _register_csv_view!(conn, name, source)
        elseif any(e -> endswith(lower_source, e), PARQUET_EXTS) ||
               occursin('*', source) || occursin('?', source)   # glob → parquet assumed
            _register_parquet_view!(conn, name, source)
        elseif any(e -> endswith(lower_source, e), DUCKDB_EXTS)
            _attach_database!(conn, name, source)
        else
            # Best-effort: try attaching as a database catalog
            _attach_database!(conn, name, source)
        end
    else
        throw(QueryError(
            "Unsupported source type: $(typeof(source)). " *
            "Expected DataFrame, CSV/Parquet path, or DuckDB file path.",
            "", source, nothing
        ))
    end
end

function _register_dataframe!(conn::DuckDB.DB, name::String, df::DataFrame)
    if isdefined(DuckDB, :register_data_frame)
        # Sanitize before native registration: DuckDB cannot handle Vector{T},
        # Symbol, pure-Missing columns, or other non-primitive Julia types.
        sanitized = _sanitize_for_duckdb(df)
        try
            DuckDB.register_data_frame(conn, sanitized, name)
            @debug "DataFrame registered (native)" name=name rows=nrow(df)
            return
        catch e
            @debug "Native DataFrame registration failed, using fallback" exception=e
        end
        _register_dataframe_fallback!(conn, name, sanitized)
    else
        _register_dataframe_fallback!(conn, name, df)
    end
end

"""
    _needs_sanitization(T) → Bool

Return `true` when Julia type `T` cannot be mapped to a native DuckDB column
type and the column's values must be serialized to VARCHAR before registration.

DuckDB natively handles: `Bool`, `Integer` subtypes (signed & unsigned),
`AbstractFloat`, `AbstractString`, `Date`, `DateTime`, and `Union{T, Missing}`
where the inner type is itself supported.
"""
function _needs_sanitization(T::Type)::Bool
    # Unwrap Union{T, Missing}  (e.g. Union{Vector{Float64}, Missing})
    if T isa Union
        inner = filter(!=(Missing), Base.uniontypes(T))
        isempty(inner) && return true   # Union{} / all-missing → treat as NULL VARCHAR
        length(inner) == 1 && return _needs_sanitization(inner[1])
        return true  # multi-type union without a single non-Missing type
    end
    T === Missing        && return true   # column is entirely missing
    T <: Bool            && return false
    T <: Integer         && return false  # Int8/16/32/64, UInt8/16/32/64, …
    T <: AbstractFloat   && return false
    T <: AbstractString  && return false
    T <: Dates.Date      && return false
    T <: Dates.DateTime  && return false
    return true  # Symbol, Vector{T}, custom structs, etc.
end

"""
    _sanitize_for_duckdb(df) → DataFrame

Return a version of `df` where columns with DuckDB-incompatible element types
(arrays, `Symbol`, pure `Missing`, unknown structs, …) have been cast to
`Union{String, Missing}` via `string`.  Columns that are already compatible
are shared by reference — no unnecessary data copying occurs.
"""
function _sanitize_for_duckdb(df::DataFrame)::DataFrame
    needs_work = any(_needs_sanitization(eltype(df[!, c])) for c in names(df))
    !needs_work && return df   # fast path: nothing to do

    result = DataFrame()
    for col in names(df)
        T = eltype(df[!, col])
        if _needs_sanitization(T)
            converted = Union{String, Missing}[
                v === missing ? missing : string(v) for v in df[!, col]
            ]
            result[!, col] = converted
            @debug "Column serialized to VARCHAR for DuckDB" column=col from=T
        else
            result[!, col] = df[!, col]
        end
    end
    return result
end

function _register_dataframe_fallback!(conn::DuckDB.DB, name::String, df::DataFrame)
    qname = "\"$(escape_identifier(name))\""
    DuckDB.execute(conn, "DROP TABLE IF EXISTS $qname")

    cols  = names(df)
    types = [eltype(df[!, c]) for c in cols]
    col_defs = join(
        ["\"$(escape_identifier(c))\" $(julia_type_to_duckdb(t))" for (c, t) in zip(cols, types)],
        ", "
    )
    DuckDB.execute(conn, "CREATE TEMPORARY TABLE $qname ($col_defs)")

    appender = DuckDB.Appender(conn, name)
    try
        for row in eachrow(df)
            for col in cols
                DuckDB.append(appender, row[col])
            end
            DuckDB.end_row(appender)
        end
        DuckDB.flush(appender)
    finally
        DuckDB.close(appender)
    end
    @debug "DataFrame registered (fallback)" name=name rows=nrow(df)
end

function _register_csv_view!(conn::DuckDB.DB, name::String, path::String)
    DuckDB.execute(conn, "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS SELECT * FROM read_csv_auto('$(escape_sql_string(path))')")
    @debug "CSV view registered" name=name path=path
end

function _register_parquet_view!(conn::DuckDB.DB, name::String, path::String;
                                  union_by_name::Bool=false)
    opts = union_by_name ? ", union_by_name=true" : ""
    DuckDB.execute(conn, "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS SELECT * FROM read_parquet('$(escape_sql_string(path))'$opts)")
    @debug "Parquet view registered" name=name path=path union_by_name=union_by_name
end

function _attach_database!(conn::DuckDB.DB, name::String, path::String)
    DuckDB.execute(conn, "ATTACH IF NOT EXISTS '$(escape_sql_string(path))' AS \"$(escape_identifier(name))\"")
    @debug "Database attached" name=name path=path
end

function _deregister_source!(conn::DuckDB.DB, name::String, source)
    qname = "\"$(escape_identifier(name))\""
    if source isa DataFrame
        try DuckDB.execute(conn, "DROP VIEW IF EXISTS $qname") catch end
        try DuckDB.execute(conn, "DROP TABLE IF EXISTS $qname") catch end
    elseif source isa ParquetSource
        try DuckDB.execute(conn, "DROP VIEW IF EXISTS $qname") catch end
    elseif source isa String && any(endswith(lowercase(source), e) for e in DUCKDB_EXTS)
        try DuckDB.execute(conn, "DETACH $qname") catch end
    else
        try DuckDB.execute(conn, "DROP VIEW IF EXISTS $qname") catch end
    end
end

# ─── Type helpers ─────────────────────────────────────────────────────────────

using Dates

function julia_type_to_duckdb(T::Type)::String
    # Unwrap Union{T, Missing}
    if T isa Union
        inner = filter(!=(Missing), Base.uniontypes(T))
        length(inner) == 1 && return julia_type_to_duckdb(inner[1])
        return "VARCHAR"
    end
    T === Missing       && return "VARCHAR"
    T <: Bool          && return "BOOLEAN"
    T <: Integer       && return "BIGINT"
    T <: AbstractFloat && return "DOUBLE"
    T <: AbstractString && return "VARCHAR"
    T <: Dates.Date    && return "DATE"
    T <: Dates.DateTime && return "TIMESTAMP"
    return "VARCHAR"
end

# Safe SQL string literal (escapes single quotes)
function _sql_literal(v)::String
    v === missing  && return "NULL"
    v isa Bool     && return v ? "TRUE" : "FALSE"
    v isa Number   && return string(v)
    v isa Dates.Date     && return "'$(v)'"
    v isa Dates.DateTime && return "'$(v)'"
    # For strings and everything else: escape single quotes
    return "'$(replace(string(v), "'" => "''"))'"
end

# Escape a SQL identifier (double-quote any embedded double-quotes)
escape_identifier(s::String) = replace(s, "\"" => "\"\"")

# Escape a SQL string value (escape single quotes)
escape_sql_string(s::String) = replace(s, "'" => "''")

function _source_type_label(src)::String
    src isa DataFrame    && return "DataFrame"
    src isa ParquetSource && return "Parquet"
    src isa String    && any(endswith(lowercase(src), e) for e in DUCKDB_EXTS) && return "DuckDB"
    src isa String    && any(endswith(lowercase(src), e) for e in PARQUET_EXTS) && return "Parquet"
    src isa String    && any(endswith(lowercase(src), e) for e in CSV_EXTS) && return "CSV"
    src isa String    && return "File"
    return "Unknown"
end

function _source_info(src)::String
    src isa DataFrame    && return "$(nrow(src)) rows × $(ncol(src)) cols"
    src isa ParquetSource && return "$(src.path)" * (src.union_by_name ? " (union_by_name=true)" : "")
    src isa String    && return src
    return string(src)
end
