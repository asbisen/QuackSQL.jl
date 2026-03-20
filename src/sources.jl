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

abstract type AbstractPathSource end

"""
    ParquetSource(path; union_by_name=false, filename=false,
                  hive_partitioning=nothing, compression=nothing)

Wrapper for a Parquet path (or glob) plus `read_parquet` options.

`compression` is accepted for forward compatibility with write/export APIs,
but is not supported by `read_parquet` and therefore causes `register!` to
throw an `ArgumentError` when set.
"""
struct ParquetSource <: AbstractPathSource
    path::String
    union_by_name::Bool
    filename::Bool
    hive_partitioning::Union{Bool, Nothing}
    compression::Union{Symbol, Nothing}
end

const PARQUET_COMPRESSION_CODECS = Set([
    :snappy, :zstd, :gzip, :brotli, :lz4, :uncompressed
])

function ParquetSource(
    path::String;
    union_by_name::Bool=false,
    filename::Bool=false,
    hive_partitioning::Union{Bool, Nothing}=nothing,
    compression::Union{Symbol, Nothing}=nothing
)
    if compression !== nothing && !(compression in PARQUET_COMPRESSION_CODECS)
        throw(ArgumentError(
            "Invalid compression codec: $compression. " *
            "Expected one of: $(collect(PARQUET_COMPRESSION_CODECS))"
        ))
    end
    return ParquetSource(path, union_by_name, filename, hive_partitioning, compression)
end

# Backward-compatible positional constructor used by existing tests/users.
ParquetSource(path::String, union_by_name::Bool) =
    ParquetSource(path; union_by_name=union_by_name)

"""
    CsvSource(path; header=nothing, delim=nothing, quotechar=nothing, escape=nothing,
              nullstr=nothing, auto_detect=true, sample_size=nothing)

Wrapper for a CSV/TSV path (or glob) plus `read_csv_auto` options.
"""
struct CsvSource <: AbstractPathSource
    path::String
    header::Union{Bool, Nothing}
    delim::Union{Char, Nothing}
    quotechar::Union{Char, Nothing}
    escape::Union{Char, Nothing}
    nullstr::Union{String, Nothing}
    auto_detect::Bool
    sample_size::Union{Int, Nothing}
end

function CsvSource(
    path::String;
    header::Union{Bool, Nothing}=nothing,
    delim::Union{Char, Nothing}=nothing,
    quotechar::Union{Char, Nothing}=nothing,
    escape::Union{Char, Nothing}=nothing,
    nullstr::Union{String, Nothing}=nothing,
    auto_detect::Bool=true,
    sample_size::Union{Int, Nothing}=nothing
)
    if sample_size !== nothing && sample_size <= 0
        throw(ArgumentError("sample_size must be positive when provided, got: $sample_size"))
    end
    return CsvSource(path, header, delim, quotechar, escape, nullstr, auto_detect, sample_size)
end

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

    had_previous = haskey(ctx.sources, name)
    previous_source = had_previous ? ctx.sources[name] : nothing

    actual_source = if union_by_name && source isa String
        _looks_like_parquet(source) || throw(ArgumentError(
            "union_by_name is only supported for Parquet/glob sources, got: $source"
        ))
        ParquetSource(source, true)
    else
        source
    end
    ctx.sources[name] = actual_source

    if ctx._pool !== nothing
        pool = ctx._pool

        lock(pool._lock) do
            pool.sources[name] = actual_source
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

    # Eagerly apply to the live single connection (if any)
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

    if !haskey(ctx.sources, name)
        @warn "Attempted to deregister unknown source" name=name
        return
    end
    src = pop!(ctx.sources, name)

    if ctx._pool !== nothing
        pool = ctx._pool

        lock(pool._lock) do
            haskey(pool.sources, name) && delete!(pool.sources, name)
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
    rows = [(
        name = k,
        type = _source_type_label(v),
        info = _source_info(v)
    ) for (k, v) in ctx.sources]
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
        source.compression === nothing || throw(ArgumentError(
            "Parquet compression is a write-time option and is not supported by read_parquet/register!. " *
            "Got compression=$(source.compression)."
        ))
        _register_parquet_view!(
            conn,
            name,
            source.path;
            union_by_name=source.union_by_name,
            filename=source.filename,
            hive_partitioning=source.hive_partitioning,
        )
    elseif source isa CsvSource
        _register_csv_view!(conn, name, source)
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
    # Use a quoted identifier to avoid SQL injection through column names
    DuckDB.execute(conn, "DROP TABLE IF EXISTS \"$(escape_identifier(name))\"")
    cols = names(df)
    types = [eltype(df[!, c]) for c in cols]
    col_defs = join(
        ["\"$(escape_identifier(c))\" $(julia_type_to_duckdb(t))" for (c, t) in zip(cols, types)],
        ", "
    )
    DuckDB.execute(conn, "CREATE TEMPORARY TABLE \"$(escape_identifier(name))\" ($col_defs)")

    for row in eachrow(df)
        vals = join([_sql_literal(row[c]) for c in cols], ", ")
        DuckDB.execute(conn, "INSERT INTO \"$(escape_identifier(name))\" VALUES ($vals)")
    end
    @debug "DataFrame registered (fallback)" name=name rows=nrow(df)
end

function _register_csv_view!(conn::DuckDB.DB, name::String, path::String)
    DuckDB.execute(conn, "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS SELECT * FROM read_csv_auto('$(escape_sql_string(path))')")
    @debug "CSV view registered" name=name path=path
end

function _register_csv_view!(conn::DuckDB.DB, name::String, src::CsvSource)
    opts = String[]
    src.header !== nothing      && push!(opts, "header=$(src.header)")
    src.delim !== nothing       && push!(opts, "delim='$(escape_sql_string(string(src.delim)))'")
    src.quotechar !== nothing   && push!(opts, "quote='$(escape_sql_string(string(src.quotechar)))'")
    src.escape !== nothing      && push!(opts, "escape='$(escape_sql_string(string(src.escape)))'")
    src.nullstr !== nothing     && push!(opts, "nullstr='$(escape_sql_string(src.nullstr))'")
    src.auto_detect != true     && push!(opts, "auto_detect=$(src.auto_detect)")
    src.sample_size !== nothing && push!(opts, "sample_size=$(src.sample_size)")

    sql_opts = isempty(opts) ? "" : ", " * join(opts, ", ")
    DuckDB.execute(
        conn,
        "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS " *
        "SELECT * FROM read_csv_auto('$(escape_sql_string(src.path))'$sql_opts)"
    )
    @debug "CSV source registered" name=name path=src.path options=opts
end

function _register_parquet_view!(conn::DuckDB.DB, name::String, path::String;
                                  union_by_name::Bool=false,
                                  filename::Bool=false,
                                  hive_partitioning::Union{Bool, Nothing}=nothing)
    opts = String[]
    union_by_name && push!(opts, "union_by_name=true")
    filename && push!(opts, "filename=true")
    hive_partitioning !== nothing && push!(opts, "hive_partitioning=$(hive_partitioning)")

    sql_opts = isempty(opts) ? "" : ", " * join(opts, ", ")
    DuckDB.execute(conn, "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS SELECT * FROM read_parquet('$(escape_sql_string(path))'$sql_opts)")
    @debug "Parquet view registered" name=name path=path union_by_name=union_by_name filename=filename hive_partitioning=hive_partitioning
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
    src isa CsvSource && return "CSV"
    src isa String    && any(endswith(lowercase(src), e) for e in DUCKDB_EXTS) && return "DuckDB"
    src isa String    && any(endswith(lowercase(src), e) for e in PARQUET_EXTS) && return "Parquet"
    src isa String    && any(endswith(lowercase(src), e) for e in CSV_EXTS) && return "CSV"
    src isa String    && return "File"
    return "Unknown"
end

function _source_info(src)::String
    src isa DataFrame    && return "$(nrow(src)) rows × $(ncol(src)) cols"
    if src isa ParquetSource
        opts = String[]
        src.union_by_name && push!(opts, "union_by_name=true")
        src.filename && push!(opts, "filename=true")
        src.hive_partitioning !== nothing && push!(opts, "hive_partitioning=$(src.hive_partitioning)")
        src.compression !== nothing && push!(opts, "compression=$(src.compression)")
        return isempty(opts) ? src.path : "$(src.path) (" * join(opts, ", ") * ")"
    end
    if src isa CsvSource
        opts = String[]
        src.header !== nothing && push!(opts, "header=$(src.header)")
        src.delim !== nothing && push!(opts, "delim=$(src.delim)")
        src.quotechar !== nothing && push!(opts, "quote=$(src.quotechar)")
        src.escape !== nothing && push!(opts, "escape=$(src.escape)")
        src.nullstr !== nothing && push!(opts, "nullstr=$(src.nullstr)")
        src.auto_detect != true && push!(opts, "auto_detect=$(src.auto_detect)")
        src.sample_size !== nothing && push!(opts, "sample_size=$(src.sample_size)")
        return isempty(opts) ? src.path : "$(src.path) (" * join(opts, ", ") * ")"
    end
    src isa String    && return src
    return string(src)
end
