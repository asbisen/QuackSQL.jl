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

# Examples
```julia
register!(ctx, "customers", df_customers)
register!(ctx, "trips",     "yellow_tripdata_2024.parquet")
register!(ctx, "events",    "events/*.csv")
register!(ctx, "archive",   "archive.duckdb")

# Bulk registration using pairs
register!(ctx,
    "customers" => df_customers,
    "trips"     => "trips.parquet"
)
```
"""
function register!(ctx::QueryContext, name::String, source)
    ctx.sources[name] = source
    # Eagerly apply to the live single connection (if any)
    if ctx._conn !== nothing
        _register_source!(ctx._conn, name, source)
    end
    @info "Source registered" name=name type=typeof(source)
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
    if !haskey(ctx.sources, name)
        @warn "Attempted to deregister unknown source" name=name
        return
    end
    src = pop!(ctx.sources, name)
    if ctx._conn !== nothing
        _deregister_source!(ctx._conn, name, src)
    end
    @info "Source deregistered" name=name
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
    if source isa DataFrame
        _register_dataframe!(conn, name, source)
    elseif source isa String
        ext = lowercase(last(splitext(source)))
        if any(source -> endswith(lowercase(source), e) for e in CSV_EXTS)
            _register_csv_view!(conn, name, source)
        elseif any(source -> endswith(lowercase(source), e) for e in PARQUET_EXTS) ||
               occursin('*', source) || occursin('?', source)   # glob → parquet assumed
            _register_parquet_view!(conn, name, source)
        elseif any(source -> endswith(lowercase(source), e) for e in DUCKDB_EXTS)
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

function _register_parquet_view!(conn::DuckDB.DB, name::String, path::String)
    DuckDB.execute(conn, "CREATE OR REPLACE VIEW \"$(escape_identifier(name))\" AS SELECT * FROM read_parquet('$(escape_sql_string(path))')")
    @debug "Parquet view registered" name=name path=path
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
    src isa DataFrame && return "DataFrame"
    src isa String    && any(endswith(lowercase(src), e) for e in DUCKDB_EXTS) && return "DuckDB"
    src isa String    && any(endswith(lowercase(src), e) for e in PARQUET_EXTS) && return "Parquet"
    src isa String    && any(endswith(lowercase(src), e) for e in CSV_EXTS) && return "CSV"
    src isa String    && return "File"
    return "Unknown"
end

function _source_info(src)::String
    src isa DataFrame && return "$(nrow(src)) rows × $(ncol(src)) cols"
    src isa String    && return src
    return string(src)
end
