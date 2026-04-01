# ─── QueryConfig ──────────────────────────────────────────────────────────────

"""
    QueryConfig

Immutable configuration snapshot for a `QueryContext` or `ConnectionPool`.
All fields have sensible defaults so you only specify what you need.

# Fields
| Field            | Default          | Description                                      |
|------------------|------------------|--------------------------------------------------|
| `threads`        | `0` (auto)       | DuckDB worker threads; 0 = DuckDB default        |
| `memory_limit`   | `""`             | E.g. `"4GB"`; empty = DuckDB default             |
| `readonly`       | `false`          | Open file databases in read-only mode            |
| `extensions`     | `String[]`       | DuckDB extensions to INSTALL and LOAD            |
| `init_sql`       | `String[]`       | SQL statements executed on every new connection  |
| `on_error`       | `:throw`         | `:throw`, `:empty`, or `:missing`                |

```julia
cfg = QueryConfig(threads=4, memory_limit="2GB", on_error=:empty)
ctx = QueryContext(":memory:"; threads=4, memory_limit="2GB")
```
"""
struct QueryConfig
    threads::Int
    memory_limit::String
    readonly::Bool
    extensions::Vector{String}
    init_sql::Vector{String}
    on_error::Symbol            # :throw | :empty | :missing
end

"""
    QueryConfig(; kwargs...) → QueryConfig

Keyword-argument constructor with validation and sensible defaults.
"""
function QueryConfig(;
    threads::Int               = 0,
    memory_limit::String       = "",
    readonly::Bool             = false,
    extensions::Vector{String} = String[],
    init_sql::Vector{String}   = String[],
    on_error::Symbol           = :throw
)::QueryConfig
    if !(on_error in (:throw, :empty, :missing))
        throw(ArgumentError("on_error must be :throw, :empty, or :missing; got :$(on_error)"))
    end
    for ext in extensions
        if !occursin(r"^[A-Za-z][A-Za-z0-9_-]*$", ext)
            throw(ArgumentError(
                "Invalid extension name: $(repr(ext)). " *
                "Extension names must start with a letter and contain only " *
                "letters, digits, underscores, or hyphens."
            ))
        end
    end
    QueryConfig(threads, memory_limit, readonly, extensions, init_sql, on_error)
end

# No-op: validation already done in the constructor above.
function _validate_config(cfg::QueryConfig) end

# ─── DuckDB connection helpers ─────────────────────────────────────────────────

"""
    _open_db(db_path, config) → DuckDB.DB

Open a DuckDB database, applying read-only mode and DuckDB-level settings.
"""
function _open_db(db_path::String, config::QueryConfig)::DuckDB.DB
    db = if config.readonly && db_path != ":memory:"
        cnf = DuckDB.Config()
        DuckDB.set_config(cnf, "access_mode", "READ_ONLY")
        DuckDB.DB(db_path, cnf)
    else
        DuckDB.DB(db_path)
    end
    return db
end

"""
    _apply_config!(conn, config)

Push DuckDB runtime settings (threads, memory_limit, extensions, init_sql)
to an open connection.  Called once right after creating a connection.
"""
function _apply_config!(conn::DuckDB.DB, config::QueryConfig)
    if config.threads > 0
        _try_set(conn, "threads", string(config.threads))
    end
    if !isempty(config.memory_limit)
        _try_set(conn, "memory_limit", "'$(config.memory_limit)'")
    end
    for ext in config.extensions
        try
            DuckDB.execute(conn, "INSTALL '$(ext)'")
            DuckDB.execute(conn, "LOAD '$(ext)'")
            @debug "Loaded extension" extension=ext
        catch e
            @warn "Failed to load DuckDB extension" extension=ext exception=e
        end
    end
    for sql in config.init_sql
        try
            DuckDB.execute(conn, sql)
            @debug "Ran init_sql" sql=sql
        catch e
            throw(QueryError("Init SQL failed", sql, nothing, e))
        end
    end
end

function _try_set(conn::DuckDB.DB, key::String, value::String)
    try
        DuckDB.execute(conn, "SET $(key) = $(value)")
    catch e
        @warn "Could not apply DuckDB setting" setting=key value=value exception=e
    end
end
