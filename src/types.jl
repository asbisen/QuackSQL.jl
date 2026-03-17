# ─── Error Types ──────────────────────────────────────────────────────────────

"""
    QueryError <: Exception

Structured exception that captures the failed SQL, bound parameters, and the
root cause so users can diagnose problems without hunting through stack traces.
"""
struct QueryError <: Exception
    message::String
    sql::String
    params::Any                           # positional Vector or NamedTuple
    cause::Union{Exception, Nothing}

    QueryError(msg::String, sql::String="", params=nothing, cause=nothing) =
        new(msg, sql, params, cause)
end

function Base.showerror(io::IO, e::QueryError)
    print(io, "QueryError: ", e.message)
    isempty(e.sql) || print(io, "\n  SQL    : ", e.sql)
    e.params === nothing || print(io, "\n  Params : ", e.params)
    if e.cause !== nothing
        print(io, "\n  Caused by: ")
        showerror(io, e.cause)
    end
end

# ─── Result Type ──────────────────────────────────────────────────────────────

"""
    QueryResult

Wraps a `DataFrame` with execution metadata (elapsed time, originating SQL).
Delegates indexing and most `DataFrame` operations so it can be used
transparently wherever a `DataFrame` is accepted.

```julia
result = execute(ctx, "SELECT * FROM trips LIMIT 5")
println(result.elapsed_ms)   # → 1.23
df = DataFrame(result)       # explicit conversion
result[1, :passenger_count]  # direct indexing
```
"""
struct QueryResult
    data::DataFrame
    elapsed_ns::Int64
    sql::String
end

elapsed_ms(r::QueryResult) = round(r.elapsed_ns / 1_000_000.0; digits=2)

function Base.show(io::IO, r::QueryResult)
    print(io, "QueryResult($(nrow(r.data)) rows × $(ncol(r.data)) cols, $(elapsed_ms(r)) ms)")
end

function Base.show(io::IO, ::MIME"text/plain", r::QueryResult)
    println(io, "QueryResult — $(nrow(r.data)) rows × $(ncol(r.data)) cols  ($(elapsed_ms(r)) ms)")
    show(io, MIME"text/plain"(), r.data)
end

# Transparent delegation to the inner DataFrame
Base.getindex(r::QueryResult, args...)    = getindex(r.data, args...)
Base.size(r::QueryResult)                = size(r.data)
Base.size(r::QueryResult, d::Integer)    = size(r.data, d)
Base.length(r::QueryResult)              = length(r.data)
Base.names(r::QueryResult)               = names(r.data)
Base.iterate(r::QueryResult, args...)    = iterate(eachrow(r.data), args...)
DataFrames.DataFrame(r::QueryResult)     = r.data
DataFrames.nrow(r::QueryResult)          = nrow(r.data)
DataFrames.ncol(r::QueryResult)          = ncol(r.data)

# Tables.jl — allows QueryResult anywhere a Tables.jl source is accepted
Tables.istable(::Type{QueryResult})      = true
Tables.columnaccess(::Type{QueryResult}) = true
Tables.columns(r::QueryResult)           = Tables.columns(r.data)
Tables.schema(r::QueryResult)            = Tables.schema(r.data)
Tables.rows(r::QueryResult)              = Tables.rows(r.data)
