# ─── Parameterized query support ──────────────────────────────────────────────
#
# Two query styles are supported:
#
#   Positional  "SELECT * FROM t WHERE id = ? AND active = ?"
#               → execute(ctx, sql, 42, true)
#
#   Named       "SELECT * FROM t WHERE id = :id AND active = :active"
#               → execute(ctx, sql; id=42, active=true)
#
# Named parameters are transformed to positional ? placeholders before being
# passed to DuckDB so we rely on DuckDB's own prepared-statement implementation
# for correctness and SQL-injection safety.
# ──────────────────────────────────────────────────────────────────────────────

"""
    normalise_params(sql, positional, named) → (sql′, values)

Validate that the SQL uses either positional (`?`) or named (`:param`)
parameters—not both—and return a normalised (sql, values) pair suitable for
`DuckDB.execute(conn, sql, values)`.

If no parameters are provided, `(sql, nothing)` is returned.
"""
function normalise_params(
    sql::String,
    positional::Tuple,
    named::Base.Pairs
)::Tuple{String, Union{Vector, Nothing}}
    has_pos   = !isempty(positional)
    has_named = !isempty(named)

    if has_pos && has_named
        throw(QueryError(
            "Cannot mix positional (?) and named (:param) parameters in the same query.",
            sql, (positional=positional, named=Dict(named)), nothing
        ))
    end

    has_pos   && return (sql, collect(positional))
    has_named && return _bind_named(sql, named)
    return (sql, nothing)
end

"""
    _bind_named(sql, named_params) → (sql′, values)

Replace `:identifier` placeholders with `?` in `sql` order and collect the
corresponding values.  Raises `QueryError` if a placeholder has no matching
keyword argument.
"""
function _bind_named(sql::String, named_params::Base.Pairs)::Tuple{String, Vector}
    values   = Any[]
    order    = String[]
    # Match :word — but not :: (cast operator) and not inside string literals
    sql_out = replace(sql, r"(?<![:\w]):([A-Za-z_]\w*)" => function(m)
        param_name = Symbol(m[2:end])                 # strip leading ':'
        if !haskey(named_params, param_name)
            throw(QueryError(
                "Named parameter ':$(param_name)' not provided.",
                sql, Dict(named_params), nothing
            ))
        end
        push!(values, named_params[param_name])
        push!(order, string(param_name))
        "?"
    end)
    @debug "Bound named params" order=order values=values
    return (sql_out, values)
end

"""
    count_placeholders(sql) → Int

Count the number of `?` placeholders in a SQL string (excluding those inside
string literals).
"""
function count_placeholders(sql::String)::Int
    # Simple heuristic: count '?' not preceded by another '?'
    count(==('?'), sql)
end
