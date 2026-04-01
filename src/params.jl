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
    io = IOBuffer()

    is_word_char(c::Char) = isletter(c) || isdigit(c) || c == '_'

    i = firstindex(sql)
    in_single = false
    in_double = false
    in_line_comment = false
    in_block_comment = false

    while i <= lastindex(sql)
        c = sql[i]
        ni = i < lastindex(sql) ? nextind(sql, i) : nothing
        nc = ni === nothing ? '\0' : sql[ni]

        if in_line_comment
            write(io, c)
            if c == '\n'
                in_line_comment = false
            end
            i = ni === nothing ? lastindex(sql) + 1 : ni
            continue
        end

        if in_block_comment
            write(io, c)
            if c == '*' && ni !== nothing && nc == '/'
                write(io, nc)
                i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
                in_block_comment = false
            else
                i = ni === nothing ? lastindex(sql) + 1 : ni
            end
            continue
        end

        if in_single
            write(io, c)
            if c == '\''
                if ni !== nothing && nc == '\''
                    write(io, nc)
                    i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
                else
                    in_single = false
                    i = ni === nothing ? lastindex(sql) + 1 : ni
                end
            else
                i = ni === nothing ? lastindex(sql) + 1 : ni
            end
            continue
        end

        if in_double
            write(io, c)
            if c == '"'
                if ni !== nothing && nc == '"'
                    write(io, nc)
                    i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
                else
                    in_double = false
                    i = ni === nothing ? lastindex(sql) + 1 : ni
                end
            else
                i = ni === nothing ? lastindex(sql) + 1 : ni
            end
            continue
        end

        if c == '-' && ni !== nothing && nc == '-'
            write(io, c)
            write(io, nc)
            in_line_comment = true
            i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
            continue
        end

        if c == '/' && ni !== nothing && nc == '*'
            write(io, c)
            write(io, nc)
            in_block_comment = true
            i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
            continue
        end

        if c == '\''
            in_single = true
            write(io, c)
            i = ni === nothing ? lastindex(sql) + 1 : ni
            continue
        end

        if c == '"'
            in_double = true
            write(io, c)
            i = ni === nothing ? lastindex(sql) + 1 : ni
            continue
        end

        if c == ':'
            prev_i = i > firstindex(sql) ? prevind(sql, i) : nothing
            prev_c = prev_i === nothing ? '\0' : sql[prev_i]

            # Skip cast operator (::) and ensure we're not in a word token.
            if ni !== nothing && nc == ':'
                write(io, c)
                write(io, nc)
                i = ni < lastindex(sql) ? nextind(sql, ni) : lastindex(sql) + 1
                continue
            elseif prev_i !== nothing && (prev_c == ':' || is_word_char(prev_c))
                write(io, c)
                i = ni === nothing ? lastindex(sql) + 1 : ni
                continue
            end

            # Parse :identifier
            if ni !== nothing
                first_name_c = sql[ni]
                if isletter(first_name_c) || first_name_c == '_'
                    j = ni
                    while true
                        jj = j < lastindex(sql) ? nextind(sql, j) : nothing
                        if jj === nothing
                            break
                        end
                        cj = sql[jj]
                        is_word_char(cj) || break
                        j = jj
                    end
                    name_str = sql[ni:j]
                    param_name = Symbol(name_str)
                    if !haskey(named_params, param_name)
                        throw(QueryError(
                            "Named parameter ':$(param_name)' not provided.",
                            sql, Dict(named_params), nothing
                        ))
                    end
                    push!(values, named_params[param_name])
                    push!(order, string(param_name))
                    write(io, '?')
                    i = j < lastindex(sql) ? nextind(sql, j) : lastindex(sql) + 1
                    continue
                end
            end
        end

        write(io, c)
        i = ni === nothing ? lastindex(sql) + 1 : ni
    end

    sql_out = String(take!(io))

    # Detect kwargs provided by the caller but never referenced in the SQL.
    # This catches typos in either the kwarg name or the :placeholder.
    consumed = Set(order)
    unused = [k for k in keys(named_params) if string(k) ∉ consumed]
    if !isempty(unused)
        throw(QueryError(
            "Named parameter(s) provided but not referenced in SQL: $(join(sort(string.(unused)), ", "))",
            sql, Dict(named_params), nothing
        ))
    end

    @debug "Bound named params" order=order values=values
    return (sql_out, values)
end

