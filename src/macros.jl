# ─── @query / @query! / @stream macros ───────────────────────────────────────
#
# These macros let you embed Julia variables directly inside a SQL string using
# standard `$var` / `$(expr)` interpolation syntax.  Each interpolation site is
# replaced with a `?` placeholder at compile time; the values are passed to
# DuckDB's prepared statement engine at run time, giving you injection-safe
# queries with no manual placeholder counting.
#
# Usage
# ─────
#   df = @query  ctx "SELECT * FROM t WHERE status = $status AND n > $(limit-1)"
#   @query! ctx "INSERT INTO log VALUES ($ts, $msg)"
#   for batch in @stream ctx "SELECT * FROM events WHERE uid = $uid" batch_size=5_000
#       process(batch)
#   end
#
# How it works
# ────────────
# Julia parses an interpolated string literal "text $expr more" into an AST node
#   Expr(:string, "text ", <expr>, " more")
# before the macro body ever runs.  _interpolate_sql walks that node, replaces
# each non-string part with '?' in the SQL output, and collects the expressions
# as positional parameters.  The macro then emits a normal execute / stream call.

"""
    _interpolate_sql(sql_expr) → (sql::String, params::Vector{Any})

Compile-time helper called from each macro.  Accepts either:
  - a plain `String` (no interpolations) → returns it unchanged with empty params
  - an `Expr(:string, ...)` (interpolated literal) → replaces each interpolation
    site with `?` and records the corresponding expression as a parameter

Throws `ArgumentError` for any other node shape so users get a clear message
rather than a cryptic lowering error.
"""
function _interpolate_sql(sql_expr)::Tuple{String, Vector{Any}}
    # Plain string with no interpolation sites
    sql_expr isa String && return (sql_expr, Any[])

    # Interpolated string: Expr(:string, part₁, part₂, ...)
    # String parts are kept verbatim; non-string parts become ? placeholders.
    if sql_expr isa Expr && sql_expr.head === :string
        buf    = IOBuffer()
        params = Any[]
        for part in sql_expr.args
            if part isa String
                write(buf, part)
            else
                write(buf, '?')
                push!(params, part)
            end
        end
        return String(take!(buf)), params
    end

    throw(ArgumentError(
        "@query / @query! / @stream expects a string literal with \$interpolations, " *
        "got $(typeof(sql_expr)).  Example: @query ctx \"SELECT * FROM t WHERE id = \$id\""
    ))
end

# ─── @query ───────────────────────────────────────────────────────────────────

"""
    @query ctx sql_string → DataFrame

Execute a SQL string that may contain `\$variable` or `\$(expression)`
interpolations.  Each interpolation is replaced with a `?` placeholder and
passed to DuckDB's prepared statement engine — safe from SQL injection by
construction, with no manual placeholder counting.

```julia
status  = "shipped"
min_amt = 100.0

df = @query ctx \"\"\"
    SELECT c.name, o.amount
    FROM   orders o
    JOIN   customers c ON c.id = o.customer_id
    WHERE  o.status = \$status
      AND  o.amount > \$min_amt
    ORDER  BY o.amount DESC
\"\"\"
```

Equivalent (and compiles down) to:

```julia
execute(ctx, "SELECT ... WHERE o.status = ? AND o.amount > ? ...", status, min_amt)
```

Full expressions work too:

```julia
df = @query ctx "SELECT * FROM t WHERE n BETWEEN \$(lo-1) AND \$(hi+1)"
```
"""
macro query(ctx, sql_expr)
    sql, params = _interpolate_sql(sql_expr)
    ctx_esc    = esc(ctx)
    params_esc = map(esc, params)
    if isempty(params)
        :(execute($ctx_esc, $sql))
    else
        :(execute($ctx_esc, $sql, $(params_esc...)))
    end
end

# ─── @query! ──────────────────────────────────────────────────────────────────

"""
    @query! ctx sql_string → nothing

Like `@query` but discards the result.  Use for DML statements where the
return value is not needed.

```julia
msg  = "job_start"
code = 42
@query! ctx "INSERT INTO log VALUES (\$msg, \$code)"
```

> **Note:** `\$interpolations` become bound `?` parameters, so they work for
> *values* (strings, numbers, dates) but not for *identifiers* such as table
> or column names.  Use normal Julia string interpolation for identifiers and
> reserve `@query!` for the value positions.
"""
macro query!(ctx, sql_expr)
    sql, params = _interpolate_sql(sql_expr)
    ctx_esc    = esc(ctx)
    params_esc = map(esc, params)
    if isempty(params)
        :(execute!($ctx_esc, $sql))
    else
        :(execute!($ctx_esc, $sql, $(params_esc...)))
    end
end

# ─── @stream ──────────────────────────────────────────────────────────────────

"""
    @stream ctx sql_string [batch_size=N] → Channel{DataFrame}

Like `@query` but returns a streaming `Channel{DataFrame}` of successive
batches.  Accepts an optional `batch_size` keyword (default `10_000`).

```julia
user_id    = 42
batch_size = 5_000

for batch in @stream ctx \"SELECT * FROM events WHERE user_id = \$user_id\" batch_size=batch_size
    process(batch)
end
```

Expressions and multiple parameters work identically to `@query`:

```julia
lo, hi = 100, 200
total = sum(nrow(b) for b in @stream ctx \"SELECT * FROM t WHERE n BETWEEN \$lo AND \$hi\")
```
"""
macro stream(ctx, sql_expr, rest...)
    sql, params = _interpolate_sql(sql_expr)
    ctx_esc    = esc(ctx)
    params_esc = map(esc, params)

    # Extract optional batch_size=N from the trailing macro arguments.
    # In Julia macro syntax, `@stream ctx "SQL" batch_size=1000` passes the
    # assignment expression Expr(:(=), :batch_size, 1000) as a positional arg.
    batch_kw = nothing
    for r in rest
        if r isa Expr && r.head === :(=) && r.args[1] === :batch_size
            batch_kw = esc(r.args[2])
        end
    end

    if isempty(params)
        batch_kw === nothing ?
            :(stream($ctx_esc, $sql)) :
            :(stream($ctx_esc, $sql; batch_size=$batch_kw))
    else
        batch_kw === nothing ?
            :(stream($ctx_esc, $sql, $(params_esc...))) :
            :(stream($ctx_esc, $sql, $(params_esc...); batch_size=$batch_kw))
    end
end
