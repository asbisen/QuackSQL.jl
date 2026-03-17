# ─────────────────────────────────────────────────────────────────────────────
# QueryDF — Quick-start examples
# Run from the QueryDF.jl/ directory: julia --project examples/quickstart.jl
# ─────────────────────────────────────────────────────────────────────────────

using QueryDF
using DataFrames
using Logging
using Dates

# Optional: see debug messages from QueryDF
# Logging.global_logger(ConsoleLogger(stderr, Logging.Debug))

println("=" ^ 60)
println("  QueryDF Quick-start")
println("=" ^ 60)

# ── 1. Simplest possible usage ────────────────────────────────────────────────
println("\n1. Basic in-memory query")

with_context() do ctx
    df = execute(ctx, "SELECT 1 AS one, 'hello' AS greeting")
    println(df)
end

# ── 2. Query a DataFrame ──────────────────────────────────────────────────────
println("\n2. Querying a DataFrame")

customers = DataFrame(
    id       = 1:5,
    name     = ["Alice","Bob","Charlie","Diana","Eve"],
    country  = ["US","UK","US","DE","UK"],
    age      = [34, 28, 45, 31, 25]
)

orders = DataFrame(
    order_id    = 101:108,
    customer_id = [1, 2, 1, 3, 4, 2, 5, 3],
    amount      = [120.0, 80.0, 230.0, 55.0, 410.0, 190.0, 35.0, 620.0],
    status      = ["shipped","shipped","pending","shipped","shipped","returned","pending","shipped"]
)

with_context() do ctx
    register!(ctx, "customers" => customers, "orders" => orders)

    df = execute(ctx, """
        SELECT c.name, c.country, SUM(o.amount) AS total_spent
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        WHERE o.status = 'shipped'
        GROUP BY c.name, c.country
        ORDER BY total_spent DESC
    """)
    println(df)
end

# ── 3. Parameterized queries ──────────────────────────────────────────────────
println("\n3. Parameterized queries")

with_context() do ctx
    register!(ctx, "orders", orders)

    # Positional
    df_pos = execute(ctx, "SELECT * FROM orders WHERE amount > ? AND status = ?", 100.0, "shipped")
    println("Positional params (amount > 100, shipped): $(nrow(df_pos)) rows")

    # Named keyword params
    df_named = execute(ctx, "SELECT * FROM orders WHERE status = :st AND amount >= :min",
                       st="shipped", min=200.0)
    println("Named params (shipped, amount ≥ 200): $(nrow(df_named)) rows")
end

# ── 4. QueryResult with metadata ──────────────────────────────────────────────
println("\n4. QueryResult with execution metadata")

with_context() do ctx
    register!(ctx, "orders", orders)
    r = query(ctx, "SELECT COUNT(*) AS n, AVG(amount) AS avg_amount FROM orders")
    println("Result: $(r)")
    println("  Rows         : $(nrow(r))")
    println("  Elapsed (ms) : $(elapsed_ms(r))")
    println("  Data         : $(DataFrame(r))")
end

# ── 5. Batch SQL execution ────────────────────────────────────────────────────
println("\n5. Batch SQL execution")

with_context() do ctx
    df = execute(ctx, [
        "CREATE TABLE nums AS SELECT generate_series AS n FROM generate_series(1, 100)",
        "CREATE TABLE squares AS SELECT n, n * n AS n2 FROM nums",
        "SELECT n, n2 FROM squares WHERE n2 > 9000 ORDER BY n"
    ])
    println("n² > 9000: $(nrow(df)) rows")
    println(first(df, 3))
end

# ── 6. Transactions ───────────────────────────────────────────────────────────
println("\n6. Transactions")

with_context() do ctx
    execute!(ctx, "CREATE TABLE accounts (id INT, balance DOUBLE)")
    execute!(ctx, "INSERT INTO accounts VALUES (1, 1000.0), (2, 500.0)")

    # Transfer 250 from account 1 to account 2
    transaction(ctx) do tx
        execute!(tx, "UPDATE accounts SET balance = balance - 250 WHERE id = 1")
        execute!(tx, "UPDATE accounts SET balance = balance + 250 WHERE id = 2")
    end

    println(execute(ctx, "SELECT * FROM accounts ORDER BY id"))
end

# ── 7. Streaming large results ────────────────────────────────────────────────
println("\n7. Streaming (batch_size=25 for demo)")

with_context() do ctx
    execute!(ctx, "CREATE TABLE big AS SELECT generate_series AS n FROM generate_series(1, 100)")
    total_rows = 0
    batch_count = 0
    for batch in stream(ctx, "SELECT * FROM big"; batch_size=25)
        total_rows  += nrow(batch)
        batch_count += 1
    end
    println("Streamed $total_rows rows in $batch_count batches")
end

# ── 8. File-based database ────────────────────────────────────────────────────
println("\n8. Persistent file database")

db_file = tempname() * ".duckdb"
try
    with_context(db_file) do ctx
        register!(ctx, "customers", customers)
        execute!(ctx, "CREATE TABLE customer_snapshot AS SELECT * FROM customers")
        println("Snapshot saved → $(db_file)")
    end

    # Re-open and query
    with_context(db_file) do ctx
        df = execute(ctx, "SELECT name, country FROM customer_snapshot ORDER BY name")
        println(df)
    end
finally
    isfile(db_file) && rm(db_file)
end

# ── 9. Query a Parquet / CSV file ─────────────────────────────────────────────
println("\n9. Query external files via view registration")
println("   (skipped in this demo — replace path with a real file)")

# with_context() do ctx
#     register!(ctx, "trips",  "yellow_tripdata_2025-01.parquet")
#     register!(ctx, "events", "events/*.csv")
#     df = execute(ctx, "SELECT * FROM trips LIMIT 5")
#     println(df)
# end

# ── 10. explain ───────────────────────────────────────────────────────────────
println("\n10. Query plan (EXPLAIN)")

with_context() do ctx
    register!(ctx, "orders", orders)
    plan = explain(ctx, "SELECT * FROM orders WHERE amount > 100")
    println(plan)
end

# ── 11. init_sql — run setup SQL on connection open ───────────────────────────
println("\n11. init_sql on connection open")

with_context(init_sql=["SET threads=2", "SET memory_limit='512MB'"]) do ctx
    println("Context with init_sql created successfully")
    df = execute(ctx, "SELECT current_setting('memory_limit') AS mem")
    println(df)
end

# ── 12. Connection pool ───────────────────────────────────────────────────────
println("\n12. ConnectionPool")

pool = ConnectionPool(":memory:"; size=4)
results = Vector{DataFrame}(undef, 4)
Threads.@threads for i in 1:4
    results[i] = with_connection(pool) do conn
        execute(conn, pool.config, "SELECT $(i*i) AS square")
    end
end
close!(pool)
for (i, r) in enumerate(results)
    println("  Thread $i → square = $(r[1,:square])")
end

println("\n✓ All examples completed.")
